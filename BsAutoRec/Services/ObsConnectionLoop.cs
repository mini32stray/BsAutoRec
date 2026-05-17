using BsAutoRec.Models;
using Microsoft.Extensions.Logging;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Channels;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Maintains the OBS WebSocket connection and routes OBS messages.
	/// </summary>
	public sealed class ObsConnectionLoop
	{
		private readonly SettingsService settingsService;
		private readonly ObsProtocol protocol;
		private readonly ObsRequestDispatcher requestDispatcher;
		private readonly ILogger<ObsConnectionLoop> logger;
		private readonly Action<ObsConnectionInfo> publishConnectionInfo;
		private readonly Action<ObsClient.ObsRecordState> publishRecordState;
		private readonly Func<CancellationToken, Task> publishCurrentRecordStateAsync;

		/// <summary>
		/// Creates the loop with protocol, dispatcher, and state publishers.
		/// </summary>
		public ObsConnectionLoop(
				SettingsService settingsService,
				ObsProtocol protocol,
				ObsRequestDispatcher requestDispatcher,
				ILogger<ObsConnectionLoop> logger,
				Action<ObsConnectionInfo> publishConnectionInfo,
				Action<ObsClient.ObsRecordState> publishRecordState,
				Func<CancellationToken, Task> publishCurrentRecordStateAsync)
		{
			this.settingsService = settingsService;
			this.protocol = protocol;
			this.requestDispatcher = requestDispatcher;
			this.logger = logger;
			this.publishConnectionInfo = publishConnectionInfo;
			this.publishRecordState = publishRecordState;
			this.publishCurrentRecordStateAsync = publishCurrentRecordStateAsync;
		}

		/// <summary>
		/// Runs the connect, receive, send, and reconnect loop.
		/// </summary>
		public async Task RunAsync(ChannelReader<ObsRequest> requestReader, CancellationToken cancellationToken)
		{
			var connectionFailureLogged = false;

			while (!cancellationToken.IsCancellationRequested)
			{
				var connected = false;
				string? lastError = null;

				using var socket = new ClientWebSocket();
				using var abortRegistration = cancellationToken.Register(static state => TryAbortSocket((ClientWebSocket)state!), socket);

				try
				{
					socket.Options.AddSubProtocol("obswebsocket.json");
					var uri = new Uri(settingsService.Current.ObsWebSocketUrl);
					logger.LogDebug("Connecting to OBS WebSocket. Uri: {Uri}", uri);
					await socket.ConnectAsync(uri, cancellationToken);

					await IdentifyAsync(socket, cancellationToken);

					connected = true;
					connectionFailureLogged = false;
					publishConnectionInfo(new ObsConnectionInfo(ObsConnectionState.Connected, true, null));
					logger.LogInformation("OBS WebSocket connected.");

					var connectionFaultSource = requestDispatcher.CreateConnectionFaultSource();
					using var connectionCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
					using var connectionAbortRegistration = connectionCts.Token.Register(
						static state => TryAbortSocket((ClientWebSocket)state!),
						socket);

					var receiveTask = ReceiveLoopAsync(socket, connectionCts.Token);
					var sendTask = SendLoopAsync(socket, requestReader, connectionCts.Token);
					var faultTask = connectionFaultSource.Task;
					_ = Task.Run(
						() => publishCurrentRecordStateAsync(cancellationToken),
						cancellationToken);

					var completedTask = await Task.WhenAny(receiveTask, sendTask, faultTask);
					Exception? connectionFault = null;
					if (completedTask == faultTask)
					{
						connectionFault = await faultTask;
						logger.LogInformation("OBS connection fault was signaled. Reconnect will be attempted. Error: {Error}", connectionFault.Message);
					}

					connectionCts.Cancel();
					await AwaitCanceledAsync(receiveTask);
					await AwaitCanceledAsync(sendTask);
					if (connectionFault is not null)
					{
						throw connectionFault;
					}

					await completedTask;
					if (!cancellationToken.IsCancellationRequested)
					{
						logger.LogInformation("OBS WebSocket worker ended. Reconnect will be attempted. WebSocketState: {WebSocketState}", socket.State);
					}
					requestDispatcher.ClearConnectionFaultSource(connectionFaultSource);
				}
				catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
				{
					break;
				}
				catch (Exception exception)
				{
					lastError = exception.Message;
					if (connected)
					{
						logger.LogInformation("OBS WebSocket disconnected. Reconnect will be attempted. Error: {Error}", exception.Message);
						logger.LogDebug(exception, "OBS WebSocket disconnected with exception details.");
					}
					else if (!connectionFailureLogged)
					{
						connectionFailureLogged = true;
						logger.LogInformation("OBS WebSocket connection failed. Retry will continue. Error: {Error}", exception.Message);
						logger.LogDebug(exception, "OBS WebSocket connection failed with exception details.");
					}
					else
					{
						logger.LogDebug(exception, "OBS WebSocket connection retry failed.");
					}
				}
				finally
				{
					requestDispatcher.FailPendingRequests(new IOException("OBS connection was lost."));
					TryAbortSocket(socket);
				}

				if (!cancellationToken.IsCancellationRequested)
				{
					publishConnectionInfo(new ObsConnectionInfo(ObsConnectionState.Reconnecting, false, lastError));
					logger.LogDebug("Waiting before retrying OBS WebSocket connection.");
					await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
				}
			}

			logger.LogInformation("OBS WebSocket connection loop stopped.");
		}

		/// <summary>
		/// Completes the OBS WebSocket hello and identify handshake.
		/// </summary>
		private async Task IdentifyAsync(ClientWebSocket socket, CancellationToken cancellationToken)
		{
			var helloPayload = await ReceivePayloadAsync(socket, cancellationToken);
			if (string.IsNullOrWhiteSpace(helloPayload))
			{
				throw new InvalidOperationException("OBS handshake failed. Hello payload was empty.");
			}

			var helloInfo = protocol.ParseHello(helloPayload);
			logger.LogDebug("OBS hello received. AuthenticationRequired: {AuthenticationRequired}", !string.IsNullOrEmpty(helloInfo.Salt) || !string.IsNullOrEmpty(helloInfo.Challenge));
			await SendPayloadAsync(socket, protocol.BuildIdentifyPayload(settingsService.Current.ObsPassword, helloInfo.Salt, helloInfo.Challenge), cancellationToken);

			while (true)
			{
				var payload = await ReceivePayloadAsync(socket, cancellationToken);
				if (string.IsNullOrWhiteSpace(payload))
				{
					throw new InvalidOperationException("OBS handshake failed. Identified payload was empty.");
				}

				if (protocol.IsIdentifiedPayload(payload))
				{
					logger.LogDebug("OBS identify completed.");
					return;
				}
			}
		}

		/// <summary>
		/// Receives OBS payloads and dispatches events or request responses.
		/// </summary>
		private async Task ReceiveLoopAsync(ClientWebSocket socket, CancellationToken cancellationToken)
		{
			while (socket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
			{
				var payload = await ReceivePayloadAsync(socket, cancellationToken);
				if (payload is null)
				{
					return;
				}

				var obsPayload = protocol.ParsePayload(payload);
				switch (obsPayload.OpCode)
				{
					case 5:
						// OpCode 5: event
						HandleEvent(obsPayload.Data);
						break;
					case 7:
						// OpCode 7: request response
						requestDispatcher.CompleteResponse(protocol.ParseRequestResponse(obsPayload.Data));
						break;
				}
			}
		}

		/// <summary>
		/// Reads one complete text payload from the WebSocket.
		/// </summary>
		private static async Task<string?> ReceivePayloadAsync(ClientWebSocket socket, CancellationToken cancellationToken)
		{
			var buffer = new byte[16 * 1024];
			using var stream = new MemoryStream();

			while (true)
			{
				var result = await socket.ReceiveAsync(buffer, cancellationToken);
				if (result.MessageType == WebSocketMessageType.Close)
				{
					return null;
				}

				stream.Write(buffer, 0, result.Count);
				if (result.EndOfMessage)
				{
					return Encoding.UTF8.GetString(stream.ToArray());
				}
			}
		}

		/// <summary>
		/// Sends queued OBS requests over the active WebSocket.
		/// </summary>
		private async Task SendLoopAsync(ClientWebSocket socket, ChannelReader<ObsRequest> requestReader, CancellationToken cancellationToken)
		{
			await foreach (var request in requestReader.ReadAllAsync(cancellationToken))
			{
				if (request.Completion.Task.IsCompleted)
				{
					continue;
				}

				if (request.CancellationToken.IsCancellationRequested)
				{
					requestDispatcher.CancelRequest(request);
					continue;
				}

				try
				{
					await SendPayloadAsync(
						socket,
						protocol.BuildRequestPayload(request),
						cancellationToken);
				}
				catch (Exception exception)
				{
					requestDispatcher.FailRequest(request, exception);
					throw;
				}
			}
		}

		/// <summary>
		/// Sends one JSON payload to OBS.
		/// </summary>
		private static async Task SendPayloadAsync(ClientWebSocket socket, byte[] bytes, CancellationToken cancellationToken)
		{
			await socket.SendAsync(
				bytes,
				WebSocketMessageType.Text,
				true,
				cancellationToken);
		}

		/// <summary>
		/// Handles OBS events that affect application state.
		/// </summary>
		private void HandleEvent(System.Text.Json.JsonElement data)
		{
			var recordState = protocol.TryParseRecordStateChanged(data);
			if (recordState is not null)
			{
				logger.LogInformation("OBS record state changed. IsRecording: {IsRecording}; OutputPath: {OutputPath}", recordState.IsRecording, recordState.OutputPath);
				publishRecordState(recordState);
			}
		}

		/// <summary>
		/// Waits for a task and ignores expected cancellation.
		/// </summary>
		private static async Task AwaitCanceledAsync(Task? task)
		{
			if (task is null)
			{
				return;
			}

			try
			{
				await task;
			}
			catch (OperationCanceledException)
			{
			}
		}

		/// <summary>
		/// Aborts a socket during shutdown or reconnect.
		/// </summary>
		private static void TryAbortSocket(ClientWebSocket webSocket)
		{
			try
			{
				webSocket.Abort();
			}
			catch
			{
			}
		}
	}
}
