using BsAutoRec.Models;
using Microsoft.Extensions.Logging;
using System.Net.WebSockets;
using System.Text;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Maintains the BS WebSocket connection and publishes parsed messages.
	/// </summary>
	public sealed class BsConnectionLoop
	{
		private readonly SettingsService settingsService;
		private readonly BsProtocolParser protocolParser;
		private readonly ILogger<BsConnectionLoop> logger;
		private readonly Func<BsStatusSnapshot> getLatestStatus;
		private readonly Action<BsConnectionInfo> publishConnectionInfo;
		private readonly Action<BsEventMessage> publishMessage;

		/// <summary>
		/// Creates the loop with parser and state publishers.
		/// </summary>
		public BsConnectionLoop(
				SettingsService settingsService,
				BsProtocolParser protocolParser,
				ILogger<BsConnectionLoop> logger,
				Func<BsStatusSnapshot> getLatestStatus,
				Action<BsConnectionInfo> publishConnectionInfo,
				Action<BsEventMessage> publishMessage)
		{
			this.settingsService = settingsService;
			this.protocolParser = protocolParser;
			this.logger = logger;
			this.getLatestStatus = getLatestStatus;
			this.publishConnectionInfo = publishConnectionInfo;
			this.publishMessage = publishMessage;
		}

		/// <summary>
		/// Runs the BS connect, receive, and reconnect loop.
		/// </summary>
		public async Task RunAsync(CancellationToken cancellationToken)
		{
			var connectionFailureLogged = false;

			while (!cancellationToken.IsCancellationRequested)
			{
				var connected = false;
				string? lastError = null;

				using var webSocket = new ClientWebSocket();
				using var abortRegistration = cancellationToken.Register(static state => TryAbortSocket((ClientWebSocket)state!), webSocket);

				try
				{
					var uri = new Uri(settingsService.Current.BsWebSocketUrl);
					logger.LogDebug("Connecting to BS WebSocket. Uri: {Uri}", uri);
					await webSocket.ConnectAsync(uri, cancellationToken);

					connected = true;
					connectionFailureLogged = false;
					logger.LogInformation("BS WebSocket connected.");
					publishConnectionInfo(new BsConnectionInfo(BsConnectionState.Connected, true, null));

					while (webSocket.State == WebSocketState.Open && !cancellationToken.IsCancellationRequested)
					{
						var text = await ReceiveTextMessageAsync(webSocket, cancellationToken);
						if (string.IsNullOrWhiteSpace(text))
						{
							continue;
						}

						var message = protocolParser.ParseMessage(text, getLatestStatus());
						logger.LogTrace("BS WebSocket message received. EventName: {EventName}; Kind: {Kind}", message.EventName, message.Kind);
						publishMessage(message);
					}

					if (!cancellationToken.IsCancellationRequested)
					{
						logger.LogInformation("BS WebSocket receive loop ended. Reconnect will be attempted. WebSocketState: {WebSocketState}", webSocket.State);
					}
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
						logger.LogInformation("BS WebSocket disconnected. Reconnect will be attempted. Error: {Error}", exception.Message);
						logger.LogDebug(exception, "BS WebSocket disconnected with exception details.");
					}
					else if (!connectionFailureLogged)
					{
						connectionFailureLogged = true;
						logger.LogInformation("BS WebSocket connection failed. Retry will continue. Error: {Error}", exception.Message);
						logger.LogDebug(exception, "BS WebSocket connection failed with exception details.");
					}
					else
					{
						logger.LogDebug(exception, "BS WebSocket connection retry failed.");
					}
				}
				finally
				{
					if (connected)
					{
						publishConnectionInfo(new BsConnectionInfo(
							cancellationToken.IsCancellationRequested ? BsConnectionState.Disconnected : BsConnectionState.Reconnecting,
							false,
							lastError));
					}

					TryAbortSocket(webSocket);
				}

				if (!cancellationToken.IsCancellationRequested)
				{
					logger.LogDebug("Waiting before retrying BS WebSocket connection.");
					await Task.Delay(TimeSpan.FromSeconds(2), cancellationToken);
				}
			}

			logger.LogInformation("BS WebSocket connection loop stopped.");
		}

		/// <summary>
		/// Reads one complete text message from the WebSocket.
		/// </summary>
		private static async Task<string?> ReceiveTextMessageAsync(ClientWebSocket webSocket, CancellationToken cancellationToken)
		{
			var buffer = new byte[16 * 1024];
			using var stream = new MemoryStream();

			while (true)
			{
				var result = await webSocket.ReceiveAsync(buffer, cancellationToken);
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
