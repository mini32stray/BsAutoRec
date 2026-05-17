using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Queues OBS requests and matches responses back to waiting callers.
	/// </summary>
	public sealed class ObsRequestDispatcher
	{
		private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(5);

		private const int MaxConsecutiveRequestTimeouts = 3;

		private readonly ILogger<ObsRequestDispatcher> logger;
		private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonElement?>> pendingRequests;

		private Channel<ObsRequest>? requestChannel;
		private TaskCompletionSource<Exception>? connectionFaultTcs;
		private int consecutiveRequestTimeouts;

		/// <summary>
		/// Creates the dispatcher.
		/// </summary>
		public ObsRequestDispatcher(ILogger<ObsRequestDispatcher> logger)
		{
			this.logger = logger;
			pendingRequests = new ConcurrentDictionary<string, TaskCompletionSource<JsonElement?>>();
		}

		/// <summary>
		/// Opens a fresh request queue for a new OBS connection loop.
		/// </summary>
		public ChannelReader<ObsRequest> Start()
		{
			logger.LogDebug("OBS request dispatcher started.");
			requestChannel = Channel.CreateUnbounded<ObsRequest>(
				new UnboundedChannelOptions
				{
					SingleReader = true,
					SingleWriter = false,
				});

			return requestChannel.Reader;
		}

		/// <summary>
		/// Stops accepting requests and fails any pending requests.
		/// </summary>
		public void Stop()
		{
			logger.LogDebug("OBS request dispatcher stopped.");
			requestChannel?.Writer.TryComplete(new OperationCanceledException("OBS request dispatcher stopped."));
			requestChannel = null;
			FailPendingRequests(new OperationCanceledException("OBS request dispatcher stopped."));
		}

		/// <summary>
		/// Cancels pending work immediately during app shutdown.
		/// </summary>
		public void RequestShutdown()
		{
			logger.LogDebug("OBS request dispatcher shutdown requested.");
			requestChannel?.Writer.TryComplete(new OperationCanceledException("OBS request dispatcher stopped."));
			FailPendingRequests(new OperationCanceledException("OBS request dispatcher stopped."));
			Volatile.Read(ref connectionFaultTcs)?.TrySetResult(new OperationCanceledException("OBS request dispatcher stopped."));
		}

		/// <summary>
		/// Sends a request and waits for its matching OBS response.
		/// </summary>
		public async Task<JsonElement?> SendRequestAsync(string requestType, object? requestData, CancellationToken cancellationToken)
		{
			var requestWriter = requestChannel?.Writer;
			if (requestWriter is null)
			{
				throw new InvalidOperationException("OBS is not running.");
			}

			var requestId = Guid.NewGuid().ToString("N");
			var requestTcs = new TaskCompletionSource<JsonElement?>(TaskCreationOptions.RunContinuationsAsynchronously);
			pendingRequests[requestId] = requestTcs;

			var request = new ObsRequest(requestType, requestId, requestData, requestTcs, cancellationToken);
			logger.LogDebug("OBS request queued. RequestType: {RequestType}; RequestId: {RequestId}", requestType, requestId);

			try
			{
				await requestWriter.WriteAsync(request, cancellationToken);
				return await requestTcs.Task.WaitAsync(RequestTimeout, cancellationToken);
			}
			catch (OperationCanceledException)
			{
				pendingRequests.TryRemove(requestId, out _);
				requestTcs.TrySetCanceled(cancellationToken);
				throw;
			}
			catch (TimeoutException exception)
			{
				pendingRequests.TryRemove(requestId, out _);
				requestTcs.TrySetException(exception);
				NotifyRequestTimedOut(requestType);
				logger.LogDebug(exception, "OBS request timed out. RequestType: {RequestType}; RequestId: {RequestId}", requestType, requestId);
				throw;
			}
			catch
			{
				pendingRequests.TryRemove(requestId, out _);
				throw;
			}
		}

		/// <summary>
		/// Completes the pending request that matches an OBS response.
		/// </summary>
		public void CompleteResponse(ObsRequestResponse response)
		{
			if (!pendingRequests.TryRemove(response.RequestId, out var tcs))
			{
				return;
			}

			NotifyRequestSucceeded();
			logger.LogDebug("OBS response received. RequestId: {RequestId}; IsSuccess: {IsSuccess}", response.RequestId, response.IsSuccess);
			if (!response.IsSuccess)
			{
				tcs.TrySetException(new InvalidOperationException(response.ErrorMessage ?? "OBS request failed."));
				return;
			}

			tcs.TrySetResult(response.ResponseData);
		}

		/// <summary>
		/// Fails one queued request.
		/// </summary>
		public void FailRequest(ObsRequest request, Exception exception)
		{
			logger.LogDebug(exception, "OBS queued request failed. RequestType: {RequestType}; RequestId: {RequestId}", request.RequestType, request.RequestId);
			pendingRequests.TryRemove(request.RequestId, out _);
			request.Completion.TrySetException(exception);
		}

		/// <summary>
		/// Cancels one queued request.
		/// </summary>
		public void CancelRequest(ObsRequest request)
		{
			logger.LogDebug("OBS queued request canceled. RequestType: {RequestType}; RequestId: {RequestId}", request.RequestType, request.RequestId);
			pendingRequests.TryRemove(request.RequestId, out _);
			request.Completion.TrySetCanceled(request.CancellationToken);
		}

		/// <summary>
		/// Fails all requests that are still waiting for OBS.
		/// </summary>
		public void FailPendingRequests(Exception exception)
		{
			foreach (var pair in pendingRequests.ToArray())
			{
				if (pendingRequests.TryRemove(pair.Key, out var tcs))
				{
					tcs.TrySetException(exception);
				}
			}
		}

		/// <summary>
		/// Creates the fault signal used to restart a broken OBS connection.
		/// </summary>
		public TaskCompletionSource<Exception> CreateConnectionFaultSource()
		{
			Interlocked.Exchange(ref consecutiveRequestTimeouts, 0);
			var source = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
			Volatile.Write(ref connectionFaultTcs, source);
			return source;
		}

		/// <summary>
		/// Clears the active fault signal after a connection ends.
		/// </summary>
		public void ClearConnectionFaultSource(TaskCompletionSource<Exception> source)
		{
			Interlocked.CompareExchange(ref connectionFaultTcs, null, source);
			Interlocked.Exchange(ref consecutiveRequestTimeouts, 0);
		}

		/// <summary>
		/// Resets timeout tracking after a successful request.
		/// </summary>
		private void NotifyRequestSucceeded()
		{
			Interlocked.Exchange(ref consecutiveRequestTimeouts, 0);
		}

		/// <summary>
		/// Tracks request timeouts and faults the connection after repeated failures.
		/// </summary>
		private void NotifyRequestTimedOut(string requestType)
		{
			var timeoutCount = Interlocked.Increment(ref consecutiveRequestTimeouts);
			logger.LogWarning("OBS request {RequestType} timed out. Consecutive timeout count: {TimeoutCount}", requestType, timeoutCount);

			if (timeoutCount < MaxConsecutiveRequestTimeouts)
			{
				return;
			}

			var exception = new TimeoutException($"OBS が {MaxConsecutiveRequestTimeouts} 回連続で応答しませんでした。再接続します。");
			logger.LogWarning(exception, "OBS request timeout threshold was reached. Connection will be restarted.");
			Volatile.Read(ref connectionFaultTcs)?.TrySetResult(exception);
		}
	}

	/// <summary>
	/// Represents one OBS request waiting to be sent and completed.
	/// </summary>
	public sealed record ObsRequest(
		string RequestType,
		string RequestId,
		object? RequestData,
		TaskCompletionSource<JsonElement?> Completion,
		CancellationToken CancellationToken);
}
