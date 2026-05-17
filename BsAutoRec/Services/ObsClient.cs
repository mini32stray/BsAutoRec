using BsAutoRec.Infrastructure;
using BsAutoRec.Models;
using Microsoft.Extensions.Logging;
using Reactive.Bindings;
using System.Reactive.Disposables;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Owns the OBS WebSocket lifecycle and exposes OBS state to the rest of the app.
	/// </summary>
	public sealed class ObsClient : IAsyncDisposable
	{
		/// <summary>
		/// Describes the current OBS recording output state.
		/// </summary>
		public sealed record ObsRecordState(
			bool IsRecording,
			string? OutputPath,
			string Summary);

		private readonly ILogger<ObsClient> logger;

		private readonly AsyncLock lifecycleLock = new();
		private readonly CompositeDisposable disposables = new();

		private readonly ObsProtocol protocol;
		private readonly ObsRequestDispatcher requestDispatcher;
		private readonly ObsConnectionLoop connectionLoop;

		public ReactivePropertySlim<ObsConnectionInfo> ConnectionInfo { get; }
		public ReactivePropertySlim<ObsRecordState> RecordState { get; }

		private CancellationTokenSource? runCts;
		private Task? runTask;

		/// <summary>
		/// Creates the OBS client and its connection helpers.
		/// </summary>
		public ObsClient(
				SettingsService settingsService,
				ILogger<ObsClient> logger,
				ILogger<ObsRequestDispatcher> requestDispatcherLogger,
				ILogger<ObsConnectionLoop> connectionLoopLogger)
		{
			this.logger = logger;

			ConnectionInfo = new ReactivePropertySlim<ObsConnectionInfo>(ObsConnectionInfo.Empty)
				.AddTo(disposables);
			RecordState = new ReactivePropertySlim<ObsRecordState>(
					new(false, null, "待機中")
				).AddTo(disposables);

			protocol = new ObsProtocol();
			requestDispatcher = new ObsRequestDispatcher(requestDispatcherLogger);
			connectionLoop = new ObsConnectionLoop(
				settingsService,
				protocol,
				requestDispatcher,
				connectionLoopLogger,
				connectionInfo => ConnectionInfo.Value = connectionInfo,
				recordState => RecordState.Value = recordState,
				PublishCurrentRecordStateAsync);
		}

		/// <summary>
		/// Starts the OBS connection loop.
		/// </summary>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			using var lease = await lifecycleLock.LockAsync(cancellationToken);
			if (runTask is not null)
			{
				return;
			}

			var requestReader = requestDispatcher.Start();
			runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
			runTask = Task.Run(() => connectionLoop.RunAsync(requestReader, runCts.Token), runCts.Token);
			ConnectionInfo.Value = new ObsConnectionInfo(ObsConnectionState.Connecting, false, null);
			logger.LogInformation("OBS client started.");
		}

		/// <summary>
		/// Stops the OBS connection loop and clears connection state.
		/// </summary>
		public async Task StopAsync()
		{
			Task? activeRunTask;
			CancellationTokenSource? activeRunCts;

			using (var lease = await lifecycleLock.LockAsync())
			{
				if (runTask is null)
				{
					return;
				}

				activeRunTask = runTask;
				activeRunCts = runCts;
				runTask = null;
				runCts = null;
			}

			requestDispatcher.Stop();
			activeRunCts?.Cancel();
			await AwaitCanceledAsync(activeRunTask);
			activeRunCts?.Dispose();
			ConnectionInfo.Value = new ObsConnectionInfo(ObsConnectionState.Disconnected, false, null);
			logger.LogInformation("OBS client stopped.");
		}

		/// <summary>
		/// Requests the OBS connection loop and pending requests to stop.
		/// </summary>
		public void RequestShutdown()
		{
			logger.LogDebug("OBS client shutdown requested.");
			requestDispatcher.RequestShutdown();
			TryCancel(runCts);
		}

		/// <summary>
		/// Stops the client and releases owned resources.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			RequestShutdown();
			await StopAsync();
			lifecycleLock.Dispose();
			disposables.Dispose();

			logger.LogInformation("Disposed");
		}

		/// <summary>
		/// Starts OBS recording if it is not already active.
		/// </summary>
		public async Task StartRecordingAsync(CancellationToken cancellationToken)
		{
			var status = await GetRecordStatusAsync(cancellationToken);
			if (status.IsRecording)
			{
				logger.LogInformation("OBS recording start skipped because recording is already active. OutputPath: {OutputPath}", status.OutputPath);
				return;
			}

			logger.LogInformation("Sending OBS StartRecord request.");
			await requestDispatcher.SendRequestAsync("StartRecord", null, cancellationToken);
			logger.LogInformation("OBS recording started.");
		}

		/// <summary>
		/// Stops OBS recording and returns the output path when OBS provides it.
		/// </summary>
		public async Task<string?> StopRecordingAsync(CancellationToken cancellationToken)
		{
			var status = await GetRecordStatusAsync(cancellationToken);
			if (!status.IsRecording)
			{
				logger.LogInformation("OBS recording stop skipped because recording is not active. OutputPath: {OutputPath}", status.OutputPath);
				return status.OutputPath;
			}

			logger.LogInformation("Sending OBS StopRecord request.");
			var response = await requestDispatcher.SendRequestAsync("StopRecord", null, cancellationToken);
			var outputPath = protocol.TryGetOutputPath(response);

			RecordState.Value = new ObsRecordState(false, outputPath, "録画停止");
			logger.LogInformation("OBS recording stopped. Output path: {outputPath}", outputPath);
			return outputPath;
		}

		/// <summary>
		/// Queries OBS for the current recording status.
		/// </summary>
		public async Task<ObsRecordState> GetRecordStatusAsync(CancellationToken cancellationToken)
		{
			var response = await requestDispatcher.SendRequestAsync("GetRecordStatus", null, cancellationToken);
			var status = protocol.ParseRecordStatus(response);
			RecordState.Value = status;
			return status;
		}

		/// <summary>
		/// Refreshes recording state right after OBS connects.
		/// </summary>
		private async Task PublishCurrentRecordStateAsync(CancellationToken cancellationToken)
		{
			try
			{
				await GetRecordStatusAsync(cancellationToken);
			}
			catch (Exception exception)
			{
				logger.LogDebug(exception, "Failed to query OBS record status immediately after connect.");
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
		/// Cancels a source if it is still usable.
		/// </summary>
		private static void TryCancel(CancellationTokenSource? cancellationTokenSource)
		{
			if (cancellationTokenSource is null)
			{
				return;
			}

			try
			{
				cancellationTokenSource.Cancel();
			}
			catch (ObjectDisposedException)
			{
			}
		}
	}
}
