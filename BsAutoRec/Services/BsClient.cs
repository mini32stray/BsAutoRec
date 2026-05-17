using BsAutoRec.Infrastructure;
using BsAutoRec.Models;
using Microsoft.Extensions.Logging;
using Reactive.Bindings;
using System.Reactive.Disposables;
using System.Reactive.Subjects;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Owns the BS WebSocket lifecycle and publishes BS status updates.
	/// </summary>
	public sealed class BsClient : IAsyncDisposable
	{
		private readonly ILogger<BsClient> logger;

		private readonly AsyncLock lifecycleLock = new();
		private readonly CompositeDisposable disposables = new();
		private readonly Subject<BsEventMessage?> latestEventSource;
		private readonly BsConnectionLoop connectionLoop;

		public ReactivePropertySlim<BsConnectionInfo> ConnectionInfo { get; }
		public ReactivePropertySlim<BsStatusSnapshot> LatestStatus { get; }
		public IObservable<BsEventMessage?> LatestEvent => latestEventSource;

		private CancellationTokenSource? runCts;
		private Task? runTask;

		/// <summary>
		/// Creates the BS client and its connection loop.
		/// </summary>
		public BsClient(
				SettingsService settingsService,
				BsProtocolParser protocolParser,
				ILogger<BsClient> logger,
				ILogger<BsConnectionLoop> connectionLoopLogger)
		{
			this.logger = logger;

			ConnectionInfo = new ReactivePropertySlim<BsConnectionInfo>(BsConnectionInfo.Empty)
				.AddTo(disposables);
			LatestStatus = new ReactivePropertySlim<BsStatusSnapshot>(BsStatusSnapshot.Empty)
				.AddTo(disposables);
			latestEventSource = new Subject<BsEventMessage?>()
				.AddTo(disposables);

			connectionLoop = new BsConnectionLoop(
				settingsService,
				protocolParser,
				connectionLoopLogger,
				() => LatestStatus.Value,
				connectionInfo => ConnectionInfo.Value = connectionInfo,
				PublishMessage);
		}

		/// <summary>
		/// Starts the BS connection loop.
		/// </summary>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			using var lease = await lifecycleLock.LockAsync(cancellationToken);
			if (runTask is not null)
			{
				return;
			}

			runCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
			runTask = Task.Run(() => connectionLoop.RunAsync(runCts.Token), runCts.Token);
			ConnectionInfo.Value = new BsConnectionInfo(BsConnectionState.Connecting, false, null);
			logger.LogInformation("BS client started.");
		}

		/// <summary>
		/// Stops the BS connection loop and clears connection state.
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

			activeRunCts?.Cancel();
			await AwaitCanceledAsync(activeRunTask);
			activeRunCts?.Dispose();
			ConnectionInfo.Value = new BsConnectionInfo(BsConnectionState.Disconnected, false, null);
			logger.LogInformation("BS client stopped.");
		}

		/// <summary>
		/// Requests the BS connection loop to stop without waiting.
		/// </summary>
		public void RequestShutdown()
		{
			logger.LogDebug("BS client shutdown requested.");
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

			logger.LogInformation("Disposed.");
		}

		/// <summary>
		/// Publishes a parsed BS message to status and event observers.
		/// </summary>
		private void PublishMessage(BsEventMessage message)
		{
			LatestStatus.Value = message.Status;
			latestEventSource.OnNext(message);
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
