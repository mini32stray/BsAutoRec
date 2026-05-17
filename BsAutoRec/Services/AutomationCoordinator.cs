using BsAutoRec.Infrastructure;
using BsAutoRec.Models;
using Microsoft.Extensions.Logging;
using Reactive.Bindings;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading.Channels;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Coordinates BS events, OBS control, and UI-facing automation state on one message loop.
	/// </summary>
	public sealed class AutomationCoordinator : IAsyncDisposable
	{
		private readonly SettingsService settingsService;
		private readonly BsClient bsClient;
		private readonly ObsClient obsClient;
		private readonly RecordingWorkflow recordingWorkflow;
		private readonly ILogger<AutomationCoordinator> logger;
		private readonly AsyncLock controlLock;
		private readonly Channel<AutomationMessage> messages;
		private readonly CancellationTokenSource disposeCts;
		private readonly Task messageLoopTask;
		private readonly RecentMessageBuffer recentMessages;
		private readonly CompositeDisposable disposables;

		public ReactivePropertySlim<AppRuntimeState> RuntimeState { get; }
		public ReactivePropertySlim<string> MessageText { get; }
		public ReactivePropertySlim<string> RecordingPathText { get; }
		public ReactivePropertySlim<string> LevelEndTypeText { get; }
		public ReactivePropertySlim<string> RecentMessagesText { get; }

		private BsConnectionInfo _bsConnection;
		private ObsConnectionInfo _obsConnection;
		private PlaySession? _currentSession;
		private CancellationTokenSource? _automationCts;
		private CancellationTokenSource? _recoveryCts;
		private bool _isAutomationRunning;
		private bool _hasObservedRecordingForCurrentSession;

		/// <summary>
		/// Creates the coordinator and wires service notifications into the message queue.
		/// </summary>
		public AutomationCoordinator(
				SettingsService settingsService,
				BsClient bsClient,
				ObsClient obsClient,
				RecordingWorkflow recordingWorkflow,
				ILogger<AutomationCoordinator> logger)
		{
			this.settingsService = settingsService;
			this.bsClient = bsClient;
			this.obsClient = obsClient;
			this.recordingWorkflow = recordingWorkflow;
			this.logger = logger;
			controlLock = new AsyncLock();
			messages = Channel.CreateUnbounded<AutomationMessage>(
				new UnboundedChannelOptions
				{
					SingleReader = true,
					SingleWriter = false,
				});
			disposeCts = new CancellationTokenSource();
			messageLoopTask = Task.Run(ProcessMessagesAsync);
			recentMessages = new RecentMessageBuffer(8);
			disposables = new CompositeDisposable();

			_bsConnection = BsConnectionInfo.Empty;
			_obsConnection = ObsConnectionInfo.Empty;

			RuntimeState = new ReactivePropertySlim<AppRuntimeState>(AppRuntimeState.Stopped)
				.AddTo(disposables);
			MessageText = new ReactivePropertySlim<string>("開始すると自動監視を開始します。")
				.AddTo(disposables);
			RecordingPathText = new ReactivePropertySlim<string>(string.Empty)
				.AddTo(disposables);
			LevelEndTypeText = new ReactivePropertySlim<string>(string.Empty)
				.AddTo(disposables);
			RecentMessagesText = new ReactivePropertySlim<string>(string.Empty)
				.AddTo(disposables);

			bsClient.ConnectionInfo
				.Skip(1)
				.Subscribe(connectionInfo => Enqueue(new BsConnectionChangedMessage(connectionInfo)))
				.AddTo(disposables);
			bsClient.LatestStatus
				.Skip(1)
				.Subscribe(snapshot => Enqueue(new BsStatusUpdatedMessage(snapshot)))
				.AddTo(disposables);
			bsClient.LatestEvent
				.Where(message => message is not null)
				.Subscribe(message => Enqueue(new BsEventReceivedMessage(message!)))
				.AddTo(disposables);
			obsClient.ConnectionInfo
				.Skip(1)
				.Subscribe(connectionInfo => Enqueue(new ObsConnectionChangedMessage(connectionInfo)))
				.AddTo(disposables);
			obsClient.RecordState
				.Skip(1)
				.Subscribe(recordState => Enqueue(new ObsRecordStateChangedMessage(recordState)))
				.AddTo(disposables);
		}

		/// <summary>
		/// Starts automatic monitoring and connects to OBS and BS.
		/// </summary>
		public async Task StartAsync(CancellationToken cancellationToken)
		{
			var completion = CreateCompletion();
			await messages.Writer.WriteAsync(new StartRequestedMessage(completion, cancellationToken), cancellationToken);
			await completion.Task.WaitAsync(cancellationToken);
		}

		/// <summary>
		/// Stops monitoring and returns the coordinator to the initial state.
		/// </summary>
		public async Task StopAsync()
		{
			RequestShutdown();

			var completion = CreateCompletion();
			if (!messages.Writer.TryWrite(new StopRequestedMessage(completion)))
			{
				return;
			}

			await completion.Task;
		}

		/// <summary>
		/// Requests all running work to stop without waiting for completion.
		/// </summary>
		public void RequestShutdown()
		{
			TryCancel(_automationCts);
			TryCancel(_recoveryCts);
			bsClient.RequestShutdown();
			obsClient.RequestShutdown();
		}

		/// <summary>
		/// Stops the coordinator and releases all subscriptions and background work.
		/// </summary>
		public async ValueTask DisposeAsync()
		{
			await StopAsync();
			disposables.Dispose();
			messages.Writer.TryComplete();
			disposeCts.Cancel();
			await AwaitCanceledAsync(messageLoopTask);
			await ClearAutomationCtsAsync();
			disposeCts.Dispose();
			controlLock.Dispose();

			logger.LogInformation("Disposed.");
		}

		/// <summary>
		/// Reads queued automation messages in order.
		/// </summary>
		private async Task ProcessMessagesAsync()
		{
			try
			{
				await foreach (var message in messages.Reader.ReadAllAsync(disposeCts.Token))
				{
					await ProcessMessageAsync(message);
				}
			}
			catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
			{
			}
			catch (Exception exception)
			{
				logger.LogError(exception, "Automation message loop stopped unexpectedly.");
			}
		}

		/// <summary>
		/// Dispatches one automation message to its handler.
		/// </summary>
		private async Task ProcessMessageAsync(AutomationMessage message)
		{
			try
			{
				switch (message)
				{
					case StartRequestedMessage startRequested:
						await HandleStartRequestedAsync(startRequested);
						return;
					case StopRequestedMessage stopRequested:
						await HandleStopRequestedAsync(stopRequested);
						return;
					case BsConnectionChangedMessage bsConnectionChanged:
						HandleBsConnectionInfoChanged(bsConnectionChanged.ConnectionInfo);
						return;
					case BsStatusUpdatedMessage bsStatusUpdated:
						HandleBsStatusUpdated(bsStatusUpdated.Snapshot);
						return;
					case BsEventReceivedMessage bsEventReceived:
						await HandleBsEventAsync(bsEventReceived.Message);
						return;
					case ObsConnectionChangedMessage obsConnectionChanged:
						HandleObsConnectionInfoChanged(obsConnectionChanged.ConnectionInfo);
						return;
					case ObsRecordStateChangedMessage obsRecordStateChanged:
						HandleObsRecordStateChanged(obsRecordStateChanged.RecordState);
						return;
					case RecoveryTimedOutMessage:
						await HandleRecoveryTimedOutAsync();
						return;
				}
			}
			catch (OperationCanceledException) when (GetAutomationToken().IsCancellationRequested || disposeCts.IsCancellationRequested)
			{
			}
			catch (Exception exception)
			{
				logger.LogError(exception, "Failed to process automation message {MessageType}.", message.GetType().Name);
			}
		}

		/// <summary>
		/// Handles a user request to start the automation.
		/// </summary>
		private async Task HandleStartRequestedAsync(StartRequestedMessage message)
		{
			if (_isAutomationRunning)
			{
				logger.LogInformation("Automation start was requested, but automation is already running.");
				message.Completion.TrySetResult();
				return;
			}

			logger.LogInformation("Automation start requested.");
			var runCts = CancellationTokenSource.CreateLinkedTokenSource(message.CancellationToken);
			await SetAutomationCtsAsync(runCts);

			try
			{
				_isAutomationRunning = true;
				_hasObservedRecordingForCurrentSession = false;
				RuntimeState.Value = AppRuntimeState.Starting;
				MessageText.Value = "BS と OBS への接続を開始しています。";
				RecordingPathText.Value = string.Empty;
				LevelEndTypeText.Value = string.Empty;
				AddRecentMessage("自動監視を開始しました。");

				await obsClient.StartAsync(runCts.Token);
				await bsClient.StartAsync(runCts.Token);

				if (RuntimeState.Value == AppRuntimeState.Starting)
				{
					RuntimeState.Value = AppRuntimeState.Connecting;
					MessageText.Value = "接続待機中です。";
				}

				logger.LogInformation("Automation started. Waiting for BS and OBS connections.");
				message.Completion.TrySetResult();
			}
			catch (Exception exception)
			{
				_isAutomationRunning = false;
				_currentSession = null;
				_hasObservedRecordingForCurrentSession = false;
				await ClearAutomationCtsAsync(runCts);
				RuntimeState.Value = AppRuntimeState.Error;
				MessageText.Value = $"自動監視開始失敗: {exception.Message}";
				AddRecentMessage(MessageText.Value);
				logger.LogError(exception, "Automation start failed.");
				message.Completion.TrySetException(exception);
			}
		}

		/// <summary>
		/// Handles a user request to stop the automation.
		/// </summary>
		private async Task HandleStopRequestedAsync(StopRequestedMessage message)
		{
			try
			{
				logger.LogInformation("Automation stop requested.");
				CancelRecoveryTimer();
				await ClearAutomationCtsAsync();
				_isAutomationRunning = false;
				_currentSession = null;
				_hasObservedRecordingForCurrentSession = false;
				RuntimeState.Value = AppRuntimeState.Stopped;
				MessageText.Value = "自動監視は停止しています。";
				RecordingPathText.Value = string.Empty;
				LevelEndTypeText.Value = string.Empty;
				AddRecentMessage("自動監視を停止しました。");

				await bsClient.StopAsync();
				await obsClient.StopAsync();

				logger.LogInformation("Automation stopped.");
				message.Completion.TrySetResult();
			}
			catch (Exception exception)
			{
				logger.LogError(exception, "Failed to stop automation cleanly.");
				message.Completion.TrySetException(exception);
			}
		}

		/// <summary>
		/// Stores the latest BS status for the current session.
		/// </summary>
		private void HandleBsStatusUpdated(BsStatusSnapshot snapshot)
		{
			if (_currentSession is not null)
			{
				_currentSession = _currentSession.WithSnapshot(snapshot);
				if (snapshot.SoftFailed)
				{
					MarkCurrentSessionSoftFailed();
				}
			}
		}

		/// <summary>
		/// Handles BS events that drive recording state changes.
		/// </summary>
		private async Task HandleBsEventAsync(BsEventMessage message)
		{
			switch (message.Kind)
			{
				case BsEventKind.Hello:
					logger.LogDebug("Event received. {Kind}", message.Kind);
					await HandleHelloAsync(message.Status);
					return;
				case BsEventKind.SongStart:
					logger.LogDebug("Event received. {Kind}", message.Kind);
					await HandleSongStartAsync(message.Status);
					return;
				case BsEventKind.Finished:
					logger.LogDebug("Event received. {Kind}", message.Kind);
					await FinalizeCurrentSessionAsync("曲を完走しました。", GetCompletedLevelEndType());
					return;
				case BsEventKind.Failed:
					logger.LogDebug("Event received. {Kind}", message.Kind);
					await FinalizeCurrentSessionAsync("曲の失敗を検出しました。", "Failed");
					return;
				case BsEventKind.Menu:
					logger.LogDebug("Event received. {Kind}", message.Kind);
					await HandleMenuAsync(message.Status);
					return;
				case BsEventKind.SoftFailed:
					logger.LogDebug("Event received. {Kind}", message.Kind);
					MarkCurrentSessionSoftFailed();
					return;
			}
		}

		/// <summary>
		/// Handles the initial BS status after a WebSocket connection opens.
		/// </summary>
		private async Task HandleHelloAsync(BsStatusSnapshot snapshot)
		{
			if (!_isAutomationRunning)
			{
				return;
			}

			if (_currentSession is null)
			{
				if (snapshot.IsPlaying)
				{
					logger.LogInformation(
						"BS connected while a song is already playing. The session will be stop-only. SongName: {SongName}; SongHash: {SongHash}",
						snapshot.SongName,
						snapshot.SongHash);
					AdoptStopOnlySession(snapshot, "進行中の曲を検出したため、この曲は録画停止のみ行います。");
				}
				else
				{
					logger.LogInformation("BS connected and is not playing. Waiting for song start.");
					SetReadyState("曲開始を待機しています。");
				}

				return;
			}

			if (RuntimeState.Value != AppRuntimeState.WaitingForBsRecovery)
			{
				return;
			}

			if (!snapshot.IsPlaying)
			{
				logger.LogInformation("BS recovered after the tracked song ended. Finalizing current session as Quit.");
				CancelRecoveryTimer();
				await FinalizeCurrentSessionAsync("BS 再接続時に曲終了を確認しました。", "Quit");
				return;
			}

			if (CanResumeTrackedSession(_currentSession.LatestSnapshot, snapshot))
			{
				logger.LogInformation(
					"BS recovered and the tracked session was resumed. SongName: {SongName}; SongHash: {SongHash}",
					snapshot.SongName,
					snapshot.SongHash);
				ResumeTrackedSession(snapshot);
			}
			else
			{
				logger.LogInformation(
					"BS recovered with a different session. The new session will be stop-only. SongName: {SongName}; SongHash: {SongHash}",
					snapshot.SongName,
					snapshot.SongHash);
				ReplaceWithStopOnlySession(snapshot, "BS に再接続しましたが別セッションと判断したため、この曲は録画停止のみ行います。");
			}
		}

		/// <summary>
		/// Starts tracking a new song session and starts OBS recording.
		/// </summary>
		private async Task HandleSongStartAsync(BsStatusSnapshot snapshot)
		{
			if (!_isAutomationRunning)
			{
				return;
			}

			CancelRecoveryTimer();
			_hasObservedRecordingForCurrentSession = false;
			_currentSession = new PlaySession(
				StartedAt: DateTimeOffset.Now,
				LatestSnapshot: snapshot,
				HandlingMode: SessionHandlingMode.RenameAndStop,
				LevelEndType: "Unknown",
				EndedAt: null,
				OriginalRecordingPath: null,
				RenamedRecordingPath: null);
			RuntimeState.Value = AppRuntimeState.Playing;
			MessageText.Value = $"曲開始を検出しました: {snapshot.SongName}";
			LevelEndTypeText.Value = "Unknown";
			AddRecentMessage($"曲開始: {snapshot.SongName}");
			logger.LogInformation(
				"Song started. SongName: {SongName}; DifficultyName: {DifficultyName}; Characteristic: {Characteristic}; SongHash: {SongHash}",
				snapshot.SongName,
				snapshot.DifficultyName,
				snapshot.Characteristic,
				snapshot.SongHash);

			await recordingWorkflow.StartRecordingAsync(GetAutomationToken(), CreateRecordingWorkflowCallbacks(), () => _isAutomationRunning);
		}

		/// <summary>
		/// Treats a menu event as the end of the current song session.
		/// </summary>
		private async Task HandleMenuAsync(BsStatusSnapshot snapshot)
		{
			if (_currentSession is null)
			{
				if (_isAutomationRunning)
				{
					SetReadyState("曲開始を待機しています。");
				}

				return;
			}

			_currentSession = _currentSession.WithSnapshot(snapshot);
			logger.LogInformation("BS menu event ended the current session. Finalizing as Quit.");
			await FinalizeCurrentSessionAsync("メニューへの遷移を検出しました。", "Quit");
		}

		/// <summary>
		/// Updates BS connection state and starts recovery when needed.
		/// </summary>
		private void HandleBsConnectionInfoChanged(BsConnectionInfo connectionInfo)
		{
			if (_bsConnection.State != connectionInfo.State || _bsConnection.IsConnected != connectionInfo.IsConnected)
			{
				logger.LogInformation("BS connection state changed. State: {State}; IsConnected: {IsConnected}", connectionInfo.State, connectionInfo.IsConnected);
			}

			_bsConnection = connectionInfo;

			if (_isAutomationRunning && _currentSession is not null && connectionInfo.State != BsConnectionState.Connected)
			{
				StartRecoveryTimer();
			}

			if (!string.IsNullOrWhiteSpace(connectionInfo.LastError))
			{
				MessageText.Value = $"BS: {connectionInfo.LastError}";
				AddRecentMessage($"BS エラー: {connectionInfo.LastError}");
			}
		}

		/// <summary>
		/// Updates OBS connection state and reports connection errors.
		/// </summary>
		private void HandleObsConnectionInfoChanged(ObsConnectionInfo connectionInfo)
		{
			if (_obsConnection.State != connectionInfo.State || _obsConnection.IsConnected != connectionInfo.IsConnected)
			{
				logger.LogInformation("OBS connection state changed. State: {State}; IsConnected: {IsConnected}", connectionInfo.State, connectionInfo.IsConnected);
			}

			_obsConnection = connectionInfo;
			if (!string.IsNullOrWhiteSpace(connectionInfo.LastError))
			{
				MessageText.Value = $"OBS: {connectionInfo.LastError}";
				AddRecentMessage($"OBS エラー: {connectionInfo.LastError}");
			}
		}

		/// <summary>
		/// Updates OBS recording status and detects external recording stops.
		/// </summary>
		private void HandleObsRecordStateChanged(ObsClient.ObsRecordState recordState)
		{
			if (_currentSession is not null)
			{
				if (recordState.IsRecording)
				{
					_hasObservedRecordingForCurrentSession = true;
				}
				else if (_hasObservedRecordingForCurrentSession && RuntimeState.Value == AppRuntimeState.Playing)
				{
					_hasObservedRecordingForCurrentSession = false;
					MessageText.Value = "OBS の録画が外部から停止されました。";
					AddRecentMessage("OBS の録画が外部から停止されました。");
					logger.LogInformation("OBS recording was stopped outside this application during a tracked session.");
				}
			}

			if (!string.IsNullOrWhiteSpace(recordState.OutputPath))
			{
				RecordingPathText.Value = recordState.OutputPath;
			}
		}

		/// <summary>
		/// Stops recording and renames the output for the current session.
		/// </summary>
		private async Task FinalizeCurrentSessionAsync(string reason, string levelEndType)
		{
			if (_currentSession is null)
			{
				return;
			}

			var cancellationToken = GetAutomationToken();
			_currentSession = _currentSession with
			{
				EndedAt = DateTimeOffset.Now,
				LevelEndType = levelEndType,
			};
			logger.LogInformation(
				"Finalizing session. Reason: {Reason}; LevelEndType: {LevelEndType}; HandlingMode: {HandlingMode}; SongName: {SongName}; SongHash: {SongHash}",
				reason,
				levelEndType,
				_currentSession.HandlingMode,
				_currentSession.LatestSnapshot.SongName,
				_currentSession.LatestSnapshot.SongHash);

			var result = await recordingWorkflow.FinalizeAsync(_currentSession, reason, levelEndType, cancellationToken, CreateRecordingWorkflowCallbacks());
			if (result.IsCanceled)
			{
				return;
			}

			if (_currentSession is null || !_isAutomationRunning)
			{
				return;
			}

			_currentSession = _currentSession with
			{
				EndedAt = _currentSession.EndedAt ?? DateTimeOffset.Now,
				OriginalRecordingPath = result.OutputPath,
				RenamedRecordingPath = result.RenamedPath,
			};

			CancelRecoveryTimer();
			_currentSession = null;
			_hasObservedRecordingForCurrentSession = false;
			logger.LogInformation(
				"Session finalized. OutputPath: {OutputPath}; RenamedPath: {RenamedPath}",
				result.OutputPath,
				result.RenamedPath);
			SetReadyState(_isAutomationRunning ? "曲開始を待機しています。" : "自動監視は停止しています。");
		}

		/// <summary>
		/// Stops recording when BS did not recover in time.
		/// </summary>
		private async Task HandleRecoveryTimedOutAsync()
		{
			if (!_isAutomationRunning || _currentSession is null || RuntimeState.Value != AppRuntimeState.WaitingForBsRecovery)
			{
				return;
			}

			await FinalizeCurrentSessionAsync("BS 復旧待機がタイムアウトしたため録画を停止します。", "Quit");
		}

		/// <summary>
		/// Starts the timer used while waiting for BS to reconnect.
		/// </summary>
		private void StartRecoveryTimer()
		{
			if (_recoveryCts is not null || _currentSession is null)
			{
				return;
			}

			_recoveryCts = CancellationTokenSource.CreateLinkedTokenSource(GetAutomationToken());
			var cancellationToken = _recoveryCts.Token;
			var waitSeconds = Math.Max(1, settingsService.Current.RecoveryWaitSeconds);

			RuntimeState.Value = AppRuntimeState.WaitingForBsRecovery;
			MessageText.Value = $"BS の復旧を {waitSeconds} 秒待機しています。";
			AddRecentMessage(MessageText.Value);
			logger.LogInformation("BS recovery wait started. WaitSeconds: {WaitSeconds}", waitSeconds);

			_ = Task.Run(async () =>
			{
				try
				{
					await Task.Delay(TimeSpan.FromSeconds(waitSeconds), cancellationToken);
					if (!cancellationToken.IsCancellationRequested)
					{
						Enqueue(new RecoveryTimedOutMessage());
					}
				}
				catch (OperationCanceledException)
				{
				}
			}, cancellationToken);
		}

		/// <summary>
		/// Cancels the BS recovery timer if it is active.
		/// </summary>
		private void CancelRecoveryTimer()
		{
			if (_recoveryCts is null)
			{
				return;
			}

			_recoveryCts.Cancel();
			_recoveryCts.Dispose();
			_recoveryCts = null;
			logger.LogDebug("BS recovery wait canceled.");
		}

		/// <summary>
		/// Tracks an already-running song only to stop OBS at the end.
		/// </summary>
		private void AdoptStopOnlySession(BsStatusSnapshot snapshot, string message)
		{
			var levelEndType = snapshot.SoftFailed ? "SoftFailed" : "Unknown";
			_hasObservedRecordingForCurrentSession = false;
			_currentSession = new PlaySession(
				StartedAt: DateTimeOffset.Now,
				LatestSnapshot: snapshot,
				HandlingMode: SessionHandlingMode.StopOnly,
				LevelEndType: levelEndType,
				EndedAt: null,
				OriginalRecordingPath: null,
				RenamedRecordingPath: null);
			RuntimeState.Value = AppRuntimeState.Playing;
			MessageText.Value = message;
			LevelEndTypeText.Value = levelEndType;
			AddRecentMessage(message);
			logger.LogInformation(
				"Stop-only session adopted. LevelEndType: {LevelEndType}; SongName: {SongName}; SongHash: {SongHash}",
				levelEndType,
				snapshot.SongName,
				snapshot.SongHash);
		}

		/// <summary>
		/// Resumes a tracked session after BS reconnects.
		/// </summary>
		private void ResumeTrackedSession(BsStatusSnapshot snapshot)
		{
			if (_currentSession is null)
			{
				return;
			}

			CancelRecoveryTimer();
			_currentSession = _currentSession.WithSnapshot(snapshot);
			RuntimeState.Value = AppRuntimeState.Playing;
			MessageText.Value = "BS に再接続し、同一曲の追跡を継続しています。";
			AddRecentMessage("BS に再接続し、同一曲の追跡を継続しました。");
			logger.LogInformation("Tracked session resumed after BS reconnect.");
		}

		/// <summary>
		/// Replaces the current session with a stop-only session.
		/// </summary>
		private void ReplaceWithStopOnlySession(BsStatusSnapshot snapshot, string message)
		{
			CancelRecoveryTimer();
			AdoptStopOnlySession(snapshot, message);
		}

		/// <summary>
		/// Sets the idle state after startup or after a session ends.
		/// </summary>
		private void SetReadyState(string message)
		{
			RuntimeState.Value = _isAutomationRunning ? AppRuntimeState.Ready : AppRuntimeState.Stopped;
			MessageText.Value = message;
		}

		/// <summary>
		/// Marks the current session as soft-failed while keeping recording active.
		/// </summary>
		private void MarkCurrentSessionSoftFailed()
		{
			if (_currentSession is null)
			{
				return;
			}

			if (_currentSession.LevelEndType == "SoftFailed")
			{
				return;
			}

			_currentSession = _currentSession with
			{
				LevelEndType = "SoftFailed",
			};
			LevelEndTypeText.Value = "SoftFailed";
			MessageText.Value = "ソフトフェイルを検出しました。";
			AddRecentMessage("ソフトフェイルを検出しました。");
			logger.LogInformation(
				"SoftFailed detected. SongName: {SongName}; SongHash: {SongHash}",
				_currentSession.LatestSnapshot.SongName,
				_currentSession.LatestSnapshot.SongHash);
		}

		/// <summary>
		/// Gets the final level end type for a completed song.
		/// </summary>
		private string GetCompletedLevelEndType()
		{
			return _currentSession?.LevelEndType == "SoftFailed" ? "SoftFailed" : "Cleared";
		}

		/// <summary>
		/// Adds a line to the recent message display.
		/// </summary>
		private void AddRecentMessage(string message)
		{
			RecentMessagesText.Value = recentMessages.Add(message);
		}

		/// <summary>
		/// Creates callbacks used by the recording workflow to update UI-facing state.
		/// </summary>
		private RecordingWorkflowCallbacks CreateRecordingWorkflowCallbacks()
		{
			return new RecordingWorkflowCallbacks(
				state => RuntimeState.Value = state,
				message => MessageText.Value = message,
				levelEndType => LevelEndTypeText.Value = levelEndType,
				path => RecordingPathText.Value = path,
				AddRecentMessage);
		}

		/// <summary>
		/// Queues a message for the coordinator loop.
		/// </summary>
		private void Enqueue(AutomationMessage message)
		{
			messages.Writer.TryWrite(message);
		}

		/// <summary>
		/// Gets the token for the current automation run.
		/// </summary>
		private CancellationToken GetAutomationToken()
		{
			return _automationCts?.Token ?? CancellationToken.None;
		}

		/// <summary>
		/// Replaces the current automation cancellation source.
		/// </summary>
		private async Task SetAutomationCtsAsync(CancellationTokenSource cancellationTokenSource)
		{
			using var lease = await controlLock.LockAsync();
			_automationCts?.Dispose();
			_automationCts = cancellationTokenSource;
		}

		/// <summary>
		/// Cancels and clears the current automation cancellation source.
		/// </summary>
		private async Task ClearAutomationCtsAsync(CancellationTokenSource? expected = null)
		{
			CancellationTokenSource? cancellationTokenSource;

			using (var lease = await controlLock.LockAsync())
			{
				if (expected is not null && !ReferenceEquals(_automationCts, expected))
				{
					return;
				}

				cancellationTokenSource = _automationCts;
				_automationCts = null;
			}

			cancellationTokenSource?.Cancel();
			cancellationTokenSource?.Dispose();
		}

		/// <summary>
		/// Creates a completion source for request messages.
		/// </summary>
		private static TaskCompletionSource CreateCompletion()
		{
			return new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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

		/// <summary>
		/// Checks whether a reconnected BS status is the same song session.
		/// </summary>
		private static bool CanResumeTrackedSession(BsStatusSnapshot current, BsStatusSnapshot candidate)
		{
			if (!current.BeatmapStartedAt.HasValue || !candidate.BeatmapStartedAt.HasValue)
			{
				return false;
			}

			return current.BeatmapStartedAt.Value == candidate.BeatmapStartedAt.Value
				&& string.Equals(current.SongHash, candidate.SongHash, StringComparison.OrdinalIgnoreCase)
				&& string.Equals(current.DifficultyEnum, candidate.DifficultyEnum, StringComparison.OrdinalIgnoreCase)
				&& string.Equals(current.Characteristic, candidate.Characteristic, StringComparison.OrdinalIgnoreCase);
		}

		private abstract record AutomationMessage;

		private sealed record StartRequestedMessage(TaskCompletionSource Completion, CancellationToken CancellationToken) : AutomationMessage;

		private sealed record StopRequestedMessage(TaskCompletionSource Completion) : AutomationMessage;

		private sealed record BsConnectionChangedMessage(BsConnectionInfo ConnectionInfo) : AutomationMessage;

		private sealed record BsStatusUpdatedMessage(BsStatusSnapshot Snapshot) : AutomationMessage;

		private sealed record BsEventReceivedMessage(BsEventMessage Message) : AutomationMessage;

		private sealed record ObsConnectionChangedMessage(ObsConnectionInfo ConnectionInfo) : AutomationMessage;

		private sealed record ObsRecordStateChangedMessage(ObsClient.ObsRecordState RecordState) : AutomationMessage;

		private sealed record RecoveryTimedOutMessage : AutomationMessage;
	}
}
