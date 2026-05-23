using BsAutoRec.Infrastructure;
using BsAutoRec.Models;
using BsAutoRec.Services;
using Microsoft.Extensions.Logging;
using Reactive.Bindings;
using System.Diagnostics;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Windows.Controls.Primitives;

namespace BsAutoRec.ViewModels
{
	public sealed class MainViewModel : IDisposable
	{
		private readonly SettingsService _settingsService;
		private readonly AutomationCoordinator _automationCoordinator;
		private readonly ILogger<MainViewModel> _logger;
		private readonly CompositeDisposable _disposables;
		private readonly TryGate buttonGate;

		public ReactiveProperty<string> StatusText { get; }

		public ReactiveProperty<StatusSeverity> AppStatusSeverity { get; }

		public ReactiveProperty<string> MessageText { get; }

		public ReactiveProperty<string> BsConnectionText { get; }

		public ReactiveProperty<StatusSeverity> BsConnectionSeverity { get; }

		public ReactiveProperty<string> ObsConnectionText { get; }

		public ReactiveProperty<StatusSeverity> ObsConnectionSeverity { get; }

		public ReactiveProperty<string> SongNameText { get; }

		public ReactiveProperty<string> SongSubNameText { get; }

		public ReactiveProperty<string> SongAuthorNameText { get; }

		public ReactiveProperty<string> LevelAuthorNameText { get; }

		public ReactiveProperty<string> DifficultyText { get; }

		public ReactiveProperty<string> ScorePercentText { get; }

		public ReactiveProperty<string> LevelEndTypeText { get; }

		public ReactiveProperty<string> RecordingPathText { get; }

		public ReactiveProperty<string> RecentMessagesText { get; }

		public ReactiveProperty<bool> AutoStart { get; }

		public ReactiveProperty<string> BsWebSocketUrl { get; }

		public ReactiveProperty<string> ObsWebSocketUrl { get; }

		public ReactiveProperty<string> ObsPassword { get; }

		public ReactiveProperty<int> RecoveryWaitSeconds { get; }

		public ReactiveProperty<double> RecordingStopDelaySeconds { get; }

		public ReactiveProperty<double> FailedRecordingStopAdditionalDelaySeconds { get; }

		public ReactiveProperty<string> RecordingOutputDirectory { get; }

		public ReactiveProperty<string> RenameTemplate { get; }

		public ReactiveProperty<bool> IsSettingsExpanded { get; }

		public ReactiveCommand StartCommand { get; }

		public ReactiveCommand StopCommand { get; }

		public ReactiveCommand SaveSettingsCommand { get; }

		public ReactiveCommand ReloadSettingsCommand { get; }

		public ReactiveCommand OutputDirectoryFolderCommand { get; }

		public ReactiveCommand OpenOutputDirectoryCommand { get; }

		public MainViewModel(SettingsService settingsService, BsClient bsClient, ObsClient obsClient, AutomationCoordinator automationCoordinator, ILogger<MainViewModel> logger)
		{
			_settingsService = settingsService;
			_automationCoordinator = automationCoordinator;
			_logger = logger;
			_disposables = new CompositeDisposable();
			buttonGate = new();

			StatusText = _automationCoordinator.RuntimeState
				.Select(state => state.ToDisplayText())
				.ToReactiveProperty<string>()
				.AddTo(_disposables);
			AppStatusSeverity = Observable.CombineLatest(
					_automationCoordinator.RuntimeState,
					bsClient.ConnectionInfo,
					obsClient.ConnectionInfo,
					BuildAppStatusSeverity)
				.ToReactiveProperty()
				.AddTo(_disposables);
			MessageText = CreateViewProperty(_automationCoordinator.MessageText);
			BsConnectionText = CreateViewProperty(bsClient.ConnectionInfo.Select(BuildBsConnectionText));
			BsConnectionSeverity = Observable.CombineLatest(
					_automationCoordinator.RuntimeState,
					bsClient.ConnectionInfo,
					BuildBsConnectionSeverity)
				.ToReactiveProperty()
				.AddTo(_disposables);
			ObsConnectionText = CreateViewProperty(obsClient.ConnectionInfo.Select(BuildObsConnectionText));
			ObsConnectionSeverity = Observable.CombineLatest(
					_automationCoordinator.RuntimeState,
					obsClient.ConnectionInfo,
					BuildObsConnectionSeverity)
				.ToReactiveProperty()
				.AddTo(_disposables);
			SongNameText = CreateViewProperty(bsClient.LatestStatus.Select(snapshot => snapshot.SongName));
			SongSubNameText = CreateViewProperty(bsClient.LatestStatus.Select(snapshot => snapshot.SongSubName));
			SongAuthorNameText = CreateViewProperty(bsClient.LatestStatus.Select(snapshot => snapshot.SongAuthorName));
			LevelAuthorNameText = CreateViewProperty(bsClient.LatestStatus.Select(snapshot => snapshot.LevelAuthorName));
			DifficultyText = CreateViewProperty(bsClient.LatestStatus.Select(BuildDifficultyText));
			ScorePercentText = CreateViewProperty(bsClient.LatestStatus.Select(snapshot => snapshot.ScorePercent?.ToString("0.00") ?? string.Empty));
			LevelEndTypeText = CreateViewProperty(_automationCoordinator.LevelEndTypeText);
			RecordingPathText = CreateViewProperty(_automationCoordinator.RecordingPathText);
			RecentMessagesText = CreateViewProperty(_automationCoordinator.RecentMessagesText);

			AutoStart = new ReactiveProperty<bool>(_settingsService.Current.AutoStart).AddTo(_disposables);
			BsWebSocketUrl = new ReactiveProperty<string>(_settingsService.Current.BsWebSocketUrl).AddTo(_disposables);
			ObsWebSocketUrl = new ReactiveProperty<string>(_settingsService.Current.ObsWebSocketUrl).AddTo(_disposables);
			ObsPassword = new ReactiveProperty<string>(_settingsService.Current.ObsPassword).AddTo(_disposables);
			RecoveryWaitSeconds = new ReactiveProperty<int>(_settingsService.Current.RecoveryWaitSeconds).AddTo(_disposables);
			RecordingStopDelaySeconds = new ReactiveProperty<double>(_settingsService.Current.RecordingStopDelaySeconds).AddTo(_disposables);
			FailedRecordingStopAdditionalDelaySeconds = new ReactiveProperty<double>(_settingsService.Current.FailedRecordingStopAdditionalDelaySeconds).AddTo(_disposables);
			RecordingOutputDirectory = new ReactiveProperty<string>(_settingsService.Current.RecordingOutputDirectory).AddTo(_disposables);
			RenameTemplate = new ReactiveProperty<string>(_settingsService.Current.RenameTemplate).AddTo(_disposables);
			IsSettingsExpanded = new ReactiveProperty<bool>(true).AddTo(_disposables);

			StartCommand = _automationCoordinator.RuntimeState
				.Select(state => state == AppRuntimeState.Stopped)
				.ToReactiveCommand(true)
				.WithSubscribe(
					async () => await StartAsync(),
					_disposables.Add)
				.AddTo(_disposables);
			StopCommand = _automationCoordinator.RuntimeState
				.Select(state => state != AppRuntimeState.Stopped)
				.ToReactiveCommand(false)
				.WithSubscribe(
					async () => await StopAsync(),
					_disposables.Add)
				.AddTo(_disposables);
			SaveSettingsCommand = new ReactiveCommand()
				.WithSubscribe(
					async () => await SaveSettingsAsync(),
					_disposables.Add)
				.AddTo(_disposables);
			ReloadSettingsCommand = new ReactiveCommand()
				.WithSubscribe(
					async () => await ReloadSettingsAsync(),
					_disposables.Add)
				.AddTo(_disposables);
			OutputDirectoryFolderCommand = _automationCoordinator.RuntimeState
				.Select(state => state == AppRuntimeState.Stopped)
				.ToReactiveCommand()
				.WithSubscribe(() =>
				{
					var dialog = new Microsoft.Win32.OpenFolderDialog
					{
						InitialDirectory = RecordingOutputDirectory.Value,
					};

					if (dialog.ShowDialog() == true)
					{
						RecordingOutputDirectory.Value = dialog.FolderName;
					}
				}, _disposables.Add)
				.AddTo(_disposables);
			OpenOutputDirectoryCommand = new ReactiveCommand()
				.WithSubscribe(OpenOutputDirectory, _disposables.Add)
				.AddTo(_disposables);

			if (_settingsService.Current.AutoStart)
			{
				_logger.LogInformation("AutoStart is enabled. Automation start is queued.");
				_ = Task.Run(StartAsync);
			}
		}

		public void Dispose()
		{
			_disposables.Dispose();
		}

		private async Task StartAsync()
		{
			await buttonGate.RunIfFreeAsync(async () =>
			{
				try
				{
					_logger.LogInformation("User requested automation start.");
					await SaveSettingsAsync();
					await _automationCoordinator.StartAsync(CancellationToken.None);
				}
				catch (Exception exception)
				{
					_logger.LogError(exception, "Failed to start automation.");
				}
			});
		}

		private async Task StopAsync()
		{
			await buttonGate.RunIfFreeAsync(async () =>
			{
				try
				{
					_logger.LogInformation("User requested automation stop.");
					await _automationCoordinator.StopAsync();
				}
				catch (Exception exception)
				{
					_logger.LogError(exception, "Failed to stop automation.");
				}
			});
		}

		private async Task SaveSettingsAsync()
		{
			_logger.LogInformation("Settings save requested.");
			var settings = new AppSettings
			{
				AutoStart = AutoStart.Value,
				BsWebSocketUrl = BsWebSocketUrl.Value.Trim(),
				ObsWebSocketUrl = ObsWebSocketUrl.Value.Trim(),
				ObsPassword = ObsPassword.Value,
				RecoveryWaitSeconds = Math.Max(1, RecoveryWaitSeconds.Value),
				RecordingStopDelaySeconds = Math.Clamp(RecordingStopDelaySeconds.Value, 0.0, 3.0),
				FailedRecordingStopAdditionalDelaySeconds = Math.Clamp(FailedRecordingStopAdditionalDelaySeconds.Value, 0.0, 10.0),
				RecordingOutputDirectory = RecordingOutputDirectory.Value.Trim(),
				RenameTemplate = RenameTemplate.Value.Trim(),
			};

			await _settingsService.SaveAsync(settings);
		}

		private async Task ReloadSettingsAsync()
		{
			_logger.LogInformation("Settings reload requested.");
			await _settingsService.ReloadAsync();
			LoadSettings(_settingsService.Current);
		}

		private void LoadSettings(AppSettings settings)
		{
			AutoStart.Value = settings.AutoStart;
			BsWebSocketUrl.Value = settings.BsWebSocketUrl;
			ObsWebSocketUrl.Value = settings.ObsWebSocketUrl;
			ObsPassword.Value = settings.ObsPassword;
			RecoveryWaitSeconds.Value = settings.RecoveryWaitSeconds;
			RecordingStopDelaySeconds.Value = Math.Clamp(settings.RecordingStopDelaySeconds, 0.0, 3.0);
			FailedRecordingStopAdditionalDelaySeconds.Value = Math.Clamp(settings.FailedRecordingStopAdditionalDelaySeconds, 0.0, 10.0);
			RecordingOutputDirectory.Value = settings.RecordingOutputDirectory;
			RenameTemplate.Value = settings.RenameTemplate;
		}

		private void OpenOutputDirectory()
		{
			var directoryPath = RecordingOutputDirectory.Value.Trim();
			if (string.IsNullOrWhiteSpace(directoryPath))
			{
				_logger.LogInformation("Open output directory skipped because output directory is empty.");
				return;
			}
			if (!Directory.Exists(directoryPath))
			{
				_logger.LogInformation($"Open output directory skipped because output directory not exists. -> {directoryPath}");
				return;
			}

			try
			{
				Directory.CreateDirectory(directoryPath);
				Process.Start(new ProcessStartInfo
				{
					FileName = directoryPath,
					UseShellExecute = true,
				});
			}
			catch (Exception exception)
			{
				_logger.LogError(exception, "Failed to open output directory. DirectoryPath: {DirectoryPath}", directoryPath);
			}
		}

		private ReactiveProperty<string> CreateViewProperty(IObservable<string> source)
		{
			return source
				.Select(value => value ?? string.Empty)
				.ToReactiveProperty()
				.AddTo(_disposables)!;
		}

		private static string BuildBsConnectionText(BsConnectionInfo connectionInfo)
		{
			return string.IsNullOrWhiteSpace(connectionInfo.LastError)
				? connectionInfo.State.ToDisplayText()
				: $"{connectionInfo.State.ToDisplayText()}{Environment.NewLine}{connectionInfo.LastError}";
		}

		private static string BuildObsConnectionText(ObsConnectionInfo connectionInfo)
		{
			return string.IsNullOrWhiteSpace(connectionInfo.LastError)
				? connectionInfo.State.ToDisplayText()
				: $"{connectionInfo.State.ToDisplayText()}{Environment.NewLine}{connectionInfo.LastError}";
		}

		private static StatusSeverity BuildAppStatusSeverity(AppRuntimeState state, BsConnectionInfo bsConnectionInfo, ObsConnectionInfo obsConnectionInfo)
		{
			if (state == AppRuntimeState.Stopped)
			{
				return StatusSeverity.Neutral;
			}

			if (state == AppRuntimeState.Error)
			{
				return StatusSeverity.Attention;
			}

			if (state is AppRuntimeState.Starting or AppRuntimeState.Connecting or AppRuntimeState.WaitingForBsRecovery)
			{
				return StatusSeverity.Waiting;
			}

			if (bsConnectionInfo.IsConnected && obsConnectionInfo.IsConnected)
			{
				return StatusSeverity.Ok;
			}

			if (bsConnectionInfo.State is BsConnectionState.Connecting or BsConnectionState.Reconnecting
					|| obsConnectionInfo.State is ObsConnectionState.Connecting or ObsConnectionState.Reconnecting)
			{
				return StatusSeverity.Waiting;
			}

			return StatusSeverity.Attention;
		}

		private static StatusSeverity BuildBsConnectionSeverity(AppRuntimeState state, BsConnectionInfo connectionInfo)
		{
			if (state == AppRuntimeState.Stopped)
			{
				return StatusSeverity.Neutral;
			}

			if (state is AppRuntimeState.Starting or AppRuntimeState.Connecting)
			{
				return connectionInfo.IsConnected ? StatusSeverity.Ok : StatusSeverity.Waiting;
			}

			return connectionInfo.State switch
			{
				BsConnectionState.Connected => StatusSeverity.Ok,
				BsConnectionState.Connecting or BsConnectionState.Reconnecting => StatusSeverity.Waiting,
				_ => StatusSeverity.Attention,
			};
		}

		private static StatusSeverity BuildObsConnectionSeverity(AppRuntimeState state, ObsConnectionInfo connectionInfo)
		{
			if (state == AppRuntimeState.Stopped)
			{
				return StatusSeverity.Neutral;
			}

			if (state is AppRuntimeState.Starting or AppRuntimeState.Connecting)
			{
				return connectionInfo.IsConnected ? StatusSeverity.Ok : StatusSeverity.Waiting;
			}

			return connectionInfo.State switch
			{
				ObsConnectionState.Connected => StatusSeverity.Ok,
				ObsConnectionState.Connecting or ObsConnectionState.Reconnecting => StatusSeverity.Waiting,
				_ => StatusSeverity.Attention,
			};
		}

		private static string BuildDifficultyText(BsStatusSnapshot snapshot)
		{
			return string.IsNullOrWhiteSpace(snapshot.Characteristic)
				? snapshot.DifficultyName
				: $"{snapshot.DifficultyName} / {snapshot.Characteristic}";
		}
	}
}
