using BsAutoRec.Models;
using Microsoft.Extensions.Logging;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Runs the recording start, stop, and rename steps for one song session.
	/// </summary>
	public sealed class RecordingWorkflow
	{
		private readonly SettingsService settingsService;
		private readonly ObsClient obsClient;
		private readonly RecordingRenameService recordingRenameService;
		private readonly ILogger<RecordingWorkflow> logger;

		/// <summary>
		/// Creates the workflow.
		/// </summary>
		public RecordingWorkflow(
				SettingsService settingsService,
				ObsClient obsClient,
				RecordingRenameService recordingRenameService,
				ILogger<RecordingWorkflow> logger)
		{
			this.settingsService = settingsService;
			this.obsClient = obsClient;
			this.recordingRenameService = recordingRenameService;
			this.logger = logger;
		}

		/// <summary>
		/// Starts OBS recording and reports failures to the UI callbacks.
		/// </summary>
		public async Task StartRecordingAsync(
				CancellationToken cancellationToken,
				RecordingWorkflowCallbacks callbacks,
				Func<bool> isAutomationRunning)
		{
			try
			{
				logger.LogInformation("Recording workflow is starting OBS recording.");
				await obsClient.StartRecordingAsync(cancellationToken);
				cancellationToken.ThrowIfCancellationRequested();
				callbacks.SetMessage("OBS 録画を開始しました。");
				callbacks.AddRecentMessage("OBS 録画を開始しました。");
			}
			catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
			{
			}
			catch (Exception exception)
			{
				logger.LogError(exception, "Failed to start OBS recording.");
				if (!isAutomationRunning())
				{
					return;
				}

				callbacks.SetRuntimeState(AppRuntimeState.Error);
				callbacks.SetMessage($"OBS 録画開始失敗: {exception.Message}");
				callbacks.AddRecentMessage($"OBS 録画開始失敗: {exception.Message}");
			}
		}

		/// <summary>
		/// Stops recording, applies the stop delay, and renames the output when needed.
		/// </summary>
		public async Task<RecordingFinalizeResult> FinalizeAsync(
				PlaySession session,
				string reason,
				LevelEndType levelEndType,
				CancellationToken cancellationToken,
				RecordingWorkflowCallbacks callbacks)
		{
			logger.LogInformation(
				"Recording workflow is finalizing. Reason: {Reason}; LevelEndType: {LevelEndType}; HandlingMode: {HandlingMode}",
				reason,
				levelEndType,
				session.HandlingMode);
			callbacks.SetRuntimeState(AppRuntimeState.StoppingRecording);
			callbacks.SetMessage(reason);
			callbacks.SetLevelEndType(levelEndType.ToString());

			var stopDelaySeconds = CalculateStopDelaySeconds(settingsService.Current, levelEndType);
			if (stopDelaySeconds > 0.0)
			{
				logger.LogInformation("Recording stop delay started. DelaySeconds: {DelaySeconds}", stopDelaySeconds);
				callbacks.SetMessage($"{reason} {stopDelaySeconds:0.0} 秒後に録画を停止します。");
				await Task.Delay(TimeSpan.FromSeconds(stopDelaySeconds), cancellationToken);
			}

			string? outputPath = null;
			try
			{
				outputPath = await obsClient.StopRecordingAsync(cancellationToken);
			}
			catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
			{
				return RecordingFinalizeResult.Canceled;
			}
			catch (Exception exception)
			{
				logger.LogWarning(exception, "Failed to stop OBS recording cleanly.");
				callbacks.SetMessage($"OBS 録画停止失敗: {exception.Message}");
				callbacks.AddRecentMessage($"OBS 録画停止失敗: {exception.Message}");
			}

			if (!string.IsNullOrWhiteSpace(outputPath))
			{
				callbacks.SetRecordingPath(outputPath);
				logger.LogInformation("OBS recording output path was received. OutputPath: {OutputPath}", outputPath);
			}

			string? renamedPath = null;
			if (!string.IsNullOrWhiteSpace(outputPath) && File.Exists(outputPath))
			{
				if (session.HandlingMode == SessionHandlingMode.RenameAndStop)
				{
					renamedPath = await RenameAsync(outputPath, session, cancellationToken, callbacks);
				}
				else
				{
					logger.LogInformation("Recording rename skipped because the session is stop-only. OutputPath: {OutputPath}", outputPath);
					callbacks.AddRecentMessage("この曲はリネーム対象外のため、録画停止のみ行いました。");
				}
			}
			else
			{
				logger.LogInformation("Recording rename skipped because no output path was available.");
			}

			return new RecordingFinalizeResult(false, outputPath, renamedPath);
		}

		/// <summary>
		/// Renames one OBS recording file for a completed session.
		/// </summary>
		private async Task<string?> RenameAsync(
				string outputPath,
				PlaySession session,
				CancellationToken cancellationToken,
				RecordingWorkflowCallbacks callbacks)
		{
			callbacks.SetRuntimeState(AppRuntimeState.Renaming);
			callbacks.SetMessage("録画ファイルをリネームしています。");
			logger.LogInformation("Recording rename started. OutputPath: {OutputPath}", outputPath);

			try
			{
				var renameResult = await recordingRenameService.RenameAsync(outputPath, session, settingsService.Current, cancellationToken);
				cancellationToken.ThrowIfCancellationRequested();
				callbacks.SetRecordingPath(renameResult.DestinationPath);
				callbacks.AddRecentMessage($"録画ファイルをリネームしました: {Path.GetFileName(renameResult.DestinationPath)}");
				return renameResult.DestinationPath;
			}
			catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
			{
				throw;
			}
			catch (Exception exception)
			{
				logger.LogError(exception, "Failed to rename recording.");
				callbacks.SetMessage($"リネーム失敗: {exception.Message}");
				callbacks.AddRecentMessage($"リネーム失敗: {exception.Message}");
				return null;
			}
		}

		/// <summary>
		/// Calculates the final recording stop delay for the session result.
		/// </summary>
		private static double CalculateStopDelaySeconds(AppSettings settings, LevelEndType levelEndType)
		{
			var baseDelaySeconds = Math.Clamp(settings.RecordingStopDelaySeconds, 0.0, 3.0);
			var failedAdditionalDelaySeconds = levelEndType == LevelEndType.Failed
				? Math.Clamp(settings.FailedRecordingStopAdditionalDelaySeconds, 0.0, 10.0)
				: 0.0;
			return baseDelaySeconds + failedAdditionalDelaySeconds;
		}
	}

	/// <summary>
	/// Provides UI state callbacks used by the recording workflow.
	/// </summary>
	public sealed record RecordingWorkflowCallbacks(
		Action<AppRuntimeState> SetRuntimeState,
		Action<string> SetMessage,
		Action<string> SetLevelEndType,
		Action<string> SetRecordingPath,
		Action<string> AddRecentMessage);

	/// <summary>
	/// Describes the result of finalizing a recording.
	/// </summary>
	public sealed record RecordingFinalizeResult(
		bool IsCanceled,
		string? OutputPath,
		string? RenamedPath)
	{
		public static RecordingFinalizeResult Canceled { get; } = new(true, null, null);
	}
}
