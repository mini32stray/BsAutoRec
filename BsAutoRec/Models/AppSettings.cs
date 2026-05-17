namespace BsAutoRec.Models
{
	public sealed class AppSettings
	{
		public bool AutoStart { get; set; } = false;

		public string BsWebSocketUrl { get; set; } = "ws://127.0.0.1:6557/socket";

		public string ObsWebSocketUrl { get; set; } = "ws://127.0.0.1:4455";

		public string ObsPassword { get; set; } = string.Empty;

		public int RecoveryWaitSeconds { get; set; } = 10;

		public double RecordingStopDelaySeconds { get; set; } = 0.0;

		public string RecordingOutputDirectory { get; set; } = string.Empty;

		public string RenameTemplate { get; set; } = "{CurrentTime:yyyyMMdd-HHmmss}_{SongName}_{LevelAuthorName}_{DifficultyName}_{ScorePercent}_{LevelEndType}";
	}
}
