namespace BsAutoRec.Models
{
	public sealed record BsStatusSnapshot(
		DateTimeOffset ObservedAt,
		string Scene,
		string SongName,
		string SongSubName,
		string SongAuthorName,
		string Characteristic,
		string DifficultyName,
		string DifficultyEnum,
		string LevelAuthorName,
		double? BeatsPerMinute,
		string SongHash,
		DateTimeOffset? BeatmapStartedAt,
		double? ScorePercent,
		int? CurrentSongTime,
		bool SoftFailed)
	{
		public bool IsPlaying => string.Equals(Scene, "Song", StringComparison.OrdinalIgnoreCase);

		public static BsStatusSnapshot Empty { get; } = new(
			ObservedAt: DateTimeOffset.MinValue,
			Scene: string.Empty,
			SongName: string.Empty,
			SongSubName: string.Empty,
			SongAuthorName: string.Empty,
			Characteristic: string.Empty,
			DifficultyName: string.Empty,
			DifficultyEnum: string.Empty,
			LevelAuthorName: string.Empty,
			BeatsPerMinute: null,
			SongHash: string.Empty,
			BeatmapStartedAt: null,
			ScorePercent: null,
			CurrentSongTime: null,
			SoftFailed: false);
	}
}
