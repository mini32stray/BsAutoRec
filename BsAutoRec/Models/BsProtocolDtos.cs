using System.Text.Json.Serialization;

namespace BsAutoRec.Models
{
	public sealed class BsProtocolEventDto
	{
		[JsonPropertyName("event")]
		public string? Event { get; set; }

		[JsonPropertyName("time")]
		public long Time { get; set; }

		[JsonPropertyName("status")]
		public BsProtocolStatusDto? Status { get; set; }
	}

	public sealed class BsProtocolStatusDto
	{
		[JsonPropertyName("game")]
		public BsProtocolGameDto? Game { get; set; }

		[JsonPropertyName("beatmap")]
		public BsProtocolBeatmapDto? Beatmap { get; set; }

		[JsonPropertyName("performance")]
		public BsProtocolPerformanceDto? Performance { get; set; }
	}

	public sealed class BsProtocolGameDto
	{
		[JsonPropertyName("scene")]
		public string? Scene { get; set; }

		[JsonPropertyName("mode")]
		public string? Mode { get; set; }
	}

	public sealed class BsProtocolBeatmapDto
	{
		[JsonPropertyName("songName")]
		public string? SongName { get; set; }

		[JsonPropertyName("songSubName")]
		public string? SongSubName { get; set; }

		[JsonPropertyName("songAuthorName")]
		public string? SongAuthorName { get; set; }

		[JsonPropertyName("levelAuthorName")]
		public string? LevelAuthorName { get; set; }

		[JsonPropertyName("songHash")]
		public string? SongHash { get; set; }

		[JsonPropertyName("songBPM")]
		public double? SongBpm { get; set; }

		[JsonPropertyName("start")]
		public long? Start { get; set; }

		[JsonPropertyName("difficulty")]
		public string? Difficulty { get; set; }

		[JsonPropertyName("difficultyEnum")]
		public string? DifficultyEnum { get; set; }

		[JsonPropertyName("characteristic")]
		public string? Characteristic { get; set; }
	}

	public sealed class BsProtocolPerformanceDto
	{
		[JsonPropertyName("relativeScore")]
		public double? RelativeScore { get; set; }

		[JsonPropertyName("currentSongTime")]
		public int? CurrentSongTime { get; set; }

		[JsonPropertyName("softFailed")]
		public bool? SoftFailed { get; set; }
	}
}
