using BsAutoRec.Models;
using System.Text.Json;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Parses BS WebSocket payloads into app-level event messages.
	/// </summary>
	public sealed class BsProtocolParser
	{
		private static readonly JsonSerializerOptions JsonSerializerOptions = new(JsonSerializerDefaults.Web);

		/// <summary>
		/// Parses one BS WebSocket JSON payload.
		/// </summary>
		public BsEventMessage ParseMessage(string json, BsStatusSnapshot previousSnapshot)
		{
			var dto = JsonSerializer.Deserialize<BsProtocolEventDto>(json, JsonSerializerOptions)
				?? throw new InvalidOperationException("BS WebSocket payload could not be parsed.");

			var eventName = dto.Event ?? string.Empty;
			var kind = ParseEventKind(eventName);
			var happenedAt = dto.Time > 0
				? DateTimeOffset.FromUnixTimeMilliseconds(dto.Time)
				: DateTimeOffset.Now;

			var snapshot = BuildSnapshot(dto.Status, previousSnapshot, kind, happenedAt);

			return new BsEventMessage(kind, eventName, happenedAt, snapshot);
		}

		/// <summary>
		/// Builds the latest known status from a partial or full BS payload.
		/// </summary>
		private static BsStatusSnapshot BuildSnapshot(BsProtocolStatusDto? status, BsStatusSnapshot previousSnapshot, BsEventKind kind, DateTimeOffset happenedAt)
		{
			if (IsFullStatusEvent(kind))
			{
				return CreateFullSnapshot(status, happenedAt);
			}

			var snapshot = previousSnapshot with
			{
				ObservedAt = happenedAt,
			};

			if (status?.Game is not null)
			{
				snapshot = snapshot with
				{
					Scene = status.Game.Scene ?? snapshot.Scene,
				};
			}

			if (status?.Beatmap is not null)
			{
				snapshot = snapshot with
				{
					SongName = status.Beatmap.SongName ?? snapshot.SongName,
					SongSubName = status.Beatmap.SongSubName ?? snapshot.SongSubName,
					SongAuthorName = status.Beatmap.SongAuthorName ?? snapshot.SongAuthorName,
					Characteristic = status.Beatmap.Characteristic ?? snapshot.Characteristic,
					DifficultyName = status.Beatmap.Difficulty ?? snapshot.DifficultyName,
					DifficultyEnum = status.Beatmap.DifficultyEnum ?? snapshot.DifficultyEnum,
					LevelAuthorName = status.Beatmap.LevelAuthorName ?? snapshot.LevelAuthorName,
					BeatsPerMinute = status.Beatmap.SongBpm ?? snapshot.BeatsPerMinute,
					SongHash = status.Beatmap.SongHash ?? snapshot.SongHash,
					BeatmapStartedAt = status.Beatmap.Start.HasValue
						? DateTimeOffset.FromUnixTimeMilliseconds(status.Beatmap.Start.Value)
						: snapshot.BeatmapStartedAt,
				};
			}

			if (status?.Performance is not null)
			{
				snapshot = snapshot with
				{
					ScorePercent = NormalizeScorePercent(status.Performance.RelativeScore) ?? snapshot.ScorePercent,
					CurrentSongTime = status.Performance.CurrentSongTime ?? snapshot.CurrentSongTime,
					SoftFailed = status.Performance.SoftFailed ?? snapshot.SoftFailed,
				};
			}

			return snapshot;
		}

		/// <summary>
		/// Creates a status snapshot from a payload that contains full status.
		/// </summary>
		private static BsStatusSnapshot CreateFullSnapshot(BsProtocolStatusDto? status, DateTimeOffset happenedAt)
		{
			var game = status?.Game;
			var beatmap = status?.Beatmap;
			var performance = status?.Performance;

			return new BsStatusSnapshot(
				ObservedAt: happenedAt,
				Scene: game?.Scene ?? string.Empty,
				SongName: beatmap?.SongName ?? string.Empty,
				SongSubName: beatmap?.SongSubName ?? string.Empty,
				SongAuthorName: beatmap?.SongAuthorName ?? string.Empty,
				Characteristic: beatmap?.Characteristic ?? string.Empty,
				DifficultyName: beatmap?.Difficulty ?? string.Empty,
				DifficultyEnum: beatmap?.DifficultyEnum ?? string.Empty,
				LevelAuthorName: beatmap?.LevelAuthorName ?? string.Empty,
				BeatsPerMinute: beatmap?.SongBpm,
				SongHash: beatmap?.SongHash ?? string.Empty,
				BeatmapStartedAt: beatmap?.Start.HasValue == true
					? DateTimeOffset.FromUnixTimeMilliseconds(beatmap.Start.Value)
					: null,
				ScorePercent: NormalizeScorePercent(performance?.RelativeScore),
				CurrentSongTime: performance?.CurrentSongTime,
				SoftFailed: performance?.SoftFailed ?? false);
		}

		/// <summary>
		/// Checks whether the event is expected to include full status.
		/// </summary>
		private static bool IsFullStatusEvent(BsEventKind kind)
		{
			return kind is BsEventKind.Hello or BsEventKind.SongStart or BsEventKind.Menu;
		}

		/// <summary>
		/// Converts a BS event name to the app enum.
		/// </summary>
		private static BsEventKind ParseEventKind(string eventName)
		{
			return eventName switch
			{
				"hello" => BsEventKind.Hello,
				"songStart" => BsEventKind.SongStart,
				"finished" => BsEventKind.Finished,
				"softFailed" => BsEventKind.SoftFailed,
				"failed" => BsEventKind.Failed,
				"menu" => BsEventKind.Menu,
				"pause" => BsEventKind.Pause,
				"resume" => BsEventKind.Resume,
				"scoreChanged" => BsEventKind.ScoreChanged,
				_ => BsEventKind.Unknown,
			};
		}

		/// <summary>
		/// Converts score values to percentage units.
		/// </summary>
		private static double? NormalizeScorePercent(double? value)
		{
			if (!value.HasValue)
			{
				return null;
			}

			return value.Value <= 1.5 ? value.Value * 100.0 : value.Value;
		}
	}
}
