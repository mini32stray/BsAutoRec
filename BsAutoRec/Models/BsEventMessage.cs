namespace BsAutoRec.Models
{
	public enum BsEventKind
	{
		Unknown,
		Hello,
		SongStart,
		Finished,
		SoftFailed,
		Failed,
		Menu,
		Pause,
		Resume,
		ScoreChanged,
	}

	public sealed record BsEventMessage(
		BsEventKind Kind,
		string EventName,
		DateTimeOffset HappenedAt,
		BsStatusSnapshot Status);
}
