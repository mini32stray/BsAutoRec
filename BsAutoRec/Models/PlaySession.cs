namespace BsAutoRec.Models
{
	public enum SessionHandlingMode
	{
		RenameAndStop,
		StopOnly,
	}

	public sealed record PlaySession(
		DateTimeOffset StartedAt,
		BsStatusSnapshot LatestSnapshot,
		SessionHandlingMode HandlingMode,
		LevelEndType LevelEndType,
		DateTimeOffset? EndedAt,
		string? OriginalRecordingPath,
		string? RenamedRecordingPath)
	{
		public PlaySession WithSnapshot(BsStatusSnapshot snapshot)
		{
			return this with
			{
				LatestSnapshot = snapshot,
			};
		}
	}
}
