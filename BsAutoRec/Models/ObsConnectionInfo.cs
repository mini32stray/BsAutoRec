namespace BsAutoRec.Models
{
	public enum ObsConnectionState
	{
		NotInitialized,
		Disconnected,
		Connecting,
		Connected,
		Reconnecting,
	}

	public static class ObsConnectionStateExtensions
	{
		public static string ToDisplayText(this ObsConnectionState state)
		{
			return state switch
			{
				ObsConnectionState.NotInitialized => "未接続",
				ObsConnectionState.Disconnected => "切断",
				ObsConnectionState.Connecting => "接続中...",
				ObsConnectionState.Connected => "接続OK",
				ObsConnectionState.Reconnecting => "再接続中...",
				_ => $"不明な状態 {state}",
			};
		}
	}

	public sealed record ObsConnectionInfo(
		ObsConnectionState State,
		bool IsConnected,
		string? LastError)
	{
		public static ObsConnectionInfo Empty => new(ObsConnectionState.NotInitialized, false, null);
	}
}
