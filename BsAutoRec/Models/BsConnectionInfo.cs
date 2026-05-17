namespace BsAutoRec.Models
{
	public enum BsConnectionState
	{
		NotInitialized,
		Disconnected,
		Connecting,
		Connected,
		Reconnecting,
	}

	public static class BsConnectionStateExtensions
	{
		public static string ToDisplayText(this BsConnectionState state)
		{
			return state switch
			{
				BsConnectionState.NotInitialized => "未接続",
				BsConnectionState.Disconnected => "切断",
				BsConnectionState.Connecting => "接続中...",
				BsConnectionState.Connected => "接続OK",
				BsConnectionState.Reconnecting => "再接続中...",
				_ => $"不明な状態 {state}",
			};
		}
	}

	public sealed record BsConnectionInfo(
		BsConnectionState State,
		bool IsConnected,
		string? LastError)
	{
		public static BsConnectionInfo Empty => new(BsConnectionState.NotInitialized, false, null);
	}
}
