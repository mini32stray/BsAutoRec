namespace BsAutoRec.Models
{
	public enum AppRuntimeState
	{
		Stopped,
		Starting,
		Connecting,
		Ready,
		Playing,
		WaitingForBsRecovery,
		StoppingRecording,
		Renaming,
		Error,
	}

	public static class AppRuntimeStateExtensions
	{
		public static string ToDisplayText(this AppRuntimeState state)
		{
			return state switch
			{
				AppRuntimeState.Stopped => $"停止 {state}",
				AppRuntimeState.Starting => $"起動中 {state}",
				AppRuntimeState.Connecting => $"接続中 {state}",
				AppRuntimeState.Ready => $"待機中 {state}",
				AppRuntimeState.Playing => $"🎵 曲プレイ中 {state}",
				AppRuntimeState.WaitingForBsRecovery => $"ゲーム復旧待機中 {state}",
				AppRuntimeState.StoppingRecording => $"録画停止中 {state}",
				AppRuntimeState.Renaming => $"リネーム中 {state}",
				AppRuntimeState.Error => $"録画開始失敗 {state}",
				_ => $"不明な状態 {state}",
			};
		}
	}
}
