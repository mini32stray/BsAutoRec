namespace BsAutoRec.Infrastructure
{
	public sealed class AppPaths
	{
		private AppPaths(string baseDirectoryPath, string settingsFilePath, string logsDirectoryPath)
		{
			BaseDirectoryPath = baseDirectoryPath;
			SettingsFilePath = settingsFilePath;
			LogsDirectoryPath = logsDirectoryPath;
		}

		public string BaseDirectoryPath { get; }

		public string SettingsFilePath { get; }

		public string LogsDirectoryPath { get; }

		public static AppPaths Create(string baseDirectoryPath)
		{
			var normalizedBaseDirectory = Path.GetFullPath(baseDirectoryPath);
			var settingsFilePath = Path.Combine(normalizedBaseDirectory, "settings.json");
			var logsDirectoryPath = Path.Combine(normalizedBaseDirectory, "Logs");

			return new AppPaths(normalizedBaseDirectory, settingsFilePath, logsDirectoryPath);
		}
	}
}
