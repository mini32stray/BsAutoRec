using BsAutoRec.Models;
using Microsoft.Extensions.Logging;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Renames OBS recording files using the configured template.
	/// </summary>
	public sealed class RecordingRenameService
	{
		/// <summary>
		/// Describes a completed recording rename.
		/// </summary>
		public sealed record RecordingRenameResult(
			string SourcePath,
			string DestinationPath);

		private const int MaxFileNameLength = 180;
		private const int MaxRenameAttempts = 5;

		private readonly FileNameTemplateService _fileNameTemplateService;
		private readonly ILogger<RecordingRenameService> _logger;

		/// <summary>
		/// Creates the rename service.
		/// </summary>
		public RecordingRenameService(FileNameTemplateService fileNameTemplateService, ILogger<RecordingRenameService> logger)
		{
			_fileNameTemplateService = fileNameTemplateService;
			_logger = logger;
		}

		/// <summary>
		/// Builds a destination path and moves the recording file there.
		/// </summary>
		public async Task<RecordingRenameResult> RenameAsync(string sourcePath, PlaySession session, AppSettings settings, CancellationToken cancellationToken)
		{
			cancellationToken.ThrowIfCancellationRequested();

			var directoryPath = Path.GetDirectoryName(sourcePath);
			if (string.IsNullOrWhiteSpace(directoryPath))
			{
				throw new InvalidOperationException("録画ファイルのディレクトリを取得できませんでした。");
			}

			var destinationDirectoryPath = string.IsNullOrWhiteSpace(settings.RecordingOutputDirectory)
				? directoryPath
				: settings.RecordingOutputDirectory;
			if (!Path.Exists(destinationDirectoryPath))
			{
				throw new InvalidOperationException("録画の出力先ディレクトリが存在しません。");
			}

			var extension = Path.GetExtension(sourcePath);
			var fileName = _fileNameTemplateService.Render(settings.RenameTemplate, session, settings);
			if (fileName.Length > MaxFileNameLength)
			{
				fileName = fileName[..MaxFileNameLength];
			}

			var destinationPath = BuildUniquePath(destinationDirectoryPath, fileName, extension);
			_logger.LogInformation("Recording rename destination decided. SourcePath: {SourcePath}; DestinationPath: {DestinationPath}", sourcePath, destinationPath);
			await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
			await MoveWithRetryAsync(sourcePath, destinationPath, cancellationToken);

			_logger.LogInformation("Recording renamed from {SourcePath} to {DestinationPath}", sourcePath, destinationPath);

			return new RecordingRenameResult(sourcePath, destinationPath);
		}

		/// <summary>
		/// Moves a file with retries while OBS may still hold the file.
		/// </summary>
		private async Task MoveWithRetryAsync(string sourcePath, string destinationPath, CancellationToken cancellationToken)
		{
			Exception? lastException = null;

			for (var attempt = 1; attempt <= MaxRenameAttempts; attempt++)
			{
				cancellationToken.ThrowIfCancellationRequested();

				try
				{
					_logger.LogDebug("Recording rename attempt started. Attempt: {Attempt}; SourcePath: {SourcePath}; DestinationPath: {DestinationPath}", attempt, sourcePath, destinationPath);
					File.Move(sourcePath, destinationPath);
					return;
				}
				catch (IOException exception) when (attempt < MaxRenameAttempts)
				{
					lastException = exception;
					_logger.LogDebug(exception, "Recording rename attempt {Attempt} failed. Retrying.", attempt);
					await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
				}
				catch (UnauthorizedAccessException exception) when (attempt < MaxRenameAttempts)
				{
					lastException = exception;
					_logger.LogDebug(exception, "Recording rename attempt {Attempt} failed. Retrying.", attempt);
					await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
				}
			}

			throw lastException ?? new IOException("録画ファイルのリネームに失敗しました。");
		}

		/// <summary>
		/// Builds a destination path that does not overwrite an existing file.
		/// </summary>
		private static string BuildUniquePath(string directoryPath, string fileName, string extension)
		{
			var basePath = Path.Combine(directoryPath, fileName + extension);
			if (!File.Exists(basePath))
			{
				return basePath;
			}

			for (var index = 2; index < 10_000; index++)
			{
				var candidatePath = Path.Combine(directoryPath, $"{fileName} ({index}){extension}");
				if (!File.Exists(candidatePath))
				{
					return candidatePath;
				}
			}

			throw new IOException("リネーム先の一意なファイル名を決定できませんでした。");
		}
	}
}
