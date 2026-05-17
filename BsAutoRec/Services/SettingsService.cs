using BsAutoRec.Infrastructure;
using BsAutoRec.Models;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Loads and saves portable application settings.
	/// </summary>
	public sealed class SettingsService
	{
		private static readonly JsonSerializerOptions JsonSerializerOptions = new()
		{
			WriteIndented = true,
		};

		private readonly AppPaths _appPaths;
		private readonly ILogger<SettingsService> _logger;
		private readonly AsyncLock _lock;

		/// <summary>
		/// Creates the settings service.
		/// </summary>
		public SettingsService(AppPaths appPaths, ILogger<SettingsService> logger)
		{
			_appPaths = appPaths;
			_logger = logger;
			_lock = new();
			Current = new AppSettings();
		}

		public AppSettings Current { get; private set; }

		/// <summary>
		/// Loads settings at application startup.
		/// </summary>
		public async Task InitializeAsync()
		{
			await ReloadAsync();
		}

		/// <summary>
		/// Reloads settings from disk, creating defaults when the file is missing.
		/// </summary>
		public async Task ReloadAsync()
		{
			using var lease = await _lock.LockAsync();

			if (!File.Exists(_appPaths.SettingsFilePath))
			{
				Current = new AppSettings();
				await SaveInternalAsync(Current);
				_logger.LogInformation("Settings file was not found. Default settings were created at {Path}", _appPaths.SettingsFilePath);
				return;
			}

			await using var stream = File.OpenRead(_appPaths.SettingsFilePath);
			var settings = await JsonSerializer.DeserializeAsync<AppSettings>(stream, JsonSerializerOptions);
			Current = settings ?? new AppSettings();
			_logger.LogInformation("Settings loaded from {Path}", _appPaths.SettingsFilePath);
		}

		/// <summary>
		/// Saves settings and updates the current in-memory copy.
		/// </summary>
		public async Task SaveAsync(AppSettings settings)
		{
			using var lease = await _lock.LockAsync();

			Current = settings;
			await SaveInternalAsync(settings);
			_logger.LogInformation("Settings saved to {Path}", _appPaths.SettingsFilePath);
		}

		/// <summary>
		/// Writes settings JSON to the portable settings file.
		/// </summary>
		private async Task SaveInternalAsync(AppSettings settings)
		{
			Directory.CreateDirectory(_appPaths.BaseDirectoryPath);
			await using var stream = new FileStream(_appPaths.SettingsFilePath, FileMode.Create, FileAccess.Write, FileShare.Read);
			await JsonSerializer.SerializeAsync(stream, settings, JsonSerializerOptions);
		}
	}
}
