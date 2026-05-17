using BsAutoRec.Infrastructure;
using BsAutoRec.Services;
using BsAutoRec.ViewModels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NLog;
using NLog.Extensions.Logging;
using System.ComponentModel;
using System.Diagnostics;
using System.Reflection;
using System.Windows;
using System.Windows.Threading;

namespace BsAutoRec
{
	public partial class App : Application
	{
		private IHost? _host;
		private SingleInstanceGuard? _singleInstanceGuard;
		private bool _shutdownStarted;
		private bool _shutdownCompleted;

		protected override async void OnStartup(StartupEventArgs e)
		{
			base.OnStartup(e);

			_singleInstanceGuard = new SingleInstanceGuard("Local\\BsAutoRec.SingleInstance");
			if (!_singleInstanceGuard.TryAcquire())
			{
				Shutdown();
				return;
			}

			DispatcherUnhandledException += OnDispatcherUnhandledException;
			AppDomain.CurrentDomain.UnhandledException += OnCurrentDomainUnhandledException;
			TaskScheduler.UnobservedTaskException += OnTaskSchedulerUnobservedTaskException;

			var appPaths = AppPaths.Create(AppContext.BaseDirectory);
			var builder = Host.CreateApplicationBuilder();

			builder.Logging.ClearProviders();
			builder.Logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Trace);
			builder.Logging.AddNLog(Path.Combine(appPaths.BaseDirectoryPath, "NLog.config"));

			builder.Services.AddSingleton(appPaths);
			builder.Services.AddSingleton<SettingsService>();
			builder.Services.AddSingleton<FileNameTemplateService>();
			builder.Services.AddSingleton<RecordingRenameService>();
			builder.Services.AddSingleton<RecordingWorkflow>();
			builder.Services.AddSingleton<BsProtocolParser>();
			builder.Services.AddSingleton<BsClient>();
			builder.Services.AddSingleton<ObsClient>();
			builder.Services.AddSingleton<AutomationCoordinator>();
			builder.Services.AddSingleton<MainViewModel>();
			builder.Services.AddTransient<MainWindow>();

			_host = builder.Build();
			await _host.StartAsync();

			var appLogger = _host.Services.GetRequiredService<ILogger<App>>();
			appLogger.LogInformation(
				"Application started. Version: {Version}; ProcessPath: {ProcessPath}; BaseDirectory: {BaseDirectory}",
				GetApplicationVersion(),
				Environment.ProcessPath,
				appPaths.BaseDirectoryPath);

			var settingsService = _host.Services.GetRequiredService<SettingsService>();
			await settingsService.InitializeAsync();

			var mainWindow = _host.Services.GetRequiredService<MainWindow>();
			MainWindow = mainWindow;
			mainWindow.Closing += OnMainWindowClosing;
			mainWindow.Show();
		}

		private async void OnMainWindowClosing(object? sender, CancelEventArgs e)
		{
			if (_shutdownCompleted)
			{
				return;
			}

			e.Cancel = true;
			if (_shutdownStarted)
			{
				return;
			}

			_host?.Services.GetService<ILogger<App>>()?.LogInformation("User requested application shutdown.");
			_shutdownStarted = true;
			await ShutdownServicesAsync();
			_shutdownCompleted = true;

			if (MainWindow is not null)
			{
				MainWindow.Closing -= OnMainWindowClosing;
				MainWindow.Close();
			}
			else
			{
				Shutdown();
			}
		}

		protected override void OnExit(ExitEventArgs e)
		{
			_host?.Services.GetService<ILogger<App>>()?.LogInformation("Application exit started. ExitCode: {ExitCode}", e.ApplicationExitCode);

			DispatcherUnhandledException -= OnDispatcherUnhandledException;
			AppDomain.CurrentDomain.UnhandledException -= OnCurrentDomainUnhandledException;
			TaskScheduler.UnobservedTaskException -= OnTaskSchedulerUnobservedTaskException;

			if (!_shutdownCompleted)
			{
				try
				{
					_host?.Services.GetService<AutomationCoordinator>()?.RequestShutdown();
				}
				catch
				{
				}
			}

			LogManager.Shutdown();
			_singleInstanceGuard?.Dispose();

			base.OnExit(e);
		}

		private static string GetApplicationVersion()
		{
			var assembly = Assembly.GetExecutingAssembly();
			var informationalVersion = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
			if (!string.IsNullOrWhiteSpace(informationalVersion))
			{
				return informationalVersion;
			}

			var processPath = Environment.ProcessPath;
			if (!string.IsNullOrWhiteSpace(processPath))
			{
				var fileVersion = FileVersionInfo.GetVersionInfo(processPath).ProductVersion;
				if (!string.IsNullOrWhiteSpace(fileVersion))
				{
					return fileVersion;
				}
			}

			return assembly.GetName().Version?.ToString() ?? "unknown";
		}

		private async Task ShutdownServicesAsync()
		{
			var host = _host;
			if (host is null)
			{
				return;
			}

			try
			{
				host.Services.GetService<AutomationCoordinator>()?.RequestShutdown();

				if (host is IAsyncDisposable asyncDisposable)
				{
					await asyncDisposable.DisposeAsync();
				}
				else
				{
					host.Dispose();
				}
			}
			catch (Exception exception)
			{
				TryLogUnhandledException(exception, "ShutdownServicesAsync");
			}
			finally
			{
				_host = null;
			}
		}

		private void OnDispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
		{
			TryLogUnhandledException(e.Exception, "DispatcherUnhandledException");
		}

		private void OnCurrentDomainUnhandledException(object sender, UnhandledExceptionEventArgs e)
		{
			if (e.ExceptionObject is Exception exception)
			{
				TryLogUnhandledException(exception, "CurrentDomainUnhandledException");
			}
		}

		private void OnTaskSchedulerUnobservedTaskException(object? sender, UnobservedTaskExceptionEventArgs e)
		{
			TryLogUnhandledException(e.Exception, "UnobservedTaskException");
		}

		private void TryLogUnhandledException(Exception exception, string source)
		{
			try
			{
				var logger = _host?.Services.GetService<ILogger<App>>();
				logger?.LogError(exception, "Unhandled exception from {Source}", source);
			}
			catch
			{
			}
		}
	}
}
