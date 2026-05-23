using BsAutoRec.Models;
using System.Globalization;
using System.Text.RegularExpressions;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Renders user file-name templates from a play session.
	/// </summary>
	public sealed class FileNameTemplateService
	{
		private static readonly Regex TemplateRegex = new(@"\{(?<name>[A-Za-z0-9]+)(:(?<format>[^}]+))?\}", RegexOptions.Compiled);

		/// <summary>
		/// Converts a template into a safe file name.
		/// </summary>
		public string Render(string template, PlaySession session, AppSettings settings)
		{
			var effectiveTemplate = string.IsNullOrWhiteSpace(template) ? settings.RenameTemplate : template;

			var rendered = TemplateRegex.Replace(effectiveTemplate, match =>
			{
				var name = match.Groups["name"].Value;
				var format = match.Groups["format"].Success ? match.Groups["format"].Value : null;
				return ResolveToken(name, format, session, settings);
			});

			return SanitizeFileName(rendered);
		}

		/// <summary>
		/// Resolves one template token.
		/// </summary>
		private static string ResolveToken(string name, string? format, PlaySession session, AppSettings settings)
		{
			var snapshot = session.LatestSnapshot;

			return name switch
			{
				"CurrentTime" => (session.EndedAt ?? DateTimeOffset.Now).ToString(format ?? "yyyyMMdd-HHmmss", CultureInfo.InvariantCulture),
				"SongName" => snapshot.SongName,
				"SongSubName" => snapshot.SongSubName,
				"SongAuthorName" => snapshot.SongAuthorName,
				"Characteristic" => snapshot.Characteristic,
				"DifficultyName" => snapshot.DifficultyName,
				"DifficultyEnum" => snapshot.DifficultyEnum,
				"LevelAuthorName" => snapshot.LevelAuthorName,
				"BeatsPerMinute" => snapshot.BeatsPerMinute?.ToString(format ?? "0.##", CultureInfo.InvariantCulture) ?? string.Empty,
				"SongHash" => snapshot.SongHash,
				"LevelEndType" => session.LevelEndType.ToString(),
				"ScorePercent" => snapshot.ScorePercent?.ToString(format ?? "0.00", CultureInfo.InvariantCulture) ?? string.Empty,
				_ => string.Empty,
			};
		}

		/// <summary>
		/// Removes characters that cannot be used in a Windows file name.
		/// </summary>
		private static string SanitizeFileName(string candidate)
		{
			var invalidCharacters = Path.GetInvalidFileNameChars();
			var sanitized = new string(candidate.Select(character => invalidCharacters.Contains(character) ? '_' : character).ToArray());
			sanitized = Regex.Replace(sanitized, @"[_\s]{2,}", "_");
			sanitized = sanitized.Trim().TrimEnd('.');
			return string.IsNullOrWhiteSpace(sanitized) ? "recording" : sanitized;
		}
	}
}
