namespace BsAutoRec.Services
{
	internal static class LogValueSanitizer
	{
		private const string MaskedUserInfo = "[userinfo-masked]";

		public static string MaskUriUserInfo(string uriText)
		{
			var uri = new Uri(uriText);
			if (string.IsNullOrEmpty(uri.UserInfo))
			{
				return uriText;
			}

			var hostAndPort = uri.GetComponents(UriComponents.HostAndPort, UriFormat.UriEscaped);
			return $"{uri.Scheme}://{MaskedUserInfo}@{hostAndPort}{uri.PathAndQuery}{uri.Fragment}";
		}
	}
}
