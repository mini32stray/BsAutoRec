using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace BsAutoRec.Services
{
	/// <summary>
	/// Builds and parses OBS WebSocket protocol payloads.
	/// </summary>
	public sealed class ObsProtocol
	{
		/// <summary>
		/// Builds the identify payload used after OBS hello.
		/// </summary>
		public byte[] BuildIdentifyPayload(string password, string salt, string challenge)
		{
			string? authentication = null;
			if (!string.IsNullOrEmpty(salt) || !string.IsNullOrEmpty(challenge))
			{
				authentication = BuildAuthenticationString(password, salt, challenge);
			}

			var identifyPayload = new
			{
				op = 1,
				d = new
				{
					rpcVersion = 1,
					authentication,
				},
			};

			return JsonSerializer.SerializeToUtf8Bytes(identifyPayload);
		}

		/// <summary>
		/// Builds a request payload for OBS.
		/// </summary>
		public byte[] BuildRequestPayload(ObsRequest request)
		{
			var payload = new
			{
				op = 6,
				d = new
				{
					requestType = request.RequestType,
					requestId = request.RequestId,
					requestData = request.RequestData,
				},
			};

			return JsonSerializer.SerializeToUtf8Bytes(payload);
		}

		/// <summary>
		/// Parses the OBS hello payload.
		/// </summary>
		public ObsHelloInfo ParseHello(string payload)
		{
			using var document = JsonDocument.Parse(payload);
			var root = document.RootElement;

			if (root.GetProperty("op").GetInt32() != 0)
			{
				throw new InvalidOperationException("OBS handshake failed. Hello was not received.");
			}

			var helloData = root.GetProperty("d");
			if (!helloData.TryGetProperty("authentication", out var authenticationElement))
			{
				return new ObsHelloInfo(string.Empty, string.Empty);
			}

			var challenge = authenticationElement.GetProperty("challenge").GetString() ?? string.Empty;
			var salt = authenticationElement.GetProperty("salt").GetString() ?? string.Empty;
			return new ObsHelloInfo(salt, challenge);
		}

		/// <summary>
		/// Checks whether a payload is the OBS identified message.
		/// </summary>
		public bool IsIdentifiedPayload(string payload)
		{
			using var document = JsonDocument.Parse(payload);
			var root = document.RootElement;
			return root.GetProperty("op").GetInt32() == 2;
		}

		/// <summary>
		/// Parses a generic OBS payload envelope.
		/// </summary>
		public ObsPayload ParsePayload(string payload)
		{
			using var document = JsonDocument.Parse(payload);
			var root = document.RootElement;
			var opCode = root.GetProperty("op").GetInt32();
			var data = root.GetProperty("d").Clone();
			return new ObsPayload(opCode, data);
		}

		/// <summary>
		/// Parses an OBS request response payload.
		/// </summary>
		public ObsRequestResponse ParseRequestResponse(JsonElement data)
		{
			var requestId = data.GetProperty("requestId").GetString();
			if (string.IsNullOrWhiteSpace(requestId))
			{
				throw new InvalidOperationException("OBS request response did not include requestId.");
			}

			var requestStatus = data.GetProperty("requestStatus");
			var result = requestStatus.GetProperty("result").GetBoolean();
			var comment = requestStatus.TryGetProperty("comment", out var commentElement) ? commentElement.GetString() : null;
			var responseData = data.TryGetProperty("responseData", out var responseDataElement) ? responseDataElement.Clone() : (JsonElement?)null;
			return new ObsRequestResponse(requestId, result, comment, responseData);
		}

		/// <summary>
		/// Parses a RecordStateChanged event when the payload is that event.
		/// </summary>
		public ObsClient.ObsRecordState? TryParseRecordStateChanged(JsonElement data)
		{
			var eventType = data.GetProperty("eventType").GetString();
			if (!string.Equals(eventType, "RecordStateChanged", StringComparison.Ordinal))
			{
				return null;
			}

			if (!data.TryGetProperty("eventData", out var eventData))
			{
				return null;
			}

			var isRecording = TryGetBooleanProperty(eventData, "outputActive") ?? false;
			var outputPath = TryGetStringProperty(eventData, "outputPath");
			var summary = isRecording ? "録画中" : "録画停止";
			return new ObsClient.ObsRecordState(isRecording, outputPath, summary);
		}

		/// <summary>
		/// Parses the response from GetRecordStatus.
		/// </summary>
		public ObsClient.ObsRecordState ParseRecordStatus(JsonElement? response)
		{
			var outputActive = TryGetBooleanProperty(response, "outputActive") ?? false;
			var outputPath = TryGetStringProperty(response, "outputPath");
			var summary = outputActive ? "録画中" : "待機中";
			return new ObsClient.ObsRecordState(outputActive, outputPath, summary);
		}

		/// <summary>
		/// Reads an OBS output path from a response payload.
		/// </summary>
		public string? TryGetOutputPath(JsonElement? response)
		{
			return TryGetStringProperty(response, "outputPath");
		}

		/// <summary>
		/// Builds the OBS authentication string.
		/// </summary>
		private static string BuildAuthenticationString(string password, string salt, string challenge)
		{
			var secretString = Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(password + salt)));
			return Convert.ToBase64String(SHA256.HashData(Encoding.UTF8.GetBytes(secretString + challenge)));
		}

		/// <summary>
		/// Reads an optional string property from JSON.
		/// </summary>
		private static string? TryGetStringProperty(JsonElement? element, string propertyName)
		{
			if (!element.HasValue)
			{
				return null;
			}

			return element.Value.TryGetProperty(propertyName, out var property) ? property.GetString() : null;
		}

		/// <summary>
		/// Reads an optional boolean property from JSON.
		/// </summary>
		private static bool? TryGetBooleanProperty(JsonElement? element, string propertyName)
		{
			if (!element.HasValue)
			{
				return null;
			}

			return element.Value.TryGetProperty(propertyName, out var property) ? property.GetBoolean() : null;
		}
	}

	/// <summary>
	/// Contains OBS hello authentication values.
	/// </summary>
	public sealed record ObsHelloInfo(string Salt, string Challenge);

	/// <summary>
	/// Contains an OBS payload envelope.
	/// </summary>
	public sealed record ObsPayload(int OpCode, JsonElement Data);

	/// <summary>
	/// Contains an OBS request response.
	/// </summary>
	public sealed record ObsRequestResponse(
		string RequestId,
		bool IsSuccess,
		string? ErrorMessage,
		JsonElement? ResponseData);
}
