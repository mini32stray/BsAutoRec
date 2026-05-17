namespace BsAutoRec.Services
{
	/// <summary>
	/// Keeps a short text buffer of recent user-facing messages.
	/// </summary>
	internal sealed class RecentMessageBuffer
	{
		private readonly Queue<string> messages;
		private readonly int capacity;

		/// <summary>
		/// Creates the buffer with a fixed maximum item count.
		/// </summary>
		public RecentMessageBuffer(int capacity)
		{
			this.capacity = Math.Max(1, capacity);
			messages = new Queue<string>();
		}

		/// <summary>
		/// Adds a message and returns the full display text.
		/// </summary>
		public string Add(string message)
		{
			if (string.IsNullOrWhiteSpace(message))
			{
				return Text;
			}

			messages.Enqueue($"{DateTime.Now:HH:mm:ss} {message}");
			while (messages.Count > capacity)
			{
				messages.Dequeue();
			}

			return Text;
		}

		public string Text => string.Join(Environment.NewLine, messages);
	}
}
