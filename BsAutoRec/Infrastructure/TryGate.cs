namespace BsAutoRec.Infrastructure
{
	public sealed class TryGate
	{
		private int _entered;

		private bool TryEnter()
		{
			return Interlocked.CompareExchange(ref _entered, 1, 0) == 0;
		}

		private void Exit()
		{
			Interlocked.Exchange(ref _entered, 0);
		}

		public async Task<bool> RunIfFreeAsync(Func<Task> action)
		{
			if (!TryEnter())
			{
				return false;
			}

			try
			{
				await action();
				return true;
			}
			finally
			{
				Exit();
			}
		}

		public bool RunIfFree(Action action)
		{
			if (!TryEnter())
			{
				return false;
			}

			try
			{
				action();
				return true;
			}
			finally
			{
				Exit();
			}
		}
	}
}
