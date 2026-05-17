namespace BsAutoRec.Infrastructure
{
	public sealed class AsyncLock : IDisposable
	{
		private readonly SemaphoreSlim _semaphore;

		public AsyncLock()
		{
			_semaphore = new SemaphoreSlim(1, 1);
		}

		public async ValueTask<Lease> LockAsync(CancellationToken cancellationToken = default)
		{
			await _semaphore.WaitAsync(cancellationToken);
			return new Lease(_semaphore);
		}

		public void Dispose()
		{
			_semaphore.Dispose();
		}

		public sealed class Lease : IDisposable
		{
			private readonly SemaphoreSlim _semaphore;
			private bool _disposed;

			internal Lease(SemaphoreSlim semaphore)
			{
				_semaphore = semaphore;
			}

			public void Dispose()
			{
				if (_disposed)
				{
					return;
				}

				_semaphore.Release();
				_disposed = true;
			}
		}
	}
}
