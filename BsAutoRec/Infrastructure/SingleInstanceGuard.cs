using System.Threading;

namespace BsAutoRec.Infrastructure
{
	public sealed class SingleInstanceGuard : IDisposable
	{
		private readonly Mutex _mutex;
		private bool _isOwned;

		public SingleInstanceGuard(string mutexName)
		{
			_mutex = new Mutex(false, mutexName);
		}

		public bool TryAcquire()
		{
			if (_isOwned)
			{
				return true;
			}

			try
			{
				_isOwned = _mutex.WaitOne(TimeSpan.Zero, false);
				return _isOwned;
			}
			catch (AbandonedMutexException)
			{
				_isOwned = true;
				return true;
			}
		}

		public void Dispose()
		{
			if (_isOwned)
			{
				_mutex.ReleaseMutex();
				_isOwned = false;
			}

			_mutex.Dispose();
		}
	}
}
