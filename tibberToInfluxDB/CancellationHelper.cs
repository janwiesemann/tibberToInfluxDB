using System;
using System.Threading;

namespace tibberToInfluxDB
{
    internal class CancellationHelper
    {
        private readonly ManualResetEvent internalEvent;
        private readonly CancellationTokenSource cancellationTokenSource;

        public CancellationHelper()
        {
            internalEvent = new ManualResetEvent(false);
            cancellationTokenSource = new CancellationTokenSource();
        }

        public bool IsCanceled => internalEvent.WaitOne(0);

        public static implicit operator bool(CancellationHelper ch) => !ch.IsCanceled;

        public CancellationToken CancellationToken => cancellationTokenSource.Token;

        public bool Cancel()
        {
            cancellationTokenSource.Cancel();
            return internalEvent.Set();
        }

        public bool Wait(TimeSpan delay) => internalEvent.WaitOne(delay);
    }
}