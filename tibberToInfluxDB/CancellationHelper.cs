using System;
using System.Threading;

namespace tibberToInfluxDB
{
    internal class CancellationHelper
    {
        private readonly ManualResetEvent internalEvent;

        public CancellationHelper()
        {
            internalEvent = new ManualResetEvent(false);
        }

        public bool IsCanceled => internalEvent.WaitOne(0);

        public static implicit operator bool(CancellationHelper ch) => !ch.IsCanceled;

        public bool Cancel() => internalEvent.Set();

        public bool Wait(TimeSpan delay) => internalEvent.WaitOne(delay);
    }
}