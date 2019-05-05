namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Log;

    public class ListLogger : ILogger
    {
        /** */
        private readonly List<string> _messages = new List<string>();

        /** */
        private readonly object _lock = new object();

        public List<string> Messages
        {
            get
            {
                lock (_lock)
                {
                    return _messages.ToList();
                }
            }
        }

        public void Clear()
        {
            lock (_lock)
            {
                _messages.Clear();
            }
        }

        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
            string nativeErrorInfo, Exception ex)
        {
            lock (_lock)
            {
                _messages.Add(message);
            }
        }

        public bool IsEnabled(LogLevel level)
        {
            return level == LogLevel.Debug;
        }
    }
}
