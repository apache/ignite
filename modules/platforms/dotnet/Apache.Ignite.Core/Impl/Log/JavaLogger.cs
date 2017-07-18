/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Log
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Logger that delegates to Java.
    /// </summary>
    internal class JavaLogger : ILogger
    {
        /** */
        private Ignite _ignite;

        /** */
        private readonly List<LogLevel> _enabledLevels = new List<LogLevel>(5);

        /** */
        private readonly List<Tuple<LogLevel, string, string, string>> _pendingLogs 
            = new List<Tuple<LogLevel, string, string, string>>();

        /** */
        private readonly object _syncRoot = new object();

        /// <summary>
        /// Sets the processor.
        /// </summary>
        /// <param name="ignite">The proc.</param>
        public void SetIgnite(Ignite ignite)
        {
            Debug.Assert(ignite != null);

            lock (_syncRoot)
            {
                _ignite = ignite;

                // Preload enabled levels.
                _enabledLevels.AddRange(
                    new[] {LogLevel.Trace, LogLevel.Debug, LogLevel.Info, LogLevel.Warn, LogLevel.Error}
                        .Where(x => ignite.LoggerIsLevelEnabled(x)));

                foreach (var log in _pendingLogs)
                {
                    Log(log.Item1, log.Item2, log.Item3, log.Item4);
                }
            }
        }

        /** <inheritdoc /> */
        public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
            string nativeErrorInfo, Exception ex)
        {
            // Java error info should not go back to Java.
            // Either we log in .NET, and Java sends us logs, or we log in Java, and .NET sends logs, not both.
            Debug.Assert(nativeErrorInfo == null);

            lock (_syncRoot)
            {
                if (!IsEnabled(level))
                    return;

                var msg = args == null ? message : string.Format(formatProvider, message, args);
                var err = ex != null ? ex.ToString() : null;

                if (_ignite != null)
                    Log(level, msg, category, err);
                else
                    _pendingLogs.Add(Tuple.Create(level, msg, category, err));
            }
        }

        /** <inheritdoc /> */
        public bool IsEnabled(LogLevel level)
        {
            lock (_syncRoot)
            {
                return _ignite == null || _enabledLevels.Contains(level);
            }
        }

        /// <summary>
        /// Logs the message.
        /// </summary>
        private void Log(LogLevel level, string msg, string category, string err)
        {
            if (IsEnabled(level))
            {
                _ignite.LoggerLog(level, msg, category, err);
            }
        }
    }
}
