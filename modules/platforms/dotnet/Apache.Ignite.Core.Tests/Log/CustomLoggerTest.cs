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

namespace Apache.Ignite.Core.Tests.Log
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Log;
    using NUnit.Framework;

    /// <summary>
    /// Tests that user-defined logger receives Ignite events.
    /// </summary>
    public class CustomLoggerTest
    {
        private static LogLevel[] _allLevels = Enum.GetValues(typeof (LogLevel)).OfType<LogLevel>().ToArray();

        // TODO: Online tests with error propagation, categories, etc.
        // TODO: QueryEntity warnings test
        // TODO: gcServer warning test?
        // TODO: Startup failure test with .NET error (exception in cache store? lifecycle bean?)

        [Test]
        public void TestStartupOutput()
        {
            Ignition.Start(GetConfigWithLogger());
            Ignition.StopAll(true);

            // Test that all levels are present
            foreach (var level in _allLevels.Where(x => x != LogLevel.Error))
                Assert.IsTrue(TestLogger.Entries.Any(x => x.Level == level), "No messages with level " + level);
        }


        [Test]
        public void TestStartupErrors()
        {
            // TODO: Startup failure with damned "GridManagerAdapter"
            // Invalid config?

        }

        private IgniteConfiguration GetConfigWithLogger()
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration()) {Logger = new TestLogger()};
        }

        private class LogEntry
        {
            public LogLevel Level;
            public string Message;
            public object[] Args;
            public IFormatProvider FormatProvider;
            public string Category;
            public string NativeErrorInfo;
            public Exception Exception;
        }

        private class TestLogger : ILogger
        {
            public static readonly List<LogEntry> Entries = new List<LogEntry>();

            private readonly ILogger _console = new ConsoleLogger(_allLevels);

            public void Log(LogLevel level, string message, object[] args, IFormatProvider formatProvider, string category,
                string nativeErrorInfo, Exception ex)
            {
                if (!IsEnabled(level))
                    return;

                lock (Entries)
                {
                    Entries.Add(new LogEntry
                    {
                        Level = level,
                        Message = message,
                        Args = args,
                        FormatProvider = formatProvider,
                        Category = category,
                        NativeErrorInfo = nativeErrorInfo,
                        Exception = ex
                    });
                }

                if (level > LogLevel.Debug)
                    _console.Log(level, message, args, formatProvider, category, nativeErrorInfo, ex);
            }

            public bool IsEnabled(LogLevel level)
            {
                return true;
            }
        }
    }
}
