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
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Log;
    using NUnit.Framework;

    /// <summary>
    /// Tests that user-defined logger receives Ignite events.
    /// </summary>
    public class CustomLoggerTest
    {
        /** */
        private static readonly LogLevel[] AllLevels = Enum.GetValues(typeof (LogLevel)).OfType<LogLevel>().ToArray();

        // TODO: Online tests with error propagation, categories, etc.
        // TODO: QueryEntity warnings test

        [SetUp]
        public void TestSetUp()
        {
            TestLogger.Entries.Clear();
        }

        [Test]
        public void TestStartupOutput()
        {
            using (Ignition.Start(GetConfigWithLogger()))
            {
                // Check initial message
                Assert.IsTrue(TestLogger.Entries[0].Message.StartsWith("Starting Ignite.NET"));

                // Check topology message
                Assert.IsTrue(
                    TestUtils.WaitForCondition(() =>
                    {
                        lock (TestLogger.Entries)
                        {
                            return TestLogger.Entries.Any(x => x.Message.Contains("Topology snapshot"));
                        }
                    }, 9000), "No topology snapshot");
            }

            // Test that all levels are present
            foreach (var level in AllLevels.Where(x => x != LogLevel.Error))
                Assert.IsTrue(TestLogger.Entries.Any(x => x.Level == level), "No messages with level " + level);
        }


        [Test]
        public void TestStartupJavaError()
        {
            // TODO: Startup failure with damned "GridManagerAdapter"
            // Invalid config?

            // Invalid home
            Ignition.Start(new IgniteConfiguration(GetConfigWithLogger())
            {
                IgniteHome = Path.GetTempPath()
            });
        }

        [Test]
        public void TestStartupDotNetError()
        {
            // TODO: Startup failure test with .NET error (exception in cache store? lifecycle bean?)
        }

        private static IgniteConfiguration GetConfigWithLogger()
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
            public static readonly List<LogEntry> Entries = new List<LogEntry>(5000);

            private readonly ILogger _console = new ConsoleLogger(AllLevels);

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
