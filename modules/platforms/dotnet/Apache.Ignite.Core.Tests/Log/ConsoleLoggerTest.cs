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
    using System.IO;
    using System.Linq;
    using System.Text;
    using Apache.Ignite.Core.Log;
    using NUnit.Framework;

    /// <summary>
    /// Tests the <see cref="ConsoleLogger"/> class.
    /// </summary>
    public class ConsoleLoggerTest
    {
        /** */
        private static readonly LogLevel[] AllLevels = Enum.GetValues(typeof(LogLevel)).OfType<LogLevel>().ToArray();

        /// <summary>
        /// Tests the logging levels.
        /// </summary>
        [Test]
        public void TestLogLevels()
        {
            foreach (var level in AllLevels)
            {
                Test(l => l.Log(level, "test msg"), "test msg", new[] { level });
                Test(l => l.Log(level, "test msg"), null, new LogLevel[0]);
            }
        }

        /// <summary>
        /// Tests the formatting.
        /// </summary>
        [Test]
        public void TestFormatting()
        {
            Test(l => l.Warn("testWarn"), "testWarn");
            Test(l => l.Warn("Hello, {0} : {1}", "World", 1), "Hello, World : 1");

        }

        /// <summary>
        /// Tests the specified log action.
        /// </summary>
        private static void Test(Action<ConsoleLogger> logAction, string expected, LogLevel[] levels = null)
        {
            var sb = new StringBuilder();
            var writer = new StringWriter(sb);
            Console.SetOut(writer);

            var log = new ConsoleLogger(
                levels ?? new[] {LogLevel.Trace, LogLevel.Info, LogLevel.Debug, LogLevel.Warn, LogLevel.Error});

            logAction(log);

            writer.Flush();

            if (expected != null)
                Assert.AreEqual(expected, ExtractMessage(sb.ToString()));
            else
                Assert.AreEqual(0, sb.Length);
        }

        /// <summary>
        /// Extracts the message text from the log, removing timestamp and line breaks.
        /// </summary>
        private static string ExtractMessage(string log)
        {
            return log.Substring(11, log.Length - 13);
        }
    }
}