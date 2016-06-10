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
    using System.Text;
    using Apache.Ignite.Core.Log;
    using NUnit.Framework;

    /// <summary>
    /// Tests the <see cref="ConsoleLogger"/> class.
    /// </summary>
    public class ConsoleLoggerTest
    {
        [Test]
        public void TestLevels()
        {
            Test(l => l.LogDebug("debug"), "debug", new[] {LogLevel.Debug});
            Test(l => l.LogDebug("debug"), null, new[] {LogLevel.Trace});
        }

        [Test]
        public void TestFormatting()
        {
            Test(l => l.LogWarning("testWarn"), "testWarn");

        }

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
                Assert.AreEqual(expected, ExtractMessage(sb));
            else
                Assert.AreEqual(0, sb.Length);
        }

        private static string ExtractMessage(StringBuilder log)
        {
            return ExtractMessage(log.ToString());
        }

        private static string ExtractMessage(string log)
        {
            return log.Substring(11, log.Length - 13);
        }
    }
}