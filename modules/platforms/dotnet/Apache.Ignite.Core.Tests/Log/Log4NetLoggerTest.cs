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
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.log4net;
    using global::log4net;
    using global::log4net.Appender;
    using global::log4net.Core;
    using global::log4net.Layout;
    using global::log4net.Repository.Hierarchy;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IgniteLog4NetLogger"/>.
    /// </summary>
    public class Log4NetLoggerTest
    {
        /// <summary>
        /// Tests the log level conversion.
        /// </summary>
        [Test]
        public void TestLogLevelConversion()
        {
            var levels = new[] { LogLevel.Trace, LogLevel.Info, LogLevel.Debug, LogLevel.Warn, LogLevel.Error };

            foreach (var igniteLevel in levels)
            {
                var log4netLevel = IgniteLog4NetLogger.ConvertLogLevel(igniteLevel);

                Assert.AreEqual(igniteLevel.ToString(), log4netLevel.ToString());
            }
        }

        [Test]
        public void Test()
        {
            var memoryLog = CreateMemoryLogger();
            var logger = new IgniteLog4NetLogger();


        }

        private static MemoryAppender CreateMemoryLogger()
        {
            var hierarchy = (Hierarchy)LogManager.GetRepository();

            var patternLayout = new PatternLayout
            {
                ConversionPattern = "%date [%thread] %-5level %logger - %message%newline"
            };
            patternLayout.ActivateOptions();

            var memory = new MemoryAppender();
            memory.ActivateOptions();
            hierarchy.Root.AddAppender(memory);

            hierarchy.Root.Level = Level.All;
            hierarchy.Configured = true;

            return memory;
        }
    }
}
