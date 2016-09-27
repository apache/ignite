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
    using System.Globalization;
    using System.Linq;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.log4net;
    using global::log4net;
    using global::log4net.Appender;
    using global::log4net.Core;
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
                var log4NetLevel = IgniteLog4NetLogger.ConvertLogLevel(igniteLevel);

                Assert.AreEqual(igniteLevel.ToString().ToUpperInvariant(), log4NetLevel.ToString());
            }
        }

        /// <summary>
        /// Tests the logger in isolated environment.
        /// </summary>
        [Test]
        public void TestLogging()
        {
            var memoryLog = CreateMemoryLogger();
            var logger = new IgniteLog4NetLogger();

            Func<LoggingEvent> getLastLog = () => memoryLog.PopAllEvents().Single();

            // All parameters.
            logger.Log(LogLevel.Trace, "msg{0}", new object[] { 1 }, CultureInfo.InvariantCulture, "category",
                "java-err", new Exception("myException"));

            Assert.AreEqual("category|Trace|msg1|myException|nativeErrorInfo=java-err", getLastLog());

            // No Java error.
            logger.Log(LogLevel.Info, "msg{0}", new object[] { 1 }, CultureInfo.InvariantCulture, "category",
                null, new Exception("myException"));

            Assert.AreEqual("category|Info|msg1|myException|", getLastLog());

            // No exception.
            logger.Log(LogLevel.Debug, "msg{0}", new object[] { 1 }, CultureInfo.InvariantCulture, "category",
                null, null);

            Assert.AreEqual("category|Debug|msg1||", getLastLog());

            // No params.
            logger.Log(LogLevel.Warn, "msg{0}", null, CultureInfo.InvariantCulture, "category", null, null);

            Assert.AreEqual("category|Warn|msg{0}||", getLastLog());

            // No formatter.
            logger.Log(LogLevel.Error, "msg{0}", null, null, "category", null, null);

            Assert.AreEqual("category|Error|msg{0}||", getLastLog());

            // No category.
            logger.Log(LogLevel.Error, "msg{0}", null, null, null, null, null);

            Assert.AreEqual("|Error|msg{0}||", getLastLog());

            // No message.
            logger.Log(LogLevel.Error, null, null, null, null, null, null);

            Assert.AreEqual("|Error|||", getLastLog());
        }


        /// <summary>
        /// Creates the memory logger.
        /// </summary>
        private static MemoryAppender CreateMemoryLogger()
        {
            var hierarchy = (Hierarchy) LogManager.GetRepository();

            var memory = new MemoryAppender();
            memory.ActivateOptions();
            hierarchy.Root.AddAppender(memory);

            hierarchy.Root.Level = Level.All;
            hierarchy.Configured = true;

            return memory;
        }
    }
}
