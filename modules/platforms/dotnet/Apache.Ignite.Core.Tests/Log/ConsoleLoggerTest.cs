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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Log;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="ConsoleLogger"/>.
    /// </summary>
    public class ConsoleLoggerTest
    {
        /// <summary>
        /// Tests that default constructor sets log level to Warn.
        /// </summary>
        [Test]
        public void TestDefaultConstructorSetsWarnMinLevel()
        {
            Assert.AreEqual(LogLevel.Warn, new ConsoleLogger().MinLevel);
        }
        
        /// <summary>
        /// Tests that IsEnabled returns false when specified level is less that MinLevel.
        /// </summary>
        [Test]
        [TestCase(LogLevel.Error, LogLevel.Error, true)]
        [TestCase(LogLevel.Error, LogLevel.Warn, false)]
        [TestCase(LogLevel.Error, LogLevel.Info, false)]
        [TestCase(LogLevel.Error, LogLevel.Debug, false)]
        [TestCase(LogLevel.Error, LogLevel.Trace, false)]
        [TestCase(LogLevel.Warn, LogLevel.Error, true)]
        [TestCase(LogLevel.Warn, LogLevel.Warn, true)]
        [TestCase(LogLevel.Warn, LogLevel.Info, false)]
        [TestCase(LogLevel.Warn, LogLevel.Trace, false)]
        public void TestIsEnabled(LogLevel loggerLevel, LogLevel level, bool expectedResult)
        {
            var logger = new ConsoleLogger {MinLevel = loggerLevel};
            Assert.AreEqual(loggerLevel, logger.MinLevel);
            Assert.AreEqual(expectedResult, logger.IsEnabled(level));
        }

        /// <summary>
        /// Tests that logger writes to console.
        /// </summary>
        [Test]
        public void TestLogWritesToConsole()
        {
            var oldWriter = Console.Out;
            var writer = new StringWriter();

            try
            {
                Console.SetOut(writer);
                
                var logger = new ConsoleLogger
                {
                    MinLevel = LogLevel.Debug,
                    DateTimeProvider = new FixedDateTimeProvider()
                }.GetLogger("my-cat");
                logger.Warn("warn!");
                logger.Error(new IgniteException("ex!"), "err!");
                logger.Trace("trace (ignored)");

                var expectedLog = string.Format("[04:05:06] [Warn] [my-cat] warn!{0}[04:05:06] [Error] [my-cat] err! " +
                                                "(exception: Apache.Ignite.Core.Common.IgniteException: ex!){0}",
                    Environment.NewLine);
                Assert.AreEqual(expectedLog, writer.ToString());
            }
            finally
            {
                Console.SetOut(oldWriter);
            }
        }
    }
}