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
    using Apache.Ignite.NLog;
    using global::NLog;
    using global::NLog.Config;
    using global::NLog.Layouts;
    using global::NLog.Targets;
    using NUnit.Framework;
    using LogLevel = Apache.Ignite.Core.Log.LogLevel;

    /// <summary>
    /// Tests the NLog integration.
    /// </summary>
    public class NLogLoggerTest
    {
        /** */
        private MemoryTarget _logTarget;

        /// <summary>
        /// Test set up.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            var cfg = new LoggingConfiguration();

            _logTarget = new MemoryTarget("mem")
            {
                Layout = new SimpleLayout("${Logger}|${Level}|${Message}|${exception}|${all-event-properties}")
            };

            cfg.AddTarget(_logTarget);

            cfg.AddRule(global::NLog.LogLevel.Trace, global::NLog.LogLevel.Error, _logTarget);

            LogManager.Configuration = cfg;
        }

        /// <summary>
        /// Tests the log level conversion.
        /// </summary>
        [Test]
        public void TestLogLevelConversion()
        {
            var levels = new[] { LogLevel.Trace, LogLevel.Info, LogLevel.Debug, LogLevel.Warn, LogLevel.Error };

            foreach (var igniteLevel in levels)
            {
                var nlogLevel = IgniteNLogLogger.ConvertLogLevel(igniteLevel);

                Assert.AreEqual(igniteLevel.ToString(), nlogLevel.ToString());
            }
        }

        /// <summary>
        /// Tests the logger in isolated environment.
        /// </summary>
        [Test]
        public void TestLogging()
        {
            var nLogger = new IgniteNLogLogger(LogManager.GetCurrentClassLogger());

            // All parameters.
            nLogger.Log(LogLevel.Trace, "msg{0}", new object[] {1}, CultureInfo.InvariantCulture, "category", 
                "java-err", new Exception("myException"));

            Assert.AreEqual("category|Trace|msg1|myException|nativeErrorInfo=java-err", GetLastLog());

            // No Java error.
            nLogger.Log(LogLevel.Info, "msg{0}", new object[] { 1 }, CultureInfo.InvariantCulture, "category",
                null, new Exception("myException"));

            Assert.AreEqual("category|Info|msg1|myException|", GetLastLog());

            // No exception.
            nLogger.Log(LogLevel.Debug, "msg{0}", new object[] { 1 }, CultureInfo.InvariantCulture, "category",
                null, null);

            Assert.AreEqual("category|Debug|msg1||", GetLastLog());
        }

        /// <summary>
        /// Tests the logger with Ignite.
        /// </summary>
        [Test]
        public void TestIgniteStartup()
        {
            // TODO
        }

        /// <summary>
        /// Gets the last log.
        /// </summary>
        private string GetLastLog()
        {
            return _logTarget.Logs.Last();
        }

    }
}
