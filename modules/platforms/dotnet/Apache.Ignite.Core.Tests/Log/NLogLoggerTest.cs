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
    using Apache.Ignite.NLog;
    using global::NLog;
    using global::NLog.Config;
    using global::NLog.Layouts;
    using NUnit.Framework;
    using LogLevel = Apache.Ignite.Core.Log.LogLevel;

    /// <summary>
    /// Tests the NLog integration.
    /// </summary>
    public class NLogLoggerTest
    {
        /** */
        private ConcurrentMemoryTarget _logTarget;

        /// <summary>
        /// Test set up.
        /// </summary>
        [SetUp]
        public void SetUp()
        {
            var cfg = new LoggingConfiguration();

            _logTarget = new ConcurrentMemoryTarget
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

            var nLogger = new IgniteNLogLogger(LogManager.GetCurrentClassLogger());

            foreach (var igniteLevel in levels)
            {
                var nlogLevel = IgniteNLogLogger.ConvertLogLevel(igniteLevel);

                Assert.AreEqual(igniteLevel.ToString(), nlogLevel.ToString());


                Assert.IsTrue(nLogger.IsEnabled(igniteLevel));
            }
        }

        /// <summary>
        /// Tests the logger in isolated environment.
        /// </summary>
        [Test]
        public void TestLogging()
        {
            var nLogger = new IgniteNLogLogger();

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

            // No params.
            nLogger.Log(LogLevel.Warn, "msg{0}", null, CultureInfo.InvariantCulture, "category", null, null);

            Assert.AreEqual("category|Warn|msg{0}||", GetLastLog());

            // No formatter.
            nLogger.Log(LogLevel.Error, "msg{0}", null, null, "category", null, null);

            Assert.AreEqual("category|Error|msg{0}||", GetLastLog());

            // No category.
            nLogger.Log(LogLevel.Error, "msg{0}", null, null, null, null, null);

            Assert.AreEqual("|Error|msg{0}||", GetLastLog());

            // No message.
            nLogger.Log(LogLevel.Error, null, null, null, null, null, null);

            Assert.AreEqual("|Error|||", GetLastLog());
        }

        /// <summary>
        /// Tests the logger with Ignite.
        /// </summary>
        [Test]
        public void TestIgniteStartup()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                Logger = new IgniteNLogLogger(LogManager.GetLogger("foo"))
            };

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.IsTrue(_logTarget.Logs.Contains(
                    string.Format("|Debug|Starting Ignite.NET {0}||", typeof(Ignition).Assembly.GetName().Version)));

                Assert.IsTrue(_logTarget.Logs.Any(x => x.Contains(">>> Topology snapshot.")));

                Assert.IsInstanceOf<IgniteNLogLogger>(ignite.Logger);

                ignite.Logger.Info("Log from user code.");

                Assert.IsTrue(_logTarget.Logs.Contains("|Info|Log from user code.||"));
            }

            Assert.IsTrue(_logTarget.Logs.Contains(
                "org.apache.ignite.internal.IgniteKernal|Debug|Grid is stopping.||"));
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
