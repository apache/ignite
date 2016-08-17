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
    using Apache.Ignite.NLog;
    using global::NLog;
    using global::NLog.Config;
    using NUnit.Framework;
    using LogLevel = Apache.Ignite.Core.Log.LogLevel;

    /// <summary>
    /// Tests the NLog integration.
    /// </summary>
    public class NLogLoggerTest
    {
        /// <summary>
        /// Test fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            LogManager.Configuration = new LoggingConfiguration
            {
                
            };
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
            // TODO: test isolated, without Ignite node
            var nLogger = LogManager.GetCurrentClassLogger();
        }

        /// <summary>
        /// Tests the logger with Ignite.
        /// </summary>
        [Test]
        public void TestIgniteStartup()
        {
            // TODO
        }
    }
}
