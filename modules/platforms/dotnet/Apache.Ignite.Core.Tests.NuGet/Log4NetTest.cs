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

namespace Apache.Ignite.Core.Tests.NuGet
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Log4Net;
    using global::log4net;
    using global::log4net.Appender;
    using global::log4net.Core;
    using global::log4net.Repository.Hierarchy;
    using NUnit.Framework;

    /// <summary>
    /// log4net tests.
    /// </summary>
    public class Log4NetTest
    {
        /// <summary>
        /// Tests the logger with Ignite.
        /// </summary>
        [Test]
        public void TestIgniteStartup()
        {
            var memoryLog = CreateMemoryLogger();
            var logger = new IgniteLog4NetLogger();

            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = TestUtil.GetLocalDiscoverySpi(),
                Logger = logger
            };

            Func<IEnumerable<string>> getLogs = () => memoryLog.GetEvents().Select(x => x.MessageObject.ToString());

            using (var ignite = Ignition.Start(cfg))
            {
                Assert.IsTrue(getLogs().Contains(
                    string.Format("Starting Ignite.NET {0}", typeof(Ignition).Assembly.GetName().Version)));

                Assert.IsTrue(getLogs().Any(x => x.Contains(">>> Topology snapshot.")));

                Assert.IsInstanceOf<IgniteLog4NetLogger>(ignite.Logger);

                ignite.Logger.Info("Log from user code.");

                Assert.IsTrue(getLogs().Contains("Log from user code."));
            }

            Assert.IsTrue(getLogs().Contains("Grid is stopping."));
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
