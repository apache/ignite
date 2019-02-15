/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Tests.NuGet
{
    using System.Linq;
    using Apache.Ignite.NLog;
    using global::NLog;
    using global::NLog.Config;
    using global::NLog.Layouts;
    using global::NLog.Targets;
    using NUnit.Framework;

    /// <summary>
    /// NLog test.
    /// </summary>
    public class NLogTest
    {
        /// <summary>
        /// The log target.
        /// </summary>
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

            cfg.AddRule(LogLevel.Trace, LogLevel.Error, _logTarget);

            LogManager.Configuration = cfg;
        }

        /// <summary>
        /// Tests the logger with Ignite.
        /// </summary>
        [Test]
        public void TestIgniteStartup()
        {
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = TestUtil.GetLocalDiscoverySpi(),
                Logger = new IgniteNLogLogger(LogManager.GetCurrentClassLogger())
            };

            using (Ignition.Start(cfg))
            {
                Assert.IsTrue(_logTarget.Logs.Contains(
                    string.Format("|Debug|Starting Ignite.NET {0}||", typeof(Ignition).Assembly.GetName().Version)));

                Assert.IsTrue(_logTarget.Logs.Any(x => x.Contains(">>> Topology snapshot.")));
            }

            Assert.IsTrue(_logTarget.Logs.Contains(
                "org.apache.ignite.internal.IgniteKernal|Debug|Grid is stopping.||"));
        }
    }
}
