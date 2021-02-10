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

namespace Apache.Ignite.Core.Tests.Client.Binary
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Net;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests automatic binary configuration retrieval.
    /// </summary>
    public class BinaryConfigurationRetrievalTest
    {
        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that <see cref="BinaryConfiguration.CompactFooter"/> sets to false on the client when it is false
        /// on the server.
        /// </summary>
        [Test]
        public void TestCompactFooterDisabledOnServerAutomaticallyDisablesOnClient()
        {
            var serverCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    CompactFooter = false
                }
            };

            Ignition.Start(serverCfg);

            var logger = GetLogger();

            using (var client = Ignition.StartClient(GetClientConfiguration(logger)))
            {
                var resCfg = client.GetConfiguration();

                Assert.IsNotNull(resCfg.BinaryConfiguration);
                Assert.IsFalse(resCfg.BinaryConfiguration.CompactFooter);

                AssertCompactFooter(client, false);

                Assert.AreEqual(1, logger.Entries.Count(e =>
                    e.Message == "Server binary configuration retrieved: " +
                    "BinaryConfigurationClientInternal [CompactFooter=False, NameMapperMode=BasicFull]"
                    && e.Level == LogLevel.Debug));

                Assert.AreEqual(1, logger.Entries.Count(e =>
                    e.Message == "BinaryConfiguration.CompactFooter set to false on client " +
                    "according to server configuration."));

                Assert.IsEmpty(logger.Entries.Where(e => e.Level > LogLevel.Info));
            }
        }

        /// <summary>
        /// Tests that <see cref="BinaryConfiguration.CompactFooter"/> sets to false on the client when it is false
        /// on the server.
        /// </summary>
        [Test]
        public void TestCompactFooterEnabledOnServerDisabledOnClientProducesWarning()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());

            var logger = GetLogger();

            var clientConfiguration = new IgniteClientConfiguration(GetClientConfiguration(logger))
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    CompactFooter = false
                }
            };

            using (var client = Ignition.StartClient(clientConfiguration))
            {
                var resCfg = client.GetConfiguration();

                Assert.IsNotNull(resCfg.BinaryConfiguration);
                Assert.IsFalse(resCfg.BinaryConfiguration.CompactFooter);

                AssertCompactFooter(client, false);

                Assert.AreEqual(1, logger.Entries.Count(e =>
                    e.Message == "Server binary configuration retrieved: " +
                    "BinaryConfigurationClientInternal [CompactFooter=True, NameMapperMode=BasicFull]"
                    && e.Level == LogLevel.Debug));

                Assert.AreEqual(1, logger.Entries.Count(e =>
                    e.Message == "BinaryConfiguration.CompactFooter is true on the server, but false on the client." +
                    "Consider enabling this setting to reduce cache entry size."
                    && e.Level == LogLevel.Info));

                Assert.IsEmpty(logger.Entries.Where(e => e.Level > LogLevel.Info));
            }
        }

        /// <summary>
        /// Tests that with default configuration on server and client there is no warnings and no changes at runtime.
        /// </summary>
        [Test]
        public void TestDefaultConfigurationDoesNotChangeClientSettingsOrLogWarnings()
        {
            var logger = GetLogger();

            Ignition.Start(TestUtils.GetTestConfiguration());

            using (var client = Ignition.StartClient(GetClientConfiguration(logger)))
            {
                var resCfg = client.GetConfiguration();
                Assert.IsNull(resCfg.BinaryConfiguration);

                AssertCompactFooter(client, true);

                Assert.AreEqual(1, logger.Entries.Count(e => e.Message == "Server binary configuration " +
                    "retrieved: BinaryConfigurationClientInternal [CompactFooter=True, NameMapperMode=BasicFull]"));

                Assert.IsEmpty(logger.Entries.Where(e => e.Level > LogLevel.Info));
            }
        }

        /// <summary>
        /// Tests that with explicit default configuration on client there is no warnings and no changes at runtime.
        /// </summary>
        [Test]
        public void TestExplicitDefaultConfigurationDoesNotChangeClientSettingsOrLogWarnings()
        {
            var logger = GetLogger();
            var clientCfg = new IgniteClientConfiguration(GetClientConfiguration(logger))
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    CompactFooter = true,
                    NameMapper = new BinaryBasicNameMapper
                    {
                        IsSimpleName = false
                    }
                }
            };

            Ignition.Start(TestUtils.GetTestConfiguration());

            using (var client = Ignition.StartClient(clientCfg))
            {
                var resCfg = client.GetConfiguration().BinaryConfiguration;
                Assert.IsNotNull(resCfg);
                Assert.IsTrue(resCfg.CompactFooter);
                Assert.IsFalse(((BinaryBasicNameMapper)resCfg.NameMapper).IsSimpleName);
                Assert.IsEmpty(logger.Entries.Where(e => e.Level > LogLevel.Info));
            }
        }

        /// <summary>
        /// Tests that simple/full name mapping mismatch produces a warning.
        /// </summary>
        [Test]
        public void TestBasicNameMapperSettingsMismatchProducesLogWarning()
        {
            var logger = GetLogger();
            var clientCfg = new IgniteClientConfiguration(GetClientConfiguration(logger))
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new BinaryBasicNameMapper
                    {
                        IsSimpleName = true
                    }
                }
            };

            Ignition.Start(TestUtils.GetTestConfiguration());

            using (var client = Ignition.StartClient(clientCfg))
            {
                var resCfg = client.GetConfiguration().BinaryConfiguration;
                Assert.IsNotNull(resCfg);
                Assert.IsTrue(((BinaryBasicNameMapper)resCfg.NameMapper).IsSimpleName);
                Assert.AreEqual(1, logger.Entries.Count(e =>
                    e.Message == "Binary name mapper mismatch: local=BasicSimple, server=BasicFull" &&
                    e.Level == LogLevel.Warn));
            }
        }

        /// <summary>
        /// Tests that custom mapper on server and default mapper on client results in a warning.
        /// </summary>
        [Test]
        public void TestCustomNameMapperOnServerProducesLogWarning()
        {
            var logger = GetLogger();

            var serverCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = Path.Combine("Config", "binary-custom-name-mapper.xml")
            };

            Ignition.Start(serverCfg);

            using (var client = Ignition.StartClient(GetClientConfiguration(logger)))
            {
                Assert.IsNull(client.GetConfiguration().BinaryConfiguration);
                Assert.AreEqual(1, logger.Entries.Count(e =>
                    e.Message == "Binary name mapper mismatch: local=BasicFull, server=Custom" &&
                    e.Level == LogLevel.Warn));
            }
        }

        /// <summary>
        /// Tests that custom mapper that extends basic name mapper on server and default mapper on client
        /// results in a warning.
        /// </summary>
        [Test]
        public void TestCustomNameMapperExtendingBasicMapperOnServerProducesLogWarning()
        {
            var logger = GetLogger();

            var serverCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = Path.Combine("Config", "binary-custom-name-mapper2.xml")
            };

            Ignition.Start(serverCfg);

            using (var client = Ignition.StartClient(GetClientConfiguration(logger)))
            {
                Assert.IsNull(client.GetConfiguration().BinaryConfiguration);
                Assert.AreEqual(1, logger.Entries.Count(e =>
                    e.Message == "Binary name mapper mismatch: local=BasicFull, server=Custom" &&
                    e.Level == LogLevel.Warn));
            }
        }

        [Test]
        public void TestCustomNameMapperOnServerAndClientProducesNoLogWarning()
        {
            var logger = GetLogger();
            var clientCfg = new IgniteClientConfiguration(GetClientConfiguration(logger))
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = new TestNameMapper()
                }
            };

            var serverCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = Path.Combine("Config", "binary-custom-name-mapper.xml")
            };

            Ignition.Start(serverCfg);

            using (var client = Ignition.StartClient(clientCfg))
            {
                var resCfg = client.GetConfiguration().BinaryConfiguration;
                Assert.IsNotNull(resCfg);
                Assert.IsInstanceOf<TestNameMapper>(resCfg.NameMapper);
                Assert.AreEqual(0, logger.Entries.Count(e => e.Level > LogLevel.Info));
            }
        }

        /// <summary>
        /// Checks the actual compact footer behavior.
        /// </summary>
        private static void AssertCompactFooter(IIgniteClient client, bool expected)
        {
            var binObj = (BinaryObject) client.GetBinary().GetBuilder("foo").Build();
            Assert.AreEqual(expected, binObj.Header.Flags.HasFlag(BinaryObjectHeader.Flag.CompactFooter));
        }

        /// <summary>
        /// Gets the logger for tests.
        /// </summary>
        private static ListLogger GetLogger()
        {
            return new ListLogger(new ConsoleLogger
            {
                MinLevel = LogLevel.Trace
            })
            {
                EnabledLevels = Enum.GetValues(typeof(LogLevel)).Cast<LogLevel>().ToArray()
            };
        }

        /// <summary>
        /// Gets the client configuration.
        /// </summary>
        private static IgniteClientConfiguration GetClientConfiguration(ListLogger logger)
        {
            return new IgniteClientConfiguration(IPAddress.Loopback.ToString())
            {
                Logger = logger
            };
        }

        /** */
        private class TestNameMapper : IBinaryNameMapper
        {
            /** */
            public string GetTypeName(string name)
            {
                return name + "_";
            }

            /** */
            public string GetFieldName(string name)
            {
                return name;
            }
        }
    }
}
