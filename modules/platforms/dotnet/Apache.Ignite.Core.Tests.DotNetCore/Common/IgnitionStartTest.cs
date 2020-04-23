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

namespace Apache.Ignite.Core.Tests.DotNetCore.Common
{
    using System;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Discovery.Tcp;
    using NUnit.Framework;

    /// <summary>
    /// Tests Ignite startup.
    /// </summary>
    public class IgnitionStartTest : TestBase
    {
        /// <summary>
        /// Tests that Ignite starts with default configuration.
        /// </summary>
        [Test]
        public void TestIgniteStartsWithDefaultConfig()
        {
            var ignite = Start();
            Assert.IsNotNull(ignite);
            Assert.AreEqual(ignite, Ignition.GetIgnite());

            // Second node.
            var ignite2 = Start("ignite-2");
            Assert.AreEqual(2, Ignition.GetAll().Count);

            // Stop node.
            Ignition.Stop(ignite.Name, true);
            Assert.AreEqual(ignite2, Ignition.GetIgnite());

            // Stop all.
            Ignition.StopAll(true);
            Assert.AreEqual(0, Ignition.GetAll().Count);
        }

        /// <summary>
        /// Tests the ignite starts from application configuration.
        /// </summary>
        [Test]
        public void TestIgniteStartsFromAppConfig()
        {
            // 1) MsTest does not pick up the config file, so we have to provide it manually.
            // 2) Note that System.Configuration.ConfigurationManager NuGet package has to be installed.
            var configPath = Path.Combine(Path.GetDirectoryName(GetType().Assembly.Location), "app.config");

            // Force test classpath.
            TestUtils.GetTestConfiguration();

            using (var ignite = Ignition.StartFromApplicationConfiguration("igniteConfiguration", configPath))
            {
                var cache = ignite.GetCache<int, int>(ignite.GetCacheNames().Single());

                Assert.AreEqual("cacheFromConfig", cache.Name);
                Assert.AreEqual(CacheMode.Replicated, cache.GetConfiguration().CacheMode);
            }
        }

        /// <summary>
        /// Tests that Ignite starts from Spring XML.
        /// </summary>
        [Test]
        public void TestIgniteStartsFromSpringXml()
        {
            // When Spring XML is used, .NET overrides Spring.
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                DataStorageConfiguration = null,
                SpringConfigUrl = @"Config\spring-test.xml",
                NetworkSendRetryDelay = TimeSpan.FromSeconds(45),
                MetricsHistorySize = 57
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var resCfg = ignite.GetConfiguration();

                Assert.AreEqual(45, resCfg.NetworkSendRetryDelay.TotalSeconds);  // .NET overrides XML
                Assert.AreEqual(2999, resCfg.NetworkTimeout.TotalMilliseconds);  // Not set in .NET -> comes from XML
                Assert.AreEqual(57, resCfg.MetricsHistorySize);  // Only set in .NET

                var disco = resCfg.DiscoverySpi as TcpDiscoverySpi;
                Assert.IsNotNull(disco);
                Assert.AreEqual(TimeSpan.FromMilliseconds(300), disco.SocketTimeout);

                // DataStorage defaults.
                var dsCfg = new DataStorageConfiguration
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = "default"
                    }
                };
                AssertExtensions.ReflectionEqual(dsCfg, resCfg.DataStorageConfiguration);
            }
        }
    }
}