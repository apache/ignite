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

namespace Apache.Ignite.Core.Tests.Client
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Net;
    using System.Threading;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Tests.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests client heartbeat functionality (<see cref="IgniteClientConfiguration.HeartbeatInterval"/>).
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ClientHeartbeatTest : ClientTestBase
    {
        /** GridNioServer has hardcoded 2000ms idle check interval, so a smaller idle timeout does not make sense. */
        private static readonly int IdleTimeout = 2000;

        public ClientHeartbeatTest() : base(gridCount: 2)
        {
            // No-op.
        }

        [Test]
        public void TestServerDisconnectsIdleClientWithoutHeartbeats()
        {
            using var client = GetClient(enableHeartbeats: false);

            Assert.AreEqual(1, client.GetCacheNames().Count);

            Thread.Sleep(IdleTimeout * 4);

            Assert.Catch(() => client.GetCacheNames());
        }

        [Test]
        public void TestServerDoesNotDisconnectIdleClientWithHeartbeats()
        {
            using var client = GetClient(enableHeartbeats: true, heartbeatInterval: 1500);

            Assert.AreEqual(1500, client.GetConfiguration().HeartbeatInterval.TotalMilliseconds);
            Assert.IsTrue(client.GetConfiguration().EnableHeartbeats);
            Assert.AreEqual(1, client.GetCacheNames().Count);

            Thread.Sleep(IdleTimeout * 3);

            Assert.DoesNotThrow(() => client.GetCacheNames());
        }

        [Test]
        public void TestDefaultZeroIdleTimeoutUsesConfiguredHeartbeatInterval()
        {
            using var ignite = Ignition.Start(TestUtils.GetTestConfiguration(name: "2"));
            using var client = GetClient(enableHeartbeats: true, port: IgniteClientConfiguration.DefaultPort + 2);

            Assert.AreEqual(IgniteClientConfiguration.DefaultHeartbeatInterval,
                client.GetConfiguration().HeartbeatInterval);

            StringAssert.Contains(
                "Server-side IdleTimeout is not set, " +
                "using configured IgniteClientConfiguration.HeartbeatInterval: 00:00:30", GetLogString(client));
        }

        [Test]
        public void TestCustomHeartbeatIntervalOverridesCalculatedFromIdleTimeout()
        {
            using var client = GetClient(enableHeartbeats: true, heartbeatInterval: 300);

            Assert.AreEqual(TimeSpan.FromMilliseconds(300), client.GetConfiguration().HeartbeatInterval);

            StringAssert.Contains("Server-side IdleTimeout is 2000ms, using configured IgniteClientConfiguration." +
                                  "HeartbeatInterval: 00:00:00.3000000", GetLogString(client));
        }

        [Test]
        public void TestCustomHeartbeatIntervalLongerThanRecommendedDoesNotOverrideCalculatedFromIdleTimeout()
        {
            using var client = GetClient(enableHeartbeats: true, heartbeatInterval: 3000);

            Assert.AreEqual(TimeSpan.FromMilliseconds(3000), client.GetConfiguration().HeartbeatInterval);

            StringAssert.Contains(
                "Server-side IdleTimeout is 2000ms, configured IgniteClientConfiguration.HeartbeatInterval is " +
                "00:00:03, which is longer than recommended IdleTimeout / 3. " +
                "Overriding heartbeat interval with IdleTimeout / 3: 00:00:00.6660000",
                GetLogString(client));
        }

        [Test]
        public void TestZeroOrNegativeHeartbeatIntervalThrows()
        {
            Assert.Throws<IgniteClientException>(() => GetClient(enableHeartbeats: true, heartbeatInterval: 0));
            Assert.Throws<IgniteClientException>(() => GetClient(enableHeartbeats: true, heartbeatInterval: -1));
        }

        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(base.GetIgniteConfiguration())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    IdleTimeout = TimeSpan.FromMilliseconds(IdleTimeout)
                }
            };
        }

        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration(base.GetClientConfiguration())
            {
                // Keep default client alive.
                EnableHeartbeats = true
            };
        }

        private static IIgniteClient GetClient(bool enableHeartbeats, int? heartbeatInterval = null,
            int port = IgniteClientConfiguration.DefaultPort)
        {
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new List<string> { $"{IPAddress.Loopback}:{port}" },
                EnableHeartbeats = enableHeartbeats,
                Logger = new ListLogger
                {
                    EnabledLevels = new[] { LogLevel.Debug, LogLevel.Info, LogLevel.Warn, LogLevel.Error }
                },
                EnablePartitionAwareness = true
            };

            if (heartbeatInterval != null)
            {
                cfg.HeartbeatInterval = TimeSpan.FromMilliseconds(heartbeatInterval.Value);
            }

            return Ignition.StartClient(cfg);
        }

        private static string GetLogString(IIgniteClient client)
        {
            var logger = (ListLogger)client.GetConfiguration().Logger;

            return string.Join(Environment.NewLine, logger.Entries.Select(e => e.Message));
        }
    }
}
