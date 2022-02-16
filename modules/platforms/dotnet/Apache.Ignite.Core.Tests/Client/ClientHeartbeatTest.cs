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
    /// Tests client heartbeat functionality (<see cref="IgniteClientConfiguration.DefaultHeartbeatInterval"/>).
    /// </summary>
    [Category(TestUtils.CategoryIntensive)]
    public class ClientHeartbeatTest : ClientTestBase
    {
        /** GridNioServer has hardcoded 2000ms idle check interval, so a smaller idle timeout does not make sense. */
        private static readonly int IdleTimeout = 2000;

        [Test]
        public void TestServerDisconnectsIdleClientWithoutHeartbeats()
        {
            using var client = GetClient(enableHeartbeats: false);

            Assert.AreEqual(1, client.GetCacheNames().Count);

            Thread.Sleep(IdleTimeout * 3);

            Assert.Catch(() => client.GetCacheNames());
        }

        [Test]
        public void TestServerDoesNotDisconnectIdleClientWithHeartbeats()
        {
            using var client = GetClient(enableHeartbeats: true, 1500);

            Assert.AreEqual(1500, client.GetConfiguration().DefaultHeartbeatInterval.TotalMilliseconds);
            Assert.IsTrue(client.GetConfiguration().EnableHeartbeats);
            Assert.AreEqual(1, client.GetCacheNames().Count);

            Thread.Sleep(IdleTimeout * 3);

            Assert.DoesNotThrow(() => client.GetCacheNames());
        }

        [Test]
        public void TestDefaultZeroIdleTimeoutUsesDefaultHeartbeatInterval()
        {
            using var ignite = Ignition.Start(TestUtils.GetTestConfiguration(name: "2"));
            using var client = GetClient(enableHeartbeats: true, port: IgniteClientConfiguration.DefaultPort + 1);

            var logger = (ListLogger)client.GetConfiguration().Logger;
            var logs = string.Join(Environment.NewLine, logger.Entries.Select(e => e.Message));

            StringAssert.Contains("Server-side IdleTimeout is not set, " +
                                  "using IgniteClientConfiguration.DefaultHeartbeatInterval: 00:00:30", logs);
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
                // ReSharper disable once PossibleLossOfFraction
                DefaultHeartbeatInterval = TimeSpan.FromMilliseconds(IdleTimeout / 4)
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
                }
            };

            if (heartbeatInterval != null)
            {
                cfg.DefaultHeartbeatInterval = TimeSpan.FromMilliseconds(heartbeatInterval.Value);
            }

            return Ignition.StartClient(cfg);
        }
    }
}
