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

        [Test]
        public void TestServerDisconnectsIdleClientWithoutHeartbeats()
        {
            using var client = GetClient(heartbeatInterval: 0);

            Assert.AreEqual(1, client.GetCacheNames().Count);

            Thread.Sleep(IdleTimeout * 2);

            Assert.Throws<IgniteClientException>(() => client.GetCacheNames());
        }

        [Test]
        public void TestServerDoesNotDisconnectIdleClientWithHeartbeats()
        {
            var heartbeatInterval = IdleTimeout / 4;
            using var client = GetClient(heartbeatInterval);

            Assert.AreEqual(heartbeatInterval, client.GetConfiguration().HeartbeatInterval);
            Assert.AreEqual(1, client.GetCacheNames().Count);

            Thread.Sleep(IdleTimeout * 3);

            Assert.DoesNotThrow(() => client.GetCacheNames());
        }

        [Test]
        public void TestHeartbeatIntervalLongerThanIdleTimeoutLogsWarning()
        {
            using var client = GetClient(heartbeatInterval: IdleTimeout * 3);

            var logger = (ListLogger)client.GetConfiguration().Logger;
            var logs = logger.Entries.Select(e => e.Message).ToArray();

            Assert.Contains(
                "Client heartbeat interval is greater than server idle timeout (00:00:06 > 00:00:02). " +
                "Server will disconnect idle client.",
                logs);
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
                HeartbeatInterval = TimeSpan.FromMilliseconds(IdleTimeout / 4)
            };
        }

        private static IIgniteClient GetClient(int heartbeatInterval)
        {
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new List<string> { IPAddress.Loopback.ToString() },
                HeartbeatInterval = TimeSpan.FromMilliseconds(heartbeatInterval),
                Logger = new ListLogger()
            };

            return Ignition.StartClient(cfg);
        }
    }
}
