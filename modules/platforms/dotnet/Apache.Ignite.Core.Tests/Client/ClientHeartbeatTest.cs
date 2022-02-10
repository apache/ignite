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
    using System.Net;
    using System.Threading;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests client heartbeat functionality (<see cref="IgniteClientConfiguration.HeartbeatInterval"/>).
    /// </summary>
    public class ClientHeartbeatTest : ClientTestBase
    {
        /** */
        private static readonly TimeSpan IdleTimeout = TimeSpan.FromMilliseconds(100);

        [Test]
        public void TestServerDisconnectsIdleClientWithoutHeartbeats()
        {
            using (var client = GetClient(TimeSpan.Zero))
            {
                Assert.AreEqual(1, client.GetCacheNames().Count);

                Thread.Sleep(IdleTimeout * 2);

                Assert.Throws<IgniteClientException>(() => client.GetCacheNames());
            }
        }

        [Test]
        public void TestServerDoesNotDisconnectIdleClientWithHeartbeats()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestServerDisconnectsIdleClientWithLongHeartbeatInterval()
        {
            Assert.Fail("TODO");
        }

        [Test]
        public void TestHeartbeatIntervalLongerThanIdleTimeoutLogsWarning()
        {
            Assert.Fail("TODO");
        }

        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(base.GetIgniteConfiguration())
            {
                ClientConnectorConfiguration = new ClientConnectorConfiguration
                {
                    IdleTimeout = IdleTimeout
                }
            };
        }

        private static IIgniteClient GetClient(TimeSpan heartbeatInterval)
        {
            var cfg = new IgniteClientConfiguration
            {
                Endpoints = new List<string> { IPAddress.Loopback.ToString() },
                HeartbeatInterval = heartbeatInterval
            };

            return Ignition.StartClient(cfg);
        }
    }
}
