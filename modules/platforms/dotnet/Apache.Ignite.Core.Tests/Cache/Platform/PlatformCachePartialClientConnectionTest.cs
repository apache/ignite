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

namespace Apache.Ignite.Core.Tests.Cache.Platform
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using NUnit.Framework;

    /// <summary>
    /// Tests platform cache with thick clients connected to different parts of the cluster.
    /// </summary>
    public class PlatformCachePartialClientConnectionTest
    {
        private const string CacheName = "cache1";
        private const string AttrMacs = "org.apache.ignite.macs";

        private const int Key = 1;
        private const int InitialValue = 0;

        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that thick client connected only to backup node 1 updates a value,
        /// and another thick client connected to a different backup node sees the update in Platform Cache.
        /// </summary>
        [Test]
        public static void TestPutFromOneClientGetFromAnother()
        {
            // Start 3 servers.
            var servers = Enumerable.Range(0, 3)
                .Select(i => Ignition.Start(GetConfiguration(false, i, 0)))
                .ToArray();

            CreateCache(servers[0]);

            // Start 2 thick clients, connect to different backup nodes only (not entire cluster).
            var primaryAndBackups = servers[0].GetAffinity(CacheName).MapKeyToPrimaryAndBackups(Key);
            var backupServer1Mac = GetMac(primaryAndBackups[1]);
            var backupServer2Mac = GetMac(primaryAndBackups[2]);

            var client1 = Ignition.Start(GetConfiguration(true, backupServer1Mac, backupServer1Mac));
            var client2 = Ignition.Start(GetConfiguration(true, backupServer2Mac, backupServer2Mac));

            // Check initial value.
            var client1Cache = client1.GetOrCreateNearCache<int, int>(CacheName, new NearCacheConfiguration());
            var client2Cache = client2.GetOrCreateNearCache<int, int>(CacheName, new NearCacheConfiguration());

            var client1Value = client1Cache.Get(Key);
            var client2Value = client2Cache.Get(Key);

            Assert.AreEqual(InitialValue, client1Value);
            Assert.AreEqual(InitialValue, client2Value);

            // Update value from client 1.
            const int newValue = 1;
            client1Cache.Put(Key, newValue);

            // Read value from client 1 and 2.
            client1Value = client1Cache.Get(Key);
            client2Value = client2Cache.Get(Key);

            Assert.AreEqual(newValue, client1Value);
            Assert.AreEqual(newValue, client2Value);
        }

        private static int GetMac(IClusterNode node) => Convert.ToInt32(node.Attributes[AttrMacs]);

        private static IgniteConfiguration GetConfiguration(bool client, int localMac, int remoteMac)
        {
            var name = (client ? "client" : "server") + localMac;
            var remotePort = 48500 + remoteMac;

            var discoverySpi = new TcpDiscoverySpi
            {
                IpFinder = new TcpDiscoveryStaticIpFinder
                {
                    Endpoints = new List<string> { $"127.0.0.1:{remotePort}" }
                }
            };

            if (!client)
            {
                discoverySpi.LocalPort = 48500 + localMac;
                discoverySpi.LocalPortRange = 1;
            }

            var igniteConfig = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = client,
                IgniteInstanceName = name,
                // ConsistentId = name,
                UserAttributes = new Dictionary<string, object>
                {
                    [$"override.{AttrMacs}"] = localMac.ToString()
                },
                DiscoverySpi = discoverySpi
            };

            return igniteConfig;
        }

        private static void CreateCache(IIgnite ignite)
        {
            var cacheConfig = new CacheConfiguration(CacheName)
            {
                CacheMode = CacheMode.Replicated,
                ReadFromBackup = true, // Does not reproduce when false.
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    KeyTypeName = typeof(int).FullName,
                    ValueTypeName = typeof(int).FullName
                }
            };

            var cache = ignite.GetOrCreateCache<int, int>(cacheConfig);
            cache.Put(Key, InitialValue);
        }
    }
}
