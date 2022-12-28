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
    /// Tests platform cache with <see cref="CacheConfiguration.ReadFromBackup"/> enabled.
    /// </summary>
    public class PlatformCacheReadFromBackupTest
    {
        private const string CacheName = "cache1";
        private const string AttrMacs = "org.apache.ignite.macs";

        private const string Key = "k";
        private const string InitialValue = "0";

        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        [Test]
        public static void TestPutFromOneClientGetFromAnother()
        {
            var servers = Enumerable.Range(0, 3)
                .Select(i => Ignition.Start(GetConfiguration(false, i, 0)))
                .ToArray();

            CreateCache(servers[0]);

            var primaryAndBackups = servers[0].GetAffinity(CacheName).MapKeyToPrimaryAndBackups(Key);
            var backupServer1Mac = GetMac(primaryAndBackups[1]);
            var backupServer2Mac = GetMac(primaryAndBackups[2]);

            var client1 = Ignition.Start(GetConfiguration(true, backupServer1Mac, backupServer1Mac));
            var client2 = Ignition.Start(GetConfiguration(true, backupServer2Mac, backupServer2Mac));

            // Check initial value.
            var client1Cache = client1.GetOrCreateNearCache<string, string>(CacheName, new NearCacheConfiguration());
            var client2Cache = client2.GetOrCreateNearCache<string, string>(CacheName, new NearCacheConfiguration());

            var client1Value = client1Cache.Get(Key);
            var client2Value = client2Cache.Get(Key);

            Assert.AreEqual(InitialValue, client1Value);
            Assert.AreEqual(InitialValue, client2Value);

            // Update value from client 1.
            const string newValue = "1";
            client1Cache.Put(Key, newValue);

            // Read value from client 1 and 2.
            client1Value = client1Cache.Get(Key);
            client2Value = client2Cache.Get(Key);

            Assert.AreEqual(newValue, client1Value);
            Assert.AreEqual(newValue, client2Value); // Second client does not have correct value.
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
                ReadFromBackup = true, // Does not reproduce without this
                PlatformCacheConfiguration = new PlatformCacheConfiguration
                {
                    KeyTypeName = typeof(string).FullName,
                    ValueTypeName = typeof(string).FullName
                }
            };

            var cache = ignite.GetOrCreateCache<string, string>(cacheConfig);
            cache.Put(Key, InitialValue);
        }
    }
}
