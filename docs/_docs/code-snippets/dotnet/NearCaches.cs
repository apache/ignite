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
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Eviction;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class NearCaches
    {
        public static void ConfiguringNearCache()
        {
            var ignite = Ignition.Start(new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    LocalPort = 48500,
                    LocalPortRange = 20,
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[]
                        {
                            "127.0.0.1:48500..48520"
                        }
                    }
                }
            });

            //tag::nearCacheConf[]
            var cacheCfg = new CacheConfiguration
            {
                Name = "myCache",
                NearConfiguration = new NearCacheConfiguration
                {
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        MaxSize = 100_000
                    }
                }
            };

            var cache = ignite.GetOrCreateCache<int, int>(cacheCfg);
            //end::nearCacheConf[]
        }

        public static void NearCacheOnClientNodeDemo()
        {
            //tag::nearCacheClientNode[]
            var ignite = Ignition.Start(new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    LocalPort = 48500,
                    LocalPortRange = 20,
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[]
                        {
                            "127.0.0.1:48500..48520"
                        }
                    }
                },
                CacheConfiguration = new[]
                {
                    new CacheConfiguration {Name = "myCache"}
                }
            });
            var client = Ignition.Start(new IgniteConfiguration
            {
                IgniteInstanceName = "clientNode",
                ClientMode = true,
                DiscoverySpi = new TcpDiscoverySpi
                {
                    LocalPort = 48500,
                    LocalPortRange = 20,
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[]
                        {
                            "127.0.0.1:48500..48520"
                        }
                    }
                }
            });
            // Create a near-cache configuration
            var nearCfg = new NearCacheConfiguration
            {
                // Use LRU eviction policy to automatically evict entries
                // from near-cache, whenever it reaches 100_000 in size.
                EvictionPolicy = new LruEvictionPolicy()
                {
                    MaxSize = 100_000
                }
            };


            // get the cache named "myCache" and create a near cache for it
            var cache = client.GetOrCreateNearCache<int, string>("myCache", nearCfg);
            //end::nearCacheClientNode[]
        }
    }
}
