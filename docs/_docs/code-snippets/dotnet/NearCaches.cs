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