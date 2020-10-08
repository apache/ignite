using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Cache.Eviction;
using Apache.Ignite.Core.Configuration;
using DataPageEvictionMode = Apache.Ignite.Core.Configuration.DataPageEvictionMode;

namespace dotnet_helloworld
{
    class EvictionPolicies
    {
        public static void RandomLRU()
        {
            // tag::randomLRU[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "20GB_Region",
                            InitialSize = 500L * 1024 * 1024,
                            MaxSize = 20L * 1024 * 1024 * 1024,
                            PageEvictionMode = DataPageEvictionMode.RandomLru
                        }
                    }
                }
            };
            // end::randomLRU[]
        }

        public static void Random2LRU()
        {
            // tag::random2LRU[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "20GB_Region",
                            InitialSize = 500L * 1024 * 1024,
                            MaxSize = 20L * 1024 * 1024 * 1024,
                            PageEvictionMode = DataPageEvictionMode.Random2Lru
                        }
                    }
                }
            };
            // end::random2LRU[]
        }

        public static void LRU()
        {
            // tag::LRU[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "cacheName",
                        OnheapCacheEnabled = true,
                        EvictionPolicy = new LruEvictionPolicy
                        {
                            MaxSize = 100000
                        }
                    }
                }
            };
            // end::LRU[]
        }

        public static void FIFO()
        {
            // tag::FIFO[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "cacheName",
                        OnheapCacheEnabled = true,
                        EvictionPolicy = new FifoEvictionPolicy
                        {
                            MaxSize = 100000
                        }
                    }
                }
            };
            // end::FIFO[]
        }
    }
}
