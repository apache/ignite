using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Configuration;

namespace dotnet_helloworld
{
    public class ConfiguringMetrics
    {
        public static void EnablingCacheMetrics()
        {
            // tag::cache-metrics[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration("my-cache")
                    {
                        EnableStatistics = true
                    }
                }
            };

            var ignite = Ignition.Start(cfg);
            // end::cache-metrics[]
        }
        
        public static void EnablingDataRegionMetrics()
        {
            // tag::data-region-metrics[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = DataStorageConfiguration.DefaultDataRegionName,
                        MetricsEnabled = true
                    },
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "myDataRegion",
                            MetricsEnabled = true
                        }
                    }
                }
            };
            
            var ignite = Ignition.Start(cfg);
            // end::data-region-metrics[]
        }
        
        public static void EnablingPersistenceMetrics()
        {
            // tag::data-storage-metrics[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    MetricsEnabled = true
                }
            };
            
            var ignite = Ignition.Start(cfg);
            // end::data-storage-metrics[]
        }
    }
}