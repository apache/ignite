using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;

namespace dotnet_helloworld
{
    class DataModellingConfiguringCaches
    {
        public static void ConfigurationExample()
        {
            // tag::cfg[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "myCache",
                        CacheMode = CacheMode.Partitioned,
                        Backups = 2,
                        RebalanceMode = CacheRebalanceMode.Sync,
                        WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync,
                        PartitionLossPolicy = PartitionLossPolicy.ReadOnlySafe
                    }
                }
            };
            Ignition.Start(cfg);
            // end::cfg[]
        }

        public static void Backups()
        {
            // tag::backups[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "myCache",
                        CacheMode = CacheMode.Partitioned,
                        Backups = 1
                    }
                }
            };
            Ignition.Start(cfg);
            // end::backups[]
        }
        
        public static void AsyncBackups()
        {
            // tag::synchronization-mode[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "myCache",
                        WriteSynchronizationMode = CacheWriteSynchronizationMode.FullSync,
                        Backups = 1
                    }
                }
            };
            Ignition.Start(cfg);
            // end::synchronization-mode[]
        }


        public static void CacheTemplates() 
        {
            // tag::template[]
            var ignite = Ignition.Start();

            var cfg = new CacheConfiguration
            {
                Name = "myCacheTemplate*",
                CacheMode = CacheMode.Partitioned,
                Backups = 2
            };

            ignite.AddCacheConfiguration(cfg);
            // end::template[]
        }

    }
}
