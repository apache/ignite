using Apache.Ignite.Core;
using Apache.Ignite.Core.Configuration;

namespace dotnet_helloworld
{
    class PersistenceIgnitePersistence
    {
        public static void DisablingWal()
        {
            // tag::disableWal[]
            var cacheName = "myCache";
            var ignite = Ignition.Start();
            ignite.GetCluster().DisableWal(cacheName);

            //load data

            ignite.GetCluster().EnableWal(cacheName);
            // end::disableWal[]
        }

        public static void Configuration()
        {
            // tag::cfg[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                  // tag::storage-path[]
                    StoragePath = "/ssd/storage",

                  // end::storage-path[]
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = "Default_Region",
                        PersistenceEnabled = true
                    }
                }
            };

            Ignition.Start(cfg);
            // end::cfg[]
        }

        public static void Swapping()
        {
            // tag::cfg-swap[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "5GB_Region",
                            InitialSize = 100L * 1024 * 1024,
                            MaxSize = 5L * 1024 * 1024 * 1024,
                            SwapPath = "/path/to/some/directory"
                        }
                    }
                }
            };
            // end::cfg-swap[]
        }
    }
}
