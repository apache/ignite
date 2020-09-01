using Apache.Ignite.Core;
using Apache.Ignite.Core.Configuration;

namespace dotnet_helloworld
{
    class MemoryArchitecture
    {
        public static void MemoryConfiguration()
        {
            // tag::mem[]
            var cfg = new IgniteConfiguration
            {
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DefaultDataRegionConfiguration = new DataRegionConfiguration
                    {
                        Name = "Default_Region",
                        InitialSize = 100 * 1024 * 1024
                    },
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "40MB_Region_Eviction",
                            InitialSize = 20 * 1024 * 1024,
                            MaxSize = 40 * 1024 * 1024,
                            PageEvictionMode = DataPageEvictionMode.Random2Lru
                        },
                        new DataRegionConfiguration
                        {
                            Name = "30MB_Region_Swapping",
                            InitialSize = 15 * 1024 * 1024,
                            MaxSize = 30 * 1024 * 1024,
                            SwapPath = "/path/to/swap/file"
                        }
                    }
                }
            };
            Ignition.Start(cfg);
            // end::mem[]
        }

        public static void DefaultDataReqion()
        {
             // tag::DefaultDataReqion[]
             var cfg = new IgniteConfiguration
             {
                 DataStorageConfiguration = new DataStorageConfiguration
                 {
                     DefaultDataRegionConfiguration = new DataRegionConfiguration
                     {
                         Name = "Default_Region",
                         InitialSize = 100 * 1024 * 1024
                     }
                 }
             };

             // Start the node.
             var ignite = Ignition.Start(cfg);
             // end::DefaultDataReqion[]
        }
    }
}
