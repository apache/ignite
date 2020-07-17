using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;

namespace dotnet_helloworld
{
    class DataRebalancing
    {
        public static void RebalanceMode()
        {
            // tag::RebalanceMode[]
            IgniteConfiguration cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "mycache",
                        RebalanceMode = CacheRebalanceMode.Sync
                    }
                }
            };

            // Start a node.
            var ignite = Ignition.Start(cfg);
            // end::RebalanceMode[]
        }

        public static void RebalanceThrottle()
        {
            // tag::RebalanceThrottle[]
            IgniteConfiguration cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "mycache",
                        RebalanceBatchSize = 2 * 1024 * 1024,
                        RebalanceThrottle = new TimeSpan(0, 0, 0, 0, 100)
                    }
                }
            };

            // Start a node.
            var ignite = Ignition.Start(cfg);
            // end::RebalanceThrottle[]
        }
    }
}