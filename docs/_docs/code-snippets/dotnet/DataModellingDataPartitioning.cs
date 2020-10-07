using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;

namespace dotnet_helloworld
{
    class DataModellingDataPartitioning
    {

        public static void Foo()
        {
            // tag::partitioning[]
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "Person",
                        Backups = 1,
                        GroupName = "group1"
                    },
                    new CacheConfiguration
                    {
                        Name = "Organization",
                        Backups = 1,
                        GroupName = "group1"
                    }
                }
            };
            Ignition.Start(cfg);
            // end::partitioning[]


        }
    }
}
