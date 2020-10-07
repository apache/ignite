using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;

namespace dotnet_helloworld
{
    class UnderstandingConfiguration
    {
        static void Foo()
        {
            // tag::UnderstandingConfigurationProgrammatic[]
            var igniteCfg = new IgniteConfiguration
            {
                WorkDirectory = "/path/to/work/directory",
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "myCache",
                        CacheMode = CacheMode.Partitioned
                    }
                }
            };
            // end::UnderstandingConfigurationProgrammatic[]

            // tag::SettingWorkDir[]
            var cfg = new IgniteConfiguration
            {
                WorkDirectory = "/path/to/work/directory"
            };
            // end::SettingWorkDir[]

        }
    }
}