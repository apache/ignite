using Apache.Ignite.Core;
using Apache.Ignite.Core.Deployment;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class PeerClassLoading
    {
        public static void PeerClassLoadingEnabled()
        {
            //tag::enable[]
            var cfg = new IgniteConfiguration
            {
                PeerAssemblyLoadingMode = PeerAssemblyLoadingMode.CurrentAppDomain
            };
            //end::enable[]
            var discoverySpi = new TcpDiscoverySpi
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
            };
            cfg.DiscoverySpi = discoverySpi;
            //tag::enable[]
            var ignite = Ignition.Start(cfg);
            //end::enable[]
        }
    }
}