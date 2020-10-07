using Apache.Ignite.Core;
using Apache.Ignite.Core.Communication.Tcp;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Multicast;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    class ClusteringTcpIpDiscovery
    {
        public static void MulticastIpFinderDemo()
        {
            //tag::multicast[]
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryMulticastIpFinder
                    {
                        MulticastGroup = "228.10.10.157"
                    }
                }
            };
            Ignition.Start(cfg);
            //end::multicast[]
        }

        public static void StaticIpFinderDemo()
        {
            //tag::static[]
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"1.2.3.4", "1.2.3.5:47500..47509" }
                    }
                }
            };
            //end::static[]
            Ignition.Start(cfg);
        }

        public static void MulticastAndStaticDemo()
        {
            //tag::multicastAndStatic[]
            var cfg = new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryMulticastIpFinder
                    {
                        MulticastGroup = "228.10.10.157",
                        Endpoints = new[] {"1.2.3.4", "1.2.3.5:47500..47509" }
                    }
                }
            };
            Ignition.Start(cfg);
            //end::multicastAndStatic[]
        }
        
        public static void IsolatedClustersDemo()
        {
            // tag::isolated1[]
            var firstCfg = new IgniteConfiguration
            {
                IgniteInstanceName = "first",
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
                CommunicationSpi = new TcpCommunicationSpi
                {
                    LocalPort = 48100
                }
            };
            Ignition.Start(firstCfg);
            // end::isolated1[]

            // tag::isolated2[]
            var secondCfg = new IgniteConfiguration
            {
                IgniteInstanceName = "second",
                DiscoverySpi = new TcpDiscoverySpi
                {
                    LocalPort = 49500,
                    LocalPortRange = 20,
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[]
                        {
                            "127.0.0.1:49500..49520"
                        }
                    }
                },
                CommunicationSpi = new TcpCommunicationSpi
                {
                    LocalPort = 49100
                }
            };
            Ignition.Start(secondCfg);
            // end::isolated2[]
            
        }
    }
}
