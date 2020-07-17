using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Compute;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class ClusterGroups
    {
        // tag::broadcastAction[]
        class PrintNodeIdAction : IComputeAction
        {
            public void Invoke()
            {
                Console.WriteLine("Hello node: " +
                                  Ignition.GetIgnite().GetCluster().GetLocalNode().Id);
            }
        }

        public static void RemotesBroadcastDemo()
        {
            var ignite = Ignition.Start();

            var cluster = ignite.GetCluster();

            // Get compute instance which will only execute
            // over remote nodes, i.e. all the nodes except for this one.
            var compute = cluster.ForRemotes().GetCompute();

            // Broadcast to all remote nodes and print the ID of the node
            // on which this closure is executing.
            compute.Broadcast(new PrintNodeIdAction());
        }
        // end::broadcastAction[]

        public static void ClusterGroupsDemo()
        {
            var ignite = Ignition.Start(
                new IgniteConfiguration
                {
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
                    }
                }
            );
    
            // tag::clusterGroups[]
            var cluster = ignite.GetCluster();
            
            // All nodes on which cache with name "myCache" is deployed,
            // either in client or server mode.
            var cacheGroup = cluster.ForCacheNodes("myCache");

            // All data nodes responsible for caching data for "myCache".
            var dataGroup = cluster.ForDataNodes("myCache");

            // All client nodes that access "myCache".
            var clientGroup = cluster.ForClientNodes("myCache");
            // end::clusterGroups[]
        }
    }
}