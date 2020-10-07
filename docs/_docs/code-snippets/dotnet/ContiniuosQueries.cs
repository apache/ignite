using System;
using System.Collections.Generic;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Event;
using Apache.Ignite.Core.Cache.Query.Continuous;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class ContinuousQueries
    {
        // tag::localListener[]
        // tag::remoteFilter[]
        class LocalListener : ICacheEntryEventListener<int, string>
        {
            public void OnEvent(IEnumerable<ICacheEntryEvent<int, string>> evts)
            {
                foreach (var cacheEntryEvent in evts)
                {
                    //react to update events here
                }
            }
        }
        // end::localListener[]
        class RemoteFilter : ICacheEntryEventFilter<int, string>
        {
            public bool Evaluate(ICacheEntryEvent<int, string> e)
            {
                if (e.Key == 1)
                {
                    return false;
                }
                Console.WriteLine("the value for key {0} was updated from {1} to {2}", e.Key, e.OldValue, e.Value);
                return true;
            }
        }
        // end::remoteFilter[]
        
        // tag::localListener[]
        public static void ContinuousQueryListenerDemo()
        {
            var ignite = Ignition.Start(new IgniteConfiguration
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
            });
            var cache = ignite.GetOrCreateCache<int, string>("myCache");

            var query = new ContinuousQuery<int, string>(new LocalListener());

            var handle = cache.QueryContinuous(query);
            
            cache.Put(1, "1");
            cache.Put(2, "2");
        }
        // end::localListener[]
        
        // tag::remoteFilter[]
        public static void ContinuousQueryFilterDemo()
        {
            var ignite = Ignition.Start(new IgniteConfiguration
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
            });
            var cache = ignite.GetOrCreateCache<int, string>("myCache");

            var query = new ContinuousQuery<int, string>(new LocalListener(), new RemoteFilter());

            var handle = cache.QueryContinuous(query);
            
            cache.Put(1, "1");
            cache.Put(2, "2");
        }
        // end::remoteFilter[]
        
    }
}