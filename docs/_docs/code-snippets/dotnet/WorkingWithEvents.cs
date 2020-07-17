using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using Apache.Ignite.Core.Events;

namespace dotnet_helloworld
{
    public class WorkingWithEvents
    {
        public static void EnablingEvents()
        {
            //tag::enablingEvents[]
            var cfg = new IgniteConfiguration
            {
                IncludedEventTypes = new[]
                {
                    EventType.CacheObjectPut,
                    EventType.CacheObjectRead,
                    EventType.CacheObjectRemoved,
                    EventType.NodeJoined,
                    EventType.NodeLeft
                }
            };
            // end::enablingEvents[]
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
            // tag::enablingEvents[]
            var ignite = Ignition.Start(cfg);
            // end::enablingEvents[]
        }

        public static void GettingEventsInterface1()
        {
            //tag::gettingEventsInterface1[]
            var ignite = Ignition.GetIgnite();
            var events = ignite.GetEvents();
            //end::gettingEventsInterface1[]
        }

        public static void GettingEventsInterface2()
        {
            //tag::gettingEventsInterface2[]
            var ignite = Ignition.GetIgnite();
            var events = ignite.GetCluster().ForCacheNodes("person").GetEvents();
            //end::gettingEventsInterface2[]
        }

        //tag::localListen[]
        class LocalListener : IEventListener<CacheEvent>
        {
            public bool Invoke(CacheEvent evt)
            {
                Console.WriteLine("Received event [evt=" + evt.Name + ", key=" + evt.Key + ", oldVal=" + evt.OldValue
                                  + ", newVal=" + evt.NewValue);
                return true;
            }
        }

        public static void LocalListenDemo()
        {
            var cfg = new IgniteConfiguration
            {
                IncludedEventTypes = new[]
                {
                    EventType.CacheObjectPut,
                    EventType.CacheObjectRead,
                    EventType.CacheObjectRemoved,
                }
            };
            //end::localListen[]
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
            // tag::localListen[]
            var ignite = Ignition.Start(cfg);
            var events = ignite.GetEvents();
            events.LocalListen(new LocalListener(), EventType.CacheObjectPut, EventType.CacheObjectRead,
                EventType.CacheObjectRemoved);

            var cache = ignite.GetOrCreateCache<int, int>("myCache");
            cache.Put(1, 1);
            cache.Put(2, 2);
        }
        //end::localListen[]


        //tag::queryRemote[]
        class EventFilter : IEventFilter<CacheEvent>
        {
            public bool Invoke(CacheEvent evt)
            {
                return true;
            }
        }
        // ....


        //end::queryRemote[]


        public static void StoringEventsDemo()
        {
            //tag::storingEvents[]
            var cfg = new IgniteConfiguration
            {
                EventStorageSpi = new MemoryEventStorageSpi()
                {
                    ExpirationTimeout = TimeSpan.FromMilliseconds(600000)
                },
                IncludedEventTypes = new[]
                {
                    EventType.CacheObjectPut,
                    EventType.CacheObjectRead,
                    EventType.CacheObjectRemoved,
                }
            };
            //end::storingEvents[]
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
            //tag::storingEvents[]
            var ignite = Ignition.Start(cfg);
            //end::storingEvents[]
            //tag::queryLocal[]
            //tag::queryRemote[]
            var events = ignite.GetEvents();
            //end::queryRemote[]
            var cacheEvents = events.LocalQuery(EventType.CacheObjectPut);
            //end::queryLocal[]
            //tag::queryRemote[]
            var storedEvents = events.RemoteQuery(new EventFilter(), null, EventType.CacheObjectPut);
            //end::queryRemote[]
        }
    }
}