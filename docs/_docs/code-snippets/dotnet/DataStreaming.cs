using System;
using System.Collections.Generic;
using System.Linq;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Datastream;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class DataStreaming
    {

        public static void DataStreamerExample()
        {
            using (var ignite = Ignition.Start(Util.getIngiteCfg()))
            {
                ignite.GetOrCreateCache<int, string>("myCache");
                //tag::dataStreamer1[]
                using (var stmr = ignite.GetDataStreamer<int, string>("myCache"))
                {
                    for (var i = 0; i < 1000; i++)
                        stmr.AddData(i, i.ToString());
                    //end::dataStreamer1[]
                    //tag::dataStreamer2[]
                    stmr.AllowOverwrite = true;
                    //end::dataStreamer2[]
                    //tag::dataStreamer1[]
                }

                //end::dataStreamer1[]
            }
        }        
        // tag::streamReceiver[]
        private class MyStreamReceiver : IStreamReceiver<int, string>
        {
            public void Receive(ICache<int, string> cache, ICollection<ICacheEntry<int, string>> entries)
            {
                foreach (var entry in entries)
                {
                    // do something with the entry

                    cache.Put(entry.Key, entry.Value);
                }
            }
        }

        public static void StreamReceiverDemo()
        {
            var ignite = Ignition.Start();

            using (var stmr = ignite.GetDataStreamer<int, string>("myCache"))
            {
                stmr.AllowOverwrite = true;
                stmr.Receiver = new MyStreamReceiver();
            }
        }
        // end::streamReceiver[]

        // tag::streamTransformer[]
        class MyEntryProcessor : ICacheEntryProcessor<string, long, object, object>
        {
            public object Process(IMutableCacheEntry<string, long> e, object arg)
            {
                //get current count
                var val = e.Value;
                
                //increment count by 1
                e.Value = val == 0 ? 1L : val + 1;

                return null;
            }
        }

        public static void StreamTransformerDemo()
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
            var cfg = new CacheConfiguration("wordCountCache");
            var stmCache = ignite.GetOrCreateCache<string, long>(cfg);

            using (var stmr = ignite.GetDataStreamer<string, long>(stmCache.Name))
            {
                //Allow data updates
                stmr.AllowOverwrite = true;
                
                //Configure data transformation to count instances of the same word
                stmr.Receiver = new StreamTransformer<string, long, object, object>(new MyEntryProcessor());
                
                //stream words into the streamer cache
                foreach (var word in GetWords())
                {
                    stmr.AddData(word, 1L);
                }
            }
            
            Console.WriteLine(stmCache.Get("a"));
            Console.WriteLine(stmCache.Get("b"));
        }

        static IEnumerable<string> GetWords()
        {
            //populate words list somehow
            return Enumerable.Repeat("a", 3).Concat(Enumerable.Repeat("b", 2));
        }
        // end::streamTransformer[]


        // tag::streamVisitor[]
        class Instrument
        {
            public readonly string Symbol;
            public double Latest { get; set; }
            public double High { get; set; }
            public double Low { get; set; }

            public Instrument(string symbol)
            {
                this.Symbol = symbol;
            }
        }

        private static Dictionary<string, double> getMarketData()
        {
            //populate market data somehow
            return new Dictionary<string, double>
            {
                ["foo"] = 1.0,
                ["foo"] = 2.0,
                ["foo"] = 3.0
            };
        }

        public static void StreamVisitorDemo()
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

            var mrktDataCfg = new CacheConfiguration("marketData");
            var instCfg = new CacheConfiguration("instruments");

            // Cache for market data ticks streamed into the system
            var mrktData = ignite.GetOrCreateCache<string, double>(mrktDataCfg);
            // Cache for financial instruments
            var instCache = ignite.GetOrCreateCache<string, Instrument>(instCfg);

            using (var mktStmr = ignite.GetDataStreamer<string, double>("marketData"))
            {
                // Note that we do not populate 'marketData' cache (it remains empty).
                // Instead we update the 'instruments' cache based on the latest market price.
                mktStmr.Receiver = new StreamVisitor<string, double>((cache, e) =>
                {
                    var symbol = e.Key;
                    var tick = e.Value;

                    Instrument inst = instCache.Get(symbol);

                    if (inst == null)
                    {
                        inst = new Instrument(symbol);
                    }

                    // Update instrument price based on the latest market tick.
                    inst.High = Math.Max(inst.High, tick);
                    inst.Low = Math.Min(inst.Low, tick);
                    inst.Latest = tick;
                });
                var marketData = getMarketData();
                foreach (var tick in marketData)
                {
                    mktStmr.AddData(tick);
                }
                mktStmr.Flush();
                Console.Write(instCache.Get("foo"));
            }
        }

        // end::streamVisitor[]
    }
}