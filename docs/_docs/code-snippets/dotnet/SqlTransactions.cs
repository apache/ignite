using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;

namespace dotnet_helloworld
{
    public class SqlTransactions
    {
        public static void EnablingMvcc()
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
                });

            // tag::mvcc[]
            var cacheCfg = new CacheConfiguration
            {
                Name = "myCache",
                AtomicityMode = CacheAtomicityMode.TransactionalSnapshot
            };
            // end::mvcc[]
            ignite.CreateCache<long, long>(cacheCfg);
            Console.Write(typeof(Person));
        }

        public static void ConcurrentUpdates()
        {
            var cfg = new IgniteConfiguration
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "mvccCache",
                        AtomicityMode = CacheAtomicityMode.TransactionalSnapshot
                    }, 
                }
            };
            var ignite = Ignition.Start(cfg);
            var cache = ignite.GetCache<int, string>("mvccCache");

            // tag::mvccConcurrentUpdates[]
            for (var i = 1; i <= 5; i++)
            {
                using (var tx = ignite.GetTransactions().TxStart())
                {
                    Console.WriteLine($"attempt #{i}, value: {cache.Get(1)}");
                    try
                    {
                        cache.Put(1, "new value");
                        tx.Commit();
                        Console.WriteLine($"attempt #{i} succeeded");
                        break;
                    }
                    catch (CacheException)
                    {
                        if (!tx.IsRollbackOnly)
                        {
                            // Transaction was not marked as "rollback only",
                            // so it's not a concurrent update issue.
                            // Process the exception here.
                            break;
                        }
                    }
                }
            }
            // end::mvccConcurrentUpdates[]
        }
    }
}