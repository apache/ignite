using System;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache.Configuration;
using Apache.Ignite.Core.Discovery.Tcp;
using Apache.Ignite.Core.Discovery.Tcp.Static;
using Apache.Ignite.Core.Transactions;

namespace dotnet_helloworld
{
    public class PerformingTransactions
    {
        public static void TransactionExecutionDemo()
        {
            // tag::executingTransactions[]
            // tag::optimisticTx[]
            // tag::deadlock[]
            var cfg = new IgniteConfiguration
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
                },
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = "cacheName",
                        AtomicityMode = CacheAtomicityMode.Transactional
                    }
                },
                TransactionConfiguration = new TransactionConfiguration
                {
                    DefaultTimeoutOnPartitionMapExchange = TimeSpan.FromSeconds(20)
                }
            };

            var ignite = Ignition.Start(cfg);
            // end::optimisticTx[]
            // end::deadlock[]
            var cache = ignite.GetCache<string, int>("cacheName");
            cache.Put("Hello", 1);
            var transactions = ignite.GetTransactions();

            using (var tx = transactions.TxStart())
            {
                int hello = cache.Get("Hello");

                if (hello == 1)
                {
                    cache.Put("Hello", 11);
                }

                cache.Put("World", 22);

                tx.Commit();
            }
            // end::executingTransactions[]

            // tag::optimisticTx[]
            // Re-try the transaction a limited number of times
            var retryCount = 10;
            var retries = 0;

            // Start a transaction in the optimistic mode with the serializable isolation level
            while (retries < retryCount)
            {
                retries++;
                try
                {
                    using (var tx = ignite.GetTransactions().TxStart(TransactionConcurrency.Optimistic,
                        TransactionIsolation.Serializable))
                    {
                        // modify cache entries as part of this transaction.

                        // commit the transaction
                        tx.Commit();

                        // the transaction succeeded. Leave the while loop.
                        break;
                    }
                }
                catch (TransactionOptimisticException)
                {
                    // Transaction has failed. Retry.
                }

            }
            // end::optimisticTx[]

            // tag::deadlock[]
            var intCache = ignite.GetOrCreateCache<int, int>("intCache");
            try
            {
                using (var tx = ignite.GetTransactions().TxStart(TransactionConcurrency.Pessimistic,
                    TransactionIsolation.ReadCommitted, TimeSpan.FromMilliseconds(300), 0))
                {
                    intCache.Put(1, 1);
                    intCache.Put(2, 1);
                    tx.Commit();
                }
            }
            catch (TransactionTimeoutException e)
            {
                Console.WriteLine(e.Message);
            }
            catch (TransactionDeadlockException e)
            {
                Console.WriteLine(e.Message);
            }

            // end::deadlock[]
        }

        public static void TxTimeoutOnPme()
        {
            // tag::pmeTimeout[]
            var cfg = new IgniteConfiguration
            {
                TransactionConfiguration = new TransactionConfiguration
                {
                    DefaultTimeoutOnPartitionMapExchange = TimeSpan.FromSeconds(20)
                }
            };
            Ignition.Start(cfg);
            // end::pmeTimeout[]
        }
    }
}