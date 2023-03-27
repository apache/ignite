/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Examples.Thick.Cache.TransactionDeadlockDetection
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Transactions;
    using Apache.Ignite.Examples.Shared;

    /// <summary>
    /// This example demonstrates the transaction deadlock detection mechanism.
    /// </summary>
    public static class Program
    {
        private const string CacheName = "dotnet_cache_tx_deadlock";

        public static void Main()
        {
            using (var ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Transaction deadlock detection example started.");

                var cache = ignite.GetOrCreateCache<int, int>(new CacheConfiguration
                {
                    Name = CacheName,
                    AtomicityMode = CacheAtomicityMode.Transactional
                });

                // Clean up caches on all nodes before run.
                cache.Clear();

                var keys = Enumerable.Range(1, 100).ToArray();

                // Modify keys in reverse order to cause a deadlock.
                var task1 = Task.Factory.StartNew(() => UpdateKeys(cache, keys, 1));
                var task2 = Task.Factory.StartNew(() => UpdateKeys(cache, keys.Reverse(), 2));

                Task.WaitAll(task1, task2);

                Console.WriteLine("\n>>> Example finished, press any key to exit ...");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Updates the specified keys.
        /// </summary>
        private static void UpdateKeys(ICache<int, int> cache, IEnumerable<int> keys, int threadId)
        {
            var txs = cache.Ignite.GetTransactions();

            try
            {
                using (var tx = txs.TxStart(TransactionConcurrency.Pessimistic, TransactionIsolation.ReadCommitted,
                    TimeSpan.FromSeconds(2), 0))
                {
                    foreach (var key in keys)
                    {
                        cache[key] = threadId;
                    }

                    // Introduce a delay to ensure lock conflict.
                    Thread.Sleep(TimeSpan.FromSeconds(3));

                    tx.Commit();
                }
            }
            catch (TransactionDeadlockException e)
            {
                // Print out detected deadlock details (participating nodes, keys, etc):
                Console.WriteLine("\n>>> Transaction deadlock in thread {0}: {1}", threadId, e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("\n>>> Update failed in thread {0}: {1}", threadId, e);
            }
        }
    }
}
