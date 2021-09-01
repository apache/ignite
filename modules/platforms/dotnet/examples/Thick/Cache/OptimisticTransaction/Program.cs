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

namespace Apache.Ignite.Examples.Thick.Cache.OptimisticTransaction
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Transactions;
    using Apache.Ignite.Examples.Shared;

    /// <summary>
    /// This example demonstrates optimistic transaction concurrency control.
    /// </summary>
    public static class Program
    {
        private const string CacheName = "dotnet_optimistic_tx_example";

        public static void Main()
        {
            using (var ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Optimistic transaction example started.");

                // Create Transactional cache.
                var cacheCfg = new CacheConfiguration(CacheName)
                {
                    AtomicityMode = CacheAtomicityMode.Transactional
                };

                var cache = ignite.GetOrCreateCache<int, int>(cacheCfg);

                // Put a value.
                cache[1] = 0;

                // Increment a value in parallel within a transaction.
                var task1 = Task.Factory.StartNew(() => IncrementCacheValue(cache, 1));
                var task2 = Task.Factory.StartNew(() => IncrementCacheValue(cache, 2));

                Task.WaitAll(task1, task2);

                Console.WriteLine();
                Console.WriteLine(">>> Resulting value in cache: " + cache[1]);

                Console.WriteLine();
                Console.WriteLine(">>> Example finished, press any key to exit ...");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Increments the cache value within a transaction.
        /// </summary>
        /// <param name="cache">The cache.</param>
        /// <param name="threadId">The thread identifier.</param>
        private static void IncrementCacheValue(ICache<int, int> cache, int threadId)
        {
            try
            {
                var transactions = cache.Ignite.GetTransactions();

                using (var tx = transactions.TxStart(TransactionConcurrency.Optimistic,
                    TransactionIsolation.Serializable))
                {
                    // Increment cache value.
                    cache[1]++;

                    // Introduce a delay to ensure lock conflict.
                    Thread.Sleep(TimeSpan.FromSeconds(2.5));

                    tx.Commit();
                }

                Console.WriteLine("\n>>> Thread {0} successfully incremented cached value.", threadId);
            }
            catch (TransactionOptimisticException ex)
            {
                Console.WriteLine("\n>>> Thread {0} failed to increment cached value. " +
                                  "Caught an expected optimistic exception: {1}", threadId, ex.Message);
            }
        }
    }
}
