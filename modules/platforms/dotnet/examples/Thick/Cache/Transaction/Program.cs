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

namespace Apache.Ignite.Examples.Thick.Cache.Transaction
{
    using System;
    using System.Transactions;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Transactions;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Models;

    /// <summary>
    /// This example demonstrates transactional data updates.
    /// </summary>
    public static class Program
    {
        private const string CacheName = "dotnet_cache_tx";

        public static void Main()
        {
            using (var ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Transaction example started.");

                var cache = ignite.GetOrCreateCache<int, Account>(new CacheConfiguration
                {
                    Name = CacheName,
                    AtomicityMode = CacheAtomicityMode.Transactional
                });

                InitAccounts(cache);

                Console.WriteLine("\n>>> Transferring with Ignite transaction API...");

                // Transfer money between accounts in a single transaction.
                using (var tx = cache.Ignite.GetTransactions().TxStart(TransactionConcurrency.Pessimistic,
                    TransactionIsolation.RepeatableRead))
                {
                    Account acc1 = cache.Get(1);
                    Account acc2 = cache.Get(2);

                    acc1.Balance += 100;
                    acc2.Balance -= 100;

                    cache.Put(1, acc1);
                    cache.Put(2, acc2);

                    tx.Commit();
                }

                DisplayAccounts(cache);

                InitAccounts(cache);

                Console.WriteLine("\n>>> Transferring with TransactionScope API...");

                // Do the same transaction with TransactionScope API.
                using (var ts = new TransactionScope())
                {
                    Account acc1 = cache.Get(1);
                    Account acc2 = cache.Get(2);

                    acc1.Balance += 100;
                    acc2.Balance -= 100;

                    cache.Put(1, acc1);
                    cache.Put(2, acc2);

                    ts.Complete();
                }

                DisplayAccounts(cache);
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Displays accounts.
        /// </summary>
        private static void DisplayAccounts(ICache<int, Account> cache)
        {
            Console.WriteLine(">>> Transfer finished.");

            Console.WriteLine();
            Console.WriteLine(">>> Accounts after transfer:");
            Console.WriteLine(">>>     " + cache.Get(1));
            Console.WriteLine(">>>     " + cache.Get(2));
            Console.WriteLine();
        }

        /// <summary>
        /// Initializes account balance.
        /// </summary>
        private static void InitAccounts(ICache<int, Account> cache)
        {
            // Clean up caches on all nodes before run.
            cache.Clear();

            // Initialize.
            cache.Put(1, new Account(1, 100));
            cache.Put(2, new Account(2, 200));

            Console.WriteLine();
            Console.WriteLine(">>> Accounts before transfer:");
            Console.WriteLine(">>>     " + cache.Get(1));
            Console.WriteLine(">>>     " + cache.Get(2));
            Console.WriteLine();
        }
    }
}
