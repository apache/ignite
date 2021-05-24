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

namespace Apache.Ignite.Examples.Thick.Cache.EntryProcessor
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Cache;

    /// <summary>
    /// This example demonstrates the affinity collocation of a closure with data
    /// by creating and modifying cache entries via an EntryProcessor.
    /// </summary>
    public static class Program
    {
        private const string CacheName = "dotnet_cache_entry_processor";

        private const int EntryCount = 20;

        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache EntryProcessor example started.");

                ICache<int, int> cache = ignite.GetOrCreateCache<int, int>(CacheName);
                cache.Clear();

                // Populate cache with Invoke.
                int[] keys = Enumerable.Range(1, EntryCount).ToArray();

                foreach (var key in keys)
                    cache.Invoke(key, new CachePutEntryProcessor(), 10);

                PrintCacheEntries(cache);

                // Increment entries by 5 with InvokeAll.
                cache.InvokeAll(keys, new CacheIncrementEntryProcessor(), 5);

                PrintCacheEntries(cache);
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        private static void PrintCacheEntries(ICache<int, int> cache)
        {
            Console.WriteLine("\n>>> Entries in cache:");

            foreach (var entry in cache)
                Console.WriteLine(entry);
        }
    }
}
