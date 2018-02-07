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

namespace Apache.Ignite.Examples.Datagrid
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.ExamplesDll.Datagrid;

    /// <summary>
    /// This example demonstrates the affinity collocation of a closure with data 
    /// by creating and modifying cache entries via an EntryProcessor.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 2) Start example.
    /// </summary>
    public static class EntryProcessorExample
    {
        /// <summary>Cache name.</summary>
        private const string CacheName = "dotnet_cache_put_get";

        /// <summary>Entry count.</summary>
        private const int EntryCount = 20;

        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
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

        /// <summary>
        /// Prints the cache entries.
        /// </summary>
        /// <param name="cache">The cache.</param>
        private static void PrintCacheEntries(ICache<int, int> cache)
        {
            Console.WriteLine("\n>>> Entries in cache:");

            foreach (var entry in cache)
                Console.WriteLine(entry);
        }
    }
}
