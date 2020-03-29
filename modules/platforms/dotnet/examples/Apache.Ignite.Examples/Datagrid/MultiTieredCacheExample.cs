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

// ReSharper disable AssignNullToNotNullAttribute
namespace Apache.Ignite.Examples.Datagrid
{
    using System;
    using System.IO;
    using System.Threading;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;

    /// <summary>
    /// This example demonstrates on how to configure a multi-tiered Ignite cache that will store data in different 
    /// memory spaces (on-heap, off-heap, swap) depending on the total cache size and eviction policies that are set.
    /// NOTE: There must be no other cluster nodes running on the host.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// </summary>
    public class MultiTieredCacheExample
    {
        /// <summary>Example cache name.</summary>
        private const string CacheName = "dotnet_multi_tiered_example_cache";

        /// <summary>Cache entry size, in bytes..</summary>
        private const int EntrySize = 1024;

        [STAThread]
        public static void Main()
        {
            Console.WriteLine();
            Console.WriteLine(">>> Multi-tiered cache example started.");

            // Configure swap in the current bin directory (where our assembly is located).
            var binDir = Path.GetDirectoryName(typeof(MultiTieredCacheExample).Assembly.Location);
            var swapDir = Path.Combine(binDir, "ignite-swap");

            Console.WriteLine(">>> Swap space directory: " + swapDir);

            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                var cacheCfg = new CacheConfiguration
                {
                    Name = CacheName,
                    Backups = 1,
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        MaxSize = 10 // Maximum number of entries that will be stored in Java heap. 
                    },
                };

                ICache<int, byte[]> cache = ignite.GetOrCreateCache<int, byte[]>(cacheCfg);

                // Sample data.
                byte[] dataBytes = new byte[EntrySize];

                // Filling out cache and printing its metrics.
                PrintCacheMetrics(cache);

                for (int i = 0; i < 100; i++)
                {
                    cache.Put(i, dataBytes);

                    if (i%10 == 0)
                    {
                        Console.WriteLine(">>> Cache entries created: {0}", i + 1);

                        PrintCacheMetrics(cache);
                    }
                }

                Console.WriteLine(">>> Waiting for metrics final update...");

                Thread.Sleep(IgniteConfiguration.DefaultMetricsUpdateFrequency);

                PrintCacheMetrics(cache);

                Console.WriteLine();
                Console.WriteLine(">>> Example finished, press any key to exit ...");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Prints the cache metrics.
        /// </summary>
        private static void PrintCacheMetrics(ICache<int, byte[]> cache)
        {
            var metrics = cache.GetLocalMetrics();

            Console.WriteLine("\n>>> Cache entries layout: [Total={0}, Java heap={1}, Off-Heap={2}]",
                cache.GetSize(CachePeekMode.All), 
                metrics.Size, metrics.OffHeapEntriesCount);
        }
    }
}