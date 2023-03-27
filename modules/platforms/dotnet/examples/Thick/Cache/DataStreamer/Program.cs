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

namespace Apache.Ignite.Examples.Thick.Cache.DataStreamer
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Models;

    /// <summary>
    /// Demonstrates how cache can be populated with data utilizing <see cref="IDataStreamer{TK,TV}"/>.
    /// Data streamer is a lot more efficient to use than standard cache put operation
    /// as it properly buffers cache requests together and properly manages load on remote nodes.
    /// </summary>
    public static class Program
    {
        private const int EntryCount = 500000;

        private const string CacheName = "dotnet_cache_data_streamer";

        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache data streamer example started.");

                // Clean up caches on all nodes before run.
                ignite.GetOrCreateCache<int, Account>(CacheName).Clear();

                var timer = new Stopwatch();

                timer.Start();

                using (var ldr = ignite.GetDataStreamer<int, Account>(CacheName))
                {
                    ldr.PerNodeBufferSize = 1024;

                    for (int i = 0; i < EntryCount; i++)
                    {
                        ldr.Add(i, new Account(i, i));

                        // Print out progress while loading cache.
                        if (i > 0 && i % 10000 == 0)
                            Console.WriteLine($"Loaded {i} accounts.");
                    }
                }

                timer.Stop();

                Console.WriteLine($">>> Loaded {EntryCount} accounts in {timer.ElapsedMilliseconds}ms.");
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
