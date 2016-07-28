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

using System;
using System.Diagnostics;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Datastream;
using Apache.Ignite.ExamplesDll.Binary;

namespace Apache.Ignite.Examples.Datagrid
{
    /// <summary>
    /// Demonstrates how cache can be populated with data utilizing <see cref="IDataStreamer{TK,TV}"/>.
    /// Data streamer is a lot more efficient to use than standard cache put operation 
    /// as it properly buffers cache requests together and properly manages load on remote nodes.
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
    public class DataStreamerExample
    {
        /// <summary>Number of entries to load.</summary>
        private const int EntryCount = 500000;

        /// <summary>Cache name.</summary>
        private const string CacheName = "dotnet_cache_data_streamer";

        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache data streamer example started.");

                // Clean up caches on all nodes before run.
                ignite.GetOrCreateCache<int, Account>(CacheName).Clear();

                Stopwatch timer = new Stopwatch();

                timer.Start();

                using (var ldr = ignite.GetDataStreamer<int, Account>(CacheName))
                {
                    ldr.PerNodeBufferSize = 1024;

                    for (int i = 0; i < EntryCount; i++)
                    {
                        ldr.AddData(i, new Account(i, i));

                        // Print out progress while loading cache.
                        if (i > 0 && i % 10000 == 0)
                            Console.WriteLine("Loaded " + i + " accounts.");
                    }
                }

                timer.Stop();

                long dur = timer.ElapsedMilliseconds;

                Console.WriteLine(">>> Loaded " + EntryCount + " accounts in " + dur + "ms.");
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
