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
using System.Collections.Generic;
using System.Diagnostics;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Datastream;
using Apache.Ignite.ExamplesDll.Portable;

namespace Apache.Ignite.Examples.Datagrid
{
    /// <summary>
    /// Demonstrates how cache can be populated with data utilizing <see cref="IDataStreamer{TK,TV}"/>.
    /// Data streamer is a lot more efficient to use than standard cache put operation 
    /// as it properly buffers cache requests together and properly manages load on remote nodes.
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run in conjunction with standalone GridGain .Net node.
    /// To start standalone node please do the following:
    /// 1) Go to .Net binaries folder [IGNITE_HOME]\platforms\dotnet and run Apache.Ignite.exe as follows:
    /// Apache.Ignite.exe -IgniteHome=[path_to_IGNITE_HOME] -springConfigUrl=modules\platform\src\main\dotnet\examples\config\example-cache.xml
    /// </summary>
    public class DataStreamerExample
    {
        /// <summary>Number of entries to load.</summary>
        private const int EntryCount = 500000;

        /// <summary>Cache name.</summary>
        private const string CacheName = "cache_data_streamer";

        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"modules\platform\src\main\dotnet\examples\config\example-cache.xml",
                JvmOptions = new List<string> {"-Xms512m", "-Xmx1024m"}
            };

            using (var ignite = Ignition.Start(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache data streamer example started.");

                // Clean up caches on all nodes before run.
                ignite.GetOrCreateCache<int, Account>(CacheName).Clear();

                Stopwatch timer = new Stopwatch();

                timer.Start();

                using (var ldr = ignite.DataStreamer<int, Account>(CacheName))
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
