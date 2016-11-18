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
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;

    /// <summary>
    /// Example demonstrates the usage of a near cache on an Ignite client node side.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example must be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config
    /// 2) Start example.
    /// </summary>
    public class NearCacheExample
    {
        private const string CacheName = "dotnet_near_cache_example";

        [STAThread]
        public static void Main()
        {
            // Make sure to start an Ignite server node before.
            Ignition.ClientMode = true;

            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine(">>> Client node connected to the cluster");

                // Creating a distributed and near cache.
                var nearCacheCfg = new NearCacheConfiguration
                {
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        // Near cache will store only 10 recently accessed/used entries.
                        MaxSize = 10
                    }
                };

                Console.WriteLine(">>> Populating the cache...");

                ICache<int, int> cache = ignite.GetOrCreateCache<int, int>(
                    new CacheConfiguration(CacheName), nearCacheCfg);

                // Adding data into the cache. 
                // Latest 10 entries will be stored in the near cache on the client node side.
                for (int i = 0; i < 1000; i++)
                    cache.Put(i, i * 10);

                Console.WriteLine(">>> Cache size: [Total={0}, Near={1}]", 
                    cache.GetSize(), cache.GetSize(CachePeekMode.Near));

                Console.WriteLine("\n>>> Reading from near cache...");

                foreach (var entry in cache.GetLocalEntries(CachePeekMode.Near))
                    Console.WriteLine(entry);

                Console.WriteLine("\n>>> Example finished, press any key to exit ...");
                Console.ReadKey();
            }
        }
    }
}
