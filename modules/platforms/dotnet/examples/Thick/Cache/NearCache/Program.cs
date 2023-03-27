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

namespace Apache.Ignite.Examples.Thick.Cache.NearCache
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Examples.Shared;

    /// <summary>
    /// Example demonstrates the usage of a near cache on an Ignite client node side.
    /// <para />
    /// This example requires an Ignite server node. You can start the node in any of the following ways:
    /// * docker run -p 10800:10800 apacheignite/ignite
    /// * dotnet run -p ServerNode.csproj
    /// * ignite.sh/ignite.bat from the distribution
    /// </summary>
    public static class Program
    {
        private const string CacheName = "dotnet_near_cache_example";

        public static void Main()
        {
            using (var ignite = Ignition.Start(Utils.GetClientNodeConfiguration()))
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
