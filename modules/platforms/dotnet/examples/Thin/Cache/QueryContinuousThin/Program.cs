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

namespace Apache.Ignite.Examples.Thin.Cache.QueryContinuousThin
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache.Event;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache.Query.Continuous;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Cache;

    /// <summary>
    /// This example demonstrates how continuous query provides a way to subscribe to cache updates.
    /// <para />
    /// This example requires an Ignite server node with <see cref="ContinuousQueryFilter"/> type loaded,
    /// run ServerNode project to start it:
    /// * dotnet run -p ServerNode.csproj
    /// </summary>
    public static class Program
    {
        private const string CacheName = "dotnet_cache_continuous_query";

        public static void Main()
        {
            using (IIgniteClient ignite = Ignition.StartClient(Utils.GetThinClientConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache continuous query example started.");

                var cache = ignite.GetOrCreateCache<int, string>(CacheName);

                // Clean up caches on all nodes before run.
                cache.Clear();

                const int keyCnt = 20;

                for (int i = 0; i < keyCnt; i++)
                    cache.Put(i, i.ToString());

                var qry = new ContinuousQueryClient<int, string>
                {
                    Listener = new Listener<string>(),
                    Filter = new ContinuousQueryFilter(15)
                };

                // Create new continuous query.
                using (cache.QueryContinuous(qry))
                {
                    // Add a few more keys and watch more query notifications.
                    for (var i = keyCnt; i < keyCnt + 5; i++)
                        cache.Put(i, i.ToString());

                    // Wait for a while while callback is notified about remaining puts.
                    Thread.Sleep(2000);
                }
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Continuous query event handler.
        /// </summary>
        private class Listener<T> : ICacheEntryEventListener<int, T>
        {
            public void OnEvent(IEnumerable<ICacheEntryEvent<int, T>> events)
            {
                foreach (var e in events)
                    Console.WriteLine("Queried entry [key=" + e.Key + ", val=" + e.Value + ']');
            }
        }
    }
}
