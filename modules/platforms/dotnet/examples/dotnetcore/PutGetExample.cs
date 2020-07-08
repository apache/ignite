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

namespace Apache.Ignite.Examples
{
    using System;
    using System.Threading.Tasks;
    using Core;
    using Core.Binary;

    /// <summary>
    /// This example demonstrates several put-get operations on Ignite cache
    /// with binary values. Note that binary object can be retrieved in
    /// fully-deserialized form or in binary object format using special
    /// cache projection.
    /// </summary>
    public static class PutGetExample
    {
        /// <summary>Cache name.</summary>
        private const string CacheName = "dotnet_cache_put_get";

        /// <summary>
        /// Runs the example.
        /// </summary>
        public static void Run()
        {
            var ignite = Ignition.TryGetIgnite() ?? Ignition.StartFromApplicationConfiguration();
            Console.WriteLine();
            Console.WriteLine(">>> Cache put-get example started.");

            // Clean up caches on all nodes before run.
            ignite.GetOrCreateCache<object, object>(CacheName).Clear();

            PutGet(ignite);
            PutGetBinary(ignite);
            PutGetAsync(ignite).Wait();
        }

        /// <summary>
        /// Execute individual Put and Get.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void PutGet(IIgnite ignite)
        {
            var cache = ignite.GetCache<int, Organization>(CacheName);

            // Create new Organization to store in cache.
            var org = new Organization("Microsoft");

            // Put created data entry to cache.
            cache.Put(1, org);

            // Get recently created employee as a strongly-typed fully de-serialized instance.
            var orgFromCache = cache.Get(1);

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization instance from cache: " + orgFromCache);
        }

        /// <summary>
        /// Execute individual Put and Get, getting value in binary format, without de-serializing it.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void PutGetBinary(IIgnite ignite)
        {
            var cache = ignite.GetCache<int, Organization>(CacheName);

            // Create new Organization to store in cache.
            var org = new Organization("Microsoft");

            // Put created data entry to cache.
            cache.Put(1, org);

            // Create projection that will get values as binary objects.
            var binaryCache = cache.WithKeepBinary<int, IBinaryObject>();

            // Get recently created organization as a binary object.
            var binaryOrg = binaryCache.Get(1);

            // Get organization's name from binary object (note that  object doesn't need to be fully deserialized).
            var name = binaryOrg.GetField<string>("name");

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization name from binary object: " + name);
        }

        /// <summary>
        /// Execute individual Put and Get.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static async Task PutGetAsync(IIgnite ignite)
        {
            var cache = ignite.GetCache<int, Organization>(CacheName);

            // Create new Organization to store in cache.
            var org = new Organization("Microsoft");

            // Put created data entry to cache.
            await cache.PutAsync(1, org);

            // Get recently created employee as a strongly-typed fully de-serialized instance.
            var orgFromCache = await cache.GetAsync(1);

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization instance from cache asynchronously: " + orgFromCache);
        }
    }
}
