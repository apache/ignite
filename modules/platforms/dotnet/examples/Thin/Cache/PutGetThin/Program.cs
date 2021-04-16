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

namespace Apache.Ignite.Examples.Thin.Cache.PutGetThin
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.Models;

    public static class Program
    {
        /// <summary>
        /// Put/Get example.
        /// <para />
        /// This example requires an Ignite server node. You can start the node in any of the following ways:
        /// * docker run -p 10800:10800 apacheignite/ignite
        /// * dotnet run -p ServerNode.csproj
        /// * ignite.sh/ignite.bat from the distribution
        /// </summary>
        private const string CacheName = "dotnet_cache_put_get";

        public static void Main()
        {
            using (IIgniteClient ignite = Ignition.StartClient(Utils.GetThinClientConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache put-get example started.");

                // Clean up caches on all nodes before run.
                ignite.GetOrCreateCache<object, object>(CacheName).Clear();

                PutGet(ignite);
                PutGetBinary(ignite);
                PutAllGetAll(ignite);
                PutAllGetAllBinary(ignite);

                Console.WriteLine();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Execute individual Put and Get.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void PutGet(IIgniteClient ignite)
        {
            ICacheClient<int, Organization> cache = ignite.GetCache<int, Organization>(CacheName);

            // Create new Organization to store in cache.
            Organization org = new Organization(
                "Microsoft",
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                OrganizationType.Private,
                DateTime.Now
            );

            // Put created data entry to cache.
            cache.Put(1, org);

            // Get recently created employee as a strongly-typed fully de-serialized instance.
            Organization orgFromCache = cache.Get(1);

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization instance from cache: " + orgFromCache);
        }

        /// <summary>
        /// Execute individual Put and Get, getting value in binary format, without de-serializing it.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void PutGetBinary(IIgniteClient ignite)
        {
            ICacheClient<int, Organization> cache = ignite.GetCache<int, Organization>(CacheName);

            // Create new Organization to store in cache.
            Organization org = new Organization(
                "Microsoft",
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                OrganizationType.Private,
                DateTime.Now
            );

            // Put created data entry to cache.
            cache.Put(1, org);

            // Create projection that will get values as binary objects.
            var binaryCache = cache.WithKeepBinary<int, IBinaryObject>();

            // Get recently created organization as a binary object.
            var binaryOrg = binaryCache.Get(1);

            // Get organization's name from binary object (note that  object doesn't need to be fully deserialized).
            string name = binaryOrg.GetField<string>("name");

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization name from binary object: " + name);
        }

        /// <summary>
        /// Execute bulk Put and Get operations.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void PutAllGetAll(IIgniteClient ignite)
        {
            ICacheClient<int, Organization> cache = ignite.GetCache<int, Organization>(CacheName);

            // Create new Organizations to store in cache.
            Organization org1 = new Organization(
                "Microsoft",
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                OrganizationType.Private,
                DateTime.Now
            );

            Organization org2 = new Organization(
                "Red Cross",
                new Address("184 Fidler Drive, San Antonio, TX", 78205),
                OrganizationType.NonProfit,
                DateTime.Now
            );

            var map = new Dictionary<int, Organization> { { 1, org1 }, { 2, org2 } };

            // Put created data entries to cache.
            cache.PutAll(map);

            // Get recently created organizations as a strongly-typed fully de-serialized instances.
            ICollection<ICacheEntry<int, Organization>> mapFromCache = cache.GetAll(new List<int> { 1, 2 });

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization instances from cache:");

            foreach (ICacheEntry<int, Organization> org in mapFromCache)
                Console.WriteLine(">>>     " + org.Value);
        }

        /// <summary>
        /// Execute bulk Put and Get operations getting values in binary format, without de-serializing it.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void PutAllGetAllBinary(IIgniteClient ignite)
        {
            ICacheClient<int, Organization> cache = ignite.GetCache<int, Organization>(CacheName);

            // Create new Organizations to store in cache.
            Organization org1 = new Organization(
                "Microsoft",
                new Address("1096 Eddy Street, San Francisco, CA", 94109),
                OrganizationType.Private,
                DateTime.Now
            );

            Organization org2 = new Organization(
                "Red Cross",
                new Address("184 Fidler Drive, San Antonio, TX", 78205),
                OrganizationType.NonProfit,
                DateTime.Now
            );

            var map = new Dictionary<int, Organization> { { 1, org1 }, { 2, org2 } };

            // Put created data entries to cache.
            cache.PutAll(map);

            // Create projection that will get values as binary objects.
            var binaryCache = cache.WithKeepBinary<int, IBinaryObject>();

            // Get recently created organizations as binary objects.
            ICollection<ICacheEntry<int, IBinaryObject>> binaryMap = binaryCache.GetAll(new List<int> { 1, 2 });

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization names from binary objects:");

            foreach (var pair in binaryMap)
                Console.WriteLine(">>>     " + pair.Value.GetField<string>("name"));
        }
    }
}
