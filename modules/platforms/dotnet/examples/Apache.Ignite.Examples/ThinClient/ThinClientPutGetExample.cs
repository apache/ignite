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

namespace Apache.Ignite.Examples.ThinClient
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.ExamplesDll.Binary;

    /// <summary>
    /// Demonstrates Ignite.NET "thin" client cache operations.
    /// <para />
    /// 1) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 2) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example must be run with standalone Apache Ignite node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config
    /// 2) Start example.
    /// <para />
    /// This example can also work via pure Java node started with ignite.bat/ignite.sh.
    /// </summary>
    public static class ThinClientPutGetExample
    {
        /// <summary> Cache name. </summary>
        private const string CacheName = "thinClientCache";

        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteClientConfiguration
            {
                Host = "127.0.0.1"
            };

            using (IIgniteClient igniteClient = Ignition.StartClient(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache put-get client example started.");

                ICacheClient<int, Organization> cache = igniteClient.GetOrCreateCache<int, Organization>(CacheName);

                PutGet(cache);
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Execute individual Put and Get.
        /// </summary>
        /// <param name="cache">Cache instance.</param>
        private static void PutGet(ICacheClient<int, Organization> cache)
        {
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
    }
}
