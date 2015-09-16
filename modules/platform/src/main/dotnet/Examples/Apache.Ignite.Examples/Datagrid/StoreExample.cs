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
using Apache.Ignite.Core;
using Apache.Ignite.Examples.Dll.Datagrid;
using Apache.Ignite.Examples.Dll.Portable;

namespace Apache.Ignite.Examples.Datagrid
{
    /// <summary>
    /// Example demonstrating cache store.
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project GridGainExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (GridGainExamples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run in conjunction with standalone GridGain .Net node.
    /// To start standalone node please do the following:
    /// 1) Build the project GridGainExamplesDll if you haven't done it yet (select it -> right-click -> Build);
    /// 2) Locate created GridGainExamplesDll.dll file (GridGainExamplesDll project -> right-click ->
    ///     Properties -> Build -> Output path);
    /// 3) Go to .Net binaries folder [GRIDGAIN_HOME]\platforms\dotnet and run GridGain.exe as follows:
    /// GridGain.exe -gridGainHome=[path_to_GRIDGAIN_HOME] -springConfigUrl=examples\config\dotnet\example-cache-store.xml -assembly=[path_to_GridGainExamplesDll.dll]
    /// </summary>
    class StoreExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"examples\config\dotnet\example-cache-store.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cache store example started.");

                var cache = ignite.Cache<int, Employee>(null);

                // Clean up caches on all nodes before run.
                cache.Clear();

                Console.WriteLine();
                Console.WriteLine(">>> Cleared values from cache.");
                Console.WriteLine(">>> Current cache size: " + cache.Size());

                // Load entries from store which pass provided filter.
                cache.LoadCache(new EmployeeStorePredicate());

                Console.WriteLine();
                Console.WriteLine(">>> Loaded entry from store through ICache.LoadCache().");
                Console.WriteLine(">>> Current cache size: " + cache.Size());
                
                // Load entry from store calling ICache.Get() method.
                Employee emp = cache.Get(2);

                Console.WriteLine();
                Console.WriteLine(">>> Loaded entry from store through ICache.Get(): " + emp);
                Console.WriteLine(">>> Current cache size: " + cache.Size());

                // Put an entry to the cache
                cache.Put(3, new Employee(
                    "James Wilson",
                    12500,
                    new Address("1096 Eddy Street, San Francisco, CA", 94109),
                    new List<string> { "Human Resources", "Customer Service" }
                    ));

                Console.WriteLine();
                Console.WriteLine(">>> Put entry to cache. ");
                Console.WriteLine(">>> Current cache size: " + cache.Size());

                // Clear values again.
                cache.Clear();
                
                Console.WriteLine();
                Console.WriteLine(">>> Cleared values from cache again.");
                Console.WriteLine(">>> Current cache size: " + cache.Size());

                // Read values from cache after clear.
                Console.WriteLine();
                Console.WriteLine(">>> Read values after clear:");

                for (int i = 1; i <= 3; i++)
                    Console.WriteLine(">>>     Key=" + i + ", value=" + cache.Get(i));
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
