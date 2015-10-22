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
using Apache.Ignite.Core.Portable;
using Apache.Ignite.ExamplesDll.Portable;

namespace Apache.Ignite.Examples.Datagrid
{
    /// <summary>
    /// This example demonstrates use of portable objects between different platforms.
    /// <para/>
    /// This example must be run with standalone Java node. To achieve this start a node from %IGNITE_HOME%
    /// using "ignite.bat" with proper configuration:
    /// <example>'bin\ignite.bat examples\config\example-server.xml'</example>.
    /// <para />
    /// Once remote node is started, launch this example as follows:
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties -> 
    ///     Application -> Startup object); 
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// To see how objects can be transferred between platforms, start cross-platform Java example 
    /// without restarting remote node.
    /// </summary>
    public class CrossPlatformExample
    {
        /// <summary>Key for Java object.</summary>
        private const int KeyJava = 100;

        /// <summary>Key for .Net object.</summary>
        private const int KeyDotnet = 200;

        /// <summary>Key for C++ object.</summary>
        private const int KeyCpp = 300;

        /// <summary>Cache Name.</summary>
        private const string CacheName = "cacheCrossPlatform";

        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"platforms\dotnet\examples\config\example-cache.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Cross-platform example started.");

                if (ignite.GetCluster().ForRemotes().GetNodes().Count == 0)
                {
                    Console.WriteLine();
                    Console.WriteLine(">>> This example requires remote nodes to be started.");
                    Console.WriteLine(">>> Please start at least 1 remote node.");
                    Console.WriteLine(">>> Refer to example's documentation for details on configuration.");
                    Console.WriteLine();
                }
                else
                {
                    var cache = ignite.GetOrCreateCache<int, Organization>(CacheName);

                    // Create new Organization object to store in cache.
                    Organization org = new Organization(
                        "Apache",
                        new Address("1065 East Hillsdale Blvd, Foster City, CA", 94404),
                        OrganizationType.Private,
                        DateTime.Now
                    );

                    // Put created data entry to cache.
                    cache.Put(KeyDotnet, org);

                    // Retrieve value stored by Java client.
                    GetFromJava(ignite);

                    // Retrieve value stored by C++ client.
                    GetFromCpp(ignite);

                    // Gets portable value from cache in portable format, without de-serializing it.
                    GetDotNetPortableInstance(ignite);

                    // Gets portable value form cache as a strongly-typed fully de-serialized instance.
                    GetDotNetTypedInstance(ignite);

                    Console.WriteLine();
                }
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Gets entry put by Java client. In order for entry to be in cache, Java client example
        /// must be run before this example.
        /// </summary>
        /// <param name="Ignite">Ignite instance.</param>
        private static void GetFromJava(IIgnite ignite)
        {
            var cache = ignite.GetOrCreateCache<int, IPortableObject>(CacheName)
                .WithKeepPortable<int, IPortableObject>().WithAsync();

            cache.Get(KeyJava);

            var orgPortable = cache.GetFuture<IPortableObject>().ToTask().Result;

            if (orgPortable == null)
            {
                Console.WriteLine(">>> Java client hasn't put entry to cache. Run Java example before this example " +
                    "to see the output.");
            }
            else
            {
                Console.WriteLine(">>> Entry from Java client:");
                Console.WriteLine(">>>     Portable:     " + orgPortable);
                Console.WriteLine(">>>     Deserialized: " + orgPortable.Deserialize<Organization>());
            }
        }

        /// <summary>
        /// Gets entry put by C++ client. In order for entry to be in cache, C++ client example
        /// must be run before this example.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void GetFromCpp(IIgnite ignite)
        {
            var cache = ignite.GetOrCreateCache<int, IPortableObject>(CacheName)
                .WithKeepPortable<int, IPortableObject>().WithAsync();

            cache.Get(KeyCpp);

            var orgPortable = cache.GetFuture<IPortableObject>().Get();

            Console.WriteLine();

            if (orgPortable == null)
            {
                Console.WriteLine(">>> CPP client hasn't put entry to cache. Run CPP example before this example " +
                    "to see the output.");
            }
            else
            {
                Console.WriteLine(">>> Entry from CPP client:");
                Console.WriteLine(">>>     Portable:     " + orgPortable);
                Console.WriteLine(">>>     Deserialized: " + orgPortable.Deserialize<Organization>());
            }
        }

        /// <summary>
        /// Gets portable value from cache in portable format, without de-serializing it.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void GetDotNetPortableInstance(IIgnite ignite)
        {
            // Apply "KeepPortable" flag on data projection.
            var cache = ignite.GetOrCreateCache<int, IPortableObject>(CacheName)
                .WithKeepPortable<int, IPortableObject>();

            var org = cache.Get(KeyDotnet);

            string name = org.GetField<string>("name");

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization name from portable field: " + name);
        }

        /// <summary>
        /// Gets portable value form cache as a strongly-typed fully de-serialized instance.
        /// </summary>
        /// <param name="ignite">Ignite instance.</param>
        private static void GetDotNetTypedInstance(IIgnite ignite)
        {
            var cache = ignite.GetOrCreateCache<int, Organization>(CacheName);

            // Get recently created employee as a strongly-typed fully de-serialized instance.
            Organization emp = cache.Get(KeyDotnet);

            string name = emp.Name;

            Console.WriteLine();
            Console.WriteLine(">>> Retrieved organization name from deserialized Organization instance: " + name);
        }
    }
}
