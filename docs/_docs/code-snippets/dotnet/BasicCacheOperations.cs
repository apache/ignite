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
using Apache.Ignite.Core;
using Apache.Ignite.Core.Compute;

namespace dotnet_helloworld
{
    public class BasicCacheOperations
    {
        public static void AtomicOperations()
        {
            // tag::atomicOperations1[]
            using (var ignite = Ignition.Start("examples/config/example-cache.xml"))
            {
                var cache = ignite.GetCache<int, string>("cache_name");

                for (var i = 0; i < 10; i++)
                {
                    cache.Put(i, i.ToString());
                }

                for (var i = 0; i < 10; i++)
                {
                    Console.Write("Got [key=" + i + ", val=" + cache.Get(i) + ']');
                }
            }
            // end::atomicOperations1[]

            // tag::atomicOperations2[]
            using (var ignite = Ignition.Start("examples/config/example-cache.xml"))
            {
                var cache = ignite.GetCache<string, int>("cache_name");

                // Put-if-absent which returns previous value.
                var oldVal = cache.GetAndPutIfAbsent("Hello", 11);

                // Put-if-absent which returns boolean success flag.
                var success = cache.PutIfAbsent("World", 22);

                // Replace-if-exists operation (opposite of getAndPutIfAbsent), returns previous value.
                oldVal = cache.GetAndReplace("Hello", 11);

                // Replace-if-exists operation (opposite of putIfAbsent), returns boolean success flag.
                success = cache.Replace("World", 22);

                // Replace-if-matches operation.
                success = cache.Replace("World", 2, 22);

                // Remove-if-matches operation.
                success = cache.Remove("Hello", 1);
            }
            // end::atomicOperations2[]
        }

        // tag::asyncExec[]
        class HelloworldFunc : IComputeFunc<string>
        {
            public string Invoke()
            {
                return "Hello World";
            }
        }
        
        public static void AsynchronousExecution()
        {
            var ignite = Ignition.Start();
            var compute = ignite.GetCompute();
            
            //Execute a closure asynchronously
            var fut = compute.CallAsync(new HelloworldFunc());
            
            // Listen for completion and print out the result
            fut.ContinueWith(Console.Write);
        }
        // end::asyncExec[]
    }
}
