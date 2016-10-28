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
using System.Threading;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Cache;
using Apache.Ignite.Core.Common;

namespace Apache.Ignite.Examples.Misc
{
    /// <summary>
    /// This example demonstrates the usage of automatic client reconnect feature. 
    /// Should be run with standalone Apache Ignite.NET node.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder;
    /// 2) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 3) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 4) Start example (F5 or Ctrl+F5).
    /// </summary>
    public class ClientReconnectExample
    {
        private const string CacheName = "dotnet_client_reconnect_cache";

        [STAThread]
        public static void Main()
        {
            Console.WriteLine();
            Console.WriteLine(">>> Client reconnect example started.");

            Ignition.ClientMode = true;

            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine(">>> Client node connected to the cluster");

                var cache = ignite.GetOrCreateCache<int, string>(CacheName);

                Random rand = new Random();

                while (true)
                {
                    try
                    {
                        int key = rand.Next(10000);

                        cache.Put(key, "val" + key);

                        Thread.Sleep(3000);

                        Console.WriteLine(">>> Put value with key:" + key);
                    }
                    catch (CacheException e)
                    {
                        if (e.InnerException is ClientDisconnectedException)
                        {
                            Console.WriteLine(">>> Client disconnected from the cluster");

                            ClientDisconnectedException ex = (ClientDisconnectedException)e.InnerException;

                            var task = ex.ClientReconnectTask;

                            Console.WriteLine(">>> Waiting while client gets reconnected to the cluster");

                            while (!task.IsCompleted) // workaround.
                                task.Wait();

                            Console.WriteLine(">>> Client has reconnected successfully");

                            // TODO
                            Thread.Sleep(3000);

                            // Updating the reference to the cache. The client reconnected to the new cluster.
                            cache = ignite.GetOrCreateCache<int, string>(CacheName);
                        }
                        else
                        {
                            Console.WriteLine(e);

                            break;
                        }
                    }

                }

                Console.WriteLine();
                Console.WriteLine(">>> Example finished, press any key to exit ...");
                Console.ReadKey();
            }
        }
    }
}
