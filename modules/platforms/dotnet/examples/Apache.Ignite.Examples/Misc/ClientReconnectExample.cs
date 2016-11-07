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
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using Apache.Ignite.Core.Events;

    /// <summary>
    /// This example demonstrates the usage of automatic client reconnect feature. 
    /// NOTE: There should be no other nodes running.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder;
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// </summary>
    public class ClientReconnectExample
    {
        private const string CacheName = "dotnet_client_reconnect_cache";

        [STAThread]
        public static void Main()
        {
            Console.WriteLine();
            Console.WriteLine(">>> Client reconnect example started.");

            ThreadPool.QueueUserWorkItem(_ => RunServer());

            Ignition.ClientMode = true;

            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine(">>> Client node connected to the cluster.");

                if (ignite.GetCluster().GetNodes().Count > 2)
                    throw new Exception("Extra nodes detected. " +
                                        "ClientReconnectExample should be run without external nodes.");

                var cache = ignite.GetCache<int, string>(CacheName);

                for (var i = 0; i < 10; i++)
                {
                    try
                    {
                        cache.Put(i, "val" + i);
                        
                        Thread.Sleep(500);

                        Console.WriteLine(">>> Put value with key:" + i);
                    }
                    catch (CacheException e)
                    {
                        var disconnectedException = e.InnerException as ClientDisconnectedException;

                        if (disconnectedException != null)
                        {
                            Console.WriteLine(">>> Client disconnected from the cluster.");

                            var task = disconnectedException.ClientReconnectTask;

                            Console.WriteLine(">>> Waiting while client gets reconnected to the cluster.");

                            task.Wait();

                            Console.WriteLine(">>> Client has reconnected successfully.");

                            // Updating the reference to the cache. The client reconnected to the new cluster.
                            cache = ignite.GetCache<int, string>(CacheName);
                        }
                        else
                        {
                            throw;
                        }
                    }

                }

                Console.WriteLine();
                Console.WriteLine(">>> Example finished, press any key to exit ...");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Runs the server node.
        /// </summary>
        private static void RunServer()
        {
            var cfg = new IgniteConfiguration
            {
                // Nodes within a single process are distinguished by GridName property.
                GridName = "serverNode",

                // Discovery settings are the same as in app.config.
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"127.0.0.1:47500"}
                    }
                },

                CacheConfiguration = new[] {new CacheConfiguration(CacheName)},

                IncludedEventTypes = new[] {EventType.NodeJoined}
            };

            // Start a server node.
            using (var ignite = Ignition.Start(cfg))
            {
                Console.WriteLine(">>> Server node started.");

                // Wait for the client node to join.
                if (ignite.GetCluster().GetNodes().Count == 1)
                    ignite.GetEvents().WaitForLocal(EventType.NodeJoined);

                // Wait some time while client node performs cache operations.
                Thread.Sleep(2000);

                // Stop the server to cause client node disconnect.
                Console.WriteLine(">>> Stopping server node...");
            }

            Console.WriteLine(">>> Server node stopped.");

            // Wait for client to detect the disconnect.
            Thread.Sleep(5000);

            Console.WriteLine(">>> Restarting server node...");

            // Start the server again.
            using (Ignition.Start(cfg))
            {
                Console.WriteLine(">>> Server node restarted.");

                Thread.Sleep(Timeout.Infinite);
            }
        }
    }
}
