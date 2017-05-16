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

namespace Apache.Ignite.Examples.Misc
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using Apache.Ignite.Core.Events;

    /// <summary>
    /// This example demonstrates the usage of client's automatic reconnection feature.
    /// NOTE: There must be no other cluster nodes running on the host.
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

            var evt = new ManualResetEvent(false);
            ThreadPool.QueueUserWorkItem(_ => RunServer(evt));

            // Wait a moment for server to begin startup.
            Thread.Sleep(200);

            var cfg = new IgniteConfiguration(GetIgniteConfiguration())
            {
                ClientMode = true
            };

            using (var ignite = Ignition.Start(cfg))
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
                        Console.WriteLine(">>> Put value with key: " + i);
                        cache.Put(i, "val" + i);

                        Thread.Sleep(500);
                    }
                    catch (CacheException e)
                    {
                        var disconnectedException = e.InnerException as ClientDisconnectedException;

                        if (disconnectedException != null)
                        {
                            Console.WriteLine(
                                "\n>>> Client disconnected from the cluster. Failed to put value with key: " + i);

                            disconnectedException.ClientReconnectTask.Wait();

                            Console.WriteLine("\n>>> Client reconnected to the cluster.");

                            // Updating the reference to the cache. The client reconnected to the new cluster.
                            cache = ignite.GetCache<int, string>(CacheName);
                        }
                        else
                        {
                            throw;
                        }
                    }

                }

                // Stop the server node.
                evt.Set();

                Console.WriteLine();
                Console.WriteLine(">>> Example finished, press any key to exit ...");
                Console.ReadKey();
            }
        }

        /// <summary>
        /// Runs the server node.
        /// </summary>
        /// <param name="evt"></param>
        private static void RunServer(WaitHandle evt)
        {
            var cfg = new IgniteConfiguration(GetIgniteConfiguration())
            {
                // Nodes within a single process are distinguished by GridName property.
                IgniteInstanceName = "serverNode",

                CacheConfiguration = new[] {new CacheConfiguration(CacheName)},

                IncludedEventTypes = new[] {EventType.NodeJoined}
            };

            // Start a server node.
            using (var ignite = Ignition.Start(cfg))
            {
                Console.WriteLine("\n>>> Server node started.");

                // Wait for the client node to join.
                if (ignite.GetCluster().GetNodes().Count == 1)
                    ignite.GetEvents().WaitForLocal(EventType.NodeJoined);

                // Wait some time while client node performs cache operations.
                Thread.Sleep(2000);
            }

            Console.WriteLine("\n>>> Server node stopped.");

            // Wait for client to detect the disconnect.
            Thread.Sleep(15000);

            Console.WriteLine("\n>>> Restarting server node...");

            // Start the server again.
            using (Ignition.Start(cfg))
            {
                evt.WaitOne();
            }
        }

        /// <summary>
        /// Gets the base Ignite configuration.
        /// </summary>
        private static IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration
            {
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] { "127.0.0.1:47500" }
                    }
                }
            };
        }
    }
}
