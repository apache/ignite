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
using System.Threading;
using Apache.Ignite.Core;
using Apache.Ignite.ExamplesDll.Messaging;

namespace Apache.Ignite.Examples.Messaging
{
    /// <summary>
    /// Example demonstrating grid messaging.
    /// <para />
    /// This example should be run in conjunction with standalone GridGain .Net node.
    /// To start standalone node please do the following:
    /// 1) Build the project Apache.Ignite.ExamplesDll if you havent't done it yet (select it -> right-click -> Build);
    /// 2) Locate created Apache.Ignite.ExamplesDll.dll file (Apache.Ignite.ExamplesDll project -> right-click ->
    ///     Properties -> Build -> Output path);
    /// 3) Go to .Net binaries folder [IGNITE_HOME]\platforms\dotnet and run Apache.Ignite.exe as follows:
    /// Apache.Ignite.exe -gridGainHome=[path_to_IGNITE_HOME] -springConfigUrl=modules\platform\src\main\dotnet\examples\config\example-compute.xml -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// <para />
    /// As a result you will see console messages output on all nodes.
    /// </summary>
    public class MessagingExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"modules\platform\src\main\dotnet\examples\config\example-compute.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };

            using (var grid = Ignition.Start(cfg))
            {
                var remotes = grid.Cluster.ForRemotes();

                if (remotes.Nodes().Count == 0)
                {
                    Console.WriteLine(">>> This example requires remote nodes to be started.");
                    Console.WriteLine(">>> Please start at least 1 remote node.");
                    Console.WriteLine(">>> Refer to example's documentation for details on configuration.");
                }
                else
                {
                    Console.WriteLine(">>> Messaging example started.");
                    Console.WriteLine();

                    // Set up local listeners
                    var localMessaging = grid.Cluster.ForLocal().Message();

                    var msgCount = remotes.Nodes().Count * 10;

                    var orderedCounter = new CountdownEvent(msgCount);
                    var unorderedCounter = new CountdownEvent(msgCount);

                    localMessaging.LocalListen(new LocalListener(unorderedCounter), Topic.Unordered);
                    localMessaging.LocalListen(new LocalListener(orderedCounter), Topic.Ordered);

                    // Set up remote listeners
                    var remoteMessaging = remotes.Message();

                    remoteMessaging.RemoteListen(new RemoteUnorderedListener(), Topic.Unordered);
                    remoteMessaging.RemoteListen(new RemoteOrderedListener(), Topic.Ordered);

                    // Send unordered
                    Console.WriteLine(">>> Sending unordered messages...");

                    for (var i = 0; i < 10; i++)
                        remoteMessaging.Send(i, Topic.Unordered);

                    Console.WriteLine(">>> Finished sending unordered messages.");

                    // Send ordered
                    Console.WriteLine(">>> Sending ordered messages...");

                    for (var i = 0; i < 10; i++)
                        remoteMessaging.SendOrdered(i, Topic.Ordered);

                    Console.WriteLine(">>> Finished sending ordered messages.");

                    Console.WriteLine(">>> Check output on all nodes for message printouts.");
                    Console.WriteLine(">>> Waiting for messages acknowledgements from all remote nodes...");

                    unorderedCounter.Wait();
                    orderedCounter.Wait();
                }
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
