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

namespace Apache.Ignite.Examples.Messaging
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core;
    using Apache.Ignite.ExamplesDll.Messaging;

    /// <summary>
    /// Example demonstrating Ignite messaging. Should be run with standalone Apache Ignite.NET node.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder;
    /// 2) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 3) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 4) Start example (F5 or Ctrl+F5).
    /// </summary>
    public class MessagingExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                var remotes = ignite.GetCluster().ForRemotes();

                if (remotes.GetNodes().Count == 0)
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
                    var localMessaging = ignite.GetCluster().ForLocal().GetMessaging();

                    var msgCount = remotes.GetNodes().Count * 10;

                    var orderedCounter = new CountdownEvent(msgCount);
                    var unorderedCounter = new CountdownEvent(msgCount);

                    localMessaging.LocalListen(new LocalListener(unorderedCounter), Topic.Unordered);

                    localMessaging.LocalListen(new LocalListener(orderedCounter), Topic.Ordered);

                    // Set up remote listeners
                    var remoteMessaging = remotes.GetMessaging();

                    var idUnordered = remoteMessaging.RemoteListen(new RemoteUnorderedListener(), Topic.Unordered);
                    var idOrdered = remoteMessaging.RemoteListen(new RemoteOrderedListener(), Topic.Ordered);

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

                    // Unsubscribe
                    remoteMessaging.StopRemoteListen(idUnordered);
                    remoteMessaging.StopRemoteListen(idOrdered);
                }
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
