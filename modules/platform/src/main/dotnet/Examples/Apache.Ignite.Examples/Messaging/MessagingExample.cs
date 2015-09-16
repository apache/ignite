/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

using System;
using System.Collections.Generic;
using System.Threading;

namespace GridGain.Examples.Messaging
{
    /// <summary>
    /// Example demonstrating grid messaging.
    /// <para />
    /// This example should be run in conjunction with standalone GridGain .Net node.
    /// To start standalone node please do the following:
    /// 1) Build the project GridGainExamplesDll if you havent't done it yet (select it -> right-click -> Build);
    /// 2) Locate created GridGainExamplesDll.dll file (GridGainExamplesDll project -> right-click ->
    ///     Properties -> Build -> Output path);
    /// 3) Go to .Net binaries folder [GRIDGAIN_HOME]\platforms\dotnet and run GridGain.exe as follows:
    /// GridGain.exe -gridGainHome=[path_to_GRIDGAIN_HOME] -springConfigUrl=examples\config\dotnet\example-compute.xml -assembly=[path_to_GridGainExamplesDll.dll]
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project GridGainExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (GridGainExamples project -> right-click -> Properties ->
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
            var cfg = new GridConfiguration
            {
                SpringConfigUrl = @"examples\config\dotnet\example-compute.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };

            using (var grid = GridFactory.Start(cfg))
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
