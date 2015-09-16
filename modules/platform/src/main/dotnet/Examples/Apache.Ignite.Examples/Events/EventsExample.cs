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
using System.Linq;
using GridGain.Events;
using GridGain.Examples.Compute;
using GridGain.Examples.Portable;

namespace GridGain.Examples.Events
{
    /// <summary>
    /// Example demonstrating grid events.
    /// <para />
    /// This example can be run in conjunction with standalone GridGain .Net node.
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
    /// As a result you will see console events output on all nodes.
    /// </summary>
    public class EventsExample
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
                JvmOptions = new List<string> {"-Xms512m", "-Xmx1024m"}
            };

            using (var grid = GridFactory.Start(cfg))
            {
                Console.WriteLine(">>> Events example started.");
                Console.WriteLine();

                // Local listen example
                Console.WriteLine(">>> Listening for a local event...");

                var listener = new LocalListener();
                grid.Events().LocalListen(listener, EventType.EVTS_TASK_EXECUTION);

                ExecuteTask(grid);

                grid.Events().StopLocalListen(listener);

                Console.WriteLine(">>> Received events count: " + listener.EventsReceived);
                Console.WriteLine();

                // Remote listen example (start standalone nodes for better demonstration)
                Console.WriteLine(">>> Listening for remote events...");

                var localListener = new LocalListener();
                var remoteFilter = new RemoteFilter();

                var listenId = grid.Events().RemoteListen(localListener: localListener, remoteFilter: remoteFilter,
                        types: EventType.EVTS_JOB_EXECUTION);

                ExecuteTask(grid);

                grid.Events().StopRemoteListen(listenId);

                Console.WriteLine(">>> Received events count: " + localListener.EventsReceived);
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Executes a task to generate events.
        /// </summary>
        /// <param name="grid">The grid.</param>
        private static void ExecuteTask(IGrid grid)
        {
            var employees = Enumerable.Range(1, 10).SelectMany(x => new[]
            {
                new Employee("Allison Mathis",
                    25300,
                    new Address("2702 Freedom Lane, San Francisco, CA", 94109),
                    new[] {"Development"}),

                new Employee("Breana Robbin",
                    6500,
                    new Address("3960 Sundown Lane, Austin, TX", 78130),
                    new[] {"Sales"})
            }).ToArray();

            grid.Compute().Execute(new AverageSalaryTask(), employees);
        }
    }
}
