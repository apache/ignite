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
using System.Linq;
using Apache.Ignite.Core;
using Apache.Ignite.Core.Events;
using Apache.Ignite.ExamplesDll.Compute;
using Apache.Ignite.ExamplesDll.Events;
using Apache.Ignite.ExamplesDll.Portable;

namespace Apache.Ignite.Examples.Events
{
    /// <summary>
    /// Example demonstrating grid events.
    /// <para />
    /// This example can be run in conjunction with standalone Apache Ignite .Net node.
    /// To start standalone node please do the following:
    /// 1) Build the project Apache.Ignite.ExamplesDll if you havent't done it yet (select it -> right-click -> Build);
    /// 2) Locate created Apache.Ignite.ExamplesDll.dll file (Apache.Ignite.ExamplesDll project -> right-click -> Properties -> Build -> Output path);
    /// 3) Locate Apache.Ignite.exe file (Apache.Ignite project -> right-click -> Properties -> Build -> Output path)
    /// Apache.Ignite.exe -IgniteHome=[path_to_IGNITE_HOME] -springConfigUrl=modules\platform\src\main\dotnet\examples\config\example-compute.xml -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
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
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"modules\platform\src\main\dotnet\examples\config\example-compute.xml",
                JvmOptions = new List<string> {"-Xms512m", "-Xmx1024m"}
            };

            using (var grid = Ignition.Start(cfg))
            {
                Console.WriteLine(">>> Events example started.");
                Console.WriteLine();

                // Local listen example
                Console.WriteLine(">>> Listening for a local event...");

                var listener = new LocalListener();
                grid.GetEvents().LocalListen(listener, EventType.EvtsTaskExecution);

                ExecuteTask(grid);

                grid.GetEvents().StopLocalListen(listener);

                Console.WriteLine(">>> Received events count: " + listener.EventsReceived);
                Console.WriteLine();

                // Remote listen example (start standalone nodes for better demonstration)
                Console.WriteLine(">>> Listening for remote events...");

                var localListener = new LocalListener();
                var remoteFilter = new RemoteFilter();

                var listenId = grid.GetEvents().RemoteListen(localListener: localListener, remoteFilter: remoteFilter,
                        types: EventType.EvtsJobExecution);

                ExecuteTask(grid);

                grid.GetEvents().StopRemoteListen(listenId);

                Console.WriteLine(">>> Received events count: " + localListener.EventsReceived);
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Executes a task to generate events.
        /// </summary>
        /// <param name="ignite">The grid.</param>
        private static void ExecuteTask(IIgnite ignite)
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

            ignite.GetCompute().Execute(new AverageSalaryTask(), employees);
        }
    }
}
