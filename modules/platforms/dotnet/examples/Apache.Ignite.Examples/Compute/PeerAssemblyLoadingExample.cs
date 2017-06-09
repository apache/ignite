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

namespace Apache.Ignite.Examples.Compute
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Example demonstrating compute execution with peer assembly loading.
    /// 
    /// Before executing the example start one or more standalone Apache Ignite.NET nodes without "-assembly"
    /// command line argument. That argument is not required on remote nodes because the assemblies are
    /// loaded automatically whenever is needed.
    /// 
    /// Modify and re-run this example while keeping standalone nodes running to see that the
    /// modified version of the computation will be executed because of automatic deployment of the updated assembly.
    /// 
    /// Version is updated automatically on build because of '*' in AssemblyVersion (see AssemblyInfo.cs).
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example must be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config
    /// 2) Start example.
    /// </summary>
    public class PeerAssemblyLoadingExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine();
                Console.WriteLine(">>> Peer loading example started.");

                var remotes = ignite.GetCluster().ForRemotes();

                if (remotes.GetNodes().Count == 0)
                {
                    throw new Exception("This example requires remote nodes to be started. " +
                                        "Please start at least 1 remote node. " +
                                        "Refer to example's documentation for details on configuration.");
                }

                Console.WriteLine(">>> Executing an action on all remote nodes...");

                // Execute action on all remote cluster nodes.
                remotes.GetCompute().Broadcast(new HelloAction());

                Console.WriteLine(">>> Action executed, check output on remote nodes.");
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }

        /// <summary>
        /// Compute action that prints a greeting and assembly version.
        /// </summary>
        private class HelloAction : IComputeAction
        {
            /// <summary>
            /// Invokes action.
            /// </summary>
            public void Invoke()
            {
                Console.WriteLine("Hello from automatically deployed assembly! Version is " +
                                  GetType().Assembly.GetName().Version);
            }
        }
    }
}
