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
using Apache.Ignite.Core;
using Apache.Ignite.ExamplesDll.Services;

namespace Apache.Ignite.Examples.Services
{
    /// <summary>
    /// Example demonstrating grid services.
    /// <para />
    /// This example can be run in conjunction with standalone GridGain .Net node.
    /// To start standalone node please do the following:
    /// 1) Build the project Apache.Ignite.ExamplesDll if you havent't done it yet (select it -> right-click -> Build);
    /// 2) Locate created Apache.Ignite.ExamplesDll.dll file (Apache.Ignite.ExamplesDll project -> right-click ->
    ///     Properties -> Build -> Output path);
    /// 3) Go to .Net binaries folder [IGNITE_HOME]\platforms\dotnet and run Apache.Ignite.exe as follows:
    /// Apache.Ignite.exe -IgniteHome=[path_to_IGNITE_HOME] -springConfigUrl=modules\platform\src\main\dotnet\examples\config\example-compute.xml -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// <para />
    /// To run the example please do the following:
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build);
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start application (F5 or Ctrl+F5).
    /// </summary>
    public class ServicesExample
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
                Console.WriteLine(">>> Services example started.");
                Console.WriteLine();

                // Deploy a service
                var svc = new MapService<int, string>();
                Console.WriteLine(">>> Deploying service to all nodes...");
                grid.Services().DeployNodeSingleton("service", svc);

                // Get a sticky service proxy so that we will always be contacting the same remote node.
                var prx = grid.Services().GetServiceProxy<IMapService<int, string>>("service", true);
                
                for (var i = 0; i < 10; i++)
                    prx.Put(i, i.ToString());

                var mapSize = prx.Size;

                Console.WriteLine(">>> Map service size: " + mapSize);

                grid.Services().CancelAll();
            }
        }
    }
}
