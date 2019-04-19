/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Examples.Services
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.ExamplesDll.Services;

    /// <summary>
    /// Example demonstrating Ignite services.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 2) Start example.
    /// </summary>
    public class ServicesExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine(">>> Services example started.");
                Console.WriteLine();

                // Deploy a service
                var svc = new MapService<int, string>();
                Console.WriteLine(">>> Deploying service to all nodes...");
                ignite.GetServices().DeployNodeSingleton("service", svc);

                // Get a sticky service proxy so that we will always be contacting the same remote node.
                var prx = ignite.GetServices().GetServiceProxy<IMapService<int, string>>("service", true);

                for (var i = 0; i < 10; i++)
                    prx.Put(i, i.ToString());

                var mapSize = prx.Size;

                Console.WriteLine(">>> Map service size: " + mapSize);

                ignite.GetServices().CancelAll();
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
