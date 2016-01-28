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

namespace Apache.Ignite.Examples.Compute
{
    using Apache.Ignite.Core;
    using Apache.Ignite.ExamplesDll.Compute;

    /// <summary>
    /// Example demonstrating anonymous function execution.
    /// <para />
    /// 1) Build the project Apache.Ignite.ExamplesDll (select it -> right-click -> Build).
    ///    Apache.Ignite.ExamplesDll.dll must appear in %IGNITE_HOME%/platforms/dotnet/examples/Apache.Ignite.ExamplesDll/bin/${Platform]/${Configuration} folder.
    /// 2) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 3) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -IgniteHome="%IGNITE_HOME%" -springConfigUrl=platforms\dotnet\examples\config\example-compute.xml -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 2) Start example.
    /// </summary>
    public class LambdaComputeExample
    {
        /// <summary>
        /// Runs the example.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            var cfg = new IgniteConfiguration
            {
                SpringConfigUrl = @"platforms\dotnet\examples\config\example-compute.xml",
                JvmOptions = new List<string> { "-Xms512m", "-Xmx1024m" }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Lambda execution example started.");

                Console.WriteLine();
                Console.WriteLine(">>> Calculating character count:");

                // Split the string by spaces to count letters in each word in parallel.
                const string data =  "Count characters using lambda expressions";

                // Anonymous functions are in the ExamplesDll assembly, which is loaded on standalone nodes.
                var totalLen = LambdaCompute.CountCharacters(ignite.GetCompute(), data);

                Console.WriteLine(">>> Total character count: " + totalLen);
                Console.WriteLine();

                Console.WriteLine(">>> Broadcasting anonymous method:");

                LambdaCompute.BroadcastConsoleMessage(ignite.GetCompute(), "Hello Node!");

                Console.WriteLine(">>> Check all nodes for hello message output.");
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
