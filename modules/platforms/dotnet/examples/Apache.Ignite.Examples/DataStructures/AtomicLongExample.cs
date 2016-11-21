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

namespace Apache.Ignite.Examples.DataStructures
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.ExamplesDll.DataStructures;

    /// <summary>
    /// The example demonstrates the usage of the distributed atomic long data structure, which has functionality 
    /// similar to <see cref="Interlocked"/>, but provides cluster-wide atomicity guarantees.
    /// <para />
    /// 1) Set this class as startup object (Apache.Ignite.Examples project -> right-click -> Properties ->
    ///     Application -> Startup object);
    /// 2) Start example (F5 or Ctrl+F5).
    /// <para />
    /// This example can be run with standalone Apache Ignite.NET node:
    /// 1) Run %IGNITE_HOME%/platforms/dotnet/bin/Apache.Ignite.exe:
    /// Apache.Ignite.exe -configFileName=platforms\dotnet\examples\apache.ignite.examples\app.config -assembly=[path_to_Apache.Ignite.ExamplesDll.dll]
    /// 2) Start example.
    /// </summary>
    public static class AtomicLongExample
    {
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
            {
                Console.WriteLine();
                Console.WriteLine(">>> Atomic long example started.");

                IAtomicLong atomicLong = ignite.GetAtomicLong(AtomicLongIncrementAction.AtomicLongName, 0, true);

                Console.WriteLine(">>> Atomic long initial value: " + atomicLong.Read());

                // Broadcast an action that increments AtomicLong a number of times.
                ignite.GetCompute().Broadcast(new AtomicLongIncrementAction());

                // Actual value will depend on a number of participating nodes.
                Console.WriteLine("\n>>> Atomic long current value: " + atomicLong.Read());
            }

            Console.WriteLine("\n>>> Check output on all nodes.");
            Console.WriteLine("\n>>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
