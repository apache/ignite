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

namespace Apache.Ignite.Examples.Thick.DataStructures.AtomicLong
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.DataStructures;

    /// <summary>
    /// The example demonstrates the usage of the distributed atomic long data structure, which has functionality
    /// similar to <see cref="Interlocked"/>, but provides cluster-wide atomicity guarantees.
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
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

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
