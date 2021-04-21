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

namespace Apache.Ignite.Examples.Thick.DataStructures.AtomicSequence
{
    using System;
    using System.Threading;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.DataStructures;

    /// <summary>
    /// The example demonstrates the usage of the distributed atomic sequence data structure, which has functionality
    /// similar to <see cref="Interlocked"/>, but provides cluster-wide atomicity guarantees.
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Atomic sequence example started.");

                IAtomicSequence atomicSequence =
                    ignite.GetAtomicSequence(AtomicSequenceIncrementAction.AtomicSequenceName, 0, true);

                Console.WriteLine(">>> Atomic sequence initial value: " + atomicSequence.Read());

                // Broadcast an action that increments AtomicSequence a number of times.
                ignite.GetCompute().Broadcast(new AtomicSequenceIncrementAction());

                // Actual value will depend on number of participating nodes.
                Console.WriteLine("\n>>> Atomic sequence current value: " + atomicSequence.Read());
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
