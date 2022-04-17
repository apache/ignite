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

namespace Apache.Ignite.Examples.Thick.DataStructures.AtomicReference
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Examples.Shared;
    using Apache.Ignite.Examples.Shared.DataStructures;

    /// <summary>
    /// This example demonstrates the usage of the distributed atomic reference data structure.
    /// </summary>
    public static class Program
    {
        public static void Main()
        {
            using (IIgnite ignite = Ignition.Start(Utils.GetServerNodeConfiguration()))
            {
                Console.WriteLine();
                Console.WriteLine(">>> Atomic reference example started.");

                // Create atomic reference with a value of empty Guid.
                IAtomicReference<Guid> atomicRef = ignite.GetAtomicReference(
                    AtomicReferenceModifyAction.AtomicReferenceName, Guid.Empty, true);

                // Make sure initial value is set to Empty.
                atomicRef.Write(Guid.Empty);

                // Attempt to modify the value on each node. Only one node will succeed.
                ignite.GetCompute().Broadcast(new AtomicReferenceModifyAction());

                // Print current value which is equal to the Id of the node that has modified the reference first.
                Console.WriteLine("\n>>> Current atomic reference value: " + atomicRef.Read());
            }

            Console.WriteLine();
            Console.WriteLine(">>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
