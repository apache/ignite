/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Examples.DataStructures
{
    using System;
    using Apache.Ignite.Core;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.ExamplesDll.DataStructures;

    /// <summary>
    /// The example demonstrates the usage of the distributed atomic reference data structure.
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
    public static class AtomicReferenceExample
    {
        [STAThread]
        public static void Main()
        {
            using (var ignite = Ignition.StartFromApplicationConfiguration())
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

            Console.WriteLine("\n>>> Example finished, press any key to exit ...");
            Console.ReadKey();
        }
    }
}
