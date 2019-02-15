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

namespace Apache.Ignite.Core.Impl.Deployment
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Reflection;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Handles assembly loading and serialization.
    /// </summary>
    internal static class AssemblyLoader
    {
        /// <summary>
        /// Cache of assemblies that are peer-loaded from byte array.
        /// Keep these byte arrays to be able to send them further, because Location for such assemblies is empty.
        /// </summary>
        private static readonly CopyOnWriteConcurrentDictionary<string, KeyValuePair<Assembly, byte[]>>
            InMemoryAssemblies
                = new CopyOnWriteConcurrentDictionary<string, KeyValuePair<Assembly, byte[]>>();

        /// <summary>
        /// Loads the assembly from bytes outside of any context.
        /// Resulting assembly can only be retrieved with <see cref="GetAssembly"/> call later.
        /// It won't be located with <see cref="Type.GetType()"/> call.
        /// </summary>
        public static Assembly LoadAssembly(byte[] bytes, string assemblyName)
        {
            Debug.Assert(bytes != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(assemblyName));

            return InMemoryAssemblies.GetOrAdd(assemblyName, _ =>
            {
                // Load is better for us than LoadFrom: we want to track loaded assemblies manually.
                // LoadFrom can cause exceptions when multiple versions of the same assembly exist.
                var asm = Assembly.Load(bytes);

                Debug.Assert(assemblyName == asm.FullName);

                return new KeyValuePair<Assembly, byte[]>(asm, bytes);
            }).Key;
        }

        /// <summary>
        /// Gets the assembly.
        /// </summary>
        public static byte[] GetAssemblyBytes(string assemblyName)
        {
            Debug.Assert(!string.IsNullOrWhiteSpace(assemblyName));

            KeyValuePair<Assembly, byte[]> res;

            return InMemoryAssemblies.TryGetValue(assemblyName, out res) ? res.Value : null;
        }

        /// <summary>
        /// Gets the assembly.
        /// </summary>
        public static Assembly GetAssembly(string assemblyName)
        {
            Debug.Assert(!string.IsNullOrWhiteSpace(assemblyName));

            KeyValuePair<Assembly, byte[]> res;

            return InMemoryAssemblies.TryGetValue(assemblyName, out res) ? res.Key : null;
        }

        /// <summary>
        /// Gets the assembly bytes.
        /// </summary>
        public static byte[] GetAssemblyBytes(Assembly assembly)
        {
            Debug.Assert(assembly != null);
            Debug.Assert(!assembly.IsDynamic);

            KeyValuePair<Assembly, byte[]> pair;

            if (InMemoryAssemblies.TryGetValue(assembly.FullName, out pair))
                return pair.Value;

            if (string.IsNullOrEmpty(assembly.Location))
                return null;

            return File.ReadAllBytes(assembly.Location);
        }
    }
}
