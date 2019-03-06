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
    using System.Diagnostics;
    using System.IO;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Compute func that returns assembly for a specified name.
    /// </summary>
    internal class GetAssemblyFunc : IComputeFunc<AssemblyRequest, AssemblyRequestResult>, IBinaryWriteAware
    {
        /** <inheritdoc /> */
        public AssemblyRequestResult Invoke(AssemblyRequest arg)
        {
            if (arg == null)
            {
                throw new IgniteException("GetAssemblyFunc does not allow null arguments.");
            }

            if (arg.AssemblyName == null)
            {
                throw new IgniteException("GetAssemblyFunc does not allow null AssemblyName.");
            }

            Debug.WriteLine("Peer assembly request: " + arg.AssemblyName);

            // Try assemblies in main context.
            var asm = LoadedAssembliesResolver.Instance.GetAssembly(arg.AssemblyName);

            if (asm != null)
            {
                if (asm.IsDynamic)
                {
                    return new AssemblyRequestResult(null, 
                        "Peer assembly loading does not support dynamic assemblies: " + asm);
                }

                return new AssemblyRequestResult(AssemblyLoader.GetAssemblyBytes(asm), null);
            }

            // Try cached assemblies.
            var bytes = AssemblyLoader.GetAssemblyBytes(arg.AssemblyName);

            if (bytes != null)
            {
                return new AssemblyRequestResult(bytes, null);
            }

            // Assembly may be present but not loaded - attempt to load into main context.
            try
            {
                asm = Assembly.Load(arg.AssemblyName);

                if (asm != null)
                {
                    return new AssemblyRequestResult(AssemblyLoader.GetAssemblyBytes(asm), null);
                }
            }
            catch (FileNotFoundException)
            {
                return null;
            }
            catch (FileLoadException ex)
            {
                return new AssemblyRequestResult(null, string.Format("Failed to load assembly: {0} ({1})", asm, ex));
            }

            catch (BadImageFormatException ex)
            {
                return new AssemblyRequestResult(null, string.Format("Failed to load assembly: {0} ({1})", asm, ex));
            }

            return null;
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            // No-op.
        }
    }
}
