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
    using System.Reflection;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Deployment;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Loads assemblies from other nodes.
    /// </summary>
    internal sealed class PeerAssemblyResolver : IDisposable
    {
        /** Assembly resolve handler. */
        private readonly ResolveEventHandler _handler;

        /// <summary>
        /// Initializes a new instance of the <see cref="PeerAssemblyResolver"/> class.
        /// </summary>
        public PeerAssemblyResolver(IIgniteInternal ignite, Guid originNodeId)
        {
            Debug.Assert(ignite != null);

            _handler = (sender, args) => GetAssembly(ignite, args.Name, originNodeId);

            // AssemblyResolve handler is called only when aseembly can't be found via normal lookup,
            // so we won't end up loading assemblies that are already present.
            AppDomain.CurrentDomain.AssemblyResolve += _handler;
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            AppDomain.CurrentDomain.AssemblyResolve -= _handler;
        }

        /// <summary>
        /// Gets an instance of <see cref="PeerAssemblyResolver"/> when peer loading is enabled; otherwise null.
        /// </summary>
        public static PeerAssemblyResolver GetInstance(IIgniteInternal ignite, Guid originNodeId)
        {
            if (ignite == null || ignite.Configuration.PeerAssemblyLoadingMode == PeerAssemblyLoadingMode.Disabled)
            {
                return null;
            }

            return new PeerAssemblyResolver(ignite, originNodeId);
        }

        /// <summary>
        /// Gets the assembly from remote nodes.
        /// </summary>
        /// <param name="typeName">Assembly-qualified type name.</param>
        /// <param name="ignite">Ignite.</param>
        /// <param name="originNodeId">Originating node identifier.</param>
        /// <returns>
        /// Resulting type or null.
        /// </returns>
        public static Type LoadAssemblyAndGetType(string typeName, IIgniteInternal ignite, Guid originNodeId)
        {
            Debug.Assert(!string.IsNullOrEmpty(typeName));

            var parsedName = TypeNameParser.Parse(typeName);

            var assemblyName = parsedName.GetAssemblyName();

            Debug.Assert(assemblyName != null);

            var asm = GetAssembly(ignite, assemblyName, originNodeId);

            if (asm == null)
            {
                return null;
            }

            // Assembly.GetType does not work for assembly-qualified names. Full name is required without assembly.
            return asm.GetType(parsedName.GetFullName(), false);
        }

        /// <summary>
        /// Gets the assembly.
        /// </summary>
        private static Assembly GetAssembly(IIgniteInternal ignite, string assemblyName, Guid originNodeId)
        {
            return LoadedAssembliesResolver.Instance.GetAssembly(assemblyName)
                   ?? AssemblyLoader.GetAssembly(assemblyName)
                   ?? LoadAssembly(ignite, assemblyName, originNodeId);
        }

        /// <summary>
        /// Loads the assembly.
        /// </summary>
        private static Assembly LoadAssembly(IIgniteInternal ignite, string assemblyName, Guid originNodeId)
        {
            var res = RequestAssembly(assemblyName, ignite, originNodeId);

            if (res == null)
                return null;

            return AssemblyLoader.LoadAssembly(res.AssemblyBytes, assemblyName);
        }

        /// <summary>
        /// Gets the assembly from remote nodes.
        /// </summary>
        /// <param name="assemblyName">Name of the assembly.</param>
        /// <param name="ignite">Ignite.</param>
        /// <param name="originNodeId">The origin node identifier.</param>
        /// <returns>
        /// Successful result or null.
        /// </returns>
        /// <exception cref="IgniteException"></exception>
        private static AssemblyRequestResult RequestAssembly(string assemblyName, IIgniteInternal ignite, 
            Guid originNodeId)
        {
            Debug.Assert(assemblyName != null);
            Debug.Assert(ignite != null);

            if (ignite.Configuration.PeerAssemblyLoadingMode == PeerAssemblyLoadingMode.Disabled)
                return null;

            Debug.WriteLine("Requesting assembly from other nodes: " + assemblyName);

            // New nodes are not tracked during the loop, since some of the existing nodes caused this call.
            var func = new GetAssemblyFunc();
            var req = new AssemblyRequest(assemblyName);

            foreach (var node in GetDotNetNodes(ignite.GetIgnite(), originNodeId))
            {
                var compute = ignite.GetIgnite().GetCluster().ForNodeIds(node).GetCompute();
                var result = ComputeApplySafe(compute, func, req);

                if (result != null)
                {
                    if (result.AssemblyBytes != null)
                    {
                        return result;
                    }

                    if (result.Message != null)
                    {
                        throw new IgniteException(result.Message);
                    }
                }
            }

            return null;
        }

        /// <summary>
        /// Gets the dot net nodes, origin node comes first.
        /// </summary>
        private static IEnumerable<Guid> GetDotNetNodes(IIgnite ignite, Guid originNodeId)
        {
            if (originNodeId != Guid.Empty)
            {
                yield return originNodeId;
            }

            foreach (var node in ignite.GetCluster().ForDotNet().ForRemotes().GetNodes())
            {
                if (node.Id != originNodeId)
                {
                    yield return node.Id;
                }
            }
        }

        /// <summary>
        /// Performs computation ignoring leaving nodes.
        /// </summary>
        private static AssemblyRequestResult ComputeApplySafe(ICompute compute, GetAssemblyFunc func,
            AssemblyRequest req)
        {
            try
            {
                return compute.Apply(func, req);
            }
            catch (ClusterGroupEmptyException)
            {
                // Normal situation: node has left.
                return null;
            }
            catch (AggregateException aex)
            {
                // Normal situation: node has left.
                aex.Handle(e => e is ClusterGroupEmptyException);
                return null;
            }
        }
    }
}
