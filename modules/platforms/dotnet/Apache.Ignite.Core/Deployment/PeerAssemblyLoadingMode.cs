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

namespace Apache.Ignite.Core.Deployment
{
    using System;
    using Apache.Ignite.Core.Compute;

    /// <summary>
    /// Peer assembly loading mode.
    /// See <see cref="IgniteConfiguration.PeerAssemblyLoadingMode"/>.
    /// </summary>
    public enum PeerAssemblyLoadingMode
    {
        /// <summary>
        /// Disabled peer assembly loading. Default mode.
        /// </summary>
        Disabled,

        /// <summary>
        /// Automatically load assemblies from remote nodes into the current <see cref="AppDomain"/>.
        /// <para />
        /// .NET does not allow assembly unloading, which means that all peer-loaded assemblies will
        /// live as long as the current AppDomain lives. This may cause increased memory usage.
        /// <para />
        /// Assemblies are distinguished using their fully qualified name. Multiple versions of the same assembly can
        /// be loaded and the correct version will be used (according to Type.AssemblyQualifiedName).
        /// So in case when a new version of some type needs to be executed on remote nodes,
        /// corresponding assembly version should be bumped up. If assembly is recompiled without version increment,
        /// it is considered the same as before and won't be updated.
        /// <para />
        /// Assemblies are requested from remote nodes on demand.
        /// For example, <see cref="IComputeFunc{TRes}"/> is sent to all nodes
        /// via <see cref="ICompute.Broadcast{TRes}"/>. Each node then deserializes the instance and,
        /// if containing assembly is not present, requests it from originating node (which did the
        /// <see cref="ICompute.Broadcast{TRes}"/> call), if it is alive, or from any other node in cluster.
        /// Therefore it is possible that eventually all nodes in cluster will have this assebly loaded.
        /// </summary>
        CurrentAppDomain
    }
}
