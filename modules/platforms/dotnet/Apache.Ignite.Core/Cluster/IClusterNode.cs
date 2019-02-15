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

namespace Apache.Ignite.Core.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Interface representing a single cluster node. Use <see cref="GetAttribute{T}"/> or
    /// <see cref="GetMetrics"/> to get static and dynamic information about remote nodes.
    /// You can get a list of all nodes in grid by calling <see cref="IClusterGroup.GetNodes"/> 
    /// on <see cref="IIgnite"/> instance.
    /// <para />
    /// You can use Ignite node attributes to provide static information about a node.
    /// This information is initialized once within grid, during node startup, and
    /// remains the same throughout the lifetime of a node. 
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IClusterNode : IBaselineNode
    {
        /// <summary>
        /// Globally unique node ID. A new ID is generated every time a node restarts.
        /// </summary>
        Guid Id { get; }

        /// <summary>
        /// Gets node's attribute. Attributes are assigned to nodes at startup.
        /// <para />
        /// Note that attributes cannot be changed at runtime.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <returns>Attribute value.</returns>
        T GetAttribute<T>(string name);

        /// <summary>
        /// Try getting node's attribute. Attributes are assigned to nodes at startup.
        /// <para />
        /// Note that attributes cannot be changed at runtime.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="attr">Attribute value.</param>
        /// <returns><c>true</c> in case such attribute exists.</returns>
        bool TryGetAttribute<T>(string name, out T attr);

        /// <summary>
        /// Gets all node attributes. Attributes are assigned to nodes at startup.
        /// <para />
        /// Note that attributes cannot be changed at runtime.
        /// </summary>
        /// <returns>All node attributes.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        [Obsolete("Use Attributes property.")]
        IDictionary<string, object> GetAttributes();

        /// <summary>
        /// Collection of addresses this node is known by. 
        /// </summary>
        /// <returns>Collection of addresses.</returns>
        ICollection<string> Addresses { get; }

        /// <summary>
        /// Collection of host names this node is known by.
        /// </summary>
        /// <returns>Collection of host names.</returns>
        ICollection<string> HostNames { get; }

        /// <summary>
        /// Node order within grid topology. Discovery SPIs that support node ordering will
        /// assign a proper order to each node and will guarantee that discovery event notifications
        /// for new nodes will come in proper order. All other SPIs not supporting ordering
        /// may choose to return node startup time here.
        /// </summary>
        long Order { get; }

        /// <summary>
        /// Tests whether or not this node is a local node.
        /// </summary>
        bool IsLocal { get; }

        /// <summary>
        /// Tests whether or not this node is a daemon.
        /// <p/>
        /// Daemon nodes are the usual Ignite nodes that participate in topology but not
        /// visible on the main APIs, i.e. they are not part of any projections.
        /// <p/>
        /// Daemon nodes are used primarily for management and monitoring functionality that
        /// is build on Ignite and needs to participate in the topology but should be
        /// excluded from "normal" topology so that it won't participate in task execution
        /// or in-memory database.
        /// <p/>
        /// Application code should never use daemon nodes.
        /// </summary>
        bool IsDaemon { get; }

        /// <summary>
        /// Gets a value indicating whether or not this node is connected to cluster as a client.
        /// <para />
        /// Do not confuse client in terms of discovery and client in terms of cache. 
        /// Cache clients cannot carry data, while topology clients connect to the topology in a different way.
        /// </summary>
        /// <value>   
        /// <c>true</c> if this node is a client node, <c>false otherwise.</c>.
        /// </value>
        bool IsClient { get; }

        /// <summary>
        /// Gets metrics snapshot for this node. Note that node metrics are constantly updated
        /// and provide up to date information about nodes. For example, you can get
        /// an idea about CPU load on remote node via <see cref="IClusterMetrics.CurrentCpuLoad"/>.
        /// <para/>
        /// Node metrics are updated with some delay which is directly related to heartbeat
        /// frequency. For example, when used with default <c>GridTcpDiscoverySpi</c> the 
        /// update will happen every <c>2</c> seconds.
        /// </summary>
        /// <returns>Runtime metrics snapshot for this node.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IClusterMetrics GetMetrics();
    }
}
