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

namespace Apache.Ignite.Core.Cluster
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Interface representing a single cluster node. Use <see cref="IClusterNode.Attribute{T}(string)"/> or
    /// <see cref="IClusterNode.Metrics()"/> to get static and dynamic information about remote nodes.
    /// You can get a list of all nodes in grid by calling <see cref="IClusterGroup.Nodes()"/> 
    /// on <see cref="IIgnite"/> instance.
    /// <para />
    /// You can use Ignite node attributes to provide static information about a node.
    /// This information is initialized once within grid, during node startup, and
    /// remains the same throughout the lifetime of a node. 
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface IClusterNode {
        /// <summary>
        /// Globally unique node ID. A new ID is generated every time a node restarts.
        /// </summary>
        Guid Id 
        {
            get;
        }

        /// <summary>
        /// Gets node's attribute. Attributes are assigned to nodes at startup.
        /// <para />
        /// Note that attributes cannot be changed at runtime.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <returns>Attribute value.</returns>
        T Attribute<T>(string name);
        
        /// <summary>
        /// Try getting node's attribute. Attributes are assigned to nodes at startup.
        /// <para />
        /// Note that attributes cannot be changed at runtime.
        /// </summary>
        /// <param name="name">Attribute name.</param>
        /// <param name="attr">Attribute value.</param>
        /// <returns><code>true</code> in case such attribute exists.</returns>
        bool TryGetAttribute<T>(string name, out T attr);

        /// <summary>
        /// Gets all node attributes. Attributes are assigned to nodes at startup.
        /// <para />
        /// Note that attributes cannot be changed at runtime.
        /// </summary>
        /// <returns>All node attributes.</returns>
        IDictionary<string, object> Attributes();

        /// <summary>
        /// Collection of addresses this node is known by. 
        /// </summary>
        /// <returns>Collection of addresses.</returns>
        ICollection<string> Addresses
        {
            get;
        }

        /// <summary>
        /// Collection of host names this node is known by.
        /// </summary>
        /// <returns>Collection of host names.</returns>
        ICollection<string> HostNames
        {
            get;
        }

        /// <summary>
        /// Node order within grid topology. Discovery SPIs that support node ordering will
        /// assign a proper order to each node and will guarantee that discovery event notifications
        /// for new nodes will come in proper order. All other SPIs not supporting ordering
        /// may choose to return node startup time here.
        /// </summary>
        long Order
        {
            get;
        }

        /// <summary>
        /// Tests whether or not this node is a local node.
        /// </summary>
        bool IsLocal
        {
            get;
        }
        
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
        bool IsDaemon
        {
            get;
        }

        /// <summary>
        /// Gets metrics snapshot for this node. Note that node metrics are constantly updated
        /// and provide up to date information about nodes. For example, you can get
        /// an idea about CPU load on remote node via <see cref="IClusterMetrics.CurrentCpuLoad"/>.
        /// <para/>
        /// Node metrics are updated with some delay which is directly related to heartbeat
        /// frequency. For example, when used with default <code>GridTcpDiscoverySpi</code> the 
        /// update will happen every <code>2</code> seconds.
        /// </summary>
        /// <returns>Runtime metrics snapshot for this node.</returns>
        IClusterMetrics Metrics();
    }
}
