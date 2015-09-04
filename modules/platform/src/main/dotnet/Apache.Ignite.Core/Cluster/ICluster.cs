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
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Represents whole cluster (group of all nodes in a cluster).
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface ICluster : IClusterGroup
    {
        /// <summary>
        /// Gets monadic projection consisting from the local node.
        /// </summary>
        /// <returns>Monadic projection consisting from the local node.</returns>
        IClusterGroup ForLocal();

        /// <summary>
        /// Gets local Ignite node.
        /// </summary>
        /// <returns>Local Ignite node.</returns>
        IClusterNode LocalNode
        {
            get;
        }

        /// <summary>
        /// Pings a remote node.
        /// </summary>
        /// <param name="nodeId">ID of a node to ping.</param>
        /// <returns>True if node for a given ID is alive, false otherwise.</returns>
        bool PingNode(Guid nodeId);

        /// <summary>
        /// Gets current topology version. In case of TCP discovery topology versions are sequential 
        /// - they start from 1 and get incremented every time whenever a node joins or leaves. 
        /// For other discovery SPIs topology versions may not be (and likely are not) sequential.
        /// </summary>
        /// <value>
        /// Current topology version.
        /// </value>
        long TopologyVersion { get; }

        /// <summary>
        /// Gets a topology by version. Returns null if topology history storage doesn't contain 
        /// specified topology version (history currently keeps the last 1000 snapshots).
        /// </summary>
        /// <param name="ver">Topology version.</param>
        /// <returns>Collection of Ignite nodes which represented by specified topology version, 
        /// if it is present in history storage, null otherwise.</returns>
        /// <exception cref="IgniteException">If underlying SPI implementation does not support 
        /// topology history. Currently only <code>org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi</code>
        /// supports topology history.</exception>
        ICollection<IClusterNode> Topology(long ver);

        /// <summary>
        /// Resets local I/O, job, and task execution metrics.
        /// </summary>
        void ResetMetrics();
    }
}