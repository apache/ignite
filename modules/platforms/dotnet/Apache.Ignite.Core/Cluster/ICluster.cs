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
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
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
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", Justification = "Semantics.")]
        IClusterNode GetLocalNode();

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
        /// topology history. Currently only <c>org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi</c>
        /// supports topology history.</exception>
        ICollection<IClusterNode> GetTopology(long ver);

        /// <summary>
        /// Resets local I/O, job, and task execution metrics.
        /// </summary>
        void ResetMetrics();

        /// <summary>
        /// Gets the reconnect task, which will transition to Completed state 
        /// when local client node reconnects to the cluster. 
        /// <para />
        /// Result of the task indicates whether cluster has been restarted.
        /// <para />
        /// If local node is not in client mode or is not disconnected, returns completed task.
        /// </summary>
        /// <value>
        /// The reconnect task.
        /// </value>
        Task<bool> ClientReconnectTask { get; }

        /// <summary>
        /// Changes Ignite grid state to active or inactive.
        /// </summary>
        void SetActive(bool isActive);

        /// <summary>
        /// Determines whether this grid is in active state.
        /// </summary>
        /// <returns>
        ///   <c>true</c> if the grid is active; otherwise, <c>false</c>.
        /// </returns>
        bool IsActive();

        /// <summary>
        /// Sets the baseline topology from the cluster topology of the given version.
        /// This method requires active cluster (<see cref="IsActive"/>).
        /// </summary>
        /// <param name="topologyVersion">The topology version.</param>
        void SetBaselineTopology(long topologyVersion);

        /// <summary>
        /// Sets the baseline topology nodes.
        /// </summary>
        /// <param name="nodes">The nodes.</param>
        void SetBaselineTopology(IEnumerable<IBaselineNode> nodes);

        /// <summary>
        /// Gets the baseline topology.
        /// Returns null if <see cref="SetBaselineTopology(long)"/> has not been called.
        /// </summary>
        ICollection<IBaselineNode> GetBaselineTopology();

        /// <summary>
        /// Disables write-ahead logging for specified cache. When WAL is disabled, changes are not logged to disk.
        /// This significantly improves cache update speed.The drawback is absence of local crash-recovery guarantees.
        /// If node is crashed, local content of WAL-disabled cache will be cleared on restart
        /// to avoid data corruption.
        /// <para />
        /// Internally this method will wait for all current cache operations to finish and prevent new cache 
        /// operations from being executed.Then checkpoint is initiated to flush all data to disk.Control is returned
        /// to the callee when all dirty pages are prepared for checkpoint, but not necessarily flushed to disk.
        /// <para />
        /// WAL state can be changed only for persistent caches.
        /// </summary>
        /// <param name="cacheName">Name of the cache.</param>
        void DisableWal(string cacheName);

        /// <summary>
        /// Enables write-ahead logging for specified cache. Restoring crash-recovery guarantees of a previous call to
        /// <see cref="DisableWal"/>.
        /// <para />
        /// Internally this method will wait for all current cache operations to finish and prevent new cache
        /// operations from being executed. Then checkpoint is initiated to flush all data to disk.
        /// Control is returned to the callee when all data is persisted to disk.
        /// <para />
        /// WAL state can be changed only for persistent caches.
        /// </summary>
        /// <param name="cacheName">Name of the cache.</param>
        void EnableWal(string cacheName);

        /// <summary>
        /// Determines whether write-ahead logging is enabled for specified cache.
        /// </summary>
        /// <param name="cacheName">Name of the cache.</param>
        bool IsWalEnabled(string cacheName);

        /// <summary>
        /// Set transaction timeout on partition map exchange
        /// </summary>
        /// <param name="timeout"></param>
        void SetTxTimeoutOnPartitionMapExchange(TimeSpan timeout);

        /// <summary>
        /// Returns value of manual baseline control or auto adjusting baseline.
        /// </summary>
        /// <returns><c>true</c> If cluster in auto-adjust. <c>false</c> If cluster in manual.</returns>
        bool IsBaselineAutoAdjustEnabled();

        /// <summary>
        /// Sets the value of manual baseline control or auto adjusting baseline.
        /// </summary>
        /// <param name="isBaselineAutoAdjustEnabled"><c>true</c> If cluster in auto-adjust. <c>false</c> If cluster in manual.</param>
        void SetBaselineAutoAdjustEnabledFlag(bool isBaselineAutoAdjustEnabled);

        /// <summary>
        /// Gets the value of time which we would wait before the actual topology change since last server topology change(node join/left/fail).
        /// </summary>
        /// <returns>Timeout value</returns>
        long GetBaselineAutoAdjustTimeout();

        /// <summary>
        /// Sets the value of time which we would wait before the actual topology change since last server topology change(node join/left/fail).
        /// </summary>
        /// <param name="baselineAutoAdjustTimeout">Timeout value</param>
        void SetBaselineAutoAdjustTimeout(long baselineAutoAdjustTimeout);
    }
}
