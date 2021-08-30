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

namespace Apache.Ignite.Core.Client
{
    /// <summary>
    /// Represents whole cluster (group of all nodes in a cluster).
    /// </summary>
    public interface IClientCluster : IClientClusterGroup
    {
        /// <summary>
        /// Changes Ignite grid state to active or inactive.
        /// </summary>
        void SetActive(bool isActive);

        /// <summary>
        /// Determines whether this grid is in active state.
        /// </summary>
        /// <returns>
        ///  <c>true</c> if the grid is active; otherwise, <c>false</c>.
        /// </returns>
        bool IsActive();

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
        /// <returns>Whether WAL was enabled by this call.</returns>
        bool DisableWal(string cacheName);

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
        /// <returns>Whether WAL was disabled by this call.</returns>
        bool EnableWal(string cacheName);

        /// <summary>
        /// Determines whether write-ahead logging is enabled for specified cache.
        /// </summary>
        /// <param name="cacheName">Name of the cache.</param>
        bool IsWalEnabled(string cacheName);
    }
}
