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

package org.apache.ignite.client;

import org.apache.ignite.cluster.ClusterState;

/**
 * Thin client cluster facade. Represents whole cluster (all available nodes).
 */
public interface ClientCluster extends ClientClusterGroup {
    /**
     * Gets current cluster state.
     *
     * @return Current cluster state.
     */
    public ClusterState state();

    /**
     * Changes current cluster state to given {@code newState} cluster state.
     * <p>
     * <b>NOTE:</b>
     * Deactivation clears in-memory caches (without persistence) including the system caches.
     *
     * @param newState New cluster state.
     * @throws ClientException If change state operation failed.
     */
    public void state(ClusterState newState) throws ClientException;

    /**
     * Disables write-ahead logging for specified cache. When WAL is disabled, changes are not logged to disk.
     * This significantly improves cache update speed. The drawback is absence of local crash-recovery guarantees.
     * If node is crashed, local content of WAL-disabled cache will be cleared on restart to avoid data corruption.
     * <p>
     * Internally this method will wait for all current cache operations to finish and prevent new cache operations
     * from being executed. Then checkpoint is initiated to flush all data to disk. Control is returned to the callee
     * when all dirty pages are prepared for checkpoint, but not necessarily flushed to disk.
     * <p>
     * WAL state can be changed only for persistent caches.
     *
     * @param cacheName Cache name.
     * @return Whether WAL disabled by this call.
     * @throws ClientException If error occurs.
     * @see #enableWal(String)
     * @see #isWalEnabled(String)
     */
    public boolean disableWal(String cacheName) throws ClientException;

    /**
     * Enables write-ahead logging for specified cache. Restoring crash-recovery guarantees of a previous call to
     * {@link #disableWal(String)}.
     * <p>
     * Internally this method will wait for all current cache operations to finish and prevent new cache operations
     * from being executed. Then checkpoint is initiated to flush all data to disk. Control is returned to the callee
     * when all data is persisted to disk.
     * <p>
     * WAL state can be changed only for persistent caches.
     *
     * @param cacheName Cache name.
     * @return Whether WAL enabled by this call.
     * @throws ClientException If error occurs.
     * @see #disableWal(String)
     * @see #isWalEnabled(String)
     */
    public boolean enableWal(String cacheName) throws ClientException;

    /**
     * Checks if write-ahead logging is enabled for specified cache.
     *
     * @param cacheName Cache name.
     * @return {@code True} if WAL is enabled for cache.
     * @see #disableWal(String)
     * @see #enableWal(String)
     */
    public boolean isWalEnabled(String cacheName);
}
