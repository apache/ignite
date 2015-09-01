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

package org.apache.ignite.internal.client;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * A data projection of grid client. Contains various methods for cache operations and metrics retrieval.
 * An instance of data projection over some remote cache is provided via
 * {@link GridClient#data(String)} method.
 * <h1 class="header">Affinity Awareness</h1>
 * One of the unique properties of the Ignite remote clients is that they are
 * affinity aware. In other words, both compute and data APIs will optionally
 * contact exactly the node where the data is cached based on some affinity key.
 * This allows for collocation of computations and data and avoids extra network
 * hops that would be necessary if non-affinity nodes were contacted. By default
 * all operations on {@code GridClientData} API will be affinity-aware unless
 * such behavior is overridden by pinning one or more remote nodes
 * (see {@link #pinNodes(GridClientNode, GridClientNode...)} for more information).
 */
public interface GridClientData {
    /**
     * Gets name of the remote cache. The cache name for this projection was specified
     * via {@link GridClient#data(String)} method at the time of creation.
     *
     * @return Name of the remote cache.
     */
    public String cacheName();

    /**
     * Gets client data projection which will only contact specified remote grid node. By default, remote
     * node is determined based on {@link GridClientDataAffinity} provided - this method allows
     * to override default behavior and use only specified server for all cache operations.
     * <p>
     * Use this method when there are other than {@code key-affinity} reasons why a certain
     * node should be contacted.
     *
     * @param node Node to be contacted (optional).
     * @param nodes Additional nodes (optional).
     * @return Client data which will only contact server with given node ID.
     * @throws GridClientException If resulting projection is empty.
     */
    public GridClientData pinNodes(GridClientNode node, GridClientNode... nodes) throws GridClientException;

    /**
     * Gets pinned node or {@code null} if no nodes were pinned.
     *
     * @return Pinned node.
     */
    public Collection<GridClientNode> pinnedNodes();

    /**
     * Puts value to cache on remote grid.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to put in cache.
     * @param val Value to put in cache.
     * @return Whether value was actually put to cache.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> boolean put(K key, V val) throws GridClientException;

    /**
     * Asynchronously puts value to cache on remote node.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to put in cache.
     * @param val Value to put in cache.
     * @return Future whether value was actually put to cache.
     */
    public <K, V> GridClientFuture<Boolean> putAsync(K key, V val);

    /**
     * Puts entries to cache on remote grid.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote nodes on which these keys are supposed to be cached (unless
     * some nodes were {@code pinned}). If entries do not map to one node, then the node
     * which has most mapped entries will be contacted.
     *
     * @param entries Entries to put in cache.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> void putAll(Map<K, V> entries) throws GridClientException;

    /**
     * Asynchronously puts entries to cache on remote grid.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote nodes on which these keys are supposed to be cached (unless
     * some nodes were {@code pinned}). If entries do not map to one node, then the node
     * which has most mapped entries will be contacted.
     *
     * @param entries Entries to put in cache.
     * @return Future whether this operation completes.
     */
    public <K, V> GridClientFuture<?> putAllAsync(Map<K, V> entries);

    /**
     * Gets value from cache on remote node.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to get from cache.
     * @return Value for given key or {@code null} if no value was cached.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> V get(K key) throws GridClientException;

    /**
     * Asynchronously gets value from cache on remote grid.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to get from cache.
     * @return Future with value for given key or with {@code null} if no value was cached.
     */
    public <K, V> GridClientFuture<V> getAsync(K key);

    /**
     * Gets entries from cache on remote grid.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote nodes on which these keys are supposed to be cached (unless
     * some nodes were {@code pinned}). If entries do not map to one node, then the node
     * which has most mapped entries will be contacted.
     *
     * @param keys Keys to get.
     * @throws GridClientException In case of error.
     * @return Entries retrieved from remote cache nodes.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> Map<K, V> getAll(Collection<K> keys) throws GridClientException;

    /**
     * Asynchronously gets entries from cache on remote grid.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote nodes on which these keys are supposed to be cached (unless
     * some nodes were {@code pinned}). If entries do not map to one node, then the node
     * which has most mapped entries will be contacted.
     *
     * @param keys Keys to get.
     * @return Future with entries retrieved from remote cache nodes.
     */
    public <K, V> GridClientFuture<Map<K, V>> getAllAsync(Collection<K> keys);

    /**
     * Removes value from cache on remote node.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to remove.
     * @return Whether value was actually removed.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K> boolean remove(K key) throws GridClientException;

    /**
     * Asynchronously removes value from cache on remote grid.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to remove.
     * @return Future whether value was actually removed.
     */
    public <K> GridClientFuture<Boolean> removeAsync(K key);

    /**
     * Removes entries from cache on remote node.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote nodes on which these keys are supposed to be cached (unless
     * some nodes were {@code pinned}). If entries do not map to one node, then the node
     * which has most mapped entries will be contacted.
     *
     * @param keys Keys to remove.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K> void removeAll(Collection<K> keys) throws GridClientException;

    /**
     * Asynchronously removes entries from cache on remote grid.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote nodes on which these keys are supposed to be cached (unless
     * some nodes were {@code pinned}). If entries do not map to one node, then the node
     * which has most mapped entries will be contacted.
     *
     * @param keys Keys to remove.
     * @return Future whether operation finishes.
     */
    public <K> GridClientFuture<?> removeAllAsync(Collection<K> keys);

    /**
     * Replaces value in cache on remote grid only if there was a {@code non-null}
     * value associated with this key.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to replace.
     * @param val Value to replace.
     * @return Whether value was actually replaced.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> boolean replace(K key, V val) throws GridClientException;

    /**
     * Asynchronously replaces value in cache on remote grid only if there was a {@code non-null}
     * value associated with this key.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to replace.
     * @param val Value to replace.
     * @return Future whether value was actually replaced.
     */
    public <K, V> GridClientFuture<Boolean> replaceAsync(K key, V val);

    /**
     * Sets entry value to {@code val1} if current value is {@code val2} with
     * following conditions:
     * <ul>
     * <li>
     * If {@code val1} is {@code null} and {@code val2} is equal to current value,
     * entry is removed from cache.
     * </li>
     * <li>
     * If {@code val2} is {@code null}, entry is created if it doesn't exist.
     * </li>
     * <li>
     * If both {@code val1} and {@code val2} are {@code null}, entry is removed.
     * </li>
     * </ul>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to set.
     * @param val1 Value to set.
     * @param val2 Check value.
     * @return Whether value of entry was changed.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public <K, V> boolean cas(K key, V val1, V val2) throws GridClientException;

    /**
     * Asynchronously sets entry value to {@code val1} if current value is {@code val2}
     * with following conditions:
     * <ul>
     * <li>
     * If {@code val1} is {@code null} and {@code val2} is equal to current value,
     * entry is removed from cache.
     * </li>
     * <li>
     * If {@code val2} is {@code null}, entry is created if it doesn't exist.
     * </li>
     * <li>
     * If both {@code val1} and {@code val2} are {@code null}, entry is removed.
     * </li>
     * </ul>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to set.
     * @param val1 Value to set.
     * @param val2 Check value.
     * @return Future whether value of entry was changed.
     */
    public <K, V> GridClientFuture<Boolean> casAsync(K key, V val1, V val2);

    /**
     * Gets affinity node ID for provided key. This method will return {@code null} if no
     * affinity was configured for the given cache for this client or there are no nodes in topology with
     * cache enabled.
     *
     * @param key Key.
     * @return Node ID.
     * @throws GridClientException In case of error.
     */
    public <K> UUID affinity(K key) throws GridClientException;

    /**
     * Fetches metrics for cache from remote grid.
     *
     * @return Cache metrics.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientDataMetrics metrics() throws GridClientException;

    /**
     * Asynchronously fetches metrics for cache from remote grid.
     *
     * @return Future with cache metrics.
     */
    public GridClientFuture<GridClientDataMetrics> metricsAsync();

    /**
     * Tries to get metrics from local cache.
     * <p>
     * Local cache is updated on every {@link #metrics()} or {@link #metricsAsync()} call
     * if {@link GridClientConfiguration#isEnableMetricsCache()} is enabled. If it is
     * disabled then this method will always return {@code null}.
     *
     * @return Cached metrics or {@code null} if no cached metrics available.
     * @throws GridClientException In case of error.
     * @throws GridServerUnreachableException If none of the servers can be reached.
     * @throws GridClientClosedException If client was closed manually.
     */
    public GridClientDataMetrics cachedMetrics() throws GridClientException;

    /**
     * Append requested value to already cached one. This method supports work with strings, lists and maps.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to manipulate cache value for.
     * @param val Value to append to the cached one.
     * @return Whether value of entry was changed.
     * @throws GridClientException In case of error.
     */
    public <K, V> boolean append(K key, V val) throws GridClientException;

    /**
     * Append requested value to already cached one. This method supports work with strings, lists and maps.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to manipulate cache value for.
     * @param val Value to append to the cached one.
     * @return Future whether value of entry was changed.
     * @throws GridClientException In case of error.
     */
    public <K, V> GridClientFuture<Boolean> appendAsync(K key, V val) throws GridClientException;

    /**
     * Prepend requested value to already cached one. This method supports work with strings, lists and maps.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to manipulate cache value for.
     * @param val Value to prepend to the cached one.
     * @return Whether value of entry was changed.
     * @throws GridClientException In case of error.
     */
    public <K, V> boolean prepend(K key, V val) throws GridClientException;

    /**
     * Prepend requested value to already cached one. This method supports work with strings, lists and maps.
     * <p>
     * Note that this operation is affinity-aware and will immediately contact
     * exactly the remote node on which this key is supposed to be cached (unless
     * some nodes were {@code pinned}).
     *
     * @param key Key to manipulate cache value for.
     * @param val Value to prepend to the cached one.
     * @return Future whether value of entry was changed.
     * @throws GridClientException In case of error.
     */
    public <K, V> GridClientFuture<Boolean> prependAsync(K key, V val) throws GridClientException;

    /**
     * Gets cache flags enabled on this data projection.
     *
     * @return Flags for this data projection (empty set if no flags have been set).
     */
    public Set<GridClientCacheFlag> flags();

    /**
     * Creates new client data object with enabled cache flags.
     *
     * @param flags Optional cache flags to be enabled.
     * @return New client data object.
     * @throws GridClientException In case of error.
     */
    public GridClientData flagsOn(GridClientCacheFlag... flags) throws GridClientException;

    /**
     * Creates new client data object with disabled cache flags.
     *
     * @param flags Cache flags to be disabled.
     * @return New client data object.
     * @throws GridClientException In case of error.
     */
    public GridClientData flagsOff(GridClientCacheFlag... flags) throws GridClientException;
}