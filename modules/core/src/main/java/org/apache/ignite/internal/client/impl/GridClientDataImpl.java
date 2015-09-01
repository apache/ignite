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

package org.apache.ignite.internal.client.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.client.GridClientCacheFlag;
import org.apache.ignite.internal.client.GridClientClosedException;
import org.apache.ignite.internal.client.GridClientData;
import org.apache.ignite.internal.client.GridClientDataAffinity;
import org.apache.ignite.internal.client.GridClientDataMetrics;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientFuture;
import org.apache.ignite.internal.client.GridClientFutureListener;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientPredicate;
import org.apache.ignite.internal.client.balancer.GridClientLoadBalancer;
import org.apache.ignite.internal.client.impl.connection.GridClientConnection;
import org.apache.ignite.internal.client.impl.connection.GridClientConnectionResetException;
import org.apache.ignite.internal.client.util.GridClientUtils;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Data projection that serves one cache instance and handles communication errors.
 */
public class GridClientDataImpl extends GridClientAbstractProjection<GridClientDataImpl> implements GridClientData {
    /** Cache metrics. */
    private final boolean cacheMetrics;

    /** Cache name. */
    private String cacheName;

    /** Client data metrics. */
    private volatile GridClientDataMetrics metrics;

    /** Cache flags to be enabled. */
    private final Set<GridClientCacheFlag> flags;

    /**
     * Creates a data projection.
     *
     * @param cacheName Cache name for projection. If {@code null}, then default cache will be used.
     * @param client Client instance to resolve connection failures.
     * @param nodes Pinned nodes. If {@code null}, then no nodes will be pinned.
     * @param filter Node filter. If {@code null}, then no filter would be applied to the node list.
     * @param balancer Pinned node balancer. If {@code null}, then no balancer will be used.
     * @param flags Cache flags to be enabled. If {@code null}, then no flags will be used.
     * @param cacheMetrics Whether to cache received metrics.
     */
    GridClientDataImpl(String cacheName, GridClientImpl client, Collection<GridClientNode> nodes,
        GridClientPredicate<? super GridClientNode> filter, GridClientLoadBalancer balancer,
        Set<GridClientCacheFlag> flags, boolean cacheMetrics) {
        super(client, nodes, filter, balancer);

        this.cacheName = cacheName;
        this.cacheMetrics = cacheMetrics;
        this.flags = flags == null ? Collections.<GridClientCacheFlag>emptySet() : Collections.unmodifiableSet(flags);
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return cacheName;
    }

    /** {@inheritDoc} */
    @Override public GridClientData pinNodes(GridClientNode node, GridClientNode... nodes) throws GridClientException {
        Collection<GridClientNode> pinnedNodes = new ArrayList<>(nodes != null ? nodes.length + 1 : 1);

        if (node != null)
            pinnedNodes.add(node);

        if (nodes != null && nodes.length != 0)
            pinnedNodes.addAll(Arrays.asList(nodes));

        return createProjection(pinnedNodes.isEmpty() ? null : pinnedNodes,
            null, null, new GridClientDataFactory(flags));
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> pinnedNodes() {
        return nodes;
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean put(K key, V val) throws GridClientException {
        return putAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> putAsync(final K key, final V val) {
        A.notNull(key, "key");
        A.notNull(val, "val");

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cachePut(cacheName, key, val, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> void putAll(Map<K, V> entries) throws GridClientException {
        putAllAsync(entries).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<?> putAllAsync(final Map<K, V> entries) {
        A.notNull(entries, "entries");

        if (entries.isEmpty())
            return new GridClientFutureAdapter<>(false);

        K key = GridClientUtils.first(entries.keySet());

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cachePutAll(cacheName, entries, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> V get(K key) throws GridClientException {
        return this.<K, V>getAsync(key).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<V> getAsync(final K key) {
        A.notNull(key, "key");

        return withReconnectHandling(new ClientProjectionClosure<V>() {
            @Override public GridClientFuture<V> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheGet(cacheName, key, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> Map<K, V> getAll(Collection<K> keys) throws GridClientException {
        return this.<K, V>getAllAsync(keys).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Map<K, V>> getAllAsync(final Collection<K> keys) {
        A.notNull(keys, "keys");

        if (keys.isEmpty())
            return new GridClientFutureAdapter<>(Collections.<K, V>emptyMap());

        K key = GridClientUtils.first(keys);

        return withReconnectHandling(new ClientProjectionClosure<Map<K, V>>() {
            @Override public GridClientFuture<Map<K, V>> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheGetAll(cacheName, keys, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K> boolean remove(K key) throws GridClientException {
        return removeAsync(key).get();
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFuture<Boolean> removeAsync(final K key) {
        A.notNull(key, "key");

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override
            public GridClientFuture<Boolean> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheRemove(cacheName, key, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K> void removeAll(Collection<K> keys) throws GridClientException {
        removeAllAsync(keys).get();
    }

    /** {@inheritDoc} */
    @Override public <K> GridClientFuture<?> removeAllAsync(final Collection<K> keys) {
        A.notNull(keys, "keys");

        if (keys.isEmpty())
            return new GridClientFutureAdapter<>(false);

        K key = GridClientUtils.first(keys);

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheRemoveAll(cacheName, keys, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean replace(K key, V val) throws GridClientException {
        return replaceAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> replaceAsync(final K key, final V val) {
        A.notNull(key, "key");
        A.notNull(val, "val");

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheReplace(cacheName, key, val, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean cas(K key, V val1, V val2) throws GridClientException {
        return casAsync(key, val1, val2).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> casAsync(final K key, final V val1, final V val2) {
        A.notNull(key, "key");

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheCompareAndSet(cacheName, key, val1, val2, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K> UUID affinity(K key) throws GridClientException {
        A.notNull(key, "key");

        GridClientDataAffinity affinity = client.affinity(cacheName);

        if (affinity == null)
            return null;

        Collection<? extends GridClientNode> prj = projectionNodes();

        if (prj.isEmpty())
            throw new GridClientException("Failed to get affinity node (projection node set for cache is empty): " +
                cacheName());

        GridClientNode node = affinity.node(key, prj);

        assert node != null;

        return node.nodeId();
    }

    /** {@inheritDoc} */
    @Override public GridClientDataMetrics metrics() throws GridClientException {
        return metricsAsync().get();
    }

    /** {@inheritDoc} */
    @Override public GridClientFuture<GridClientDataMetrics> metricsAsync() {
        GridClientFuture<GridClientDataMetrics> fut = withReconnectHandling(
            new ClientProjectionClosure<GridClientDataMetrics>() {
                @Override public GridClientFuture<GridClientDataMetrics> apply(
                    GridClientConnection conn, UUID affinityNodeId)
                    throws GridClientConnectionResetException, GridClientClosedException {
                    return conn.cacheMetrics(cacheName, affinityNodeId);
                }
            });

        if (cacheMetrics)
            fut.listen(new GridClientFutureListener<GridClientDataMetrics>() {
                @Override public void onDone(GridClientFuture<GridClientDataMetrics> fut) {
                    try {
                        metrics = fut.get();
                    }
                    catch (GridClientException ignored) {
                        // It's just a cache, so ignore failures.
                    }
                }
            });

        return fut;
    }

    /** {@inheritDoc} */
    @Override public GridClientDataMetrics cachedMetrics() throws GridClientException {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean append(K key, V val) throws GridClientException {
        return appendAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> appendAsync(final K key, final V val) throws GridClientException {
        A.notNull(key, "key");
        A.notNull(val, "val");

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn, UUID destNodeId)
                throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cacheAppend(cacheName, key, val, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean prepend(K key, V val) throws GridClientException {
        return prependAsync(key, val).get();
    }

    /** {@inheritDoc} */
    @Override public <K, V> GridClientFuture<Boolean> prependAsync(final K key, final V val)
        throws GridClientException {
        A.notNull(key, "key");
        A.notNull(val, "val");

        return withReconnectHandling(new ClientProjectionClosure<Boolean>() {
            @Override public GridClientFuture<Boolean> apply(GridClientConnection conn,
                UUID destNodeId) throws GridClientConnectionResetException, GridClientClosedException {
                return conn.cachePrepend(cacheName, key, val, flags, destNodeId);
            }
        }, cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public Set<GridClientCacheFlag> flags() {
        return flags;
    }

    /** {@inheritDoc} */
    @Override public GridClientData flagsOn(GridClientCacheFlag... flags) throws GridClientException {
        if (flags == null || flags.length == 0)
            return this;

        EnumSet<GridClientCacheFlag> flagSet = this.flags == null || this.flags.isEmpty() ?
            EnumSet.noneOf(GridClientCacheFlag.class) : EnumSet.copyOf(this.flags);

        flagSet.addAll(Arrays.asList(flags));

        return createProjection(nodes, filter, balancer, new GridClientDataFactory(flagSet));
    }

    /** {@inheritDoc} */
    @Override public GridClientData flagsOff(GridClientCacheFlag... flags) throws GridClientException {
        if (flags == null || flags.length == 0 || this.flags == null || this.flags.isEmpty())
            return this;

        EnumSet<GridClientCacheFlag> flagSet = EnumSet.copyOf(this.flags);

        flagSet.removeAll(Arrays.asList(flags));

        return createProjection(nodes, filter, balancer, new GridClientDataFactory(flagSet));
    }

    /** {@inheritDoc} */
    private class GridClientDataFactory implements ProjectionFactory<GridClientDataImpl> {
        /** */
        private Set<GridClientCacheFlag> flags;

        /**
         * Factory which creates projections with given flags.
         *
         * @param flags Flags to create projection with.
         */
        GridClientDataFactory(Set<GridClientCacheFlag> flags) {
            this.flags = flags;
        }

        /** {@inheritDoc} */
        @Override public GridClientDataImpl create(Collection<GridClientNode> nodes,
            GridClientPredicate<? super GridClientNode> filter, GridClientLoadBalancer balancer) {
            return new GridClientDataImpl(cacheName, client, nodes, filter, balancer, flags, cacheMetrics);
        }
    }
}