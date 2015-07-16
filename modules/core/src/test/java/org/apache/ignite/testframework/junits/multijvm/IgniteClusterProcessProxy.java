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

package org.apache.ignite.testframework.junits.multijvm;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Proxy class for cluster at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class IgniteClusterProcessProxy implements IgniteClusterEx {
    /** Grid id. */
    private final UUID gridId;

    /** Compute. */
    private final transient IgniteCompute compute;

    /** */
    private final IgniteProcessProxy proxy;

    /**
     * @param proxy Ignite Proxy.
     */
    public IgniteClusterProcessProxy(IgniteProcessProxy proxy) {
        this.proxy = proxy;
        gridId = proxy.getId();
        compute = proxy.remoteCompute();
    }

    /**
     * Returns cluster instance. Method to be called from closure at another JVM.
     *
     * @return Cache.
     */
    private IgniteClusterEx cluster() {
        return (IgniteClusterEx)Ignition.ignite(gridId).cluster();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroupEx forSubjectId(final UUID subjId) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forCacheNodes(@Nullable String cacheName, boolean affNodes, boolean nearNodes,
        boolean clientNodes) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return compute.call(new IgniteCallable<ClusterNode>() {
            @Override public ClusterNode call() throws Exception {
                return cluster().localNode();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forLocal() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> ConcurrentMap<K, V> nodeLocalMap() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> topology(long topVer) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
        @Nullable Collection<? extends K> keys) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterStartNodeResult> startNodes(File file, boolean restart, int timeout,
        int maxConn) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterStartNodeResult> startNodes(Collection<Map<String, Object>> hosts,
        @Nullable Map<String, Object> dflts, boolean restart, int timeout, int maxConn) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void stopNodes() throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void stopNodes(Collection<UUID> ids) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void restartNodes() throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void restartNodes(Collection<UUID> ids) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster withAsync() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean isAsync() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public <R> IgniteFuture<R> future() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Ignite ignite() {
        return proxy;
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forNodes(Collection<? extends ClusterNode> nodes) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forNode(ClusterNode node, ClusterNode... nodes) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forOthers(ClusterNode node, ClusterNode... nodes) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forOthers(ClusterGroup prj) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forNodeIds(Collection<UUID> ids) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forNodeId(UUID id, UUID... ids) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forPredicate(IgnitePredicate<ClusterNode> p) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forAttribute(String name, @Nullable Object val) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forServers() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forClients() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forCacheNodes(String cacheName) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forDataNodes(String cacheName) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forClientNodes(String cacheName) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forRemotes() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forHost(ClusterNode node) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forHost(String host, String... hosts) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forDaemons() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forRandom() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forOldest() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forYoungest() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        return compute.call(new IgniteCallable<Collection<ClusterNode>>() {
            @Override public Collection<ClusterNode> call() throws Exception {
                return cluster().nodes();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public ClusterNode node(final UUID nid) {
        return compute.call(new IgniteCallable<ClusterNode>() {
            @Override public ClusterNode call() throws Exception {
                return cluster().node(nid);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public ClusterNode node() {
        return compute.call(new IgniteCallable<ClusterNode>() {
            @Override public ClusterNode call() throws Exception {
                return cluster().node();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return compute.call(new IgniteCallable<Collection<String>>() {
            @Override public Collection<String> call() throws Exception {
                return cluster().hostNames();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<ClusterNode> predicate() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteFuture<?> clientReconnectFuture() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }
}
