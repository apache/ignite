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

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterStartNodeResult;
import org.apache.ignite.internal.cluster.ClusterGroupEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Proxy class for cluster at another JVM.
 */
@SuppressWarnings("TransientFieldInNonSerializableClass")
public class IgniteClusterProcessProxy implements IgniteClusterEx {
    /** Compute. */
    private final transient IgniteCompute compute;

    /** */
    private final IgniteProcessProxy proxy;

    /**
     * @param proxy Ignite Proxy.
     */
    public IgniteClusterProcessProxy(IgniteProcessProxy proxy) {
        this.proxy = proxy;
        compute = proxy.remoteCompute();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroupEx forSubjectId(UUID subjId) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forCacheNodes(@Nullable String cacheName, boolean affNodes, boolean nearNodes,
        boolean clientNodes) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forIgfsMetadataDataNodes(String igfsName, @Nullable String metaCacheName) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return compute.call(new LocalNodeTask());
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
    @Override public Collection<ClusterStartNodeResult> startNodes(File file, boolean restart, int timeout,
        int maxConn) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Collection<ClusterStartNodeResult>> startNodesAsync(File file, boolean restart,
        int timeout, int maxConn) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterStartNodeResult> startNodes(Collection<Map<String, Object>> hosts,
        @Nullable Map<String, Object> dflts, boolean restart, int timeout, int maxConn) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<Collection<ClusterStartNodeResult>> startNodesAsync(
        Collection<Map<String, Object>> hosts, @Nullable Map<String, Object> dflts,
        boolean restart, int timeout, int maxConn) throws IgniteException {
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
    @Override public void enableStatistics(Collection<String> caches, boolean enabled) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void clearStatistics(Collection<String> caches) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void setTxTimeoutOnPartitionMapExchange(long timeout) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public IgniteCluster withAsync() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean enableWal(String cacheName) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean disableWal(String cacheName) throws IgniteException {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }
    
    /** {@inheritDoc} */
    @Override public boolean isWalEnabled(String cacheName) {
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
    @Override public ClusterGroup forCacheNodes(@NotNull String cacheName) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forDataNodes(@NotNull String cacheName) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forClientNodes(@NotNull String cacheName) {
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
        return compute.call(new NodesTask());
    }

    /** {@inheritDoc} */
    @Override public ClusterNode node(UUID nid) {
        return compute.call(new NodeTask(nid));
    }

    /** {@inheritDoc} */
    @Override public ClusterNode node() {
        return compute.call(new NodeTask(null));
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return compute.call(new HostNamesTask());
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

    /** {@inheritDoc} */
    @Override public boolean active() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void active(boolean active) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<BaselineNode> currentBaselineTopology() {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void setBaselineTopology(Collection<? extends BaselineNode> baselineTop) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /** {@inheritDoc} */
    @Override public void setBaselineTopology(long topVer) {
        throw new UnsupportedOperationException("Operation is not supported yet.");
    }

    /**
     *
     */
    private static class LocalNodeTask extends ClusterTaskAdapter<ClusterNode> {
        /** {@inheritDoc} */
        @Override public ClusterNode call() throws Exception {
            return cluster().localNode();
        }
    }

    /**
     *
     */
    private static class NodesTask extends ClusterTaskAdapter<Collection<ClusterNode>> {
        /** {@inheritDoc} */
        @Override public Collection<ClusterNode> call() throws Exception {
            return cluster().nodes();
        }
    }

    /**
     *
     */
    private static class NodeTask extends ClusterTaskAdapter<ClusterNode> {
        /** Node id. */
        private final UUID nodeId;

        /**
         * @param nodeId Node id.
         */
        public NodeTask(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /** {@inheritDoc} */
        @Override public ClusterNode call() throws Exception {
            return nodeId == null ? cluster().node() : cluster().node(nodeId);
        }
    }

    /**
     *
     */
    private static class HostNamesTask extends ClusterTaskAdapter<Collection<String>> {
        /** {@inheritDoc} */
        @Override public Collection<String> call() throws Exception {
            return cluster().hostNames();
        }
    }

    /**
     *
     */
    private abstract static class ClusterTaskAdapter<R> implements IgniteCallable<R> {
        /** Ignite. */
        @IgniteInstanceResource
        protected Ignite ignite;

        /**
         *
         */
        protected IgniteCluster cluster() {
            return ignite.cluster();
        }
    }
}