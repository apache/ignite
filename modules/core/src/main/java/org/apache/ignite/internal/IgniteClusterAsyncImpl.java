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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 *
 */
public class IgniteClusterAsyncImpl extends IgniteAsyncSupportAdapter<IgniteCluster> implements IgniteCluster {
    /** */
    private final IgniteKernal grid;

    /**
     * @param grid Grid.
     */
    public IgniteClusterAsyncImpl(IgniteKernal grid) {
        super(true);

        this.grid = grid;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return grid.localNode();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forLocal() {
        return grid.forLocal();
    }

    /** {@inheritDoc} */
    @Override public <K, V> ClusterNodeLocalMap<K, V> nodeLocalMap() {
        return grid.nodeLocalMap();
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return grid.pingNode(nodeId);
    }

    /** {@inheritDoc} */
    @Override public long topologyVersion() {
        return grid.topologyVersion();
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<ClusterNode> topology(long topVer) {
        return grid.topology(topVer);
    }

    /** {@inheritDoc} */
    @Override public <K> Map<ClusterNode, Collection<K>> mapKeysToNodes(@Nullable String cacheName,
        @Nullable Collection<? extends K> keys) {
        return grid.mapKeysToNodes(cacheName, keys);
    }

    /** {@inheritDoc} */
    @Nullable @Override public <K> ClusterNode mapKeyToNode(@Nullable String cacheName, K key) {
        return grid.mapKeyToNode(cacheName, key);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridTuple3<String, Boolean, String>> startNodes(File file,
        boolean restart, int timeout, int maxConn) throws IgniteCheckedException {
        return saveOrGet(grid.startNodesAsync(file, restart, timeout, maxConn));
    }

    /** {@inheritDoc} */
    @Override public Collection<GridTuple3<String, Boolean, String>> startNodes(
        Collection<Map<String, Object>> hosts, @Nullable Map<String, Object> dflts, boolean restart, int timeout,
        int maxConn) throws IgniteCheckedException {
        return saveOrGet(grid.startNodesAsync(hosts, dflts, restart, timeout, maxConn));
    }

    /** {@inheritDoc} */
    @Override public void stopNodes() throws IgniteCheckedException {
        grid.stopNodes();
    }

    /** {@inheritDoc} */
    @Override public void stopNodes(Collection<UUID> ids) throws IgniteCheckedException {
        grid.stopNodes(ids);
    }

    /** {@inheritDoc} */
    @Override public void restartNodes() throws IgniteCheckedException {
        grid.restartNodes();
    }

    /** {@inheritDoc} */
    @Override public void restartNodes(Collection<UUID> ids) throws IgniteCheckedException {
        grid.restartNodes(ids);
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        grid.resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public Ignite ignite() {
        return grid.ignite();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forNodes(Collection<? extends ClusterNode> nodes) {
        return grid.forNodes(nodes);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forNode(ClusterNode node, ClusterNode... nodes) {
        return grid.forNode(node, nodes);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forOthers(ClusterNode node, ClusterNode... nodes) {
        return grid.forOthers(node, nodes);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forOthers(ClusterGroup prj) {
        return grid.forOthers(prj);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forNodeIds(Collection<UUID> ids) {
        return grid.forNodeIds(ids);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forNodeId(UUID id, UUID... ids) {
        return grid.forNodeId(id, ids);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forPredicate(IgnitePredicate<ClusterNode> p) {
        return grid.forPredicate(p);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forAttribute(String name, @Nullable String val) {
        return grid.forAttribute(name, val);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forCache(String cacheName, @Nullable String... cacheNames) {
        return grid.forCache(cacheName, cacheNames);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forStreamer(String streamerName, @Nullable String... streamerNames) {
        return grid.forStreamer(streamerName, streamerNames);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forRemotes() {
        return grid.forRemotes();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forHost(ClusterNode node) {
        return grid.forHost(node);
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forDaemons() {
        return grid.forDaemons();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forRandom() {
        return grid.forRandom();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forOldest() {
        return grid.forOldest();
    }

    /** {@inheritDoc} */
    @Override public ClusterGroup forYoungest() {
        return grid.forYoungest();
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        return grid.nodes();
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode node(UUID id) {
        return grid.node(id);
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode node() {
        return grid.node();
    }

    /** {@inheritDoc} */
    @Override public IgnitePredicate<ClusterNode> predicate() {
        return grid.predicate();
    }

    /** {@inheritDoc} */
    @Override public ClusterMetrics metrics() throws IgniteCheckedException {
        return grid.metrics();
    }
}
