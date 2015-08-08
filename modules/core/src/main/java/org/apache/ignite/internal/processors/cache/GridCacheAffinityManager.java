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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Cache affinity manager.
 */
public class GridCacheAffinityManager extends GridCacheManagerAdapter {
    /** */
    private static final AffinityTopologyVersion TOP_FIRST = new AffinityTopologyVersion(1);

    /** Affinity cached function. */
    private GridAffinityAssignmentCache aff;

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        aff = new GridAffinityAssignmentCache(cctx, cctx.namex(), cctx.config().getAffinity(),
            cctx.config().getAffinityMapper(), cctx.config().getBackups());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        if (cctx.isLocal())
            // No discovery event needed for local affinity.
            aff.calculate(TOP_FIRST, null);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        IgniteCheckedException err =
            new IgniteCheckedException("Failed to wait for topology update, cache (or node) is stopping.");

        aff.onKernalStop(err);
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture reconnectFut) {
        IgniteCheckedException err = new IgniteClientDisconnectedCheckedException(reconnectFut,
            "Failed to wait for topology update, client disconnected.");

        aff.onKernalStop(err);
    }

    /**
     *
     */
    public void onReconnected() {
        aff.onReconnected();
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        aff = null;
    }

    /**
     * Gets affinity ready future, a future that will be completed after affinity with given
     * topology version is calculated.
     *
     * @param topVer Topology version to wait.
     * @return Affinity ready future.
     */
    public IgniteInternalFuture<AffinityTopologyVersion> affinityReadyFuture(long topVer) {
        return affinityReadyFuture(new AffinityTopologyVersion(topVer));
    }

    /**
     * Gets affinity ready future, a future that will be completed after affinity with given
     * topology version is calculated.
     *
     * @param topVer Topology version to wait.
     * @return Affinity ready future.
     */
    public IgniteInternalFuture<AffinityTopologyVersion> affinityReadyFuture(AffinityTopologyVersion topVer) {
        assert !cctx.isLocal();

        IgniteInternalFuture<AffinityTopologyVersion> fut = aff.readyFuture(topVer);

        return fut != null ? fut : new GridFinishedFuture<>(topVer);
    }

    /**
     * Gets affinity ready future that will be completed after affinity with given topology version is calculated.
     * Will return {@code null} if topology with given version is ready by the moment method is invoked.
     *
     * @param topVer Topology version to wait.
     * @return Affinity ready future or {@code null}.
     */
    @Nullable public IgniteInternalFuture<AffinityTopologyVersion> affinityReadyFuturex(AffinityTopologyVersion topVer) {
        assert !cctx.isLocal();

        return aff.readyFuture(topVer);
    }

    /**
     * Clean up outdated cache items.
     *
     * @param topVer Actual topology version, older versions will be removed.
     */
    public void cleanUpCache(AffinityTopologyVersion topVer) {
        assert !cctx.isLocal();

        aff.cleanUpCache(topVer);
    }

    /**
     * Initializes affinity for joined node.
     *
     * @param topVer Topology version.
     * @param affAssignment Affinity assignment for this topology version.
     */
    public void initializeAffinity(AffinityTopologyVersion topVer, List<List<ClusterNode>> affAssignment) {
        assert !cctx.isLocal();

        aff.initialize(topVer, affAssignment);
    }

    /**
     * @param topVer Topology version.
     * @return Affinity assignments.
     */
    public List<List<ClusterNode>> assignments(AffinityTopologyVersion topVer) {
        if (cctx.isLocal())
            topVer = new AffinityTopologyVersion(1);

        return aff.assignments(topVer);
    }

    /**
     * Calculates affinity cache for given topology version.
     *
     * @param topVer Topology version to calculate affinity for.
     * @param discoEvt Discovery event that causes this topology change.
     * @return Affinity assignments.
     */
    public List<List<ClusterNode>> calculateAffinity(AffinityTopologyVersion topVer, DiscoveryEvent discoEvt) {
        assert !cctx.isLocal();

        return aff.calculate(topVer, discoEvt);
    }

    /**
     * Copies previous affinity assignment when discovery event does not cause affinity assignment changes
     * (e.g. client node joins on leaves).
     *
     * @param evt Event.
     * @param topVer Topology version.
     */
    public void clientEventTopologyChange(DiscoveryEvent evt, AffinityTopologyVersion topVer) {
        assert !cctx.isLocal();

        aff.clientEventTopologyChange(evt, topVer);
    }

    /**
     * @return Partition count.
     */
    public int partitions() {
        return aff.partitions();
    }

    /**
     * NOTE: Use this method always when you need to calculate partition id for
     * a key provided by user. It's required since we should apply affinity mapper
     * logic in order to find a key that will eventually be passed to affinity function.
     *
     * @param key Key.
     * @return Partition.
     */
    public int partition(Object key) {
        return aff.partition(key);
    }

    /**
     * @param key Key.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodes(Object key, AffinityTopologyVersion topVer) {
        return nodes(partition(key), topVer);
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Affinity nodes.
     */
    public List<ClusterNode> nodes(int part, AffinityTopologyVersion topVer) {
        if (cctx.isLocal())
            topVer = new AffinityTopologyVersion(1);

        return aff.nodes(part, topVer);
    }

    /**
     * @param key Key to check.
     * @param topVer Topology version.
     * @return Primary node for given key.
     */
    @Nullable public ClusterNode primary(Object key, AffinityTopologyVersion topVer) {
        return primary(partition(key), topVer);
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Primary node for given key.
     */
    @Nullable public ClusterNode primary(int part, AffinityTopologyVersion topVer) {
        List<ClusterNode> nodes = nodes(part, topVer);

        if (nodes.isEmpty())
            return null;

        return nodes.get(0);
    }

    /**
     * @param n Node to check.
     * @param key Key to check.
     * @param topVer Topology version.
     * @return {@code True} if checked node is primary for given key.
     */
    public boolean primary(ClusterNode n, Object key, AffinityTopologyVersion topVer) {
        return F.eq(primary(key, topVer), n);
    }

    /**
     * @param n Node to check.
     * @param part Partition.
     * @param topVer Topology version.
     * @return {@code True} if checked node is primary for given key.
     */
    public boolean primary(ClusterNode n, int part, AffinityTopologyVersion topVer) {
        return F.eq(primary(part, topVer), n);
    }

    /**
     * @param key Key to check.
     * @param topVer Topology version.
     * @return Backup nodes.
     */
    public Collection<ClusterNode> backups(Object key, AffinityTopologyVersion topVer) {
        return backups(partition(key), topVer);
    }

    /**
     * @param part Partition.
     * @param topVer Topology version.
     * @return Backup nodes.
     */
    public Collection<ClusterNode> backups(int part, AffinityTopologyVersion topVer) {
        List<ClusterNode> nodes = nodes(part, topVer);

        assert !F.isEmpty(nodes);

        if (nodes.size() == 1)
            return Collections.emptyList();

        return F.view(nodes, F.notEqualTo(nodes.get(0)));
    }

    /**
     * @param keys keys.
     * @param topVer Topology version.
     * @return Nodes for the keys.
     */
    public Collection<ClusterNode> remoteNodes(Iterable keys, AffinityTopologyVersion topVer) {
        Collection<Collection<ClusterNode>> colcol = new GridLeanSet<>();

        for (Object key : keys)
            colcol.add(nodes(key, topVer));

        return F.view(F.flatCollections(colcol), F.remoteNodes(cctx.localNodeId()));
    }

    /**
     * @param key Key to check.
     * @param topVer Topology version.
     * @return {@code true} if given key belongs to local node.
     */
    public boolean localNode(Object key, AffinityTopologyVersion topVer) {
        return localNode(partition(key), topVer);
    }

    /**
     * @param part Partition number to check.
     * @param topVer Topology version.
     * @return {@code true} if given partition belongs to local node.
     */
    public boolean localNode(int part, AffinityTopologyVersion topVer) {
        assert part >= 0 : "Invalid partition: " + part;

        return nodes(part, topVer).contains(cctx.localNode());
    }

    /**
     * @param node Node.
     * @param part Partition number to check.
     * @param topVer Topology version.
     * @return {@code true} if given partition belongs to specified node.
     */
    public boolean belongs(ClusterNode node, int part, AffinityTopologyVersion topVer) {
        assert node != null;
        assert part >= 0 : "Invalid partition: " + part;

        return nodes(part, topVer).contains(node);
    }

    /**
     * @param nodeId Node ID.
     * @param topVer Topology version to calculate affinity.
     * @return Partitions for which given node is primary.
     */
    public Set<Integer> primaryPartitions(UUID nodeId, AffinityTopologyVersion topVer) {
        if (cctx.isLocal())
            topVer = new AffinityTopologyVersion(1);

        return aff.primaryPartitions(nodeId, topVer);
    }

    /**
     * @param nodeId Node ID.
     * @param topVer Topology version to calculate affinity.
     * @return Partitions for which given node is backup.
     */
    public Set<Integer> backupPartitions(UUID nodeId, AffinityTopologyVersion topVer) {
        if (cctx.isLocal())
            topVer = new AffinityTopologyVersion(1);

        return aff.backupPartitions(nodeId, topVer);
    }

    /**
     * @return Affinity-ready topology version.
     */
    public AffinityTopologyVersion affinityTopologyVersion() {
        return aff.lastVersion();
    }
}
