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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.mvcc.MvccCoordinator;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;

/**
 * Partition topology for node which does not have any local partitions.
 */
@GridToStringExclude
public class GridClientPartitionTopology implements GridDhtPartitionTopology {
    /** */
    private static final GridDhtPartitionState[] MOVING_STATES = new GridDhtPartitionState[] {MOVING};

    /** If true, then check consistency. */
    private static final boolean CONSISTENCY_CHECK = false;

    /** Flag to control amount of output for full map. */
    private static final boolean FULL_MAP_DEBUG = false;

    /** Cache shared context. */
    private GridCacheSharedContext cctx;

    /** Cache ID. */
    private int grpId;

    /** Logger. */
    private final IgniteLogger log;

    /** Node to partition map. */
    private GridDhtPartitionFullMap node2part;

    /** Partition to node map. */
    private final Map<Integer, Set<UUID>> part2node = new HashMap<>();

    /** */
    private AffinityTopologyVersion lastExchangeVer;

    /** */
    private AffinityTopologyVersion topVer = AffinityTopologyVersion.NONE;

    /** */
    private volatile boolean stopping;

    /** A future that will be completed when topology with version topVer will be ready to use. */
    private volatile GridDhtTopologyFuture topReadyFut;

    /** */
    private final GridAtomicLong updateSeq = new GridAtomicLong(1);

    /** Lock. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Partition update counters. */
    private CachePartitionFullCountersMap cntrMap;

    /** */
    private final Object similarAffKey;

    /** */
    private volatile DiscoCache discoCache;

    /** */
    private final int parts;

    /** */
    private volatile Map<Integer, Long> globalPartSizes;

    /**
     * @param cctx Context.
     * @param discoCache Discovery data cache.
     * @param grpId Group ID.
     * @param parts Number of partitions in the group.
     * @param similarAffKey Key to find caches with similar affinity.
     */
    public GridClientPartitionTopology(
        GridCacheSharedContext<?, ?> cctx,
        DiscoCache discoCache,
        int grpId,
        int parts,
        Object similarAffKey
    ) {
        this.cctx = cctx;
        this.discoCache = discoCache;
        this.grpId = grpId;
        this.similarAffKey = similarAffKey;
        this.parts = parts;

        topVer = AffinityTopologyVersion.NONE;

        log = cctx.logger(getClass());

        node2part = new GridDhtPartitionFullMap(cctx.localNode().id(),
            cctx.localNode().order(),
            updateSeq.get());

        cntrMap = new CachePartitionFullCountersMap(parts);
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return parts;
    }

    /**
     * @return Key to find caches with similar affinity.
     */
    @Nullable public Object similarAffinityKey() {
        return similarAffKey;
    }

    /**
     * @return Full map string representation.
     */
    @SuppressWarnings({"ConstantConditions"})
    private String fullMapString() {
        return node2part == null ? "null" : FULL_MAP_DEBUG ? node2part.toFullString() : node2part.toString();
    }

    /**
     * @param map Map to get string for.
     * @return Full map string representation.
     */
    @SuppressWarnings({"ConstantConditions"})
    private String mapString(GridDhtPartitionMap map) {
        return map == null ? "null" : FULL_MAP_DEBUG ? map.toFullString() : map.toString();
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return grpId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"LockAcquiredButNotSafelyReleased"})
    @Override public void readLock() {
        lock.readLock().lock();
    }

    /** {@inheritDoc} */
    @Override public void readUnlock() {
        lock.readLock().unlock();
    }

    /** {@inheritDoc} */
    @Override public MvccCoordinator mvccCoordinator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean holdsLock() {
        return lock.isWriteLockedByCurrentThread() || lock.getReadHoldCount() > 0;
    }

    /** {@inheritDoc} */
    @Override public void updateTopologyVersion(
        GridDhtTopologyFuture exchFut,
        DiscoCache discoCache,
        MvccCoordinator mvccCrd,
        long updSeq,
        boolean stopping
    ) throws IgniteInterruptedCheckedException {
        U.writeLock(lock);

        try {
            AffinityTopologyVersion exchTopVer = exchFut.initialVersion();

            assert exchTopVer.compareTo(topVer) > 0 : "Invalid topology version [grp=" + grpId +
                ", topVer=" + topVer +
                ", exchVer=" + exchTopVer +
                ", discoCacheVer=" + (this.discoCache != null ? this.discoCache.version() : "None") +
                ", exchDiscoCacheVer=" + discoCache.version() + ']';

            this.stopping = stopping;

            topVer = exchTopVer;
            this.discoCache = discoCache;

            updateSeq.setIfGreater(updSeq);

            topReadyFut = exchFut;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion readyTopologyVersion() {
        lock.readLock().lock();

        try {
            assert topVer.topologyVersion() > 0;

            return topVer;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion lastTopologyChangeVersion() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyVersionFuture() {
        assert topReadyFut != null;

        return topReadyFut;
    }

    /** {@inheritDoc} */
    @Override public boolean stopping() {
        return stopping;
    }

    /** {@inheritDoc} */
    @Override public boolean initPartitionsWhenAffinityReady(AffinityTopologyVersion affVer,
        GridDhtPartitionsExchangeFuture exchFut) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(GridDhtPartitionsExchangeFuture exchFut,
        boolean initParts,
        boolean updateMoving)
        throws IgniteCheckedException
    {
        ClusterNode loc = cctx.localNode();

        U.writeLock(lock);

        try {
            if (stopping)
                return;

            discoCache = exchFut.events().discoveryCache();

            beforeExchange0(loc, exchFut);

            if (updateMoving) {
                ExchangeDiscoveryEvents evts = exchFut.context().events();

                GridAffinityAssignmentCache aff = cctx.affinity().affinity(grpId);

                assert aff.lastVersion().equals(evts.topologyVersion());

                createMovingPartitions(aff.readyAffinity(evts.topologyVersion()));
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param aff Affinity.
     */
    private void createMovingPartitions(AffinityAssignment aff) {
        for (Map.Entry<UUID, GridDhtPartitionMap> e : node2part.entrySet()) {
            GridDhtPartitionMap map = e.getValue();

            addMoving(map, aff.backupPartitions(e.getKey()));
            addMoving(map, aff.primaryPartitions(e.getKey()));
        }
    }

    /**
     * @param map Node partition state map.
     * @param parts Partitions assigned to node.
     */
    private void addMoving(GridDhtPartitionMap map, Set<Integer> parts) {
        if (F.isEmpty(parts))
            return;

        for (Integer p : parts) {
            GridDhtPartitionState state = map.get(p);

            if (state == null || state == EVICTED)
                map.put(p, MOVING);
        }
    }

    /**
     * @param loc Local node.
     * @param exchFut Exchange future.
     */
    private void beforeExchange0(ClusterNode loc, GridDhtPartitionsExchangeFuture exchFut) {
        GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

        if (exchFut.context().events().hasServerLeft()) {
            List<DiscoveryEvent> evts0 = exchFut.context().events().events();

            for (int i = 0; i < evts0.size(); i++) {
                DiscoveryEvent evt = evts0.get(i);

                if (ExchangeDiscoveryEvents.serverLeftEvent(evt))
                    removeNode(evt.eventNode().id());
            }
        }

        // In case if node joins, get topology at the time of joining node.
        ClusterNode oldest = discoCache.oldestAliveServerNode();

        assert oldest != null;

        if (log.isDebugEnabled())
            log.debug("Partition map beforeExchange [exchId=" + exchId + ", fullMap=" + fullMapString() + ']');

        long updateSeq = this.updateSeq.incrementAndGet();

        // If this is the oldest node.
        if (oldest.id().equals(loc.id()) || exchFut.dynamicCacheGroupStarted(grpId)) {
            if (node2part == null) {
                node2part = new GridDhtPartitionFullMap(oldest.id(), oldest.order(), updateSeq);

                if (log.isDebugEnabled())
                    log.debug("Created brand new full topology map on oldest node [exchId=" +
                        exchId + ", fullMap=" + fullMapString() + ']');
            }
            else if (!node2part.valid()) {
                node2part = new GridDhtPartitionFullMap(oldest.id(), oldest.order(), updateSeq, node2part, false);

                if (log.isDebugEnabled())
                    log.debug("Created new full topology map on oldest node [exchId=" + exchId + ", fullMap=" +
                        node2part + ']');
            }
            else if (!node2part.nodeId().equals(loc.id())) {
                node2part = new GridDhtPartitionFullMap(oldest.id(), oldest.order(), updateSeq, node2part, false);

                if (log.isDebugEnabled())
                    log.debug("Copied old map into new map on oldest node (previous oldest node left) [exchId=" +
                        exchId + ", fullMap=" + fullMapString() + ']');
            }
        }

        consistencyCheck();

        if (log.isDebugEnabled())
            log.debug("Partition map after beforeExchange [exchId=" + exchId + ", fullMap=" +
                fullMapString() + ']');
    }

    /** {@inheritDoc} */
    @Override public void afterStateRestored(AffinityTopologyVersion topVer) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public boolean afterExchange(GridDhtPartitionsExchangeFuture exchFut) throws IgniteCheckedException {
        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        lock.writeLock().lock();

        try {
            assert topVer.equals(exchFut.topologyVersion()) : "Invalid topology version [topVer=" +
                topVer + ", exchId=" + exchFut.exchangeId() + ']';

            if (log.isDebugEnabled())
                log.debug("Partition map before afterExchange [exchId=" + exchFut.exchangeId() + ", fullMap=" +
                    fullMapString() + ']');

            updateSeq.incrementAndGet();

            consistencyCheck();
        }
        finally {
            lock.writeLock().unlock();
        }

        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDhtLocalPartition localPartition(
        int p,
        AffinityTopologyVersion topVer,
        boolean create
    )
        throws GridDhtInvalidPartitionException {
        if (!create)
            return null;

        throw new GridDhtInvalidPartitionException(p, "Adding entry to evicted partition (often may be caused by " +
            "inconsistent 'key.hashCode()' implementation) " +
            "[part=" + p + ", topVer=" + topVer + ", this.topVer=" + this.topVer + ']');
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDhtLocalPartition localPartition(int p, AffinityTopologyVersion topVer,
        boolean create, boolean showRenting) throws GridDhtInvalidPartitionException {
        return localPartition(p, topVer, create);
    }

    /** {@inheritDoc} */
    @Override public GridDhtLocalPartition forceCreatePartition(int p) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public GridDhtLocalPartition localPartition(int p) {
        return localPartition(p, AffinityTopologyVersion.NONE, false);
    }

    /** {@inheritDoc} */
    @Override public void releasePartitions(int... parts) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public List<GridDhtLocalPartition> localPartitions() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDhtLocalPartition> currentLocalPartitions() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void onRemoved(GridDhtCacheEntry e) {
        assert false : "Entry should not be removed from client topology: " + e;
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap localPartitionMap() {
        lock.readLock().lock();

        try {
            return new GridDhtPartitionMap(cctx.localNodeId(), updateSeq.get(), topVer,
                GridPartitionStateMap.EMPTY, true);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionState partitionState(UUID nodeId, int part) {
        lock.readLock().lock();

        try {
            GridDhtPartitionMap partMap = node2part.get(nodeId);

            if (partMap != null) {
                GridDhtPartitionState state = partMap.get(part);

                return state == null ? EVICTED : state;
            }

            return EVICTED;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public List<ClusterNode> nodes(int p,
        AffinityAssignment affAssignment,
        List<ClusterNode> affNodes) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> nodes(int p, AffinityTopologyVersion topVer) {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer=" + topVer +
                ", node2part=" + node2part + ']';

            List<ClusterNode> nodes = null;

            Collection<UUID> nodeIds = part2node.get(p);

            if (!F.isEmpty(nodeIds)) {
                for (UUID nodeId : nodeIds) {
                    ClusterNode n = discoCache.node(nodeId);

                    if (n != null && (topVer.topologyVersion() < 0 || n.order() <= topVer.topologyVersion())) {
                        if (nodes == null)
                            nodes = new ArrayList<>(nodeIds.size());

                        nodes.add(n);
                    }
                }
            }

            return nodes;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param p Partition.
     * @param topVer Topology version ({@code -1} for all nodes).
     * @param state Partition state.
     * @param states Additional partition states.
     * @return List of nodes for the partition.
     */
    private List<ClusterNode> nodes(int p, AffinityTopologyVersion topVer, GridDhtPartitionState state, GridDhtPartitionState... states) {
        Collection<UUID> allIds = F.nodeIds(discoCache.cacheGroupAffinityNodes(grpId));

        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer=" + topVer +
                ", allIds=" + allIds + ", node2part=" + node2part + ']';

            Collection<UUID> nodeIds = part2node.get(p);

            // Node IDs can be null if both, primary and backup, nodes disappear.
            int size = nodeIds == null ? 0 : nodeIds.size();

            if (size == 0)
                return Collections.emptyList();

            List<ClusterNode> nodes = new ArrayList<>(size);

            for (UUID id : nodeIds) {
                if (topVer.topologyVersion() > 0 && !F.contains(allIds, id))
                    continue;

                if (hasState(p, id, state, states)) {
                    ClusterNode n = discoCache.node(id);

                    if (n != null && (topVer.topologyVersion() < 0 || n.order() <= topVer.topologyVersion()))
                        nodes.add(n);
                }
            }

            return nodes;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> owners(int p, AffinityTopologyVersion topVer) {
        return nodes(p, topVer, OWNING, null);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> owners(int p) {
        return owners(p, AffinityTopologyVersion.NONE);
    }

    /** {@inheritDoc} */
    @Override public List<List<ClusterNode>> allOwners() {
        lock.readLock().lock();

        try {
            int parts = partitions();

            List<List<ClusterNode>> res = new ArrayList<>(parts);

            for (int i = 0; i < parts; i++)
                res.add(new ArrayList<>());

            List<ClusterNode> allNodes = discoCache.cacheGroupAffinityNodes(grpId);

            for (int i = 0; i < allNodes.size(); i++) {
                ClusterNode node = allNodes.get(i);

                GridDhtPartitionMap nodeParts = node2part.get(node.id());

                if (nodeParts != null) {
                    for (Map.Entry<Integer, GridDhtPartitionState> e : nodeParts.map().entrySet()) {
                        if (e.getValue() == OWNING) {
                            int part = e.getKey();

                            List<ClusterNode> owners = res.get(part);

                            owners.add(node);
                        }
                    }
                }
            }

            return res;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> moving(int p) {
        return nodes(p, AffinityTopologyVersion.NONE, MOVING, null);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return List of nodes in state OWNING or MOVING.
     */
    private List<ClusterNode> ownersAndMoving(int p, AffinityTopologyVersion topVer) {
        return nodes(p, topVer, OWNING, MOVING_STATES);
    }

    /** {@inheritDoc} */
    @Override public long updateSequence() {
        return updateSeq.get();
    }

    /**
     * @return Last update sequence.
     */
    public long lastUpdateSequence() {
        lock.writeLock().lock();

        try {
            return updateSeq.incrementAndGet();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionFullMap partitionMap(boolean onlyActive) {
        lock.readLock().lock();

        try {
            if (stopping || node2part == null)
                return null;

            assert node2part.valid() : "Invalid node2part [node2part: " + node2part +
                ", locNodeId=" + cctx.localNodeId() +
                ", igniteInstanceName=" + cctx.igniteInstanceName() + ']';

            GridDhtPartitionFullMap m = node2part;

            return new GridDhtPartitionFullMap(m.nodeId(), m.nodeOrder(), m.updateSequence(), m, onlyActive);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks should current partition map overwritten by new partition map
     * Method returns true if topology version or update sequence of new map are greater than of current map.
     *
     * @param currentMap Current partition map.
     * @param newMap New partition map.
     * @return True if current partition map should be overwritten by new partition map, false in other case.
     */
    private boolean shouldOverridePartitionMap(GridDhtPartitionMap currentMap, GridDhtPartitionMap newMap) {
        return newMap != null &&
            (newMap.topologyVersion().compareTo(currentMap.topologyVersion()) > 0 ||
                newMap.topologyVersion().compareTo(currentMap.topologyVersion()) == 0 && newMap.updateSequence() > currentMap.updateSequence());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public boolean update(
        @Nullable AffinityTopologyVersion exchangeVer,
        GridDhtPartitionFullMap partMap,
        @Nullable CachePartitionFullCountersMap cntrMap,
        Set<Integer> partsToReload,
        @Nullable Map<Integer, Long> partSizes,
        @Nullable AffinityTopologyVersion msgTopVer) {
        if (log.isDebugEnabled())
            log.debug("Updating full partition map [exchVer=" + exchangeVer + ", parts=" + fullMapString() + ']');

        lock.writeLock().lock();

        try {
            if (exchangeVer != null && lastExchangeVer != null && lastExchangeVer.compareTo(exchangeVer) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for full partition map update (will ignore) [lastExchId=" +
                        lastExchangeVer + ", exchVer=" + exchangeVer + ']');

                return false;
            }

            if (msgTopVer != null && lastExchangeVer != null && lastExchangeVer.compareTo(msgTopVer) > 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale topology version for full partition map update message (will ignore) " +
                        "[lastExchId=" + lastExchangeVer + ", topVersion=" + msgTopVer + ']');

                return false;
            }

            boolean fullMapUpdated = (node2part == null);

            if (node2part != null) {
                for (GridDhtPartitionMap part : node2part.values()) {
                    GridDhtPartitionMap newPart = partMap.get(part.nodeId());

                    if (shouldOverridePartitionMap(part, newPart)) {
                        fullMapUpdated = true;

                        if (log.isDebugEnabled())
                            log.debug("Overriding partition map in full update map [exchId=" + exchangeVer + ", curPart=" +
                                    mapString(part) + ", newPart=" + mapString(newPart) + ']');
                    }
                    else {
                        // If for some nodes current partition has a newer map,
                        // then we keep the newer value.
                        partMap.put(part.nodeId(), part);
                    }
                }

                // Check that we have new nodes.
                for (GridDhtPartitionMap part : partMap.values()) {
                    if (fullMapUpdated)
                        break;

                    fullMapUpdated = !node2part.containsKey(part.nodeId());
                }

                // Remove entry if node left.
                for (Iterator<UUID> it = partMap.keySet().iterator(); it.hasNext(); ) {
                    UUID nodeId = it.next();

                    if (!cctx.discovery().alive(nodeId)) {
                        if (log.isDebugEnabled())
                            log.debug("Removing left node from full map update [nodeId=" + nodeId + ", partMap=" +
                                partMap + ']');

                        it.remove();
                    }
                }
            }

            if (!fullMapUpdated) {
                if (log.isDebugEnabled())
                    log.debug("No updates for full partition map (will ignore) [lastExch=" +
                            lastExchangeVer + ", exch=" + exchangeVer + ", curMap=" + node2part + ", newMap=" + partMap + ']');

                return false;
            }

            if (exchangeVer != null)
                lastExchangeVer = exchangeVer;

            node2part = partMap;

            updateSeq.incrementAndGet();

            part2node.clear();

            for (Map.Entry<UUID, GridDhtPartitionMap> e : node2part.entrySet()) {
                for (Map.Entry<Integer, GridDhtPartitionState> e0 : e.getValue().entrySet()) {
                    if (e0.getValue() != MOVING && e0.getValue() != OWNING)
                        continue;

                    int p = e0.getKey();

                    Set<UUID> ids = part2node.get(p);

                    if (ids == null)
                        // Initialize HashSet to size 3 in anticipation that there won't be
                        // more than 3 nodes per partitions.
                        part2node.put(p, ids = U.newHashSet(3));

                    ids.add(e.getKey());
                }
            }

            if (cntrMap != null)
                this.cntrMap = new CachePartitionFullCountersMap(cntrMap);

            if (partSizes != null)
                this.globalPartSizes = partSizes;

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after full update: " + fullMapString());

            return false;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void collectUpdateCounters(CachePartitionPartialCountersMap cntrMap) {
        assert cntrMap != null;

        lock.writeLock().lock();

        try {
            for (int i = 0; i < cntrMap.size(); i++) {
                int pId = cntrMap.partitionAt(i);

                long initialUpdateCntr = cntrMap.initialUpdateCounterAt(i);
                long updateCntr = cntrMap.updateCounterAt(i);

                if (this.cntrMap.updateCounter(pId) < updateCntr) {
                    this.cntrMap.initialUpdateCounter(pId, initialUpdateCntr);
                    this.cntrMap.updateCounter(pId, updateCntr);
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void applyUpdateCounters() {
        // No-op on client topology.
    }

    /**
     * Method checks is new partition map more stale than current partition map
     * New partition map is stale if topology version or update sequence are less or equal than of current map
     *
     * @param currentMap Current partition map
     * @param newMap New partition map
     * @return True if new partition map is more stale than current partition map, false in other case
     */
    private boolean isStaleUpdate(GridDhtPartitionMap currentMap, GridDhtPartitionMap newMap) {
        return currentMap != null && newMap.compareTo(currentMap) <= 0;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public boolean update(
        @Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap parts,
        boolean force
    ) {
        if (log.isDebugEnabled())
            log.debug("Updating single partition map [exchId=" + exchId + ", parts=" + mapString(parts) + ']');

        if (!cctx.discovery().alive(parts.nodeId())) {
            if (log.isDebugEnabled())
                log.debug("Received partition update for non-existing node (will ignore) [exchId=" + exchId +
                    ", parts=" + parts + ']');

            return false;
        }

        lock.writeLock().lock();

        try {
            if (stopping)
                return false;

            if (!force) {
                if (lastExchangeVer != null && exchId != null && lastExchangeVer.compareTo(exchId.topologyVersion()) > 0) {
                    if (log.isDebugEnabled())
                        log.debug("Stale exchange id for single partition map update (will ignore) [lastExchVer=" +
                            lastExchangeVer + ", exchId=" + exchId + ']');

                    return false;
                }
            }

            if (exchId != null)
                lastExchangeVer = exchId.topologyVersion();

            if (node2part == null)
                // Create invalid partition map.
                node2part = new GridDhtPartitionFullMap();

            GridDhtPartitionMap cur = node2part.get(parts.nodeId());

            if (force) {
                if (cur != null && cur.topologyVersion().initialized())
                    parts.updateSequence(cur.updateSequence(), cur.topologyVersion());
            }
            else if (isStaleUpdate(cur, parts)) {
                if (log.isDebugEnabled())
                    log.debug("Stale update for single partition map update (will ignore) [exchId=" + exchId +
                            ", curMap=" + cur + ", newMap=" + parts + ']');

                return false;
            }

            long updateSeq = this.updateSeq.incrementAndGet();

            node2part = new GridDhtPartitionFullMap(node2part, updateSeq);

            boolean changed = false;

            if (cur == null || !cur.equals(parts))
                changed = true;

            node2part.put(parts.nodeId(), parts);

            // Add new mappings.
            for (Map.Entry<Integer,GridDhtPartitionState> e : parts.entrySet()) {
                int p = e.getKey();

                Set<UUID> ids = part2node.get(p);

                if (e.getValue() == MOVING || e.getValue() == OWNING) {
                    if (ids == null)
                        // Initialize HashSet to size 3 in anticipation that there won't be
                        // more than 3 nodes per partition.
                        part2node.put(p, ids = U.newHashSet(3));

                    changed |= ids.add(parts.nodeId());
                }
                else {
                    if (ids != null)
                        changed |= ids.remove(parts.nodeId());
                }
            }

            // Remove obsolete mappings.
            if (cur != null) {
                for (Integer p : F.view(cur.keySet(), F0.notIn(parts.keySet()))) {
                    Set<UUID> ids = part2node.get(p);

                    if (ids != null)
                        changed |= ids.remove(parts.nodeId());
                }
            }

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after single update: " + fullMapString());

            return changed;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onExchangeDone(GridDhtPartitionsExchangeFuture fut, AffinityAssignment assignment,
        boolean updateRebalanceVer) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean detectLostPartitions(AffinityTopologyVersion affVer, DiscoveryEvent discoEvt) {
        assert false : "detectLostPartitions should never be called on client topology";

        return false;
    }

    /** {@inheritDoc} */
    @Override public void resetLostPartitions(AffinityTopologyVersion affVer) {
        assert false : "resetLostPartitions should never be called on client topology";
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        assert false : "lostPartitions should never be called on client topology";

        return Collections.emptyList();
    }

    /**
     * Updates value for single partition.
     *
     * @param p Partition.
     * @param nodeId Node ID.
     * @param state State.
     * @param updateSeq Update sequence.
     */
    private void updateLocal(int p, UUID nodeId, GridDhtPartitionState state, long updateSeq) {
        assert lock.isWriteLockedByCurrentThread();
        assert nodeId.equals(cctx.localNodeId());

        // In case if node joins, get topology at the time of joining node.
        ClusterNode oldest = discoCache.oldestAliveServerNode();

        // If this node became the oldest node.
        if (cctx.localNode().equals(oldest)) {
            long seq = node2part.updateSequence();

            if (seq != updateSeq) {
                if (seq > updateSeq) {
                    if (this.updateSeq.get() < seq) {
                        // Update global counter if necessary.
                        boolean b = this.updateSeq.compareAndSet(this.updateSeq.get(), seq + 1);

                        assert b : "Invalid update sequence [updateSeq=" + updateSeq + ", seq=" + seq +
                            ", curUpdateSeq=" + this.updateSeq.get() + ", node2part=" + node2part.toFullString() + ']';

                        updateSeq = seq + 1;
                    }
                    else
                        updateSeq = seq;
                }

                node2part.updateSequence(updateSeq);
            }
        }

        GridDhtPartitionMap map = node2part.get(nodeId);

        if (map == null)
            node2part.put(nodeId, map = new GridDhtPartitionMap(nodeId, updateSeq, topVer,
                GridPartitionStateMap.EMPTY, false));

        map.updateSequence(updateSeq, topVer);

        map.put(p, state);

        Set<UUID> ids = part2node.get(p);

        if (ids == null)
            part2node.put(p, ids = U.newHashSet(3));

        ids.add(nodeId);
    }

    /**
     * @param nodeId Node to remove.
     */
    private void removeNode(UUID nodeId) {
        assert nodeId != null;
        assert lock.writeLock().isHeldByCurrentThread();

        ClusterNode loc = cctx.localNode();

        if (node2part != null) {
            if (!node2part.nodeId().equals(loc.id())) {
                updateSeq.setIfGreater(node2part.updateSequence());

                node2part = new GridDhtPartitionFullMap(loc.id(), loc.order(), updateSeq.incrementAndGet(),
                    node2part, false);
            }
            else
                node2part = new GridDhtPartitionFullMap(node2part, node2part.updateSequence());

            GridDhtPartitionMap parts = node2part.remove(nodeId);

            if (parts != null) {
                for (Integer p : parts.keySet()) {
                    Set<UUID> nodeIds = part2node.get(p);

                    if (nodeIds != null) {
                        nodeIds.remove(nodeId);

                        if (nodeIds.isEmpty())
                            part2node.remove(p);
                    }
                }
            }

            consistencyCheck();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean own(GridDhtLocalPartition part) {
        assert false : "Client topology should never own a partition: " + part;

        return false;
    }

    /** {@inheritDoc} */
    @Override public void ownMoving(AffinityTopologyVersion topVer) {
        // No-op
    }

    /** {@inheritDoc} */
    @Override public void onEvicted(GridDhtLocalPartition part, boolean updateSeq) {
        assert updateSeq || lock.isWriteLockedByCurrentThread();

        lock.writeLock().lock();

        try {
            if (stopping)
                return;

            assert part.state() == EVICTED;

            long seq = updateSeq ? this.updateSeq.incrementAndGet() : this.updateSeq.get();

            updateLocal(part.id(), cctx.localNodeId(), part.state(), seq);

            consistencyCheck();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDhtPartitionMap partitions(UUID nodeId) {
        lock.readLock().lock();

        try {
            return node2part.get(nodeId);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Set<Integer>> resetOwners(Map<Integer, Set<UUID>> ownersByUpdCounters, Set<Integer> haveHistory) {
        Map<UUID, Set<Integer>> result = new HashMap<>();

        lock.writeLock().lock();

        try {
            // Process remote partitions.
            for (Map.Entry<Integer, Set<UUID>> entry : ownersByUpdCounters.entrySet()) {
                int part = entry.getKey();
                Set<UUID> newOwners = entry.getValue();

                for (Map.Entry<UUID, GridDhtPartitionMap> remotes : node2part.entrySet()) {
                    UUID remoteNodeId = remotes.getKey();
                    GridDhtPartitionMap partMap = remotes.getValue();

                    GridDhtPartitionState state = partMap.get(part);

                    if (state == null || state != OWNING)
                        continue;

                    if (!newOwners.contains(remoteNodeId)) {
                        partMap.put(part, MOVING);

                        partMap.updateSequence(partMap.updateSequence() + 1, partMap.topologyVersion());

                        result.computeIfAbsent(remoteNodeId, n -> new HashSet<>());
                        result.get(remoteNodeId).add(part);
                    }
                }
            }

            for (Map.Entry<UUID, Set<Integer>> entry : result.entrySet()) {
                UUID nodeId = entry.getKey();
                Set<Integer> partsToRebalance = entry.getValue();

                if (!partsToRebalance.isEmpty()) {
                    Set<Integer> historical = partsToRebalance.stream()
                        .filter(haveHistory::contains)
                        .collect(Collectors.toSet());

                    // Filter out partitions having WAL history.
                    partsToRebalance.removeAll(historical);

                    U.warn(log, "Partitions have been scheduled for rebalancing due to outdated update counter "
                        + "[grpId=" + grpId
                        + ", nodeId=" + nodeId
                        + ", partsFull=" + S.compact(partsToRebalance)
                        + ", partsHistorical=" + S.compact(historical) + "]");
                }
            }

            for (Map.Entry<Integer, Set<UUID>> entry : ownersByUpdCounters.entrySet())
                part2node.put(entry.getKey(), entry.getValue());

            updateSeq.incrementAndGet();
        } finally {
            lock.writeLock().unlock();
        }

        return result;
    }

    /** {@inheritDoc} */
    @Override public CachePartitionFullCountersMap fullUpdateCounters() {
        lock.readLock().lock();

        try {
            return new CachePartitionFullCountersMap(cntrMap);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public CachePartitionPartialCountersMap localUpdateCounters(boolean skipZeros) {
        return CachePartitionPartialCountersMap.EMPTY;
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> partitionSizes() {
        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override @Nullable public Map<Integer, Long> globalPartSizes() {
        lock.readLock().lock();

        try {
            if (globalPartSizes == null)
                return Collections.emptyMap();

            return Collections.unmodifiableMap(globalPartSizes);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void globalPartSizes(@Nullable Map<Integer, Long> partSizes) {
        lock.writeLock().lock();

        try {
            this.globalPartSizes = partSizes;
        }
        finally {
            lock.writeLock().unlock();
        }
    }


    /** {@inheritDoc} */
    @Override public boolean rebalanceFinished(AffinityTopologyVersion topVer) {
        assert false : "Should not be called on non-affinity node";

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasMovingPartitions() {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node2part [node2part: " + node2part +
                ", locNodeId=" + cctx.localNodeId() +
                ", igniteInstanceName=" + cctx.igniteInstanceName() + ']';

            for (GridDhtPartitionMap map : node2part.values()) {
                if (map.hasMovingPartitions())
                    return true;
            }

            return false;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats(int threshold) {
        X.println(">>>  Cache partition topology stats [igniteInstanceName=" + cctx.igniteInstanceName() +
            ", grpId=" + grpId + ']');
    }

    /**
     * @param p Partition.
     * @param nodeId Node ID.
     * @param match State to match.
     * @param matches Additional states.
     * @return Filter for owners of this partition.
     */
    private boolean hasState(final int p, @Nullable UUID nodeId, final GridDhtPartitionState match,
        final GridDhtPartitionState... matches) {
        if (nodeId == null)
            return false;

        GridDhtPartitionMap parts = node2part.get(nodeId);

        // Set can be null if node has been removed.
        if (parts != null) {
            GridDhtPartitionState state = parts.get(p);

            if (state == match)
                return true;

            if (matches != null && matches.length > 0)
                for (GridDhtPartitionState s : matches)
                    if (state == s)
                        return true;
        }

        return false;
    }

    /**
     * Checks consistency after all operations.
     */
    private void consistencyCheck() {
        if (CONSISTENCY_CHECK) {
            assert lock.writeLock().isHeldByCurrentThread();

            if (node2part == null)
                return;

            for (Map.Entry<UUID, GridDhtPartitionMap> e : node2part.entrySet()) {
                for (Integer p : e.getValue().keySet()) {
                    Set<UUID> nodeIds = part2node.get(p);

                    assert nodeIds != null : "Failed consistency check [part=" + p + ", nodeId=" + e.getKey() + ']';
                    assert nodeIds.contains(e.getKey()) : "Failed consistency check [part=" + p + ", nodeId=" +
                        e.getKey() + ", nodeIds=" + nodeIds + ']';
                }
            }

            for (Map.Entry<Integer, Set<UUID>> e : part2node.entrySet()) {
                for (UUID nodeId : e.getValue()) {
                    GridDhtPartitionMap map = node2part.get(nodeId);

                    assert map != null : "Failed consistency check [part=" + e.getKey() + ", nodeId=" + nodeId + ']';
                    assert map.containsKey(e.getKey()) : "Failed consistency check [part=" + e.getKey() +
                        ", nodeId=" + nodeId + ']';
                }
            }
        }
    }
}
