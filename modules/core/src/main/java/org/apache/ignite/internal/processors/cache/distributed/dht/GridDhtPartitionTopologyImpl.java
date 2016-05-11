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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignment;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntryFactory;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap2;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

/**
 * Partition topology.
 */
@GridToStringExclude
class GridDhtPartitionTopologyImpl implements GridDhtPartitionTopology {
    /** If true, then check consistency. */
    private static final boolean CONSISTENCY_CHECK = false;

    /** Flag to control amount of output for full map. */
    private static final boolean FULL_MAP_DEBUG = false;

    /** Context. */
    private final GridCacheContext<?, ?> cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private final GridDhtLocalPartition[] locParts;

    /** Node to partition map. */
    private GridDhtPartitionFullMap node2part;

    /** Partition to node map. */
    private Map<Integer, Set<UUID>> part2node = new HashMap<>();

    /** */
    private GridDhtPartitionExchangeId lastExchangeId;

    /** */
    private volatile AffinityTopologyVersion topVer = AffinityTopologyVersion.NONE;

    /** */
    private volatile boolean stopping;

    /** A future that will be completed when topology with version topVer will be ready to use. */
    private GridDhtTopologyFuture topReadyFut;

    /** */
    private final GridAtomicLong updateSeq = new GridAtomicLong(1);

    /** Lock. */
    private final StripedCompositeReadWriteLock lock = new StripedCompositeReadWriteLock(16);

    /** */
    private final GridCacheMapEntryFactory entryFactory;

    /** Partition update counter. */
    private Map<Integer, Long> cntrMap = new HashMap<>();

    /** */
    private volatile AffinityTopologyVersion rebalancedTopVer = AffinityTopologyVersion.NONE;

    /**
     * @param cctx Context.
     */
    GridDhtPartitionTopologyImpl(GridCacheContext<?, ?> cctx, GridCacheMapEntryFactory entryFactory) {
        assert cctx != null;

        this.cctx = cctx;
        this.entryFactory = entryFactory;

        log = cctx.logger(getClass());

        locParts = new GridDhtLocalPartition[cctx.config().getAffinity().partitions()];
    }

    /**
     *
     */
    public void onReconnected() {
        lock.writeLock().lock();

        try {
            node2part = null;

            part2node = new HashMap<>();

            lastExchangeId = null;

            updateSeq.set(1);

            topReadyFut = null;

            rebalancedTopVer = AffinityTopologyVersion.NONE;

            topVer = AffinityTopologyVersion.NONE;
        }
        finally {
            lock.writeLock().unlock();
        }
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
    private String mapString(GridDhtPartitionMap2 map) {
        return map == null ? "null" : FULL_MAP_DEBUG ? map.toFullString() : map.toString();
    }

    /**
     * Waits for renting partitions.
     *
     * @return {@code True} if mapping was changed.
     * @throws IgniteCheckedException If failed.
     */
    private boolean waitForRent() throws IgniteCheckedException {
        boolean changed = false;

        GridDhtLocalPartition[] locPartsCopy = new GridDhtLocalPartition[locParts.length];

        lock.readLock().lock();

        try {
            for (int i = 0; i < locParts.length; i++)
                locPartsCopy[i] = locParts[i];
        }
        finally {
            lock.readLock().unlock();
        }

        GridDhtLocalPartition part;

        for (int i = 0; i < locPartsCopy.length; i++) {
            part = locPartsCopy[i];

            if (part == null)
                continue;

            GridDhtPartitionState state = part.state();

            if (state == RENTING || state == EVICTED) {
                if (log.isDebugEnabled())
                    log.debug("Waiting for renting partition: " + part);

                // Wait for partition to empty out.
                part.rent(true).get();

                if (log.isDebugEnabled())
                    log.debug("Finished waiting for renting partition: " + part);
            }
        }

        // Remove evicted partition.
        lock.writeLock().lock();

        try {
            for (int i = 0; i < locParts.length; i++) {
                part = locParts[i];

                if (part == null)
                    continue;

                if (part.state() == EVICTED) {
                    locParts[i] = null;
                    changed = true;
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        return changed;
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
    @Override public void updateTopologyVersion(
        GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture exchFut,
        long updSeq,
        boolean stopping
    ) throws IgniteInterruptedCheckedException {
        U.writeLock(lock);

        try {
            assert exchId.topologyVersion().compareTo(topVer) > 0 : "Invalid topology version [topVer=" + topVer +
                ", exchId=" + exchId + ']';

            this.stopping = stopping;

            updateSeq.setIfGreater(updSeq);

            topReadyFut = exchFut;

            rebalancedTopVer = AffinityTopologyVersion.NONE;

            topVer = exchId.topologyVersion();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        AffinityTopologyVersion topVer = this.topVer;

        assert topVer.topologyVersion() > 0 : "Invalid topology version [topVer=" + topVer +
            ", cacheName=" + cctx.name() + ']';

        return topVer;
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyVersionFuture() {
        lock.readLock().lock();

        try {
            assert topReadyFut != null;

            return topReadyFut;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean stopping() {
        return stopping;
    }

    /** {@inheritDoc} */
    @Override public void initPartitions(
        GridDhtPartitionsExchangeFuture exchFut) throws IgniteInterruptedCheckedException {
        U.writeLock(lock);

        try {
            if (stopping)
                return;

            long updateSeq = this.updateSeq.incrementAndGet();

            initPartitions0(exchFut, updateSeq);

            consistencyCheck();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param exchFut Exchange future.
     * @param updateSeq Update sequence.
     */
    private void initPartitions0(GridDhtPartitionsExchangeFuture exchFut, long updateSeq) {
        ClusterNode loc = cctx.localNode();

        ClusterNode oldest = CU.oldestAliveCacheServerNode(cctx.shared(), topVer);

        assert oldest != null || cctx.kernalContext().clientNode();

        GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

        assert topVer.equals(exchFut.topologyVersion()) :
            "Invalid topology [topVer=" + topVer +
                ", cache=" + cctx.name() +
                ", futVer=" + exchFut.topologyVersion() + ']';
        assert cctx.affinity().affinityTopologyVersion().equals(exchFut.topologyVersion()) :
            "Invalid affinity [topVer=" + cctx.affinity().affinityTopologyVersion() +
                ", cache=" + cctx.name() +
                ", futVer=" + exchFut.topologyVersion() + ']';

        List<List<ClusterNode>> aff = cctx.affinity().assignments(exchFut.topologyVersion());

        int num = cctx.affinity().partitions();

        if (cctx.rebalanceEnabled()) {
            boolean added = exchFut.isCacheAdded(cctx.cacheId(), exchId.topologyVersion());

            boolean first = (loc.equals(oldest) && loc.id().equals(exchId.nodeId()) && exchId.isJoined()) || added;

            if (first) {
                assert exchId.isJoined() || added;

                for (int p = 0; p < num; p++) {
                    if (localNode(p, aff)) {
                        GridDhtLocalPartition locPart = createPartition(p);

                        boolean owned = locPart.own();

                        assert owned : "Failed to own partition for oldest node [cacheName" + cctx.name() +
                            ", part=" + locPart + ']';

                        if (log.isDebugEnabled())
                            log.debug("Owned partition for oldest node: " + locPart);

                        updateLocal(p, loc.id(), locPart.state(), updateSeq);
                    }
                }
            }
            else
                createPartitions(aff, updateSeq);
        }
        else {
            // If preloader is disabled, then we simply clear out
            // the partitions this node is not responsible for.
            for (int p = 0; p < num; p++) {
                GridDhtLocalPartition locPart = localPartition(p, topVer, false, false);

                boolean belongs = localNode(p, aff);

                if (locPart != null) {
                    if (!belongs) {
                        GridDhtPartitionState state = locPart.state();

                        if (state.active()) {
                            locPart.rent(false);

                            updateLocal(p, loc.id(), locPart.state(), updateSeq);

                            if (log.isDebugEnabled())
                                log.debug("Evicting partition with rebalancing disabled " +
                                    "(it does not belong to affinity): " + locPart);
                        }
                    }
                }
                else if (belongs)
                    createPartition(p);
            }
        }

        if (node2part != null && node2part.valid())
            checkEvictions(updateSeq, aff);

        updateRebalanceVersion(aff);
    }

    /**
     * @param aff Affinity assignments.
     * @param updateSeq Update sequence.
     */
    private void createPartitions(List<List<ClusterNode>> aff, long updateSeq) {
        ClusterNode loc = cctx.localNode();

        int num = cctx.affinity().partitions();

        for (int p = 0; p < num; p++) {
            if (node2part != null && node2part.valid()) {
                if (localNode(p, aff)) {
                    // This will make sure that all non-existing partitions
                    // will be created in MOVING state.
                    GridDhtLocalPartition locPart = createPartition(p);

                    updateLocal(p, loc.id(), locPart.state(), updateSeq);
                }
            }
            // If this node's map is empty, we pre-create local partitions,
            // so local map will be sent correctly during exchange.
            else if (localNode(p, aff))
                createPartition(p);
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(GridDhtPartitionsExchangeFuture exchFut, boolean affReady)
        throws IgniteCheckedException {
        waitForRent();

        ClusterNode loc = cctx.localNode();

        U.writeLock(lock);

        try {
            GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

            if (stopping)
                return;

            assert topVer.equals(exchId.topologyVersion()) : "Invalid topology version [topVer=" +
                topVer + ", exchId=" + exchId + ']';

            if (exchId.isLeft())
                removeNode(exchId.nodeId());

            // In case if node joins, get topology at the time of joining node.
            ClusterNode oldest = CU.oldestAliveCacheServerNode(cctx.shared(), topVer);

            assert oldest != null || cctx.kernalContext().clientNode();

            if (log.isDebugEnabled())
                log.debug("Partition map beforeExchange [exchId=" + exchId + ", fullMap=" + fullMapString() + ']');

            long updateSeq = this.updateSeq.incrementAndGet();

            cntrMap.clear();

            // If this is the oldest node.
            if (oldest != null && (loc.equals(oldest) || exchFut.isCacheAdded(cctx.cacheId(), exchId.topologyVersion()))) {
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

            if (affReady)
                initPartitions0(exchFut, updateSeq);
            else {
                List<List<ClusterNode>> aff = cctx.affinity().idealAssignment();

                createPartitions(aff, updateSeq);
            }

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after beforeExchange [exchId=" + exchId + ", fullMap=" +
                    fullMapString() + ']');
        }
        finally {
            lock.writeLock().unlock();
        }

        // Wait for evictions.
        waitForRent();
    }

    /** {@inheritDoc} */
    @Override public boolean afterExchange(GridDhtPartitionsExchangeFuture exchFut) throws IgniteCheckedException {
        boolean changed = waitForRent();

        ClusterNode loc = cctx.localNode();

        int num = cctx.affinity().partitions();

        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        assert cctx.affinity().affinityTopologyVersion().equals(topVer) : "Affinity is not initialized " +
            "[topVer=" + topVer +
            ", affVer=" + cctx.affinity().affinityTopologyVersion() +
            ", fut=" + exchFut + ']';

        lock.writeLock().lock();

        try {
            if (stopping)
                return false;

            assert topVer.equals(exchFut.topologyVersion()) : "Invalid topology version [topVer=" +
                topVer + ", exchId=" + exchFut.exchangeId() + ']';

            if (log.isDebugEnabled())
                log.debug("Partition map before afterExchange [exchId=" + exchFut.exchangeId() + ", fullMap=" +
                    fullMapString() + ']');

            long updateSeq = this.updateSeq.incrementAndGet();

            for (int p = 0; p < num; p++) {
                GridDhtLocalPartition locPart = localPartition(p, topVer, false, false);

                if (cctx.affinity().localNode(p, topVer)) {
                    // This partition will be created during next topology event,
                    // which obviously has not happened at this point.
                    if (locPart == null) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping local partition afterExchange (will not create): " + p);

                        continue;
                    }

                    GridDhtPartitionState state = locPart.state();

                    if (state == MOVING) {
                        if (cctx.rebalanceEnabled()) {
                            Collection<ClusterNode> owners = owners(p);

                            // If there are no other owners, then become an owner.
                            if (F.isEmpty(owners)) {
                                boolean owned = locPart.own();

                                assert owned : "Failed to own partition [cacheName" + cctx.name() + ", locPart=" +
                                    locPart + ']';

                                updateLocal(p, loc.id(), locPart.state(), updateSeq);

                                changed = true;

                                if (cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_DATA_LOST)) {
                                    DiscoveryEvent discoEvt = exchFut.discoveryEvent();

                                    cctx.events().addPreloadEvent(p,
                                        EVT_CACHE_REBALANCE_PART_DATA_LOST, discoEvt.eventNode(),
                                        discoEvt.type(), discoEvt.timestamp());
                                }

                                if (log.isDebugEnabled())
                                    log.debug("Owned partition: " + locPart);
                            }
                            else if (log.isDebugEnabled())
                                log.debug("Will not own partition (there are owners to rebalance from) [locPart=" +
                                    locPart + ", owners = " + owners + ']');
                        }
                        else
                            updateLocal(p, loc.id(), locPart.state(), updateSeq);
                    }
                }
                else {
                    if (locPart != null) {
                        GridDhtPartitionState state = locPart.state();

                        if (state == MOVING) {
                            locPart.rent(false);

                            updateLocal(p, loc.id(), locPart.state(), updateSeq);

                            changed = true;

                            if (log.isDebugEnabled())
                                log.debug("Evicting moving partition (it does not belong to affinity): " + locPart);
                        }
                    }
                }
            }

            updateRebalanceVersion(cctx.affinity().assignments(topVer));

            consistencyCheck();
        }
        finally {
            lock.writeLock().unlock();
        }

        return changed;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDhtLocalPartition localPartition(int p, AffinityTopologyVersion topVer,
        boolean create)
        throws GridDhtInvalidPartitionException {
        return localPartition(p, topVer, create, true);
    }

    /**
     * @param p Partition number.
     * @return Partition.
     */
    private GridDhtLocalPartition createPartition(int p) {
        assert lock.isWriteLockedByCurrentThread();

        GridDhtLocalPartition loc = locParts[p];

        if (loc == null || loc.state() == EVICTED)
            locParts[p] = loc = new GridDhtLocalPartition(cctx, p, entryFactory);

        return loc;
    }

    /**
     * @param p Partition number.
     * @param topVer Topology version.
     * @param create Create flag.
     * @param updateSeq Update sequence.
     * @return Local partition.
     */
    private GridDhtLocalPartition localPartition(int p,
        AffinityTopologyVersion topVer,
        boolean create,
        boolean updateSeq) {
        GridDhtLocalPartition loc;

        lock.readLock().lock();

        try {
            loc = locParts[p];
        }
        finally {
            lock.readLock().unlock();
        }

        if (loc != null && loc.state() != EVICTED)
            return loc;

        if (!create)
            return null;

        lock.writeLock().lock();

        try {
            loc = locParts[p];

            boolean belongs = cctx.affinity().localNode(p, topVer);

            if (loc != null && loc.state() == EVICTED) {
                locParts[p] = loc = null;

                if (!belongs)
                    throw new GridDhtInvalidPartitionException(p, "Adding entry to evicted partition " +
                        "(often may be caused by inconsistent 'key.hashCode()' implementation) " +
                        "[part=" + p + ", topVer=" + topVer + ", this.topVer=" + this.topVer + ']');
            }

            if (loc == null) {
                if (!belongs)
                    throw new GridDhtInvalidPartitionException(p, "Creating partition which does not belong to " +
                        "local node (often may be caused by inconsistent 'key.hashCode()' implementation) " +
                        "[part=" + p + ", topVer=" + topVer + ", this.topVer=" + this.topVer + ']');

                locParts[p] = loc = new GridDhtLocalPartition(cctx, p, entryFactory);

                if (updateSeq)
                    this.updateSeq.incrementAndGet();

                if (log.isDebugEnabled())
                    log.debug("Created local partition: " + loc);
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        return loc;
    }

    /** {@inheritDoc} */
    @Override public void releasePartitions(int... parts) {
        assert parts != null;
        assert parts.length > 0;

        GridDhtLocalPartition[] locPartsCopy = new GridDhtLocalPartition[parts.length];

        lock.readLock().lock();

        try {
            for (int i = 0; i < parts.length; i++)
                locPartsCopy[i] = locParts[parts[i]];
        }
        finally {
            lock.readLock().unlock();
        }

        for (int i = 0; i < parts.length; i++)
            locPartsCopy[i].release();
    }

    /** {@inheritDoc} */
    @Override public GridDhtLocalPartition localPartition(Object key, boolean create) {
        return localPartition(cctx.affinity().partition(key), AffinityTopologyVersion.NONE, create);
    }

    /** {@inheritDoc} */
    @Override public List<GridDhtLocalPartition> localPartitions() {
        LinkedList<GridDhtLocalPartition> list = new LinkedList<>();

        lock.readLock().lock();

        try {
            for (int i = 0; i < locParts.length; i++) {
                GridDhtLocalPartition part = locParts[i];

                if (part != null)
                    list.add(part);
            }

            return list;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<GridDhtLocalPartition> currentLocalPartitions() {
        return localPartitions();
    }

    /** {@inheritDoc} */
    @Override public void onRemoved(GridDhtCacheEntry e) {
        /*
         * Make sure not to acquire any locks here as this method
         * may be called from sensitive synchronization blocks.
         * ===================================================
         */

        GridDhtLocalPartition loc = localPartition(cctx.affinity().partition(e.key()), topologyVersion(), false);

        if (loc != null)
            loc.onRemoved(e);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap2 localPartitionMap() {
        Map<Integer, GridDhtPartitionState> map = new HashMap<>();

        lock.readLock().lock();

        try {
            for (int i = 0; i < locParts.length; i++) {
                GridDhtLocalPartition part = locParts[i];

                if (part == null)
                    continue;

                map.put(i, part.state());
            }

            return new GridDhtPartitionMap2(cctx.nodeId(), updateSeq.get(), topVer,
                Collections.unmodifiableMap(map), true);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionState partitionState(UUID nodeId, int part) {
        lock.readLock().lock();

        try {
            GridDhtPartitionMap2 partMap = node2part.get(nodeId);

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
    @Override public List<ClusterNode> nodes(int p, AffinityTopologyVersion topVer) {
        GridAffinityAssignment affAssignment = cctx.affinity().assignment(topVer);

        List<ClusterNode> affNodes = affAssignment.get(p);

        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer1=" + topVer +
                ", topVer2=" + this.topVer +
                ", node=" + cctx.gridName() +
                ", cache=" + cctx.name() +
                ", node2part=" + node2part + ']';

            List<ClusterNode> nodes = null;

            Collection<UUID> nodeIds = part2node.get(p);

            if (!F.isEmpty(nodeIds)) {
                for (UUID nodeId : nodeIds) {
                    HashSet<UUID> affIds = affAssignment.getIds(p);

                    if (!affIds.contains(nodeId) && hasState(p, nodeId, OWNING, MOVING, RENTING)) {
                        ClusterNode n = cctx.discovery().node(nodeId);

                        if (n != null && (topVer.topologyVersion() < 0 || n.order() <= topVer.topologyVersion())) {
                            if (nodes == null) {
                                nodes = new ArrayList<>(affNodes.size() + 2);

                                nodes.addAll(affNodes);
                            }

                            nodes.add(n);
                        }
                    }
                }
            }

            return nodes != null ? nodes : affNodes;
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
    private List<ClusterNode> nodes(int p,
        AffinityTopologyVersion topVer,
        GridDhtPartitionState state,
        GridDhtPartitionState... states) {
        Collection<UUID> allIds = topVer.topologyVersion() > 0 ? F.nodeIds(CU.affinityNodes(cctx, topVer)) : null;

        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer=" + topVer +
                ", allIds=" + allIds +
                ", node2part=" + node2part +
                ", cache=" + cctx.name() + ']';

            Collection<UUID> nodeIds = part2node.get(p);

            // Node IDs can be null if both, primary and backup, nodes disappear.
            int size = nodeIds == null ? 0 : nodeIds.size();

            if (size == 0)
                return Collections.emptyList();

            List<ClusterNode> nodes = new ArrayList<>(size);

            for (UUID id : nodeIds) {
                if (topVer.topologyVersion() > 0 && !allIds.contains(id))
                    continue;

                if (hasState(p, id, state, states)) {
                    ClusterNode n = cctx.discovery().node(id);

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
        if (!cctx.rebalanceEnabled())
            return ownersAndMoving(p, topVer);

        return nodes(p, topVer, OWNING);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> owners(int p) {
        return owners(p, AffinityTopologyVersion.NONE);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> moving(int p) {
        if (!cctx.rebalanceEnabled())
            return ownersAndMoving(p, AffinityTopologyVersion.NONE);

        return nodes(p, AffinityTopologyVersion.NONE, MOVING);
    }

    /**
     * @param p Partition.
     * @param topVer Topology version.
     * @return List of nodes in state OWNING or MOVING.
     */
    private List<ClusterNode> ownersAndMoving(int p, AffinityTopologyVersion topVer) {
        return nodes(p, topVer, OWNING, MOVING);
    }

    /** {@inheritDoc} */
    @Override public long updateSequence() {
        return updateSeq.get();
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionFullMap partitionMap(boolean onlyActive) {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node2part [node2part: " + node2part +
                ", cache=" + cctx.name() +
                ", started=" + cctx.started() +
                ", stopping=" + stopping +
                ", locNodeId=" + cctx.localNode().id() +
                ", locName=" + cctx.gridName() + ']';

            GridDhtPartitionFullMap m = node2part;

            return new GridDhtPartitionFullMap(m.nodeId(), m.nodeOrder(), m.updateSequence(), m, onlyActive);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Nullable @Override public GridDhtPartitionMap2 update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionFullMap partMap,
        @Nullable Map<Integer, Long> cntrMap) {
        if (log.isDebugEnabled())
            log.debug("Updating full partition map [exchId=" + exchId + ", parts=" + fullMapString() + ']');

        assert partMap != null;

        lock.writeLock().lock();

        try {
            if (stopping)
                return null;

            if (cntrMap != null) {
                for (Map.Entry<Integer, Long> e : cntrMap.entrySet()) {
                    Long cntr = this.cntrMap.get(e.getKey());

                    if (cntr == null || cntr < e.getValue())
                        this.cntrMap.put(e.getKey(), e.getValue());
                }

                for (int i = 0; i < locParts.length; i++) {
                    GridDhtLocalPartition part = locParts[i];

                    if (part == null)
                        continue;

                    Long cntr = cntrMap.get(part.id());

                    if (cntr != null)
                        part.updateCounter(cntr);
                }
            }

            if (exchId != null && lastExchangeId != null && lastExchangeId.compareTo(exchId) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for full partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ']');

                return null;
            }

            if (node2part != null && node2part.compareTo(partMap) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale partition map for full partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ", curMap=" + node2part + ", newMap=" + partMap + ']');

                return null;
            }

            long updateSeq = this.updateSeq.incrementAndGet();

            if (exchId != null)
                lastExchangeId = exchId;

            if (node2part != null) {
                for (GridDhtPartitionMap2 part : node2part.values()) {
                    GridDhtPartitionMap2 newPart = partMap.get(part.nodeId());

                    // If for some nodes current partition has a newer map,
                    // then we keep the newer value.
                    if (newPart != null &&
                        (newPart.updateSequence() < part.updateSequence() || (
                            cctx.startTopologyVersion() != null &&
                                newPart.topologyVersion() != null && // Backward compatibility.
                                cctx.startTopologyVersion().compareTo(newPart.topologyVersion()) > 0))
                        ) {
                        if (log.isDebugEnabled())
                            log.debug("Overriding partition map in full update map [exchId=" + exchId + ", curPart=" +
                                mapString(part) + ", newPart=" + mapString(newPart) + ']');

                        partMap.put(part.nodeId(), part);
                    }
                }

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

            node2part = partMap;

            Map<Integer, Set<UUID>> p2n = new HashMap<>(cctx.affinity().partitions(), 1.0f);

            for (Map.Entry<UUID, GridDhtPartitionMap2> e : partMap.entrySet()) {
                for (Integer p : e.getValue().keySet()) {
                    Set<UUID> ids = p2n.get(p);

                    if (ids == null)
                        // Initialize HashSet to size 3 in anticipation that there won't be
                        // more than 3 nodes per partitions.
                        p2n.put(p, ids = U.newHashSet(3));

                    ids.add(e.getKey());
                }
            }

            part2node = p2n;

            boolean changed = false;

            AffinityTopologyVersion affVer = cctx.affinity().affinityTopologyVersion();

            if (!affVer.equals(AffinityTopologyVersion.NONE) && affVer.compareTo(topVer) >= 0) {
                List<List<ClusterNode>> aff = cctx.affinity().assignments(topVer);

                changed = checkEvictions(updateSeq, aff);

                updateRebalanceVersion(aff);
            }

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after full update: " + fullMapString());

            return changed ? localPartitionMap() : null;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Nullable @Override public GridDhtPartitionMap2 update(@Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap2 parts,
        @Nullable Map<Integer, Long> cntrMap) {
        if (log.isDebugEnabled())
            log.debug("Updating single partition map [exchId=" + exchId + ", parts=" + mapString(parts) + ']');

        if (!cctx.discovery().alive(parts.nodeId())) {
            if (log.isDebugEnabled())
                log.debug("Received partition update for non-existing node (will ignore) [exchId=" + exchId +
                    ", parts=" + parts + ']');

            return null;
        }

        lock.writeLock().lock();

        try {
            if (stopping)
                return null;

            if (cntrMap != null) {
                for (Map.Entry<Integer, Long> e : cntrMap.entrySet()) {
                    Long cntr = this.cntrMap.get(e.getKey());

                    if (cntr == null || cntr < e.getValue())
                        this.cntrMap.put(e.getKey(), e.getValue());
                }

                for (int i = 0; i < locParts.length; i++) {
                    GridDhtLocalPartition part = locParts[i];

                    if (part == null)
                        continue;

                    Long cntr = cntrMap.get(part.id());

                    if (cntr != null)
                        part.updateCounter(cntr);
                }
            }

            if (lastExchangeId != null && exchId != null && lastExchangeId.compareTo(exchId) > 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for single partition map update (will ignore) [lastExchId=" +
                        lastExchangeId + ", exchId=" + exchId + ']');

                return null;
            }

            if (exchId != null)
                lastExchangeId = exchId;

            if (node2part == null)
                // Create invalid partition map.
                node2part = new GridDhtPartitionFullMap();

            GridDhtPartitionMap2 cur = node2part.get(parts.nodeId());

            if (cur != null && cur.updateSequence() >= parts.updateSequence()) {
                if (log.isDebugEnabled())
                    log.debug("Stale update sequence for single partition map update (will ignore) [exchId=" + exchId +
                        ", curSeq=" + cur.updateSequence() + ", newSeq=" + parts.updateSequence() + ']');

                return null;
            }

            long updateSeq = this.updateSeq.incrementAndGet();

            node2part = new GridDhtPartitionFullMap(node2part, updateSeq);

            boolean changed = false;

            if (cur == null || !cur.equals(parts))
                changed = true;

            node2part.put(parts.nodeId(), parts);

            part2node = new HashMap<>(part2node);

            // Add new mappings.
            for (Integer p : parts.keySet()) {
                Set<UUID> ids = part2node.get(p);

                if (ids == null)
                    // Initialize HashSet to size 3 in anticipation that there won't be
                    // more than 3 nodes per partition.
                    part2node.put(p, ids = U.newHashSet(3));

                changed |= ids.add(parts.nodeId());
            }

            // Remove obsolete mappings.
            if (cur != null) {
                for (Integer p : F.view(cur.keySet(), F0.notIn(parts.keySet()))) {
                    Set<UUID> ids = part2node.get(p);

                    if (ids != null)
                        changed |= ids.remove(parts.nodeId());
                }
            }

            AffinityTopologyVersion affVer = cctx.affinity().affinityTopologyVersion();

            if (!affVer.equals(AffinityTopologyVersion.NONE) && affVer.compareTo(topVer) >= 0) {
                List<List<ClusterNode>> aff = cctx.affinity().assignments(topVer);

                changed |= checkEvictions(updateSeq, aff);

                updateRebalanceVersion(aff);
            }

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after single update: " + fullMapString());

            return changed ? localPartitionMap() : null;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param updateSeq Update sequence.
     * @param aff Affinity assignments.
     * @return Checks if any of the local partitions need to be evicted.
     */
    private boolean checkEvictions(long updateSeq, List<List<ClusterNode>> aff) {
        boolean changed = false;

        UUID locId = cctx.nodeId();

        for (int p = 0; p < locParts.length; p++) {
            GridDhtLocalPartition part = locParts[p];

            if (part == null)
                continue;

            GridDhtPartitionState state = part.state();

            if (state.active()) {
                List<ClusterNode> affNodes = aff.get(p);

                if (!affNodes.contains(cctx.localNode())) {
                    Collection<UUID> nodeIds = F.nodeIds(nodes(p, topVer, OWNING));

                    // If all affinity nodes are owners, then evict partition from local node.
                    if (nodeIds.containsAll(F.nodeIds(affNodes))) {
                        part.rent(false);

                        updateLocal(part.id(), locId, part.state(), updateSeq);

                        changed = true;

                        if (log.isDebugEnabled())
                            log.debug("Evicted local partition (all affinity nodes are owners): " + part);
                    }
                    else {
                        int ownerCnt = nodeIds.size();
                        int affCnt = affNodes.size();

                        if (ownerCnt > affCnt) {
                            List<ClusterNode> sorted = new ArrayList<>(cctx.discovery().nodes(nodeIds));

                            // Sort by node orders in ascending order.
                            Collections.sort(sorted, CU.nodeComparator(true));

                            int diff = sorted.size() - affCnt;

                            for (int i = 0; i < diff; i++) {
                                ClusterNode n = sorted.get(i);

                                if (locId.equals(n.id())) {
                                    part.rent(false);

                                    updateLocal(part.id(), locId, part.state(), updateSeq);

                                    changed = true;

                                    if (log.isDebugEnabled())
                                        log.debug("Evicted local partition (this node is oldest non-affinity node): " +
                                            part);

                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        return changed;
    }

    /**
     * Updates value for single partition.
     *
     * @param p Partition.
     * @param nodeId Node ID.
     * @param state State.
     * @param updateSeq Update sequence.
     */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    private void updateLocal(int p, UUID nodeId, GridDhtPartitionState state, long updateSeq) {
        assert nodeId.equals(cctx.nodeId());

        // In case if node joins, get topology at the time of joining node.
        ClusterNode oldest = CU.oldestAliveCacheServerNode(cctx.shared(), topVer);

        assert oldest != null || cctx.kernalContext().clientNode();

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

        GridDhtPartitionMap2 map = node2part.get(nodeId);

        if (map == null)
            node2part.put(nodeId, map = new GridDhtPartitionMap2(nodeId, updateSeq, topVer,
                Collections.<Integer, GridDhtPartitionState>emptyMap(), false));

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

        ClusterNode oldest = CU.oldest(cctx.discovery().serverNodes(topVer));

        assert oldest != null;

        ClusterNode loc = cctx.localNode();

        if (node2part != null) {
            if (oldest.equals(loc) && !node2part.nodeId().equals(loc.id())) {
                updateSeq.setIfGreater(node2part.updateSequence());

                node2part = new GridDhtPartitionFullMap(loc.id(), loc.order(), updateSeq.incrementAndGet(),
                    node2part, false);
            }
            else
                node2part = new GridDhtPartitionFullMap(node2part, node2part.updateSequence());

            part2node = new HashMap<>(part2node);

            GridDhtPartitionMap2 parts = node2part.remove(nodeId);

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
        ClusterNode loc = cctx.localNode();

        lock.writeLock().lock();

        try {
            if (part.own()) {
                updateLocal(part.id(), loc.id(), part.state(), updateSeq.incrementAndGet());

                consistencyCheck();

                return true;
            }

            consistencyCheck();

            return false;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onEvicted(GridDhtLocalPartition part, boolean updateSeq) {
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
    @Nullable @Override public GridDhtPartitionMap2 partitions(UUID nodeId) {
        lock.readLock().lock();

        try {
            return node2part.get(nodeId);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> updateCounters() {
        lock.readLock().lock();

        try {
            Map<Integer, Long> res = new HashMap<>(cntrMap);

            for (int i = 0; i < locParts.length; i++) {
                GridDhtLocalPartition part = locParts[i];

                if (part == null)
                    continue;

                Long cntr0 = res.get(part.id());
                Long cntr1 = part.updateCounter();

                if (cntr0 == null || cntr1 > cntr0)
                    res.put(part.id(), cntr1);
            }

            return res;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean rebalanceFinished(AffinityTopologyVersion topVer) {
        AffinityTopologyVersion curTopVer = this.topVer;

        return curTopVer.equals(topVer) && curTopVer.equals(rebalancedTopVer);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats(int threshold) {
        X.println(">>>  Cache partition topology stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() + ']');

        lock.readLock().lock();

        try {
            for (int i = 0; i < locParts.length; i++) {
                GridDhtLocalPartition part = locParts[i];

                if (part == null)
                    continue;

                int size = part.size();

                if (size >= threshold)
                    X.println(">>>   Local partition [part=" + part.id() + ", size=" + size + ']');
            }
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * @param part Partition.
     * @param aff Affinity assignments.
     * @return {@code True} if given partition belongs to local node.
     */
    private boolean localNode(int part, List<List<ClusterNode>> aff) {
        return aff.get(part).contains(cctx.localNode());
    }

    /**
     * @param aff Affinity assignments.
     */
    private void updateRebalanceVersion(List<List<ClusterNode>> aff) {
        if (!rebalancedTopVer.equals(topVer)) {
            if (node2part == null || !node2part.valid())
                return;

            for (int i = 0; i < cctx.affinity().partitions(); i++) {
                List<ClusterNode> affNodes = aff.get(i);

                // Topology doesn't contain server nodes (just clients).
                if (affNodes.isEmpty())
                    continue;

                List<ClusterNode> owners = owners(i);

                if (affNodes.size() != owners.size() || !owners.containsAll(affNodes))
                    return;
            }

            rebalancedTopVer = topVer;

            if (log.isDebugEnabled())
                log.debug("Updated rebalanced version [cache=" + cctx.name() + ", ver=" + rebalancedTopVer + ']');
        }
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

        GridDhtPartitionMap2 parts = node2part.get(nodeId);

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
            if (node2part == null)
                return;

            for (Map.Entry<UUID, GridDhtPartitionMap2> e : node2part.entrySet()) {
                for (Integer p : e.getValue().keySet()) {
                    Set<UUID> nodeIds = part2node.get(p);

                    assert nodeIds != null : "Failed consistency check [part=" + p + ", nodeId=" + e.getKey() + ']';
                    assert nodeIds.contains(e.getKey()) : "Failed consistency check [part=" + p + ", nodeId=" +
                        e.getKey() + ", nodeIds=" + nodeIds + ']';
                }
            }

            for (Map.Entry<Integer, Set<UUID>> e : part2node.entrySet()) {
                for (UUID nodeId : e.getValue()) {
                    GridDhtPartitionMap2 map = node2part.get(nodeId);

                    assert map != null : "Failed consistency check [part=" + e.getKey() + ", nodeId=" + nodeId + ']';
                    assert map.containsKey(e.getKey()) : "Failed consistency check [part=" + e.getKey() +
                        ", nodeId=" + nodeId + ']';
                }
            }
        }
    }
}
