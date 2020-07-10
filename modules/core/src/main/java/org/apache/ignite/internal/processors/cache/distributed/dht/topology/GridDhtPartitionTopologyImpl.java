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

package org.apache.ignite.internal.processors.cache.distributed.dht.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.RollbackRecord;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloader;
import org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.PartitionLossPolicy.IGNORE;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture.ExchangeType.ALL;
import static org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture.ExchangeType.NONE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * Partition topology.
 */
@GridToStringExclude
public class GridDhtPartitionTopologyImpl implements GridDhtPartitionTopology {
    /** */
    private static final GridDhtPartitionState[] MOVING_STATES = new GridDhtPartitionState[] {MOVING};

    /** Flag to control amount of output for full map. */
    private static final boolean FULL_MAP_DEBUG = false;

    /** */
    private static final boolean FAST_DIFF_REBUILD = false;

    /** */
    private final GridCacheSharedContext ctx;

    /** */
    private final CacheGroupContext grp;

    /** Logger. */
    private final IgniteLogger log;

    /** Time logger. */
    private final IgniteLogger timeLog;

    /** */
    private final AtomicReferenceArray<GridDhtLocalPartition> locParts;

    /** Node to partition map. */
    private GridDhtPartitionFullMap node2part;

    /** */
    private Set<Integer> lostParts;

    /** */
    private final Map<Integer, Set<UUID>> diffFromAffinity = new HashMap<>();

    /** */
    private volatile AffinityTopologyVersion diffFromAffinityVer = AffinityTopologyVersion.NONE;

    /** Last started exchange version (always >= readyTopVer). */
    private volatile AffinityTopologyVersion lastTopChangeVer = AffinityTopologyVersion.NONE;

    /** Last finished exchange version. */
    private volatile AffinityTopologyVersion readyTopVer = AffinityTopologyVersion.NONE;

    /** Discovery cache. */
    private volatile DiscoCache discoCache;

    /** */
    private volatile boolean stopping;

    /** A future that will be completed when topology with version topVer will be ready to use. */
    private volatile GridDhtTopologyFuture topReadyFut;

    /** */
    private final GridAtomicLong updateSeq = new GridAtomicLong(1);

    /** Lock. */
    private final StripedCompositeReadWriteLock lock = new StripedCompositeReadWriteLock(16);

    /** Partition update counter. */
    private final CachePartitionFullCountersMap cntrMap;

    /** */
    private volatile Map<Integer, Long> globalPartSizes;

    /** */
    private volatile AffinityTopologyVersion rebalancedTopVer = AffinityTopologyVersion.NONE;

    /** Factory used for re-creating partition during it's lifecycle. */
    private PartitionFactory partFactory;

    /**
     * @param ctx Cache shared context.
     * @param grp Cache group.
     */
    public GridDhtPartitionTopologyImpl(
        GridCacheSharedContext ctx,
        CacheGroupContext grp
    ) {
        assert ctx != null;
        assert grp != null;

        this.ctx = ctx;
        this.grp = grp;

        log = ctx.logger(getClass());

        timeLog = ctx.logger(GridDhtPartitionsExchangeFuture.EXCHANGE_LOG);

        locParts = new AtomicReferenceArray<>(grp.affinityFunction().partitions());

        cntrMap = new CachePartitionFullCountersMap(locParts.length());

        partFactory = (ctx1, grp1, id) -> new GridDhtLocalPartition(ctx1, grp1, id, false);
    }

    /**
     * Set partition factory to use. Currently is used for tests.
     *
     * @param factory Factory.
     */
    public void partitionFactory(PartitionFactory factory) {
        this.partFactory = factory;
    }

    /** {@inheritDoc} */
    @Override public int partitions() {
        return grp.affinityFunction().partitions();
    }

    /** {@inheritDoc} */
    @Override public int groupId() {
        return grp.groupId();
    }

    /**
     *
     */
    public void onReconnected() {
        lock.writeLock().lock();

        try {
            node2part = null;

            diffFromAffinity.clear();

            updateSeq.set(1);

            topReadyFut = null;

            diffFromAffinityVer = AffinityTopologyVersion.NONE;

            rebalancedTopVer = AffinityTopologyVersion.NONE;

            readyTopVer = AffinityTopologyVersion.NONE;

            lastTopChangeVer = AffinityTopologyVersion.NONE;

            discoCache = ctx.discovery().discoCache();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @return Full map string representation.
     */
    private String fullMapString() {
        return node2part == null ? "null" : FULL_MAP_DEBUG ? node2part.toFullString() : node2part.toString();
    }

    /**
     * @param map Map to get string for.
     * @return Full map string representation.
     */
    private String mapString(GridDhtPartitionMap map) {
        return map == null ? "null" : FULL_MAP_DEBUG ? map.toFullString() : map.toString();
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
    @Override public boolean holdsLock() {
        return lock.isWriteLockedByCurrentThread() || lock.getReadHoldCount() > 0;
    }

    /** {@inheritDoc} */
    @Override public void updateTopologyVersion(
        GridDhtTopologyFuture exchFut,
        @NotNull DiscoCache discoCache,
        long updSeq,
        boolean stopping
    ) throws IgniteInterruptedCheckedException {
        U.writeLock(lock);

        try {
            AffinityTopologyVersion exchTopVer = exchFut.initialVersion();

            assert exchTopVer.compareTo(readyTopVer) > 0 : "Invalid topology version [grp=" + grp.cacheOrGroupName() +
                ", topVer=" + readyTopVer +
                ", exchTopVer=" + exchTopVer +
                ", discoCacheVer=" + (this.discoCache != null ? this.discoCache.version() : "None") +
                ", exchDiscoCacheVer=" + discoCache.version() +
                ", fut=" + exchFut + ']';

            this.stopping = stopping;

            updateSeq.setIfGreater(updSeq);

            topReadyFut = exchFut;

            rebalancedTopVer = AffinityTopologyVersion.NONE;

            lastTopChangeVer = exchTopVer;

            this.discoCache = discoCache;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean initialized() {
        AffinityTopologyVersion topVer = readyTopVer;

        assert topVer != null;

        return topVer.initialized();
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion readyTopologyVersion() {
        AffinityTopologyVersion topVer = this.readyTopVer;

        assert topVer.topologyVersion() > 0 : "Invalid topology version [topVer=" + topVer +
            ", group=" + grp.cacheOrGroupName() + ']';

        return topVer;
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion lastTopologyChangeVersion() {
        AffinityTopologyVersion topVer = this.lastTopChangeVer;

        assert topVer.topologyVersion() > 0 : "Invalid topology version [topVer=" + topVer +
            ", group=" + grp.cacheOrGroupName() + ']';

        return topVer;
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyVersionFuture() {
        GridDhtTopologyFuture topReadyFut0 = topReadyFut;

        assert topReadyFut0 != null;

        if (!topReadyFut0.changedAffinity()) {
            GridDhtTopologyFuture lastFut = ctx.exchange().lastFinishedFuture();

            if (lastFut != null)
                return lastFut;
        }

        return topReadyFut0;
    }

    /** {@inheritDoc} */
    @Override public boolean stopping() {
        return stopping;
    }

    /** {@inheritDoc} */
    @Override public boolean initPartitionsWhenAffinityReady(AffinityTopologyVersion affVer,
        GridDhtPartitionsExchangeFuture exchFut)
        throws IgniteInterruptedCheckedException
    {
        boolean needRefresh;

        ctx.database().checkpointReadLock();

        try {
            U.writeLock(lock);

            try {
                if (stopping)
                    return false;

                long updateSeq = this.updateSeq.incrementAndGet();

                needRefresh = initPartitions(affVer, grp.affinity().readyAssignments(affVer), exchFut, updateSeq);

                consistencyCheck();
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }

        return needRefresh;
    }

    /**
     * Creates and initializes partitions using given {@code affVer} and {@code affAssignment}.
     *
     * @param affVer Affinity version to use.
     * @param affAssignment Affinity assignment to use.
     * @param exchFut Exchange future.
     * @param updateSeq Update sequence.
     * @return {@code True} if partitions must be refreshed.
     */
    private boolean initPartitions(
        AffinityTopologyVersion affVer,
        List<List<ClusterNode>> affAssignment,
        GridDhtPartitionsExchangeFuture exchFut,
        long updateSeq
    ) {
        boolean needRefresh = false;

        if (grp.affinityNode()) {
            ClusterNode loc = ctx.localNode();

            ClusterNode oldest = discoCache.oldestAliveServerNode();

            GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

            int partitions = grp.affinity().partitions();

            if (grp.rebalanceEnabled()) {
                boolean added = exchFut.cacheGroupAddedOnExchange(grp.groupId(), grp.receivedFrom());

                boolean first = added || (loc.equals(oldest) && loc.id().equals(exchId.nodeId()) && exchId.isJoined())
                    || exchFut.activateCluster();

                if (first) {
                    assert exchId.isJoined() || added || exchFut.activateCluster();

                    if (log.isDebugEnabled()) {
                        String reason;

                        if (exchId.isJoined())
                            reason = "First node in cluster";
                        else if (added)
                            reason = "Cache group added";
                        else
                            reason = "Cluster activate";

                        log.debug("Initialize partitions (" + reason + ")" + " [grp=" + grp.cacheOrGroupName() + "]");
                    }

                    for (int p = 0; p < partitions; p++) {
                        if (localNode(p, affAssignment)) {
                            // Partition is created first time, so it's safe to own it.
                            boolean shouldOwn = locParts.get(p) == null;

                            GridDhtLocalPartition locPart = getOrCreatePartition(p);

                            if (shouldOwn) {
                                locPart.own();

                                if (log.isDebugEnabled())
                                    log.debug("Partition has been owned (created first time) " +
                                        "[grp=" + grp.cacheOrGroupName() + ", p=" + locPart.id() + ']');
                            }

                            needRefresh = true;

                            updateSeq = updateLocal(p, locPart.state(), updateSeq, affVer);
                        }
                        else {
                            // Apply partitions not belonging by affinity to partition map.
                            GridDhtLocalPartition locPart = locParts.get(p);

                            if (locPart != null) {
                                needRefresh = true;

                                updateSeq = updateLocal(p, locPart.state(), updateSeq, affVer);
                            }
                        }
                    }
                }
                else
                    createPartitions(affVer, affAssignment, updateSeq);
            }
            else {
                // If preloader is disabled, then we simply clear out
                // the partitions this node is not responsible for.
                for (int p = 0; p < partitions; p++) {
                    GridDhtLocalPartition locPart = localPartition0(p, affVer, false, true);

                    boolean belongs = localNode(p, affAssignment);

                    if (locPart != null) {
                        if (!belongs) {
                            GridDhtPartitionState state = locPart.state();

                            if (state.active()) {
                                locPart.rent(false);

                                updateSeq = updateLocal(p, locPart.state(), updateSeq, affVer);

                                if (log.isDebugEnabled()) {
                                    log.debug("Evicting partition with rebalancing disabled (it does not belong to " +
                                        "affinity) [grp=" + grp.cacheOrGroupName() + ", part=" + locPart + ']');
                                }
                            }
                        }
                        else {
                            locPart.own();

                            // Make sure partition map is initialized.
                            updateSeq = updateLocal(p, locPart.state(), updateSeq, affVer);
                        }
                    }
                    else if (belongs) {
                        locPart = getOrCreatePartition(p);

                        locPart.own();

                        updateLocal(p, locPart.state(), updateSeq, affVer);
                    }
                }
            }
        }

        updateRebalanceVersion(affVer, affAssignment);

        return needRefresh;
    }

    /**
     * Creates non-existing partitions belong to given affinity {@code aff}.
     *
     * @param affVer Affinity version.
     * @param aff Affinity assignments.
     * @param updateSeq Update sequence.
     */
    private void createPartitions(
        AffinityTopologyVersion affVer,
        List<List<ClusterNode>> aff,
        long updateSeq
    ) {
        if (!grp.affinityNode())
            return;

        int partitions = grp.affinity().partitions();

        if (log.isDebugEnabled())
            log.debug("Create non-existing partitions [grp=" + grp.cacheOrGroupName() + "]");

        for (int p = 0; p < partitions; p++) {
            if (node2part != null && node2part.valid()) {
                if (localNode(p, aff)) {
                    GridDhtLocalPartition locPart = getOrCreatePartition(p);

                    updateSeq = updateLocal(p, locPart.state(), updateSeq, affVer);
                }
            }
            // If this node's map is empty, we pre-create local partitions,
            // so local map will be sent correctly during exchange.
            else if (localNode(p, aff))
                getOrCreatePartition(p);
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(
        GridDhtPartitionsExchangeFuture exchFut,
        boolean affReady,
        boolean updateMoving
    ) throws IgniteCheckedException {
        ctx.database().checkpointReadLock();

        try {
            U.writeLock(lock);

            try {
                if (stopping)
                    return;

                assert lastTopChangeVer.equals(exchFut.initialVersion()) : "Invalid topology version [topVer=" + lastTopChangeVer +
                    ", exchId=" + exchFut.exchangeId() + ']';

                ExchangeDiscoveryEvents evts = exchFut.context().events();

                if (affReady) {
                    assert grp.affinity().lastVersion().equals(evts.topologyVersion()) : "Invalid affinity version [" +
                        "grp=" + grp.cacheOrGroupName() +
                        ", affVer=" + grp.affinity().lastVersion() +
                        ", evtsVer=" + evts.topologyVersion() + ']';

                    lastTopChangeVer = readyTopVer = evts.topologyVersion();

                    discoCache = evts.discoveryCache();
                }

                if (log.isDebugEnabled()) {
                    log.debug("Partition map beforeExchange [grp=" + grp.cacheOrGroupName() +
                        ", exchId=" + exchFut.exchangeId() + ", fullMap=" + fullMapString() + ']');
                }

                long updateSeq = this.updateSeq.incrementAndGet();

                cntrMap.clear();

                initializeFullMap(updateSeq);

                boolean grpStarted = exchFut.cacheGroupAddedOnExchange(grp.groupId(), grp.receivedFrom());

                if (evts.hasServerLeft()) {
                    for (DiscoveryEvent evt : evts.events()) {
                        if (ExchangeDiscoveryEvents.serverLeftEvent(evt))
                            removeNode(evt.eventNode().id());
                    }
                }
                else if (affReady && grpStarted && exchFut.exchangeType() == NONE) {
                    assert !exchFut.context().mergeExchanges() : exchFut;
                    assert node2part != null && node2part.valid() : exchFut;

                    // Initialize node maps if group was started from joining client.
                    final List<ClusterNode> nodes = exchFut.firstEventCache().cacheGroupAffinityNodes(grp.groupId());

                    for (ClusterNode node : nodes) {
                        if (!node2part.containsKey(node.id()) && ctx.discovery().alive(node)) {
                            final GridDhtPartitionMap partMap = new GridDhtPartitionMap(node.id(),
                                1L,
                                exchFut.initialVersion(),
                                new GridPartitionStateMap(),
                                false);

                            final AffinityAssignment aff = grp.affinity().cachedAffinity(exchFut.initialVersion());

                            for (Integer p0 : aff.primaryPartitions(node.id()))
                                partMap.put(p0, OWNING);

                            for (Integer p0 : aff.backupPartitions(node.id()))
                                partMap.put(p0, OWNING);

                            node2part.put(node.id(), partMap);
                        }
                    }
                }

                if (grp.affinityNode()) {
                    if (grpStarted ||
                        exchFut.firstEvent().type() == EVT_DISCOVERY_CUSTOM_EVT ||
                        exchFut.serverNodeDiscoveryEvent()) {

                        AffinityTopologyVersion affVer;
                        List<List<ClusterNode>> affAssignment;

                        if (affReady) {
                            affVer = evts.topologyVersion();

                            assert grp.affinity().lastVersion().equals(affVer) :
                                    "Invalid affinity [topVer=" + grp.affinity().lastVersion() +
                                            ", grp=" + grp.cacheOrGroupName() +
                                            ", affVer=" + affVer +
                                            ", fut=" + exchFut + ']';

                            affAssignment = grp.affinity().readyAssignments(affVer);
                        }
                        else {
                            assert !exchFut.context().mergeExchanges();

                            affVer = exchFut.initialVersion();
                            affAssignment = grp.affinity().idealAssignmentRaw();
                        }

                        initPartitions(affVer, affAssignment, exchFut, updateSeq);
                    }
                }

                consistencyCheck();

                if (updateMoving) {
                    assert grp.affinity().lastVersion().equals(evts.topologyVersion());

                    createMovingPartitions(grp.affinity().readyAffinity(evts.topologyVersion()));
                }

                if (log.isDebugEnabled()) {
                    log.debug("Partition map after beforeExchange [grp=" + grp.cacheOrGroupName() + ", " +
                        "exchId=" + exchFut.exchangeId() + ", fullMap=" + fullMapString() + ']');
                }

                if (log.isTraceEnabled()) {
                    log.trace("Partition states after beforeExchange [grp=" + grp.cacheOrGroupName()
                        + ", exchId=" + exchFut.exchangeId() + ", states=" + dumpPartitionStates() + ']');
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }
    }

    /**
     * Initializes full map if current full map is empty or invalid in case of coordinator or cache groups start.
     *
     * @param updateSeq Update sequence to initialize full map.
     */
    private void initializeFullMap(long updateSeq) {
        if (!(topReadyFut instanceof GridDhtPartitionsExchangeFuture))
            return;

        GridDhtPartitionsExchangeFuture exchFut = (GridDhtPartitionsExchangeFuture) topReadyFut;

        boolean grpStarted = exchFut.cacheGroupAddedOnExchange(grp.groupId(), grp.receivedFrom());

        ClusterNode oldest = discoCache.oldestAliveServerNode();

        // If this is the oldest node (coordinator) or cache was added during this exchange
        if (oldest != null && (ctx.localNode().equals(oldest) || grpStarted)) {
            if (node2part == null) {
                node2part = new GridDhtPartitionFullMap(oldest.id(), oldest.order(), updateSeq);

                if (log.isDebugEnabled()) {
                    log.debug("Created brand new full topology map on oldest node [" +
                        "grp=" + grp.cacheOrGroupName() + ", exchId=" + exchFut.exchangeId() +
                        ", fullMap=" + fullMapString() + ']');
                }
            }
            else if (!node2part.valid()) {
                node2part = new GridDhtPartitionFullMap(oldest.id(),
                    oldest.order(),
                    updateSeq,
                    node2part,
                    false);

                if (log.isDebugEnabled()) {
                    log.debug("Created new full topology map on oldest node [" +
                        "grp=" + grp.cacheOrGroupName() + ", exchId=" + exchFut.exchangeId() +
                        ", fullMap=" + node2part + ']');
                }
            }
            else if (!node2part.nodeId().equals(ctx.localNode().id())) {
                node2part = new GridDhtPartitionFullMap(oldest.id(),
                    oldest.order(),
                    updateSeq,
                    node2part,
                    false);

                if (log.isDebugEnabled()) {
                    log.debug("Copied old map into new map on oldest node (previous oldest node left) [" +
                        "grp=" + grp.cacheOrGroupName() + ", exchId=" + exchFut.exchangeId() +
                        ", fullMap=" + fullMapString() + ']');
                }
            }
        }
    }

    /**
     * @param p Partition number.
     * @param topVer Topology version.
     * @return {@code True} if given partition belongs to local node.
     */
    private boolean partitionLocalNode(int p, AffinityTopologyVersion topVer) {
        return grp.affinity().nodes(p, topVer).contains(ctx.localNode());
    }

    /** {@inheritDoc} */
    @Override public void afterStateRestored(AffinityTopologyVersion topVer) {
        /** Partition maps are initialized as a part of partition map exchange protocol,
         * see {@link #beforeExchange(GridDhtPartitionsExchangeFuture, boolean, boolean)}. */
        for (GridDhtLocalPartition locPart : currentLocalPartitions()) {
            if (locPart != null && locPart.state() == RENTING)
                locPart.clearAsync(); // Resume clearing
        }
    }

    /** {@inheritDoc} */
    @Override public boolean afterExchange(GridDhtPartitionsExchangeFuture exchFut) {
        boolean changed = false;

        int partitions = grp.affinity().partitions();

        AffinityTopologyVersion topVer = exchFut.context().events().topologyVersion();

        assert grp.affinity().lastVersion().equals(topVer) : "Affinity is not initialized " +
            "[grp=" + grp.cacheOrGroupName() +
            ", topVer=" + topVer +
            ", affVer=" + grp.affinity().lastVersion() +
            ", fut=" + exchFut + ']';

        ctx.database().checkpointReadLock();

        try {

            lock.writeLock().lock();

            try {
                if (stopping)
                    return false;

                assert readyTopVer.initialized() : readyTopVer;
                assert lastTopChangeVer.equals(readyTopVer);

                if (log.isDebugEnabled()) {
                    log.debug("Partition map before afterExchange [grp=" + grp.cacheOrGroupName() +
                        ", exchId=" + exchFut.exchangeId() +
                        ", fullMap=" + fullMapString() + ']');
                }

                if (log.isTraceEnabled()) {
                    log.trace("Partition states before afterExchange [grp=" + grp.cacheOrGroupName()
                        + ", exchVer=" + exchFut.exchangeId() + ", states=" + dumpPartitionStates() + ']');
                }

                long updateSeq = this.updateSeq.incrementAndGet();

                // Skip partition updates in case of not real exchange.
                if (!ctx.localNode().isClient() && exchFut.exchangeType() == ALL) {
                    for (int p = 0; p < partitions; p++) {
                        GridDhtLocalPartition locPart = localPartition0(p, topVer, false, true);

                        if (partitionLocalNode(p, topVer)) {
                            // Prepare partition to rebalance if it's not happened on full map update phase.
                            if (locPart == null || locPart.state() == RENTING || locPart.state() == EVICTED)
                                locPart = rebalancePartition(p, true, exchFut);

                            GridDhtPartitionState state = locPart.state();

                            if (state == MOVING) {
                                if (grp.rebalanceEnabled()) {
                                    Collection<ClusterNode> owners = owners(p);

                                    // If an owner node left during exchange, then new exchange should be started with detecting lost partitions.
                                    if (!F.isEmpty(owners)) {
                                        if (log.isDebugEnabled())
                                            log.debug("Will not own partition (there are owners to rebalance from) " +
                                                "[grp=" + grp.cacheOrGroupName() + ", p=" + p + ", owners = " + owners + ']');
                                    }
                                }
                                else
                                    updateSeq = updateLocal(p, locPart.state(), updateSeq, topVer);
                            }
                        }
                        else {
                            if (locPart != null) {
                                GridDhtPartitionState state = locPart.state();

                                if (state == MOVING) {
                                    locPart.rent(false);

                                    updateSeq = updateLocal(p, locPart.state(), updateSeq, topVer);

                                    changed = true;

                                    if (log.isDebugEnabled()) {
                                        log.debug("Evicting " + state + " partition (it does not belong to affinity) [" +
                                            "grp=" + grp.cacheOrGroupName() + ", p=" + locPart.id() + ']');
                                    }
                                }
                            }
                        }
                    }
                }

                AffinityAssignment aff = grp.affinity().readyAffinity(topVer);

                if (node2part != null && node2part.valid())
                    changed |= checkEvictions(updateSeq, aff);

                updateRebalanceVersion(aff.topologyVersion(), aff.assignment());

                consistencyCheck();

                if (log.isTraceEnabled()) {
                    log.trace("Partition states after afterExchange [grp=" + grp.cacheOrGroupName()
                        + ", exchVer=" + exchFut.exchangeId() + ", states=" + dumpPartitionStates() + ']');
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }

        return changed;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDhtLocalPartition localPartition(int p, AffinityTopologyVersion topVer,
        boolean create)
        throws GridDhtInvalidPartitionException {
        return localPartition0(p, topVer, create, false);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridDhtLocalPartition localPartition(int p, AffinityTopologyVersion topVer,
        boolean create, boolean showRenting) throws GridDhtInvalidPartitionException {
        return localPartition0(p, topVer, create, showRenting);
    }

    /**
     * Creates partition with id {@code p} if it doesn't exist or evicted.
     * In other case returns existing partition.
     *
     * @param p Partition number.
     * @return Partition.
     */
    public GridDhtLocalPartition getOrCreatePartition(int p) {
        assert lock.isWriteLockedByCurrentThread();

        assert ctx.database().checkpointLockIsHeldByThread();

        GridDhtLocalPartition loc = locParts.get(p);

        if (loc == null || loc.state() == EVICTED) {
            boolean recreate = false;

            // Make sure that after eviction partition is destroyed.
            if (loc != null) {
                loc.awaitDestroy();

                recreate = true;
            }

            locParts.set(p, loc = partFactory.create(ctx, grp, p));

            if (recreate)
                loc.resetUpdateCounter();

            long updCntr = cntrMap.updateCounter(p);

            if (updCntr != 0)
                loc.updateCounter(updCntr);

            // Create a partition in lost state.
            if (lostParts != null && lostParts.contains(p))
                loc.markLost();

            if (ctx.pageStore() != null) {
                try {
                    ctx.pageStore().onPartitionCreated(grp.groupId(), p);
                }
                catch (IgniteCheckedException e) {
                    // TODO ignite-db
                    throw new IgniteException(e);
                }
            }
        }

        return loc;
    }

    /** {@inheritDoc} */
    @Override public GridDhtLocalPartition forceCreatePartition(int p) throws IgniteCheckedException {
        lock.writeLock().lock();

        try {
            GridDhtLocalPartition part = locParts.get(p);

            boolean recreate = false;

            if (part != null) {
                if (part.state() != EVICTED)
                    return part;
                else {
                    part.awaitDestroy();

                    recreate = true;
                }
            }

            part = new GridDhtLocalPartition(ctx, grp, p, true);

            if (recreate)
                part.resetUpdateCounter();

            locParts.set(p, part);

            return part;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param p Partition number.
     * @param topVer Topology version.
     * @param create If {@code true} create partition if it doesn't exists or evicted.
     * @param showRenting If {@code true} return partition in RENTING state if exists.
     * @return Local partition.
     */
    @SuppressWarnings("TooBroadScope")
    private GridDhtLocalPartition localPartition0(int p,
        AffinityTopologyVersion topVer,
        boolean create,
        boolean showRenting) {
        GridDhtLocalPartition loc;

        loc = locParts.get(p);

        GridDhtPartitionState state = loc != null ? loc.state() : null;

        if (loc != null && state != EVICTED && (state != RENTING || showRenting))
            return loc;

        if (!create)
            return null;

        boolean created = false;

        ctx.database().checkpointReadLock();

        try {
            lock.writeLock().lock();

            try {
                loc = locParts.get(p);

                state = loc != null ? loc.state() : null;

                boolean belongs = partitionLocalNode(p, topVer);

                boolean recreate = false;

                if (loc != null && state == EVICTED) {
                    // Make sure that after eviction partition is destroyed.
                    loc.awaitDestroy();

                    recreate = true;

                    locParts.set(p, loc = null);

                    if (!belongs) {
                        throw new GridDhtInvalidPartitionException(p, "Adding entry to evicted partition " +
                            "(often may be caused by inconsistent 'key.hashCode()' implementation) " +
                            "[grp=" + grp.cacheOrGroupName() + ", part=" + p + ", topVer=" + topVer +
                            ", this.topVer=" + this.readyTopVer + ']');
                    }
                }
                else if (loc != null && state == RENTING && !showRenting) {
                    boolean belongsNow = topVer.equals(this.readyTopVer) ? belongs : partitionLocalNode(p, this.readyTopVer);

                    throw new GridDhtInvalidPartitionException(p, "Adding entry to partition that is concurrently " +
                        "evicted [grp=" + grp.cacheOrGroupName() + ", part=" + p + ", belongsNow=" + belongsNow
                        + ", belongs=" + belongs + ", topVer=" + topVer + ", curTopVer=" + this.readyTopVer + "]");
                }

                if (loc == null) {
                    if (!belongs)
                        throw new GridDhtInvalidPartitionException(p, "Creating partition which does not belong to " +
                            "local node (often may be caused by inconsistent 'key.hashCode()' implementation) " +
                            "[grp=" + grp.cacheOrGroupName() + ", part=" + p + ", topVer=" + topVer +
                            ", this.topVer=" + this.readyTopVer + ']');

                    locParts.set(p, loc = partFactory.create(ctx, grp, p));

                    if (recreate)
                        loc.resetUpdateCounter();

                    this.updateSeq.incrementAndGet();

                    created = true;
                }
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }

        if (created && ctx.pageStore() != null) {
            try {
                ctx.pageStore().onPartitionCreated(grp.groupId(), p);
            }
            catch (IgniteCheckedException e) {
                // TODO ignite-db
                throw new IgniteException(e);
            }
        }

        return loc;
    }

    /** {@inheritDoc} */
    @Override public void releasePartitions(int... parts) {
        assert parts != null;
        assert parts.length > 0;

        for (int i = 0; i < parts.length; i++) {
            GridDhtLocalPartition part = locParts.get(parts[i]);

            if (part != null)
                part.release();
        }
    }

    /** {@inheritDoc} */
    @Override public GridDhtLocalPartition localPartition(int part) {
        return locParts.get(part);
    }

    /** {@inheritDoc} */
    @Override public List<GridDhtLocalPartition> localPartitions() {
        List<GridDhtLocalPartition> list = new ArrayList<>(locParts.length());

        for (int i = 0; i < locParts.length(); i++) {
            GridDhtLocalPartition part = locParts.get(i);

            if (part != null && part.state().active())
                list.add(part);
        }

        return list;
    }

    /** {@inheritDoc} */
    @Override public Iterable<GridDhtLocalPartition> currentLocalPartitions() {
        return new Iterable<GridDhtLocalPartition>() {
            @Override public Iterator<GridDhtLocalPartition> iterator() {
                return new CurrentPartitionsIterator();
            }
        };
    }

    /** {@inheritDoc} */
    @Override public void onRemoved(GridDhtCacheEntry e) {
        /*
         * Make sure not to acquire any locks here as this method
         * may be called from sensitive synchronization blocks.
         * ===================================================
         */

        GridDhtLocalPartition loc = localPartition(e.partition(), readyTopVer, false);

        if (loc != null)
            loc.onRemoved(e);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap localPartitionMap() {
        GridPartitionStateMap map = new GridPartitionStateMap(locParts.length());

        lock.readLock().lock();

        try {
            for (int i = 0; i < locParts.length(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part == null)
                    continue;

                map.put(i, part.state());
            }

            GridDhtPartitionMap locPartMap = node2part != null ? node2part.get(ctx.localNodeId()) : null;

            return new GridDhtPartitionMap(ctx.localNodeId(),
                updateSeq.get(),
                locPartMap != null ? locPartMap.topologyVersion() : readyTopVer,
                map,
                true);
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
        return nodes0(p, affAssignment, affNodes);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> nodes(int p, AffinityTopologyVersion topVer) {
        AffinityAssignment affAssignment = grp.affinity().cachedAffinity(topVer);

        List<ClusterNode> affNodes = affAssignment.get(p);

        List<ClusterNode> nodes = nodes0(p, affAssignment, affNodes);

        return nodes != null ? nodes : affNodes;
    }

    /**
     * @param p Partition.
     * @param affAssignment Assignments.
     * @param affNodes Node assigned for given partition by affinity.
     * @return Nodes responsible for given partition (primary is first).
     */
    @Nullable private List<ClusterNode> nodes0(int p, AffinityAssignment affAssignment, List<ClusterNode> affNodes) {
        if (grp.isReplicated())
            return affNodes;

        AffinityTopologyVersion topVer = affAssignment.topologyVersion();

        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer1=" + topVer +
                ", topVer2=" + readyTopVer +
                ", node=" + ctx.igniteInstanceName() +
                ", grp=" + grp.cacheOrGroupName() +
                ", node2part=" + node2part + ']';

            List<ClusterNode> nodes = null;

            AffinityTopologyVersion diffVer = diffFromAffinityVer;

            if (!diffVer.equals(topVer)) {
                if (log.isDebugEnabled()) {
                    log.debug("Requested topology version does not match calculated diff, need to check if " +
                        "affinity has changed [grp=" + grp.cacheOrGroupName() + ", topVer=" + topVer +
                        ", diffVer=" + diffVer + "]");
                }

                boolean affChanged;

                if (diffVer.compareTo(topVer) < 0)
                    affChanged = ctx.exchange().affinityChanged(diffVer, topVer);
                else
                    affChanged = ctx.exchange().affinityChanged(topVer, diffVer);

                if (affChanged) {
                    if (log.isDebugEnabled()) {
                        log.debug("Requested topology version does not match calculated diff, will require full iteration to" +
                            "calculate mapping [grp=" + grp.cacheOrGroupName() + ", topVer=" + topVer +
                            ", diffVer=" + diffVer + "]");
                    }

                    nodes = new ArrayList<>();

                    nodes.addAll(affNodes);

                    for (Map.Entry<UUID, GridDhtPartitionMap> entry : node2part.entrySet()) {
                        GridDhtPartitionState state = entry.getValue().get(p);

                        ClusterNode n = ctx.discovery().node(entry.getKey());

                        if (n != null && state != null && (state == MOVING || state == OWNING || state == RENTING)
                            && !nodes.contains(n) && (topVer.topologyVersion() < 0 || n.order() <= topVer.topologyVersion()))
                            nodes.add(n);
                    }
                }

                return nodes;
            }

            Collection<UUID> diffIds = diffFromAffinity.get(p);

            if (!F.isEmpty(diffIds)) {
                Collection<UUID> affIds = affAssignment.getIds(p);

                for (UUID nodeId : diffIds) {
                    if (affIds.contains(nodeId))
                        continue;

                    if (hasState(p, nodeId, OWNING, MOVING, RENTING)) {
                        ClusterNode n = ctx.discovery().node(nodeId);

                        if (n != null && (topVer.topologyVersion() < 0 || n.order() <= topVer.topologyVersion())) {
                            if (nodes == null) {
                                nodes = new ArrayList<>(affNodes.size() + diffIds.size());

                                nodes.addAll(affNodes);
                            }

                            nodes.add(n);
                        }
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
    private List<ClusterNode> nodes(
        int p,
        AffinityTopologyVersion topVer,
        GridDhtPartitionState state,
        GridDhtPartitionState... states
    ) {
        Collection<UUID> allIds = F.nodeIds(discoCache.cacheGroupAffinityNodes(grp.groupId()));

        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer=" + topVer +
                ", grp=" + grp.cacheOrGroupName() +
                ", allIds=" + allIds +
                ", node2part=" + node2part + ']';

            // Node IDs can be null if both, primary and backup, nodes disappear.
            // Empirical size to reduce growing of ArrayList.
            // We bear in mind that most of the time we filter OWNING partitions.
            List<ClusterNode> nodes = new ArrayList<>(allIds.size() / 2 + 1);
            for (UUID id : allIds) {
                if (hasState(p, id, state, states)) {
                    ClusterNode n = ctx.discovery().node(id);

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
        if (!grp.rebalanceEnabled())
            return ownersAndMoving(p, topVer);

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

            List<ClusterNode> allNodes = discoCache.cacheGroupAffinityNodes(grp.groupId());

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
        if (!grp.rebalanceEnabled())
            return ownersAndMoving(p, AffinityTopologyVersion.NONE);

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

    /** {@inheritDoc} */
    @Override public GridDhtPartitionFullMap partitionMap(boolean onlyActive) {
        lock.readLock().lock();

        try {
            if (node2part == null || stopping)
                return null;

            assert node2part.valid() : "Invalid node2part [node2part=" + node2part +
                ", grp=" + grp.cacheOrGroupName() +
                ", stopping=" + stopping +
                ", locNodeId=" + ctx.localNode().id() +
                ", locName=" + ctx.igniteInstanceName() + ']';

            GridDhtPartitionFullMap m = node2part;

            return new GridDhtPartitionFullMap(m.nodeId(), m.nodeOrder(), m.updateSequence(), m, onlyActive);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks should current partition map overwritten by new partition map
     * Method returns true if topology version or update sequence of new map are greater than of current map
     *
     * @param currentMap Current partition map
     * @param newMap New partition map
     * @return True if current partition map should be overwritten by new partition map, false in other case
     */
    private boolean shouldOverridePartitionMap(GridDhtPartitionMap currentMap, GridDhtPartitionMap newMap) {
        return newMap != null && newMap.compareTo(currentMap) > 0;
    }

    /** {@inheritDoc} */
    @Override public boolean update(
        @Nullable AffinityTopologyVersion exchangeVer,
        GridDhtPartitionFullMap partMap,
        @Nullable CachePartitionFullCountersMap incomeCntrMap,
        Set<Integer> partsToReload,
        @Nullable Map<Integer, Long> partSizes,
        @Nullable AffinityTopologyVersion msgTopVer,
        @Nullable GridDhtPartitionsExchangeFuture exchFut,
        @Nullable Set<Integer> lostParts
    ) {
        if (log.isDebugEnabled()) {
            log.debug("Updating full partition map " +
                "[grp=" + grp.cacheOrGroupName() + ", exchVer=" + exchangeVer + ", fullMap=" + fullMapString() + ']');
        }

        assert partMap != null;

        ctx.database().checkpointReadLock();

        try {
            lock.writeLock().lock();

            try {
                if (log.isTraceEnabled() && exchangeVer != null) {
                    log.trace("Partition states before full update [grp=" + grp.cacheOrGroupName()
                        + ", exchVer=" + exchangeVer + ", states=" + dumpPartitionStates() + ']');
                }

                if (stopping || !lastTopChangeVer.initialized() ||
                    // Ignore message not-related to exchange if exchange is in progress.
                    (exchangeVer == null && !lastTopChangeVer.equals(readyTopVer)))
                    return false;

                if (incomeCntrMap != null) {
                    // update local counters in partitions
                    for (int i = 0; i < locParts.length(); i++) {
                        cntrMap.updateCounter(i, incomeCntrMap.updateCounter(i));

                        GridDhtLocalPartition part = locParts.get(i);

                        if (part == null)
                            continue;

                        if (part.state() == OWNING || part.state() == MOVING) {
                            long updCntr = incomeCntrMap.updateCounter(part.id());
                            long curCntr = part.updateCounter();

                            // Avoid zero counter update to empty partition to prevent lazy init.
                            if (updCntr != 0 || curCntr != 0) {
                                part.updateCounter(updCntr);

                                if (updCntr > curCntr) {
                                    if (log.isDebugEnabled())
                                        log.debug("Partition update counter has updated [grp=" + grp.cacheOrGroupName() + ", p=" + part.id()
                                            + ", state=" + part.state() + ", prevCntr=" + curCntr + ", nextCntr=" + updCntr + "]");
                                }
                            }
                        }
                    }
                }

                // TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11800
                if (exchangeVer != null) {
                    // Ignore if exchange already finished or new exchange started.
                    if (readyTopVer.after(exchangeVer) || lastTopChangeVer.after(exchangeVer)) {
                        U.warn(log, "Stale exchange id for full partition map update (will ignore) [" +
                            "grp=" + grp.cacheOrGroupName() +
                            ", lastTopChange=" + lastTopChangeVer +
                            ", readTopVer=" + readyTopVer +
                            ", exchVer=" + exchangeVer + ']');

                        return false;
                    }
                }

                boolean fullMapUpdated = node2part == null;

                if (node2part != null) {
                    // Merge maps.
                    for (GridDhtPartitionMap part : node2part.values()) {
                        GridDhtPartitionMap newPart = partMap.get(part.nodeId());

                        if (shouldOverridePartitionMap(part, newPart)) {
                            fullMapUpdated = true;

                            if (log.isDebugEnabled()) {
                                log.debug("Overriding partition map in full update map [" +
                                    "node=" + part.nodeId() +
                                    ", grp=" + grp.cacheOrGroupName() +
                                    ", exchVer=" + exchangeVer +
                                    ", curPart=" + mapString(part) +
                                    ", newPart=" + mapString(newPart) + ']');
                            }

                            if (newPart.nodeId().equals(ctx.localNodeId()))
                                updateSeq.setIfGreater(newPart.updateSequence());
                        }
                        else {
                            // If for some nodes current partition has a newer map, then we keep the newer value.
                            if (log.isDebugEnabled()) {
                                log.debug("Partitions map for the node keeps newer value than message [" +
                                    "node=" + part.nodeId() +
                                    ", grp=" + grp.cacheOrGroupName() +
                                    ", exchVer=" + exchangeVer +
                                    ", curPart=" + mapString(part) +
                                    ", newPart=" + mapString(newPart) + ']');
                            }

                            partMap.put(part.nodeId(), part);
                        }
                    }

                    // Check that we have new nodes.
                    for (GridDhtPartitionMap part : partMap.values()) {
                        if (fullMapUpdated)
                            break;

                        fullMapUpdated = !node2part.containsKey(part.nodeId());
                    }

                    GridDhtPartitionsExchangeFuture topFut =
                        exchFut == null ? ctx.exchange().lastFinishedFuture() : exchFut;

                    // topFut can be null if lastFinishedFuture has completed with error.
                    if (topFut != null) {
                        for (Iterator<UUID> it = partMap.keySet().iterator(); it.hasNext(); ) {
                            UUID nodeId = it.next();

                            final ClusterNode node = topFut.events().discoveryCache().node(nodeId);

                            if (node == null) {
                                if (log.isTraceEnabled())
                                    log.trace("Removing left node from full map update [grp=" + grp.cacheOrGroupName() +
                                        ", exchTopVer=" + exchangeVer + ", futVer=" + topFut.initialVersion() +
                                        ", nodeId=" + nodeId + ", partMap=" + partMap + ']');

                                it.remove();
                            }
                        }
                    }
                }
                else {
                    GridDhtPartitionMap locNodeMap = partMap.get(ctx.localNodeId());

                    if (locNodeMap != null)
                        updateSeq.setIfGreater(locNodeMap.updateSequence());
                }

                if (!fullMapUpdated) {
                    if (log.isTraceEnabled()) {
                        log.trace("No updates for full partition map (will ignore) [" +
                            "grp=" + grp.cacheOrGroupName() +
                            ", lastExch=" + lastTopChangeVer +
                            ", exchVer=" + exchangeVer +
                            ", curMap=" + node2part +
                            ", newMap=" + partMap + ']');
                    }

                    return false;
                }

                if (exchangeVer != null) {
                    assert exchangeVer.compareTo(readyTopVer) >= 0 && exchangeVer.compareTo(lastTopChangeVer) >= 0;

                    lastTopChangeVer = readyTopVer = exchangeVer;

                    // Apply lost partitions from full message.
                    if (lostParts != null) {
                        this.lostParts = new HashSet<>(lostParts);

                        for (Integer part : lostParts) {
                            GridDhtLocalPartition locPart = localPartition(part);

                            // EVICTED partitions should not be marked directly as LOST, or
                            // part.clearFuture lifecycle will be broken after resetting.
                            // New partition should be created instead.
                            if (locPart != null && locPart.state() != EVICTED) {
                                locPart.markLost();

                                GridDhtPartitionMap locMap = partMap.get(ctx.localNodeId());

                                locMap.put(part, LOST);
                            }
                        }
                    }
                }

                node2part = partMap;

                if (log.isDebugEnabled()) {
                    log.debug("Partition map after processFullMessage [grp=" + grp.cacheOrGroupName() +
                        ", exchId=" + (exchFut == null ? null : exchFut.exchangeId()) +
                        ", fullMap=" + fullMapString() + ']');
                }

                if (exchangeVer == null && !grp.isReplicated() &&
                        (readyTopVer.initialized() && readyTopVer.compareTo(diffFromAffinityVer) >= 0)) {
                    AffinityAssignment affAssignment = grp.affinity().readyAffinity(readyTopVer);

                    for (Map.Entry<UUID, GridDhtPartitionMap> e : partMap.entrySet()) {
                        for (Map.Entry<Integer, GridDhtPartitionState> e0 : e.getValue().entrySet()) {
                            int p = e0.getKey();

                            Set<UUID> diffIds = diffFromAffinity.get(p);

                            if ((e0.getValue() == MOVING || e0.getValue() == OWNING || e0.getValue() == RENTING) &&
                                !affAssignment.getIds(p).contains(e.getKey())) {

                                if (diffIds == null)
                                    diffFromAffinity.put(p, diffIds = U.newHashSet(3));

                                diffIds.add(e.getKey());
                            }
                            else {
                                if (diffIds != null && diffIds.remove(e.getKey())) {
                                    if (diffIds.isEmpty())
                                        diffFromAffinity.remove(p);
                                }
                            }
                        }
                    }

                    diffFromAffinityVer = readyTopVer;
                }

                boolean changed = false;

                GridDhtPartitionMap nodeMap = partMap.get(ctx.localNodeId());

                // Only in real exchange occurred.
                if (exchangeVer != null &&
                    nodeMap != null &&
                    grp.persistenceEnabled() &&
                    readyTopVer.initialized()) {

                    assert exchFut != null;

                    for (Map.Entry<Integer, GridDhtPartitionState> e : nodeMap.entrySet()) {
                        int p = e.getKey();
                        GridDhtPartitionState state = e.getValue();

                        if (state == OWNING) {
                            GridDhtLocalPartition locPart = locParts.get(p);

                            assert locPart != null : grp.cacheOrGroupName();

                            if (locPart.state() == MOVING) {
                                boolean success = locPart.own();

                                assert success : locPart;

                                changed |= success;
                            }
                        }
                        else if (state == MOVING) {
                            GridDhtLocalPartition locPart = locParts.get(p);

                            rebalancePartition(p, partsToReload.contains(p) ||
                                locPart != null && locPart.state() == MOVING && exchFut.localJoinExchange(), exchFut);

                            changed = true;
                        }
                    }
                }

                long updateSeq = this.updateSeq.incrementAndGet();

                if (readyTopVer.initialized() && readyTopVer.equals(lastTopChangeVer)) {
                    AffinityAssignment aff = grp.affinity().readyAffinity(readyTopVer);

                    if (exchangeVer == null)
                        changed |= checkEvictions(updateSeq, aff);

                    updateRebalanceVersion(aff.topologyVersion(), aff.assignment());
                }

                if (partSizes != null)
                    this.globalPartSizes = partSizes;

                consistencyCheck();

                if (log.isDebugEnabled()) {
                    log.debug("Partition map after full update [grp=" + grp.cacheOrGroupName() +
                        ", map=" + fullMapString() + ']');
                }

                if (log.isTraceEnabled() && exchangeVer != null) {
                    log.trace("Partition states after full update [grp=" + grp.cacheOrGroupName()
                        + ", exchVer=" + exchangeVer + ", states=" + dumpPartitionStates() + ']');
                }

                if (changed) {
                    if (log.isDebugEnabled())
                        log.debug("Partitions have been scheduled to resend [reason=" +
                            "Full map update [grp" + grp.cacheOrGroupName() + "]");

                    ctx.exchange().scheduleResendPartitions();
                }

                return changed;
            } finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void collectUpdateCounters(CachePartitionPartialCountersMap cntrMap) {
        assert cntrMap != null;

        long nowNanos = System.nanoTime();

        lock.writeLock().lock();

        try {
            long acquiredNanos = System.nanoTime();

            if (acquiredNanos - nowNanos >= U.millisToNanos(100)) {
                if (timeLog.isInfoEnabled())
                    timeLog.info("Waited too long to acquire topology write lock " +
                        "[grp=" + grp.cacheOrGroupName() + ", waitTime=" +
                        U.nanosToMillis(acquiredNanos - nowNanos) + ']');
            }

            if (stopping)
                return;

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
        long nowNanos = System.nanoTime();

        lock.writeLock().lock();

        try {
            long acquiredNanos = System.nanoTime();

            if (acquiredNanos - nowNanos >= U.millisToNanos(100)) {
                if (timeLog.isInfoEnabled())
                    timeLog.info("Waited too long to acquire topology write lock " +
                        "[grp=" + grp.cacheOrGroupName() + ", waitTime=" +
                        U.nanosToMillis(acquiredNanos - nowNanos) + ']');
            }

            if (stopping)
                return;

            for (int i = 0; i < locParts.length(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part == null)
                    continue;

                boolean reserve = part.reserve();

                try {
                    GridDhtPartitionState state = part.state();

                    if (!reserve || state == EVICTED || state == RENTING || state == LOST)
                        continue;

                    long updCntr = cntrMap.updateCounter(part.id());
                    long locUpdCntr = part.updateCounter();

                    if (updCntr != 0 || locUpdCntr != 0) { // Avoid creation of empty partition.
                        part.updateCounter(updCntr);

                        if (updCntr > locUpdCntr) {
                            if (log.isDebugEnabled())
                                log.debug("Partition update counter has updated [grp=" + grp.cacheOrGroupName() + ", p=" + part.id()
                                    + ", state=" + part.state() + ", prevCntr=" + locUpdCntr + ", nextCntr=" + updCntr + "]");
                        }
                    }

                    if (locUpdCntr > updCntr) {
                        cntrMap.initialUpdateCounter(part.id(), part.initialUpdateCounter());
                        cntrMap.updateCounter(part.id(), locUpdCntr);
                    }
                }
                finally {
                    if (reserve)
                        part.release();
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }
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
    @Override public boolean update(
        @Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap parts,
        boolean force
    ) {
        if (log.isDebugEnabled()) {
            log.debug("Updating single partition map [grp=" + grp.cacheOrGroupName() + ", exchId=" + exchId +
                ", parts=" + mapString(parts) + ']');
        }

        if (!ctx.discovery().alive(parts.nodeId())) {
            if (log.isTraceEnabled()) {
                log.trace("Received partition update for non-existing node (will ignore) [grp=" + grp.cacheOrGroupName() +
                    ", exchId=" + exchId + ", parts=" + parts + ']');
            }

            return false;
        }

        ctx.database().checkpointReadLock();

        try {
            lock.writeLock().lock();

            try {
                if (stopping)
                    return false;

                if (!force) {
                    if (lastTopChangeVer.initialized() && exchId != null && lastTopChangeVer.compareTo(exchId.topologyVersion()) > 0) {
                        U.warn(log, "Stale exchange id for single partition map update (will ignore) [" +
                            "grp=" + grp.cacheOrGroupName() +
                            ", lastTopChange=" + lastTopChangeVer +
                            ", readTopVer=" + readyTopVer +
                            ", exch=" + exchId.topologyVersion() + ']');

                        return false;
                    }
                }

                if (node2part == null)
                    // Create invalid partition map.
                    node2part = new GridDhtPartitionFullMap();

                GridDhtPartitionMap cur = node2part.get(parts.nodeId());

                if (force) {
                    if (cur != null && cur.topologyVersion().initialized())
                        parts.updateSequence(cur.updateSequence(), cur.topologyVersion());
                }
                else if (isStaleUpdate(cur, parts)) {
                    assert cur != null;

                    String msg = "Stale update for single partition map update (will ignore) [" +
                        "nodeId=" + parts.nodeId() +
                        ", grp=" + grp.cacheOrGroupName() +
                        ", exchId=" + exchId +
                        ", curMap=" + cur +
                        ", newMap=" + parts + ']';

                    // This is usual situation when partition maps are equal, just print debug message.
                    if (cur.compareTo(parts) == 0) {
                        if (log.isTraceEnabled())
                            log.trace(msg);
                    }
                    else
                        U.warn(log, msg);

                    return false;
                }

                long updateSeq = this.updateSeq.incrementAndGet();

                node2part.newUpdateSequence(updateSeq);

                boolean changed = false;

                if (cur == null || !cur.equals(parts))
                    changed = true;

                node2part.put(parts.nodeId(), parts);

                // During exchange diff is calculated after all messages are received and affinity initialized.
                if (exchId == null && !grp.isReplicated()) {
                    if (readyTopVer.initialized() && readyTopVer.compareTo(diffFromAffinityVer) >= 0) {
                        AffinityAssignment affAssignment = grp.affinity().readyAffinity(readyTopVer);

                        // Add new mappings.
                        for (Map.Entry<Integer, GridDhtPartitionState> e : parts.entrySet()) {
                            int p = e.getKey();

                            Set<UUID> diffIds = diffFromAffinity.get(p);

                            if ((e.getValue() == MOVING || e.getValue() == OWNING || e.getValue() == RENTING)
                                && !affAssignment.getIds(p).contains(parts.nodeId())) {
                                if (diffIds == null)
                                    diffFromAffinity.put(p, diffIds = U.newHashSet(3));

                                if (diffIds.add(parts.nodeId()))
                                    changed = true;
                            }
                            else {
                                if (diffIds != null && diffIds.remove(parts.nodeId())) {
                                    changed = true;

                                    if (diffIds.isEmpty())
                                        diffFromAffinity.remove(p);
                                }
                            }
                        }

                        // Remove obsolete mappings.
                        if (cur != null) {
                            for (Integer p : F.view(cur.keySet(), F0.notIn(parts.keySet()))) {
                                Set<UUID> ids = diffFromAffinity.get(p);

                                if (ids != null && ids.remove(parts.nodeId())) {
                                    changed = true;

                                    if (ids.isEmpty())
                                        diffFromAffinity.remove(p);
                                }
                            }
                        }

                        diffFromAffinityVer = readyTopVer;
                    }
                }

                if (readyTopVer.initialized() && readyTopVer.equals(lastTopChangeVer)) {
                    AffinityAssignment aff = grp.affinity().readyAffinity(readyTopVer);

                    if (exchId == null)
                        changed |= checkEvictions(updateSeq, aff);

                    updateRebalanceVersion(aff.topologyVersion(), aff.assignment());
                }

                consistencyCheck();

                if (log.isDebugEnabled())
                    log.debug("Partition map after single update [grp=" + grp.cacheOrGroupName() + ", map=" + fullMapString() + ']');

                if (changed && exchId == null) {
                    if (log.isDebugEnabled())
                        log.debug("Partitions have been scheduled to resend [reason=" +
                            "Single map update [grp" + grp.cacheOrGroupName() + "]");

                    ctx.exchange().scheduleResendPartitions();
                }

                return changed;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onExchangeDone(@Nullable GridDhtPartitionsExchangeFuture fut,
        AffinityAssignment assignment,
        boolean updateRebalanceVer) {
        lock.writeLock().lock();

        try {
            assert !(topReadyFut instanceof GridDhtPartitionsExchangeFuture) ||
                assignment.topologyVersion().equals(((GridDhtPartitionsExchangeFuture)topReadyFut).context().events().topologyVersion());

            readyTopVer = lastTopChangeVer = assignment.topologyVersion();

            if (fut != null)
                discoCache = fut.events().discoveryCache();

            if (!grp.isReplicated()) {
                boolean rebuildDiff = fut == null || fut.localJoinExchange() || fut.serverNodeDiscoveryEvent() ||
                    fut.firstEvent().type() == EVT_DISCOVERY_CUSTOM_EVT || !diffFromAffinityVer.initialized();

                if (rebuildDiff) {
                    if (assignment.topologyVersion().compareTo(diffFromAffinityVer) >= 0)
                        rebuildDiff(assignment);
                }
                else
                    diffFromAffinityVer = readyTopVer;

                if (!updateRebalanceVer)
                    updateRebalanceVersion(assignment.topologyVersion(), assignment.assignment());
            }

            if (updateRebalanceVer)
                updateRebalanceVersion(assignment.topologyVersion(), assignment.assignment());

            // Own orphan moving partitions (having no suppliers).
            if (fut != null && (fut.events().hasServerJoin() || fut.changedBaseline()))
                ownOrphans();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Check if some local moving partitions have no suitable supplier and own them.
     * This can happen if a partition has been created by affinity assignment on new node and no supplier exists.
     * For example, if a node joins topology being a first data node for cache with node filter configured.
     */
    private void ownOrphans() {
        for (int p = 0; p < grp.affinity().partitions(); p++) {
            boolean hasOwner = false;

            for (GridDhtPartitionMap map : node2part.values()) {
                if (map.get(p) == OWNING) {
                    hasOwner = true;

                    break;
                }
            }

            if (!hasOwner) {
                GridDhtLocalPartition locPart = localPartition(p);

                if (locPart != null && locPart.state() != EVICTED && locPart.state() != LOST) {
                    locPart.own();

                    for (GridDhtPartitionMap map : node2part.values()) {
                        if (map.get(p) != null)
                            map.put(p, OWNING);
                    }
                }
            }
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
     * Rebuilds {@link #diffFromAffinity} from given assignment.
     *
     * @param affAssignment New affinity assignment.
     */
    private void rebuildDiff(AffinityAssignment affAssignment) {
        assert lock.isWriteLockedByCurrentThread();

        if (node2part == null)
            return;

        if (FAST_DIFF_REBUILD) {
            Collection<UUID> affNodes = F.nodeIds(ctx.discovery().cacheGroupAffinityNodes(grp.groupId(),
                affAssignment.topologyVersion()));

            for (Map.Entry<Integer, Set<UUID>> e : diffFromAffinity.entrySet()) {
                int p = e.getKey();

                Iterator<UUID> iter = e.getValue().iterator();

                while (iter.hasNext()) {
                    UUID nodeId = iter.next();

                    if (!affNodes.contains(nodeId) || affAssignment.getIds(p).contains(nodeId))
                        iter.remove();
                }
            }
        }
        else {
            for (Map.Entry<UUID, GridDhtPartitionMap> e : node2part.entrySet()) {
                UUID nodeId = e.getKey();

                for (Map.Entry<Integer, GridDhtPartitionState> e0 : e.getValue().entrySet()) {
                    Integer p0 = e0.getKey();

                    GridDhtPartitionState state = e0.getValue();

                    Set<UUID> ids = diffFromAffinity.get(p0);

                    if ((state == MOVING || state == OWNING || state == RENTING) && !affAssignment.getIds(p0).contains(nodeId)) {
                        if (ids == null)
                            diffFromAffinity.put(p0, ids = U.newHashSet(3));

                        ids.add(nodeId);
                    }
                    else {
                        if (ids != null)
                            ids.remove(nodeId);
                    }
                }
            }
        }

        diffFromAffinityVer = affAssignment.topologyVersion();
    }

    /** {@inheritDoc} */
    @Override public boolean detectLostPartitions(AffinityTopologyVersion resTopVer, GridDhtPartitionsExchangeFuture fut) {
        ctx.database().checkpointReadLock();

        try {
            lock.writeLock().lock();

            try {
                if (node2part == null)
                    return false;

                // Do not trigger lost partition events on activation.
                DiscoveryEvent discoEvt = fut.activateCluster() ? null : fut.firstEvent();

                final GridClusterStateProcessor state = grp.shared().kernalContext().state();

                boolean isInMemoryCluster = CU.isInMemoryCluster(
                    grp.shared().kernalContext().discovery().allNodes(),
                    grp.shared().kernalContext().marshallerContext().jdkMarshaller(),
                    U.resolveClassLoader(grp.shared().kernalContext().config())
                );

                boolean compatibleWithIgnorePlc = isInMemoryCluster
                    && state.isBaselineAutoAdjustEnabled() && state.baselineAutoAdjustTimeout() == 0L;

                // Calculate how data loss is handled.
                boolean safe = grp.config().getPartitionLossPolicy() != IGNORE || !compatibleWithIgnorePlc;

                int parts = grp.affinity().partitions();

                Set<Integer> recentlyLost = null;

                boolean changed = false;

                for (int part = 0; part < parts; part++) {
                    boolean lost = F.contains(lostParts, part);

                    if (!lost) {
                        boolean hasOwner = false;

                        // Detect if all owners are left.
                        for (GridDhtPartitionMap partMap : node2part.values()) {
                            if (partMap.get(part) == OWNING) {
                                hasOwner = true;

                                break;
                            }
                        }

                        if (!hasOwner) {
                            lost = true;

                            // Do not detect and record lost partition in IGNORE mode.
                            if (safe) {
                                if (lostParts == null)
                                    lostParts = new TreeSet<>();

                                lostParts.add(part);

                                if (discoEvt != null) {
                                    if (recentlyLost == null)
                                        recentlyLost = new HashSet<>();

                                    recentlyLost.add(part);

                                    if (grp.eventRecordable(EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST)) {
                                        grp.addRebalanceEvent(part,
                                            EVT_CACHE_REBALANCE_PART_DATA_LOST,
                                            discoEvt.eventNode(),
                                            discoEvt.type(),
                                            discoEvt.timestamp());
                                    }
                                }
                            }
                        }
                    }

                    if (lost) {
                        GridDhtLocalPartition locPart = localPartition(part, resTopVer, false, true);

                        if (locPart != null) {
                            if (locPart.state() == LOST)
                                continue;

                            final GridDhtPartitionState prevState = locPart.state();

                            changed = safe ? locPart.markLost() : locPart.own();

                            if (changed) {
                                long updSeq = updateSeq.incrementAndGet();

                                updateLocal(locPart.id(), locPart.state(), updSeq, resTopVer);

                                // If a partition was lost while rebalancing reset it's counter to force demander mode.
                                if (prevState == MOVING)
                                    locPart.resetUpdateCounter();
                            }
                        }

                        // Update remote maps according to policy.
                        for (Map.Entry<UUID, GridDhtPartitionMap> entry : node2part.entrySet()) {
                            if (entry.getKey().equals(ctx.localNodeId()))
                                continue;

                            GridDhtPartitionState p0 = entry.getValue().get(part);

                            if (p0 != null && p0 != EVICTED)
                                entry.getValue().put(part, safe ? LOST : OWNING);
                        }
                    }
                }

                if (recentlyLost != null) {
                    U.warn(log, "Detected lost partitions" + (!safe ? " (will ignore)" : "")
                        + " [grp=" + grp.cacheOrGroupName()
                        + ", parts=" + S.compact(recentlyLost)
                        + ", topVer=" + resTopVer + "]");
                }

                return changed;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void resetLostPartitions(AffinityTopologyVersion resTopVer) {
        ctx.database().checkpointReadLock();

        try {
            lock.writeLock().lock();

            try {
                long updSeq = updateSeq.incrementAndGet();

                for (Map.Entry<UUID, GridDhtPartitionMap> e : node2part.entrySet()) {
                    for (Map.Entry<Integer, GridDhtPartitionState> e0 : e.getValue().entrySet()) {
                        if (e0.getValue() != LOST)
                            continue;

                        e0.setValue(OWNING);

                        GridDhtLocalPartition locPart = localPartition(e0.getKey(), resTopVer, false);

                        if (locPart != null && locPart.state() == LOST) {
                            boolean marked = locPart.own();

                            if (marked) {
                                updateLocal(locPart.id(), locPart.state(), updSeq, resTopVer);

                                // Reset counters to zero for triggering full rebalance.
                                locPart.resetInitialUpdateCounter();
                            }
                        }
                    }
                }

                lostParts = null;
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Set<Integer> lostPartitions() {
        lock.readLock().lock();

        try {
            return lostParts == null ? Collections.<Integer>emptySet() : new HashSet<>(lostParts);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Set<Integer>> resetOwners(
        Map<Integer, Set<UUID>> ownersByUpdCounters,
        Set<Integer> haveHist,
        GridDhtPartitionsExchangeFuture exchFut) {
        Map<UUID, Set<Integer>> res = new HashMap<>();

        Collection<DiscoveryEvent> evts = exchFut.events().events();

        Set<UUID> joinedNodes = U.newHashSet(evts.size());

        for (DiscoveryEvent evt : evts) {
            if (evt.type() == EVT_NODE_JOINED)
                joinedNodes.add(evt.eventNode().id());
        }

        ctx.database().checkpointReadLock();

        try {
            Map<UUID, Set<Integer>> addToWaitGroups = new HashMap<>();

            lock.writeLock().lock();

            try {
                // First process local partitions.
                UUID locNodeId = ctx.localNodeId();

                for (Map.Entry<Integer, Set<UUID>> entry : ownersByUpdCounters.entrySet()) {
                    int part = entry.getKey();
                    Set<UUID> maxCounterPartOwners = entry.getValue();

                    GridDhtLocalPartition locPart = localPartition(part);

                    if (locPart == null || locPart.state() != OWNING)
                        continue;

                    // Partition state should be mutated only on joining nodes if they are exists for the exchange.
                    if (joinedNodes.isEmpty() && !maxCounterPartOwners.contains(locNodeId)) {
                        rebalancePartition(part, !haveHist.contains(part), exchFut);

                        res.computeIfAbsent(locNodeId, n -> new HashSet<>()).add(part);
                    }
                }

                // Then process node maps.
                for (Map.Entry<Integer, Set<UUID>> entry : ownersByUpdCounters.entrySet()) {
                    int part = entry.getKey();
                    Set<UUID> maxCounterPartOwners = entry.getValue();

                    for (Map.Entry<UUID, GridDhtPartitionMap> remotes : node2part.entrySet()) {
                        UUID remoteNodeId = remotes.getKey();

                        if (!joinedNodes.isEmpty() && !joinedNodes.contains(remoteNodeId))
                            continue;

                        GridDhtPartitionMap partMap = remotes.getValue();

                        GridDhtPartitionState state = partMap.get(part);

                        if (state != OWNING)
                            continue;

                        if (!maxCounterPartOwners.contains(remoteNodeId)) {
                            partMap.put(part, MOVING);

                            partMap.updateSequence(partMap.updateSequence() + 1, partMap.topologyVersion());

                            if (partMap.nodeId().equals(locNodeId))
                                updateSeq.setIfGreater(partMap.updateSequence());

                            res.computeIfAbsent(remoteNodeId, n -> new HashSet<>()).add(part);
                        }
                    }
                }

                for (Map.Entry<UUID, Set<Integer>> entry : res.entrySet()) {
                    UUID nodeId = entry.getKey();
                    Set<Integer> rebalancedParts = entry.getValue();

                    addToWaitGroups.put(nodeId, new HashSet<>(rebalancedParts));

                    if (!rebalancedParts.isEmpty()) {
                        Set<Integer> historical = rebalancedParts.stream()
                            .filter(haveHist::contains)
                            .collect(Collectors.toSet());

                        // Filter out partitions having WAL history.
                        rebalancedParts.removeAll(historical);

                        U.warn(log, "Partitions have been scheduled for rebalancing due to outdated update counter "
                            + "[grp=" + grp.cacheOrGroupName()
                            + ", topVer=" + exchFut.initialVersion()
                            + ", nodeId=" + nodeId
                            + ", partsFull=" + S.compact(rebalancedParts)
                            + ", partsHistorical=" + S.compact(historical) + "]");
                    }
                }

                node2part = new GridDhtPartitionFullMap(node2part, updateSeq.incrementAndGet());
            }
            finally {
                lock.writeLock().unlock();
            }

            List<List<ClusterNode>> ideal = ctx.affinity().affinity(groupId()).idealAssignmentRaw();

            for (Map.Entry<UUID, Set<Integer>> entry : addToWaitGroups.entrySet()) {
                // Add to wait groups to ensure late assignment switch after all partitions are rebalanced.
                for (Integer part : entry.getValue()) {
                    ctx.cache().context().affinity().addToWaitGroup(
                        groupId(),
                        part,
                        topologyVersionFuture().initialVersion(),
                        ideal.get(part)
                    );
                }
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }

        return res;
    }

    /**
     * Prepares given partition {@code p} for rebalance.
     * Changes partition state to MOVING and starts clearing if needed.
     * Prevents ongoing renting if required.
     *
     * @param p Partition id.
     * @param clear If {@code true} partition have to be cleared before rebalance (full rebalance or rebalance restart
     * after cancellation).
     * @param exchFut Future related to partition state change.
     */
    private GridDhtLocalPartition rebalancePartition(int p, boolean clear, GridDhtPartitionsExchangeFuture exchFut) {
        GridDhtLocalPartition part = getOrCreatePartition(p);

        // Prevent renting.
        if (part.state() == RENTING) {
            if (part.reserve()) {
                part.moving();
                part.release();
            }
            else {
                assert part.state() == EVICTED : part;

                part = getOrCreatePartition(p);
            }
        }

        if (part.state() != MOVING)
            part.moving();

        if (clear)
            exchFut.addClearingPartition(grp, part.id());

        assert part.state() == MOVING : part;

        return part;
    }

    /**
     * Finds local partitions which don't belong to affinity and runs eviction process for such partitions.
     *
     * @param updateSeq Update sequence.
     * @param aff Affinity assignments.
     * @return {@code True} if there are local partitions need to be evicted.
     */
    @SuppressWarnings("unchecked")
    private boolean checkEvictions(long updateSeq, AffinityAssignment aff) {
        if (!ctx.kernalContext().state().evictionsAllowed())
            return false;

        boolean hasEvictedPartitions = false;

        UUID locId = ctx.localNodeId();

        List<IgniteInternalFuture<?>> rentingFutures = new ArrayList<>();

        for (int p = 0; p < locParts.length(); p++) {
            GridDhtLocalPartition part = locParts.get(p);

            if (part == null || !part.state().active())
                continue;

            List<ClusterNode> affNodes = aff.get(p);

            // This node is affinity node for partition, no need to run eviction.
            if (affNodes.contains(ctx.localNode()))
                continue;

            List<ClusterNode> nodes = nodes(p, aff.topologyVersion(), OWNING);
            Collection<UUID> nodeIds = F.nodeIds(nodes);

            // If all affinity nodes are owners, then evict partition from local node.
            if (nodeIds.containsAll(F.nodeIds(affNodes))) {
                GridDhtPartitionState state0 = part.state();

                // There is no need to track a renting future of a partition which is already renting/evicted.
                IgniteInternalFuture<?> rentFut = part.rent(false, false);

                if (rentFut != null)
                    rentingFutures.add(rentFut);

                updateSeq = updateLocal(part.id(), part.state(), updateSeq, aff.topologyVersion());

                boolean stateChanged = state0 != part.state();

                hasEvictedPartitions |= stateChanged;

                if (stateChanged && log.isDebugEnabled()) {
                    log.debug("Partition has been scheduled for eviction (all affinity nodes are owners) " +
                        "[grp=" + grp.cacheOrGroupName() + ", p=" + part.id() + ", prevState=" + state0 + ", state=" + part.state() + "]");
                }
            }
            else {
                int ownerCnt = nodeIds.size();
                int affCnt = affNodes.size();

                if (ownerCnt > affCnt) {
                    // Sort by node orders in ascending order.
                    Collections.sort(nodes, CU.nodeComparator(true));

                    int diff = nodes.size() - affCnt;

                    for (int i = 0; i < diff; i++) {
                        ClusterNode n = nodes.get(i);

                        if (locId.equals(n.id())) {
                            GridDhtPartitionState state0 = part.state();

                            // There is no need to track a renting future of a partition
                            // which is already renting/evicted.
                            IgniteInternalFuture<?> rentFut = part.rent(false, false);

                            if (rentFut != null)
                                rentingFutures.add(rentFut);

                            updateSeq = updateLocal(part.id(), part.state(), updateSeq, aff.topologyVersion());

                            boolean stateChanged = state0 != part.state();

                            hasEvictedPartitions |= stateChanged;

                            if (stateChanged && log.isDebugEnabled()) {
                                log.debug("Partition has been scheduled for eviction (this node is oldest non-affinity node) " +
                                    "[grp=" + grp.cacheOrGroupName() + ", p=" + part.id() + ", prevState=" + state0 + ", state=" + part.state() + "]");
                            }

                            break;
                        }
                    }
                }
            }
        }

        // After all rents are finished resend partitions.
        if (!rentingFutures.isEmpty()) {
            final AtomicInteger rentingPartitions = new AtomicInteger(rentingFutures.size());

            IgniteInClosure c = new IgniteInClosure() {
                @Override public void apply(Object o) {
                    int remaining = rentingPartitions.decrementAndGet();

                    if (remaining == 0) {
                        lock.writeLock().lock();

                        try {
                            GridDhtPartitionTopologyImpl.this.updateSeq.incrementAndGet();

                            if (log.isDebugEnabled())
                                log.debug("Partitions have been scheduled to resend [reason=" +
                                    "Evictions are done [grp=" + grp.cacheOrGroupName() + "]");

                            ctx.exchange().scheduleResendPartitions();
                        }
                        finally {
                            lock.writeLock().unlock();
                        }
                    }
                }
            };

            for (IgniteInternalFuture<?> rentingFuture : rentingFutures)
                rentingFuture.listen(c);
        }

        return hasEvictedPartitions;
    }

    /**
     * Updates state of partition in local {@link #node2part} map and recalculates {@link #diffFromAffinity}.
     *
     * @param p Partition.
     * @param state Partition state.
     * @param updateSeq Update sequence.
     * @param affVer Affinity version.
     * @return Update sequence.
     */
    private long updateLocal(int p, GridDhtPartitionState state, long updateSeq, AffinityTopologyVersion affVer) {
        assert lock.isWriteLockedByCurrentThread();

        ClusterNode oldest = discoCache.oldestAliveServerNode();

        assert oldest != null || ctx.kernalContext().clientNode();

        // If this node became the oldest node.
        if (ctx.localNode().equals(oldest) && node2part != null) {
            long seq = node2part.updateSequence();

            if (seq != updateSeq) {
                if (seq > updateSeq) {
                    long seq0 = this.updateSeq.get();

                    if (seq0 < seq) {
                        // Update global counter if necessary.
                        boolean b = this.updateSeq.compareAndSet(seq0, seq + 1);

                        assert b : "Invalid update sequence [updateSeq=" + updateSeq +
                            ", grp=" + grp.cacheOrGroupName() +
                            ", seq=" + seq +
                            ", curUpdateSeq=" + this.updateSeq.get() +
                            ", node2part=" + node2part.toFullString() + ']';

                        updateSeq = seq + 1;
                    }
                    else
                        updateSeq = seq;
                }

                node2part.updateSequence(updateSeq);
            }
        }

        if (node2part != null) {
            UUID locNodeId = ctx.localNodeId();

            GridDhtPartitionMap map = node2part.get(locNodeId);

            if (map == null) {
                map = new GridDhtPartitionMap(locNodeId,
                    updateSeq,
                    affVer,
                    GridPartitionStateMap.EMPTY,
                    false);

                node2part.put(locNodeId, map);
            }
            else
                map.updateSequence(updateSeq, affVer);

            map.put(p, state);

            if (!grp.isReplicated() && (state == MOVING || state == OWNING || state == RENTING)) {
                AffinityAssignment assignment = grp.affinity().cachedAffinity(diffFromAffinityVer);

                if (!assignment.getIds(p).contains(ctx.localNodeId())) {
                    Set<UUID> diffIds = diffFromAffinity.get(p);

                    if (diffIds == null)
                        diffFromAffinity.put(p, diffIds = U.newHashSet(3));

                    diffIds.add(ctx.localNodeId());
                }
            }
        }

        return updateSeq;
    }

    /**
     * Removes node from local {@link #node2part} map and recalculates {@link #diffFromAffinity}.
     *
     * @param nodeId Node to remove.
     */
    private void removeNode(UUID nodeId) {
        assert nodeId != null;
        assert lock.isWriteLockedByCurrentThread();

        ClusterNode oldest = discoCache.oldestAliveServerNode();

        assert oldest != null || ctx.kernalContext().clientNode();

        ClusterNode loc = ctx.localNode();

        if (node2part != null) {
            if (loc.equals(oldest) && !node2part.nodeId().equals(loc.id()))
                node2part = new GridDhtPartitionFullMap(loc.id(), loc.order(), updateSeq.get(),
                    node2part, false);
            else
                node2part = new GridDhtPartitionFullMap(node2part, node2part.updateSequence());

            GridDhtPartitionMap parts = node2part.remove(nodeId);

            if (!grp.isReplicated()) {
                if (parts != null) {
                    for (Integer p : parts.keySet()) {
                        Set<UUID> diffIds = diffFromAffinity.get(p);

                        if (diffIds != null)
                            diffIds.remove(nodeId);
                    }
                }
            }

            consistencyCheck();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean own(GridDhtLocalPartition part) {
        lock.writeLock().lock();

        try {
            if (part.own()) {
                assert lastTopChangeVer.initialized() : lastTopChangeVer;

                long updSeq = updateSeq.incrementAndGet();

                updateLocal(part.id(), part.state(), updSeq, lastTopChangeVer);

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
    @Override public void ownMoving(AffinityTopologyVersion rebFinishedTopVer) {
        lock.writeLock().lock();

        try {
            AffinityTopologyVersion lastAffChangeVer = ctx.exchange().lastAffinityChangedTopologyVersion(lastTopChangeVer);

            if (lastAffChangeVer.compareTo(rebFinishedTopVer) > 0) {
                if (log.isInfoEnabled()) {
                    log.info("Affinity topology changed, no MOVING partitions will be owned " +
                        "[rebFinishedTopVer=" + rebFinishedTopVer +
                        ", lastAffChangeVer=" + lastAffChangeVer + "]");
                }

                if (!((GridDhtPreloader)grp.preloader()).disableRebalancingCancellationOptimization())
                    grp.preloader().forceRebalance();

                return;
            }

            for (GridDhtLocalPartition locPart : currentLocalPartitions()) {
                if (locPart.state() == MOVING) {
                    boolean reserved = locPart.reserve();

                    try {
                        if (reserved && locPart.state() == MOVING &&
                            lastAffChangeVer.compareTo(rebFinishedTopVer) <= 0 &&
                            rebFinishedTopVer.compareTo(lastTopChangeVer) <= 0)
                            own(locPart);
                    }
                    finally {
                        if (reserved)
                            locPart.release();
                    }
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void onEvicted(GridDhtLocalPartition part, boolean updateSeq) {
        ctx.database().checkpointReadLock();

        try {
            lock.writeLock().lock();

            try {
                if (stopping)
                    return;

                assert part.state() == EVICTED;

                long seq = updateSeq ? this.updateSeq.incrementAndGet() : this.updateSeq.get();

                assert lastTopChangeVer.initialized() : lastTopChangeVer;

                updateLocal(part.id(), part.state(), seq, lastTopChangeVer);

                consistencyCheck();
            }
            finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
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
    @Override public void finalizeUpdateCounters(Set<Integer> parts) {
        // It is need to acquire checkpoint lock before topology lock acquiring.
        ctx.database().checkpointReadLock();

        try {
            WALPointer ptr = null;

            lock.readLock().lock();

            try {
                for (int p : parts) {
                    GridDhtLocalPartition part = locParts.get(p);

                    if (part != null && part.state().active()) {
                        // We need to close all gaps in partition update counters sequence. We assume this finalizing is
                        // happened on exchange and hence all txs are completed. Therefore each gap in update counters
                        // sequence is a result of undelivered DhtTxFinishMessage on backup (sequences on primary nodes
                        // do not have gaps). Here we close these gaps and asynchronously notify continuous query engine
                        // about the skipped events.
                        AffinityTopologyVersion topVer = ctx.exchange().readyAffinityVersion();

                        GridLongList gaps = part.finalizeUpdateCounters();

                        if (gaps != null) {
                            for (int j = 0; j < gaps.size() / 2; j++) {
                                long gapStart = gaps.get(j * 2);
                                long gapStop = gaps.get(j * 2 + 1);

                                if (part.group().persistenceEnabled() &&
                                    part.group().walEnabled() &&
                                    !part.group().mvccEnabled()) {
                                    // Rollback record tracks applied out-of-order updates while finalizeUpdateCounters
                                    // return gaps (missing updates). The code below transforms gaps to updates.
                                    RollbackRecord rec = new RollbackRecord(part.group().groupId(), part.id(),
                                        gapStart - 1, gapStop - gapStart + 1);

                                    try {
                                        ptr = ctx.wal().log(rec);
                                    }
                                    catch (IgniteCheckedException e) {
                                        throw new IgniteException(e);
                                    }
                                }
                            }

                            for (GridCacheContext ctx0 : grp.caches())
                                ctx0.continuousQueries().closeBackupUpdateCountersGaps(ctx0, part.id(), topVer, gaps);
                        }
                    }
                }
            }
            finally {
                try {
                    if (ptr != null)
                        ctx.wal().flush(ptr, false);
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
                finally {
                    lock.readLock().unlock();
                }
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }
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
        lock.readLock().lock();

        try {
            int locPartCnt = 0;

            for (int i = 0; i < locParts.length(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part != null)
                    locPartCnt++;
            }

            CachePartitionPartialCountersMap res = new CachePartitionPartialCountersMap(locPartCnt);

            for (int i = 0; i < locParts.length(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part == null)
                    continue;

                long initCntr = part.initialUpdateCounter();
                long updCntr = part.updateCounter();

                if (skipZeros && initCntr == 0L && updCntr == 0L)
                    continue;

                res.add(part.id(), initCntr, updCntr);
            }

            res.trim();

            return res;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> partitionSizes() {
        lock.readLock().lock();

        try {
            Map<Integer, Long> partitionSizes = new HashMap<>();

            for (int p = 0; p < locParts.length(); p++) {
                GridDhtLocalPartition part = locParts.get(p);
                if (part == null || part.fullSize() == 0)
                    continue;

                partitionSizes.put(part.id(), part.fullSize());
            }

            return partitionSizes;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Integer, Long> globalPartSizes() {
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
        AffinityTopologyVersion curTopVer = this.readyTopVer;

        return curTopVer.equals(topVer) && curTopVer.equals(rebalancedTopVer);
    }

    /** {@inheritDoc} */
    @Override public boolean hasMovingPartitions() {
        lock.readLock().lock();

        try {
            if (node2part == null)
                return false;

            assert node2part.valid() : "Invalid node2part [node2part: " + node2part +
                ", grp=" + grp.cacheOrGroupName() +
                ", stopping=" + stopping +
                ", locNodeId=" + ctx.localNodeId() +
                ", locName=" + ctx.igniteInstanceName() + ']';

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

    /**
     * @param cacheId Cache ID.
     */
    public void onCacheStopped(int cacheId) {
        if (!grp.sharedGroup())
            return;

        for (int i = 0; i < locParts.length(); i++) {
            GridDhtLocalPartition part = locParts.get(i);

            if (part != null)
                part.onCacheStopped(cacheId);
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats(int threshold) {
        X.println(">>>  Cache partition topology stats [igniteInstanceName=" + ctx.igniteInstanceName() +
            ", grp=" + grp.cacheOrGroupName() + ']');

        lock.readLock().lock();

        try {
            for (int i = 0; i < locParts.length(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part == null)
                    continue;

                long size = part.dataStore().fullSize();

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
        return aff.get(part).contains(ctx.localNode());
    }

    /**
     * @param affVer Affinity version.
     * @param aff Affinity assignments.
     */
    private void updateRebalanceVersion(AffinityTopologyVersion affVer, List<List<ClusterNode>> aff) {
        if (!grp.isReplicated() && !affVer.equals(diffFromAffinityVer))
            return;

        if (!rebalancedTopVer.equals(readyTopVer)) {
            if (node2part == null || !node2part.valid())
                return;

            for (int i = 0; i < grp.affinity().partitions(); i++) {
                List<ClusterNode> affNodes = aff.get(i);

                // Topology doesn't contain server nodes (just clients).
                if (affNodes.isEmpty())
                    continue;

                for (ClusterNode node : affNodes) {
                    if (!hasState(i, node.id(), OWNING))
                        return;
                }

                if (!grp.isReplicated()) {
                    Set<UUID> diff = diffFromAffinity.get(i);

                    if (diff != null) {
                        for (UUID nodeId : diff) {
                            if (hasState(i, nodeId, OWNING)) {
                                ClusterNode node = ctx.discovery().node(nodeId);

                                if (node != null && !affNodes.contains(node))
                                    return;
                            }
                        }
                    }
                }
            }

            rebalancedTopVer = readyTopVer;

            if (log.isDebugEnabled())
                log.debug("Updated rebalanced version [grp=" + grp.cacheOrGroupName() + ", ver=" + rebalancedTopVer + ']');
        }
    }

    /**
     * Checks that state of partition {@code p} for node {@code nodeId} in local {@code node2part} map
     * matches to one of the specified states {@code match} or {@ode matches}.
     *
     * @param p Partition id.
     * @param nodeId Node ID.
     * @param match State to match.
     * @param matches Additional states.
     * @return {@code True} if partition matches to one of the specified states.
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

            if (matches != null && matches.length > 0) {
                for (GridDhtPartitionState s : matches) {
                    if (state == s)
                        return true;
                }
            }
        }

        return false;
    }

    /**
     * Checks consistency after all operations.
     */
    private void consistencyCheck() {
        // No-op.
    }

    /**
     * Collects states of local partitions.
     *
     * @return String representation of all local partition states.
     */
    private String dumpPartitionStates() {
        SB sb = new SB();

        for (int p = 0; p < locParts.length(); p++) {
            GridDhtLocalPartition part = locParts.get(p);

            if (part == null)
                continue;

            sb.a("Part [");
            sb.a("id=" + part.id() + ", ");
            sb.a("state=" + part.state() + ", ");
            sb.a("initCounter=" + part.initialUpdateCounter() + ", ");
            sb.a("updCounter=" + part.updateCounter() + ", ");
            sb.a("size=" + part.fullSize() + "] ");
        }

        return sb.toString();
    }

    /**
     * Iterator over current local partitions.
     */
    private class CurrentPartitionsIterator implements Iterator<GridDhtLocalPartition> {
        /** Next index. */
        private int nextIdx;

        /** Next partition. */
        private GridDhtLocalPartition nextPart;

        /**
         * Constructor
         */
        private CurrentPartitionsIterator() {
            advance();
        }

        /**
         * Try to advance to next partition.
         */
        private void advance() {
            while (nextIdx < locParts.length()) {
                GridDhtLocalPartition part = locParts.get(nextIdx);

                if (part != null && part.state().active()) {
                    nextPart = part;
                    return;
                }

                nextIdx++;
            }
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return nextPart != null;
        }

        /** {@inheritDoc} */
        @Override public GridDhtLocalPartition next() {
            if (nextPart == null)
                throw new NoSuchElementException();

            GridDhtLocalPartition retVal = nextPart;

            nextPart = null;
            nextIdx++;

            advance();

            return retVal;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException("remove");
        }
    }

    /**
     * Partition factory used for (re-)creating partitions during their lifecycle.
     * Currently used in tests for overriding default partition behavior.
     */
    public interface PartitionFactory {
        /**
         * @param ctx Context.
         * @param grp Group.
         * @param id Partition id.
         * @return New partition instance.
         */
        public GridDhtLocalPartition create(GridCacheSharedContext ctx,
            CacheGroupContext grp,
            int id);
    }
}
