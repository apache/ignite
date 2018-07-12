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
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.ExchangeDiscoveryEvents;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionFullCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.GridPartitionStateMap;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.RENTING;

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

    /** Partitions map for left nodes. */
    private GridDhtPartitionFullMap leftNode2Part = new GridDhtPartitionFullMap();

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
        assert topReadyFut != null;

        return topReadyFut;
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
    private boolean initPartitions(AffinityTopologyVersion affVer, List<List<ClusterNode>> affAssignment, GridDhtPartitionsExchangeFuture exchFut, long updateSeq) {
        boolean needRefresh = false;

        if (grp.affinityNode()) {
            ClusterNode loc = ctx.localNode();

            ClusterNode oldest = discoCache.oldestAliveServerNode();

            GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

            int num = grp.affinity().partitions();

            if (grp.rebalanceEnabled()) {
                boolean added = exchFut.cacheGroupAddedOnExchange(grp.groupId(), grp.receivedFrom());

                boolean first = added || (loc.equals(oldest) && loc.id().equals(exchId.nodeId()) && exchId.isJoined()) || exchFut.activateCluster();

                if (first) {
                    assert exchId.isJoined() || added || exchFut.activateCluster();

                    for (int p = 0; p < num; p++) {
                        if (localNode(p, affAssignment)) {
                            // Partition is created first time, so it's safe to own it.
                            boolean shouldOwn = locParts.get(p) == null;

                            GridDhtLocalPartition locPart = getOrCreatePartition(p);

                            if (shouldOwn) {
                                locPart.own();

                                if (log.isDebugEnabled())
                                    log.debug("Owned partition for oldest node [grp=" + grp.cacheOrGroupName() +
                                        ", part=" + locPart + ']');
                            }

                            needRefresh = true;

                            updateSeq = updateLocal(p, locPart.state(), updateSeq, affVer);
                        }
                    }
                }
                else
                    createPartitions(affVer, affAssignment, updateSeq);
            }
            else {
                // If preloader is disabled, then we simply clear out
                // the partitions this node is not responsible for.
                for (int p = 0; p < num; p++) {
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
                        else
                            locPart.own();
                    }
                    else if (belongs) {
                        locPart = getOrCreatePartition(p);

                        locPart.own();

                        updateLocal(p, locPart.state(), updateSeq, affVer);
                    }
                }
            }
        }

        updateRebalanceVersion(affAssignment);

        return needRefresh;
    }

    /**
     * Creates non-existing partitions belong to given affinity {@code aff}.
     *
     * @param affVer Affinity version.
     * @param aff Affinity assignments.
     * @param updateSeq Update sequence.
     */
    private void createPartitions(AffinityTopologyVersion affVer, List<List<ClusterNode>> aff, long updateSeq) {
        if (!grp.affinityNode())
            return;

        int num = grp.affinity().partitions();

        for (int p = 0; p < num; p++) {
            if (node2part != null && node2part.valid()) {
                if (localNode(p, aff)) {
                    // This will make sure that all non-existing partitions
                    // will be created in MOVING state.
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
    @Override public void beforeExchange(GridDhtPartitionsExchangeFuture exchFut,
        boolean affReady,
        boolean updateMoving)
        throws IgniteCheckedException {
        ctx.database().checkpointReadLock();

        try {
            synchronized (ctx.exchange().interruptLock()) {
                if (Thread.currentThread().isInterrupted())
                    throw new IgniteInterruptedCheckedException("Thread is interrupted: " + Thread.currentThread());

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
                        List<DiscoveryEvent> evts0 = evts.events();

                        for (int i = 0; i < evts0.size(); i++) {
                            DiscoveryEvent evt = evts0.get(i);

                            if (ExchangeDiscoveryEvents.serverLeftEvent(evt))
                                removeNode(evt.eventNode().id());
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
                                affAssignment = grp.affinity().idealAssignment();
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

        // If this is the oldest node.
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
                        "grp=" +  grp.cacheOrGroupName() + ", exchId=" + exchFut.exchangeId() +
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
        lock.writeLock().lock();

        try {
            long updateSeq = this.updateSeq.incrementAndGet();

            initializeFullMap(updateSeq);

            for (int p = 0; p < grp.affinity().partitions(); p++) {
                GridDhtLocalPartition locPart = locParts.get(p);

                if (locPart == null)
                    updateLocal(p, EVICTED, updateSeq, topVer);
                else
                    updateLocal(p, locPart.state(), updateSeq, topVer);
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean afterExchange(GridDhtPartitionsExchangeFuture exchFut) {
        boolean changed = false;

        int num = grp.affinity().partitions();

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

                for (int p = 0; p < num; p++) {
                    GridDhtLocalPartition locPart = localPartition0(p, topVer, false, true);

                    if (partitionLocalNode(p, topVer)) {
                        // Prepare partition to rebalance if it's not happened on full map update phase.
                        if (locPart == null || locPart.state() == RENTING || locPart.state() == EVICTED)
                            locPart = rebalancePartition(p, false);

                        GridDhtPartitionState state = locPart.state();

                        if (state == MOVING) {
                            if (grp.rebalanceEnabled()) {
                                Collection<ClusterNode> owners = owners(p);

                                // If an owner node left during exchange, then new exchange should be started with detecting lost partitions.

                                if (!F.isEmpty(owners)) {
                                    if (log.isDebugEnabled())
                                        log.debug("Will not own partition (there are owners to rebalance from) [grp=" + grp.cacheOrGroupName() +
                                            ", locPart=" + locPart + ", owners = " + owners + ']');
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
                                        "grp=" + grp.cacheOrGroupName() + ", part=" + locPart + ']');
                                }
                            }
                        }
                    }
                }

                AffinityAssignment aff = grp.affinity().readyAffinity(topVer);

                if (node2part != null && node2part.valid())
                    changed |= checkEvictions(updateSeq, aff);

                updateRebalanceVersion(aff.assignment());

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
    private GridDhtLocalPartition getOrCreatePartition(int p) {
        assert lock.isWriteLockedByCurrentThread();

        assert ctx.database().checkpointLockIsHeldByThread();

        GridDhtLocalPartition loc = locParts.get(p);

        if (loc == null || loc.state() == EVICTED) {
            // Make sure that after eviction partition is destroyed.
            if (loc != null)
                loc.awaitDestroy();

            locParts.set(p, loc = new GridDhtLocalPartition(ctx, grp, p));

            long updCntr = cntrMap.updateCounter(p);

            if (updCntr != 0)
                loc.updateCounter(updCntr);

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

            if (part != null && part.state() != EVICTED)
                return part;

            part = new GridDhtLocalPartition(ctx, grp, p);

            locParts.set(p, part);

            ctx.pageStore().onPartitionCreated(grp.groupId(), p);

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

                if (loc != null && state == EVICTED) {
                    // Make sure that after eviction partition is destroyed.
                    loc.awaitDestroy();

                    locParts.set(p, loc = null);

                    if (!belongs) {
                        throw new GridDhtInvalidPartitionException(p, "Adding entry to evicted partition " +
                            "(often may be caused by inconsistent 'key.hashCode()' implementation) " +
                            "[grp=" + grp.cacheOrGroupName() + ", part=" + p + ", topVer=" + topVer +
                            ", this.topVer=" + this.readyTopVer + ']');
                    }
                }
                else if (loc != null && state == RENTING && !showRenting) {
                    throw new GridDhtInvalidPartitionException(p, "Adding entry to partition that is concurrently " +
                        "evicted [grp=" + grp.cacheOrGroupName() + ", part=" + p + ", shouldBeMoving="
                        + ", belongs=" + belongs + ", topVer=" + topVer + ", curTopVer=" + this.readyTopVer + "]");
                }

                if (loc == null) {
                    if (!belongs)
                        throw new GridDhtInvalidPartitionException(p, "Creating partition which does not belong to " +
                            "local node (often may be caused by inconsistent 'key.hashCode()' implementation) " +
                            "[grp=" + grp.cacheOrGroupName() + ", part=" + p + ", topVer=" + topVer +
                            ", this.topVer=" + this.readyTopVer + ']');

                    locParts.set(p, loc = new GridDhtLocalPartition(ctx, grp, p));

                    this.updateSeq.incrementAndGet();

                    created = true;

                    if (log.isDebugEnabled())
                        log.debug("Created local partition [grp=" + grp.cacheOrGroupName() + ", part=" + loc + ']');
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
                ", topVer2=" + this.readyTopVer +
                ", node=" + ctx.igniteInstanceName() +
                ", grp=" + grp.cacheOrGroupName() +
                ", node2part=" + node2part + ']';

            List<ClusterNode> nodes = null;

            if (!topVer.equals(diffFromAffinityVer)) {
                LT.warn(log, "Requested topology version does not match calculated diff, will require full iteration to" +
                    "calculate mapping [grp=" + grp.cacheOrGroupName() + ", topVer=" + topVer +
                    ", diffVer=" + diffFromAffinityVer + "]");

                nodes = new ArrayList<>();

                nodes.addAll(affNodes);

                for (Map.Entry<UUID, GridDhtPartitionMap> entry : node2part.entrySet()) {
                    GridDhtPartitionState state = entry.getValue().get(p);

                    ClusterNode n = ctx.discovery().node(entry.getKey());

                    if (n != null && state != null && (state == MOVING || state == OWNING || state == RENTING)
                        && !nodes.contains(n) && (topVer.topologyVersion() < 0 || n.order() <= topVer.topologyVersion())) {
                        nodes.add(n);
                    }

                }

                return nodes;
            }

            Collection<UUID> diffIds = diffFromAffinity.get(p);

            if (!F.isEmpty(diffIds)) {
                HashSet<UUID> affIds = affAssignment.getIds(p);

                for (UUID nodeId : diffIds) {
                    if (affIds.contains(nodeId)) {
                        U.warn(log, "Node from diff is affinity node, skipping it [grp=" + grp.cacheOrGroupName() +
                            ", node=" + nodeId + ']');

                        continue;
                    }

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
            List<ClusterNode> nodes = new ArrayList<>();

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
        return newMap != null &&
                (newMap.topologyVersion().compareTo(currentMap.topologyVersion()) > 0 ||
                 newMap.topologyVersion().compareTo(currentMap.topologyVersion()) == 0 && newMap.updateSequence() > currentMap.updateSequence());
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public boolean update(
        @Nullable AffinityTopologyVersion exchangeVer,
        GridDhtPartitionFullMap partMap,
        @Nullable CachePartitionFullCountersMap incomeCntrMap,
        Set<Integer> partsToReload,
        @Nullable Map<Integer, Long> partSizes,
        @Nullable AffinityTopologyVersion msgTopVer) {
        if (log.isDebugEnabled()) {
            log.debug("Updating full partition map [grp=" + grp.cacheOrGroupName() + ", exchVer=" + exchangeVer +
                ", fullMap=" + fullMapString() + ']');
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

                            if (updCntr != 0 && updCntr > part.updateCounter())
                                part.updateCounter(updCntr);
                        }
                    }
                }

                if (exchangeVer != null) {
                    // Ignore if exchange already finished or new exchange started.
                    if (readyTopVer.compareTo(exchangeVer) > 0 || lastTopChangeVer.compareTo(exchangeVer) > 0) {
                        U.warn(log, "Stale exchange id for full partition map update (will ignore) [" +
                            "grp=" + grp.cacheOrGroupName() +
                            ", lastTopChange=" + lastTopChangeVer +
                            ", readTopVer=" + readyTopVer +
                            ", exchVer=" + exchangeVer + ']');

                        return false;
                    }
                }

                if (msgTopVer != null && lastTopChangeVer.compareTo(msgTopVer) > 0) {
                    U.warn(log, "Stale version for full partition map update message (will ignore) [" +
                        "grp=" + grp.cacheOrGroupName() +
                        ", lastTopChange=" + lastTopChangeVer +
                        ", readTopVer=" + readyTopVer +
                        ", msgVer=" + msgTopVer + ']');

                    return false;
                }

                boolean fullMapUpdated = (node2part == null);

                if (node2part != null) {
                    for (GridDhtPartitionMap part : node2part.values()) {
                        GridDhtPartitionMap newPart = partMap.get(part.nodeId());

                        if (shouldOverridePartitionMap(part, newPart)) {
                            fullMapUpdated = true;

                            if (log.isDebugEnabled()) {
                                log.debug("Overriding partition map in full update map [" +
                                    "grp=" + grp.cacheOrGroupName() +
                                    ", exchVer=" + exchangeVer +
                                    ", curPart=" + mapString(part) +
                                    ", newPart=" + mapString(newPart) + ']');
                            }

                            if (newPart.nodeId().equals(ctx.localNodeId()))
                                updateSeq.setIfGreater(newPart.updateSequence());
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

                        if (!ctx.discovery().alive(nodeId)) {
                            if (log.isDebugEnabled())
                                log.debug("Removing left node from full map update [grp=" + grp.cacheOrGroupName() +
                                    ", nodeId=" + nodeId + ", partMap=" + partMap + ']');

                            it.remove();
                        }
                    }
                }
                else {
                    GridDhtPartitionMap locNodeMap = partMap.get(ctx.localNodeId());

                    if (locNodeMap != null)
                        updateSeq.setIfGreater(locNodeMap.updateSequence());
                }

                if (!fullMapUpdated) {
                    if (log.isDebugEnabled()) {
                        log.debug("No updates for full partition map (will ignore) [" +
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
                }

                node2part = partMap;

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
                            boolean haveHistory = !partsToReload.contains(p);

                            rebalancePartition(p, haveHistory);

                            changed = true;
                        }
                    }
                }

                long updateSeq = this.updateSeq.incrementAndGet();

                if (readyTopVer.initialized() && readyTopVer.equals(lastTopChangeVer)) {
                    AffinityAssignment aff = grp.affinity().readyAffinity(readyTopVer);

                    if (exchangeVer == null)
                        changed |= checkEvictions(updateSeq, aff);

                    updateRebalanceVersion(aff.assignment());
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

                if (changed)
                    ctx.exchange().scheduleResendPartitions();

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

        long now = U.currentTimeMillis();

        lock.writeLock().lock();

        try {
            long acquired = U.currentTimeMillis();

            if (acquired - now >= 100) {
                if (timeLog.isInfoEnabled())
                    timeLog.info("Waited too long to acquire topology write lock " +
                        "[grp=" + grp.cacheOrGroupName() + ", waitTime=" + (acquired - now) + ']');
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
        long now = U.currentTimeMillis();

        lock.writeLock().lock();

        try {
            long acquired = U.currentTimeMillis();

            if (acquired - now >= 100) {
                if (timeLog.isInfoEnabled())
                    timeLog.info("Waited too long to acquire topology write lock " +
                        "[grp=" + grp.cacheOrGroupName() + ", waitTime=" + (acquired - now) + ']');
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

                    if (!reserve || state == EVICTED || state == RENTING)
                        continue;

                    long updCntr = cntrMap.updateCounter(part.id());

                    if (updCntr > part.updateCounter())
                        part.updateCounter(updCntr);
                    else if (part.updateCounter() > 0) {
                        cntrMap.initialUpdateCounter(part.id(), part.initialUpdateCounter());
                        cntrMap.updateCounter(part.id(), part.updateCounter());
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
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
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
            if (log.isDebugEnabled()) {
                log.debug("Received partition update for non-existing node (will ignore) [grp=" + grp.cacheOrGroupName() +
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
                        "grp=" + grp.cacheOrGroupName() +
                        ", exchId=" + exchId +
                        ", curMap=" + cur +
                        ", newMap=" + parts + ']';

                    // This is usual situation when partition maps are equal, just print debug message.
                    if (cur.compareTo(parts) == 0) {
                        if (log.isDebugEnabled())
                            log.debug(msg);
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

                    updateRebalanceVersion(aff.assignment());
                }

                consistencyCheck();

                if (log.isDebugEnabled())
                    log.debug("Partition map after single update [grp=" + grp.cacheOrGroupName() + ", map=" + fullMapString() + ']');

                if (changed && exchId == null)
                    ctx.exchange().scheduleResendPartitions();

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
            }

            if (updateRebalanceVer)
                updateRebalanceVersion(assignment.assignment());
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
    @Override public boolean detectLostPartitions(AffinityTopologyVersion resTopVer, DiscoveryEvent discoEvt) {
        ctx.database().checkpointReadLock();

        try {
            lock.writeLock().lock();

            try {
                if (node2part == null)
                    return false;

                int parts = grp.affinity().partitions();

                Set<Integer> lost = new HashSet<>(parts);

                for (int p = 0; p < parts; p++)
                    lost.add(p);

                for (GridDhtPartitionMap partMap : node2part.values()) {
                    for (Map.Entry<Integer, GridDhtPartitionState> e : partMap.entrySet()) {
                        if (e.getValue() == OWNING) {
                            lost.remove(e.getKey());

                            if (lost.isEmpty())
                                break;
                        }
                    }
                }

                boolean changed = false;

                if (!F.isEmpty(lost)) {
                    PartitionLossPolicy plc = grp.config().getPartitionLossPolicy();

                    assert plc != null;

                    Set<Integer> recentlyLost = new HashSet<>();

                    for (Map.Entry<UUID, GridDhtPartitionMap> leftEntry : leftNode2Part.entrySet()) {
                        for (Map.Entry<Integer, GridDhtPartitionState> entry : leftEntry.getValue().entrySet()) {
                            if (entry.getValue() == OWNING)
                                recentlyLost.add(entry.getKey());
                        }
                    }

                    // Update partition state on all nodes.
                    for (Integer part : lost) {
                        long updSeq = updateSeq.incrementAndGet();

                        GridDhtLocalPartition locPart = localPartition(part, resTopVer, false, true);

                        if (locPart != null) {
                            if (locPart.state() == LOST)
                                continue;

                            boolean marked = plc == PartitionLossPolicy.IGNORE ? locPart.own() : locPart.markLost();

                            if (marked)
                                updateLocal(locPart.id(), locPart.state(), updSeq, resTopVer);

                            changed |= marked;
                        }
                        // Update map for remote node.
                        else if (plc != PartitionLossPolicy.IGNORE) {
                            for (Map.Entry<UUID, GridDhtPartitionMap> e : node2part.entrySet()) {
                                if (e.getKey().equals(ctx.localNodeId()))
                                    continue;

                                if (e.getValue().get(part) != EVICTED)
                                    e.getValue().put(part, LOST);
                            }
                        }

                        if (recentlyLost.contains(part) && grp.eventRecordable(EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST)) {
                            grp.addRebalanceEvent(part,
                                EVT_CACHE_REBALANCE_PART_DATA_LOST,
                                discoEvt.eventNode(),
                                discoEvt.type(),
                                discoEvt.timestamp());
                        }
                    }

                    if (plc != PartitionLossPolicy.IGNORE)
                        grp.needsRecovery(true);
                }

                leftNode2Part.clear();

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

                            if (marked)
                                updateLocal(locPart.id(), locPart.state(), updSeq, resTopVer);
                        }
                    }
                }

                checkEvictions(updSeq, grp.affinity().readyAffinity(resTopVer));

                grp.needsRecovery(false);
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
    @Override public Collection<Integer> lostPartitions() {
        if (grp.config().getPartitionLossPolicy() == PartitionLossPolicy.IGNORE)
            return Collections.emptySet();

        lock.readLock().lock();

        try {
            Set<Integer> res = null;

            int parts = grp.affinity().partitions();

            for (GridDhtPartitionMap partMap : node2part.values()) {
                for (Map.Entry<Integer, GridDhtPartitionState> e : partMap.entrySet()) {
                    if (e.getValue() == LOST) {
                        if (res == null)
                            res = new HashSet<>(parts);

                        res.add(e.getKey());
                    }
                }
            }

            return res == null ? Collections.<Integer>emptySet() : res;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Set<Integer>> resetOwners(Map<Integer, Set<UUID>> ownersByUpdCounters, Set<Integer> haveHistory) {
        Map<UUID, Set<Integer>> result = new HashMap<>();

        ctx.database().checkpointReadLock();

        try {
            lock.writeLock().lock();

            try {
                // First process local partitions.
                for (Map.Entry<Integer, Set<UUID>> entry : ownersByUpdCounters.entrySet()) {
                    int part = entry.getKey();
                    Set<UUID> newOwners = entry.getValue();

                    GridDhtLocalPartition locPart = localPartition(part);

                    if (locPart == null || locPart.state() != OWNING)
                        continue;

                    if (!newOwners.contains(ctx.localNodeId())) {
                        rebalancePartition(part, haveHistory.contains(part));

                        result.computeIfAbsent(ctx.localNodeId(), n -> new HashSet<>());
                        result.get(ctx.localNodeId()).add(part);
                    }
                }

                // Then process remote partitions.
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

                            if (partMap.nodeId().equals(ctx.localNodeId()))
                                updateSeq.setIfGreater(partMap.updateSequence());

                            result.computeIfAbsent(remoteNodeId, n -> new HashSet<>());
                            result.get(remoteNodeId).add(part);
                        }
                    }
                }

                for (Map.Entry<UUID, Set<Integer>> entry : result.entrySet()) {
                    UUID nodeId = entry.getKey();
                    Set<Integer> rebalancedParts = entry.getValue();

                    if (!rebalancedParts.isEmpty()) {
                        Set<Integer> historical = rebalancedParts.stream()
                            .filter(haveHistory::contains)
                            .collect(Collectors.toSet());

                        // Filter out partitions having WAL history.
                        rebalancedParts.removeAll(historical);

                        U.warn(log, "Partitions have been scheduled for rebalancing due to outdated update counter "
                            + "[grp=" + grp.cacheOrGroupName()
                            + ", nodeId=" + nodeId
                            + ", partsFull=" + S.compact(rebalancedParts)
                            + ", partsHistorical=" + S.compact(historical) + "]");
                    }
                }

                node2part = new GridDhtPartitionFullMap(node2part, updateSeq.incrementAndGet());
            } finally {
                lock.writeLock().unlock();
            }
        }
        finally {
            ctx.database().checkpointReadUnlock();
        }

        return result;
    }

    /**
     * Prepares given partition {@code p} for rebalance.
     * Changes partition state to MOVING and starts clearing if needed.
     * Prevents ongoing renting if required.
     *
     * @param p Partition id.
     * @param haveHistory If {@code true} there is WAL history to rebalance partition,
     *                    in other case partition will be cleared for full rebalance.
     */
    private GridDhtLocalPartition rebalancePartition(int p, boolean haveHistory) {
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

        if (!haveHistory)
            part.clearAsync();

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
    private boolean checkEvictions(long updateSeq, AffinityAssignment aff) {
        if (!ctx.kernalContext().state().evictionsAllowed())
            return false;

        boolean changed = false;

        UUID locId = ctx.localNodeId();

        List<IgniteInternalFuture<?>> rentingFutures = new ArrayList<>();

        for (int p = 0; p < locParts.length(); p++) {
            GridDhtLocalPartition part = locParts.get(p);

            if (part == null)
                continue;

            GridDhtPartitionState state = part.state();

            if (state.active()) {
                List<ClusterNode> affNodes = aff.get(p);

                if (!affNodes.contains(ctx.localNode())) {
                    List<ClusterNode> nodes = nodes(p, aff.topologyVersion(), OWNING, null);
                    Collection<UUID> nodeIds = F.nodeIds(nodes);

                    // If all affinity nodes are owners, then evict partition from local node.
                    if (nodeIds.containsAll(F.nodeIds(affNodes))) {
                        IgniteInternalFuture<?> rentFuture = part.rent(false);
                        rentingFutures.add(rentFuture);

                        updateSeq = updateLocal(part.id(), part.state(), updateSeq, aff.topologyVersion());

                        changed = true;

                        if (log.isDebugEnabled()) {
                            log.debug("Evicted local partition (all affinity nodes are owners) [grp=" + grp.cacheOrGroupName() +
                                ", part=" + part + ']');
                        }
                    }
                    else {
                        int ownerCnt = nodeIds.size();
                        int affCnt = affNodes.size();

                        if (ownerCnt > affCnt) { //TODO !!! we could loss all owners in such case. Should be fixed by GG-13223
                            // Sort by node orders in ascending order.
                            Collections.sort(nodes, CU.nodeComparator(true));

                            int diff = nodes.size() - affCnt;

                            for (int i = 0; i < diff; i++) {
                                ClusterNode n = nodes.get(i);

                                if (locId.equals(n.id())) {
                                    IgniteInternalFuture<?> rentFuture = part.rent(false);
                                    rentingFutures.add(rentFuture);

                                    updateSeq = updateLocal(part.id(), part.state(), updateSeq, aff.topologyVersion());

                                    changed = true;

                                    if (log.isDebugEnabled()) {
                                        log.debug("Evicted local partition (this node is oldest non-affinity node) [" +
                                            "grp=" + grp.cacheOrGroupName() + ", part=" + part + ']');
                                    }

                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        // After all rents are finished resend partitions.
        if (!rentingFutures.isEmpty()) {
            final AtomicInteger rentingPartitions = new AtomicInteger(rentingFutures.size());

            for (IgniteInternalFuture<?> rentingFuture : rentingFutures) {
                rentingFuture.listen(f -> {
                    int remaining = rentingPartitions.decrementAndGet();

                    if (remaining == 0) {
                        lock.writeLock().lock();

                        try {
                            this.updateSeq.incrementAndGet();

                            ctx.exchange().scheduleResendPartitions();
                        }
                        finally {
                            lock.writeLock().unlock();
                        }
                    }
                });
            }
        }

        return changed;
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
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
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

            if (parts != null)
                leftNode2Part.put(nodeId, parts);

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
    @Override public void ownMoving(AffinityTopologyVersion topVer) {
        lock.writeLock().lock();

        try {
            for (GridDhtLocalPartition locPart : grp.topology().currentLocalPartitions()) {
                if (locPart.state() == MOVING) {
                    boolean reserved = locPart.reserve();

                    try {
                        if (reserved && locPart.state() == MOVING && lastTopChangeVer.equals(topVer))
                            grp.topology().own(locPart);
                        else // topology changed, rebalancing must be restarted
                            return;
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

                long updCntr = part.updateCounter();
                long initCntr = part.initialUpdateCounter();

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
     * @param aff Affinity assignments.
     */
    private void updateRebalanceVersion(List<List<ClusterNode>> aff) {
        if (!rebalancedTopVer.equals(readyTopVer)) {
            if (node2part == null || !node2part.valid())
                return;

            for (int i = 0; i < grp.affinity().partitions(); i++) {
                List<ClusterNode> affNodes = aff.get(i);

                // Topology doesn't contain server nodes (just clients).
                if (affNodes.isEmpty())
                    continue;

                Set<ClusterNode> owners = U.newHashSet(affNodes.size());

                for (ClusterNode node : affNodes) {
                    if (hasState(i, node.id(), OWNING))
                        owners.add(node);
                }

                if (!grp.isReplicated()) {
                    Set<UUID> diff = diffFromAffinity.get(i);

                    if (diff != null) {
                        for (UUID nodeId : diff) {
                            if (hasState(i, nodeId, OWNING)) {
                                ClusterNode node = ctx.discovery().node(nodeId);

                                if (node != null)
                                    owners.add(node);
                            }
                        }
                    }
                }

                if (affNodes.size() != owners.size() || !owners.containsAll(affNodes))
                    return;
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
        // no-op
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
}
