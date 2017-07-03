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
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.ClusterState;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionExchangeId;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridAtomicLong;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT;
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
    /** If true, then check consistency. */
    private static final boolean CONSISTENCY_CHECK = false;

    /** Flag to control amount of output for full map. */
    private static final boolean FULL_MAP_DEBUG = false;

    /** */
    private static final int DUMP_PENDING_OBJECTS_THRESHOLD =
        IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_DUMP_PENDING_OBJECTS_THRESHOLD, 10);

    /** */
    private static final Long ZERO = 0L;

    /** */
    private final GridCacheSharedContext ctx;

    /** */
    private final CacheGroupContext grp;

    /** Logger. */
    private final IgniteLogger log;

    /** */
    private final AtomicReferenceArray<GridDhtLocalPartition> locParts;

    /** Node to partition map. */
    private GridDhtPartitionFullMap node2part;

    /** Partition to node map. */
    private Map<Integer, Set<UUID>> part2node = new HashMap<>();

    /** */
    private AffinityTopologyVersion lastExchangeVer;

    /** */
    private volatile AffinityTopologyVersion topVer = AffinityTopologyVersion.NONE;

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
    private Map<Integer, T2<Long, Long>> cntrMap = new HashMap<>();

    /** */
    private volatile AffinityTopologyVersion rebalancedTopVer = AffinityTopologyVersion.NONE;

    /** */
    private volatile boolean treatAllPartAsLoc;

    /**
     * @param ctx Cache shared context.
     * @param grp Cache group.
     */
    public GridDhtPartitionTopologyImpl(GridCacheSharedContext ctx,
        CacheGroupContext grp) {
        assert ctx != null;
        assert grp != null;

        this.ctx = ctx;
        this.grp = grp;

        log = ctx.logger(getClass());

        locParts = new AtomicReferenceArray<>(grp.affinityFunction().partitions());
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

            part2node = new HashMap<>();

            lastExchangeVer = null;

            updateSeq.set(1);

            topReadyFut = null;

            rebalancedTopVer = AffinityTopologyVersion.NONE;

            topVer = AffinityTopologyVersion.NONE;

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

    /**
     * Waits for renting partitions.
     *
     * @return {@code True} if mapping was changed.
     * @throws IgniteCheckedException If failed.
     */
    private boolean waitForRent() throws IgniteCheckedException {
        if (!grp.affinityNode())
            return false;

        final long longOpDumpTimeout =
            IgniteSystemProperties.getLong(IgniteSystemProperties.IGNITE_LONG_OPERATIONS_DUMP_TIMEOUT, 60_000);

        int dumpCnt = 0;

        GridDhtLocalPartition part;

        for (int i = 0; i < locParts.length(); i++) {
            part = locParts.get(i);

            if (part == null)
                continue;

            GridDhtPartitionState state = part.state();

            if (state == RENTING || state == EVICTED) {
                if (log.isDebugEnabled())
                    log.debug("Waiting for renting partition: " + part);

                part.tryEvictAsync(false);

                // Wait for partition to empty out.
                if (longOpDumpTimeout > 0) {
                    while (true) {
                        try {
                            part.rent(true).get(longOpDumpTimeout);

                            break;
                        }
                        catch (IgniteFutureTimeoutCheckedException ignored) {
                            if (dumpCnt++ < DUMP_PENDING_OBJECTS_THRESHOLD) {
                                U.warn(log, "Failed to wait for partition eviction [" +
                                    "topVer=" + topVer +
                                    ", group=" + grp.cacheOrGroupName() +
                                    ", part=" + part.id() +
                                    ", partState=" + part.state() +
                                    ", size=" + part.internalSize() +
                                    ", reservations=" + part.reservations() +
                                    ", grpReservations=" + part.groupReserved() +
                                    ", node=" + ctx.localNodeId() + "]");

                                if (IgniteSystemProperties.getBoolean(IGNITE_THREAD_DUMP_ON_EXCHANGE_TIMEOUT, false))
                                    U.dumpThreads(log);
                            }
                        }
                    }
                }
                else
                    part.rent(true).get();

                if (log.isDebugEnabled())
                    log.debug("Finished waiting for renting partition: " + part);
            }
        }

        // Remove evicted partition.
        lock.writeLock().lock();

        try {
            boolean changed = false;

            for (int i = 0; i < locParts.length(); i++) {
                part = locParts.get(i);

                if (part == null)
                    continue;

                if (part.state() == EVICTED) {
                    locParts.set(i, null);
                    changed = true;
                }
            }

            return changed;
        }
        finally {
            lock.writeLock().unlock();
        }
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
        GridDhtTopologyFuture exchFut,
        DiscoCache discoCache,
        long updSeq,
        boolean stopping
    ) throws IgniteInterruptedCheckedException {
        U.writeLock(lock);

        try {
            AffinityTopologyVersion exchTopVer = exchFut.topologyVersion();

            assert exchTopVer.compareTo(topVer) > 0 : "Invalid topology version [topVer=" + topVer +
                ", exchTopVer=" + exchTopVer +
                ", fut=" + exchFut + ']';

            this.stopping = stopping;

            updateSeq.setIfGreater(updSeq);

            topReadyFut = exchFut;

            rebalancedTopVer = AffinityTopologyVersion.NONE;

            topVer = exchTopVer;

            this.discoCache = discoCache;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public AffinityTopologyVersion topologyVersion() {
        AffinityTopologyVersion topVer = this.topVer;

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
        List<List<ClusterNode>> aff = grp.affinity().assignments(exchFut.topologyVersion());

        if (grp.affinityNode()) {
            ClusterNode loc = ctx.localNode();

            ClusterNode oldest = discoCache.oldestAliveServerNodeWithCache();

            GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

            assert topVer.equals(exchFut.topologyVersion()) :
                "Invalid topology [topVer=" + topVer +
                    ", grp=" + grp.cacheOrGroupName() +
                    ", futVer=" + exchFut.topologyVersion() +
                    ", fut=" + exchFut + ']';
            assert grp.affinity().lastVersion().equals(exchFut.topologyVersion()) :
                "Invalid affinity [topVer=" + grp.affinity().lastVersion() +
                    ", grp=" + grp.cacheOrGroupName() +
                    ", futVer=" + exchFut.topologyVersion() +
                    ", fut=" + exchFut + ']';

            int num = grp.affinity().partitions();

            if (grp.rebalanceEnabled()) {
                boolean added = exchFut.cacheGroupAddedOnExchange(grp.groupId(), grp.receivedFrom());

                boolean first = added || (loc.equals(oldest) && loc.id().equals(exchId.nodeId()) && exchId.isJoined());

                if (first) {
                    assert exchId.isJoined() || added;

                    for (int p = 0; p < num; p++) {
                        if (localNode(p, aff)) {
                            GridDhtLocalPartition locPart = createPartition(p);

                            boolean owned = locPart.own();

                            assert owned : "Failed to own partition for oldest node [grp=" + grp.cacheOrGroupName() +
                                ", part=" + locPart + ']';

                            if (log.isDebugEnabled())
                                log.debug("Owned partition for oldest node: " + locPart);

                            updateSeq = updateLocal(p, locPart.state(), updateSeq);
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

                                updateSeq = updateLocal(p, locPart.state(), updateSeq);

                                if (log.isDebugEnabled())
                                    log.debug("Evicting partition with rebalancing disabled " +
                                        "(it does not belong to affinity): " + locPart);
                            }
                        }
                        else
                            locPart.own();
                    }
                    else if (belongs) {
                        locPart = createPartition(p);

                        locPart.own();

                        updateLocal(p, locPart.state(), updateSeq);
                    }
                }
            }

            if (node2part != null && node2part.valid())
                checkEvictions(updateSeq, aff);
        }

        updateRebalanceVersion(aff);
    }

    /**
     * @param aff Affinity assignments.
     * @param updateSeq Update sequence.
     */
    private void createPartitions(List<List<ClusterNode>> aff, long updateSeq) {
        if (!grp.affinityNode())
            return;

        int num = grp.affinity().partitions();

        for (int p = 0; p < num; p++) {
            if (node2part != null && node2part.valid()) {
                if (localNode(p, aff)) {
                    // This will make sure that all non-existing partitions
                    // will be created in MOVING state.
                    GridDhtLocalPartition locPart = createPartition(p);

                    updateSeq = updateLocal(p, locPart.state(), updateSeq);
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
        DiscoveryEvent discoEvt = exchFut.discoveryEvent();

        ClusterState newState = exchFut.newClusterState();

        treatAllPartAsLoc = (newState != null && newState == ClusterState.ACTIVE)
            || (ctx.kernalContext().state().active()
            && discoEvt.type() == EventType.EVT_NODE_JOINED
            && discoEvt.eventNode().isLocal()
            && !ctx.kernalContext().clientNode()
        );

        // Wait for rent outside of checkpoint lock.
        waitForRent();

        ClusterNode loc = ctx.localNode();

        ctx.database().checkpointReadLock();

        synchronized (ctx.exchange().interruptLock()) {
            if (Thread.currentThread().isInterrupted())
                throw new IgniteInterruptedCheckedException("Thread is interrupted: " + Thread.currentThread());

            try {
                U.writeLock(lock);
            }
            catch (IgniteInterruptedCheckedException e) {
                ctx.database().checkpointReadUnlock();

                throw e;
            }

            try {
                if (stopping)
                    return;

                GridDhtPartitionExchangeId exchId = exchFut.exchangeId();

                assert topVer.equals(exchId.topologyVersion()) : "Invalid topology version [topVer=" +
                    topVer + ", exchId=" + exchId + ']';

                if (exchId.isLeft() && exchFut.serverNodeDiscoveryEvent())
                    removeNode(exchId.nodeId());

                ClusterNode oldest = discoCache.oldestAliveServerNodeWithCache();

                if (log.isDebugEnabled())
                    log.debug("Partition map beforeExchange [exchId=" + exchId + ", fullMap=" + fullMapString() + ']');

                long updateSeq = this.updateSeq.incrementAndGet();

                cntrMap.clear();

                boolean grpStarted = exchFut.cacheGroupAddedOnExchange(grp.groupId(), grp.receivedFrom());

                // If this is the oldest node.
                if (oldest != null && (loc.equals(oldest) || grpStarted)) {
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

                if (grpStarted ||
                    exchFut.discoveryEvent().type() == EVT_DISCOVERY_CUSTOM_EVT ||
                    exchFut.serverNodeDiscoveryEvent()) {
                    if (affReady)
                        initPartitions0(exchFut, updateSeq);
                    else {
                        List<List<ClusterNode>> aff = grp.affinity().idealAssignment();

                        createPartitions(aff, updateSeq);
                    }
                }

                consistencyCheck();

                if (log.isDebugEnabled())
                    log.debug("Partition map after beforeExchange [exchId=" + exchId + ", fullMap=" +
                        fullMapString() + ']');
            }
            finally {
                lock.writeLock().unlock();

                ctx.database().checkpointReadUnlock();
            }
        }

        // Wait for evictions.
        waitForRent();
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
    @Override public boolean afterExchange(GridDhtPartitionsExchangeFuture exchFut) throws IgniteCheckedException {
        treatAllPartAsLoc = false;

        boolean changed = waitForRent();

        int num = grp.affinity().partitions();

        AffinityTopologyVersion topVer = exchFut.topologyVersion();

        assert grp.affinity().lastVersion().equals(topVer) : "Affinity is not initialized " +
            "[topVer=" + topVer +
            ", affVer=" + grp.affinity().lastVersion() +
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

                if (partitionLocalNode(p, topVer)) {
                    // This partition will be created during next topology event,
                    // which obviously has not happened at this point.
                    if (locPart == null) {
                        if (log.isDebugEnabled())
                            log.debug("Skipping local partition afterExchange (will not create): " + p);

                        continue;
                    }

                    GridDhtPartitionState state = locPart.state();

                    if (state == MOVING) {
                        if (grp.rebalanceEnabled()) {
                            Collection<ClusterNode> owners = owners(p);

                            // If there are no other owners, then become an owner.
                            if (F.isEmpty(owners)) {
                                boolean owned = locPart.own();

                                assert owned : "Failed to own partition [grp=" + grp.cacheOrGroupName() + ", locPart=" +
                                    locPart + ']';

                                updateSeq = updateLocal(p, locPart.state(), updateSeq);

                                changed = true;

                                if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_DATA_LOST)) {
                                    DiscoveryEvent discoEvt = exchFut.discoveryEvent();

                                    grp.addRebalanceEvent(p,
                                        EVT_CACHE_REBALANCE_PART_DATA_LOST,
                                        discoEvt.eventNode(),
                                        discoEvt.type(),
                                        discoEvt.timestamp());
                                }

                                if (log.isDebugEnabled())
                                    log.debug("Owned partition: " + locPart);
                            }
                            else if (log.isDebugEnabled())
                                log.debug("Will not own partition (there are owners to rebalance from) [locPart=" +
                                    locPart + ", owners = " + owners + ']');
                        }
                        else
                            updateSeq = updateLocal(p, locPart.state(), updateSeq);
                    }
                }
                else {
                    if (locPart != null) {
                        GridDhtPartitionState state = locPart.state();

                        if (state == MOVING && ctx.kernalContext().state().active()) {
                            locPart.rent(false);

                            updateSeq = updateLocal(p, locPart.state(), updateSeq);

                            changed = true;

                            if (log.isDebugEnabled())
                                log.debug("Evicting moving partition (it does not belong to affinity): " + locPart);
                        }
                    }
                }
            }

            updateRebalanceVersion(grp.affinity().assignments(topVer));

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

        GridDhtLocalPartition loc = locParts.get(p);

        if (loc == null || loc.state() == EVICTED) {
            locParts.set(p, loc = new GridDhtLocalPartition(ctx, grp, p));

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

    /**
     * @param p Partition number.
     * @param topVer Topology version.
     * @param create Create flag.
     * @param updateSeq Update sequence.
     * @return Local partition.
     */
    @SuppressWarnings("TooBroadScope")
    private GridDhtLocalPartition localPartition(int p,
        AffinityTopologyVersion topVer,
        boolean create,
        boolean updateSeq) {
        GridDhtLocalPartition loc;

        loc = locParts.get(p);

        GridDhtPartitionState state = loc != null ? loc.state() : null;

        if (loc != null && state != EVICTED && (state != RENTING || !grp.allowFastEviction()))
            return loc;

        if (!create)
            return null;

        boolean created = false;

        lock.writeLock().lock();

        try {
            loc = locParts.get(p);

            state = loc != null ? loc.state() : null;

            boolean belongs = partitionLocalNode(p, topVer);

            if (loc != null && state == EVICTED) {
                locParts.set(p, loc = null);

                if (!treatAllPartAsLoc && !belongs)
                    throw new GridDhtInvalidPartitionException(p, "Adding entry to evicted partition " +
                        "(often may be caused by inconsistent 'key.hashCode()' implementation) " +
                        "[part=" + p + ", topVer=" + topVer + ", this.topVer=" + this.topVer + ']');
            }
            else if (loc != null && state == RENTING && grp.allowFastEviction())
                throw new GridDhtInvalidPartitionException(p, "Adding entry to partition that is concurrently evicted.");

            if (loc == null) {
                if (!treatAllPartAsLoc && !belongs)
                    throw new GridDhtInvalidPartitionException(p, "Creating partition which does not belong to " +
                        "local node (often may be caused by inconsistent 'key.hashCode()' implementation) " +
                        "[part=" + p + ", topVer=" + topVer + ", this.topVer=" + this.topVer + ']');

                locParts.set(p, loc = new GridDhtLocalPartition(ctx, grp, p));

                if (updateSeq)
                    this.updateSeq.incrementAndGet();

                created = true;

                if (log.isDebugEnabled())
                    log.debug("Created local partition: " + loc);
            }
        }
        finally {
            lock.writeLock().unlock();
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

        GridDhtLocalPartition loc = localPartition(e.partition(), topologyVersion(), false);

        if (loc != null)
            loc.onRemoved(e);
    }

    /** {@inheritDoc} */
    @Override public GridDhtPartitionMap localPartitionMap() {
        Map<Integer, GridDhtPartitionState> map = new HashMap<>();

        lock.readLock().lock();

        try {
            for (int i = 0; i < locParts.length(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part == null)
                    continue;

                map.put(i, part.state());
            }

            return new GridDhtPartitionMap(ctx.localNodeId(),
                updateSeq.get(),
                topVer,
                Collections.unmodifiableMap(map),
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
        AffinityTopologyVersion topVer = affAssignment.topologyVersion();

        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer1=" + topVer +
                ", topVer2=" + this.topVer +
                ", node=" + ctx.igniteInstanceName() +
                ", grp=" + grp.cacheOrGroupName() +
                ", node2part=" + node2part + ']';

            List<ClusterNode> nodes = null;

            Collection<UUID> nodeIds = part2node.get(p);

            if (!F.isEmpty(nodeIds)) {
                for (UUID nodeId : nodeIds) {
                    HashSet<UUID> affIds = affAssignment.getIds(p);

                    if (!affIds.contains(nodeId) && hasState(p, nodeId, OWNING, MOVING, RENTING)) {
                        ClusterNode n = ctx.discovery().node(nodeId);

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
    private List<ClusterNode> nodes(int p,
        AffinityTopologyVersion topVer,
        GridDhtPartitionState state,
        GridDhtPartitionState... states) {
        Collection<UUID> allIds = topVer.topologyVersion() > 0 ?
            F.nodeIds(discoCache.cacheGroupAffinityNodes(grp.groupId())) :
            null;

        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node-to-partitions map [topVer=" + topVer +
                ", allIds=" + allIds +
                ", node2part=" + node2part +
                ", grp=" + grp.cacheOrGroupName() + ']';

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

        return nodes(p, topVer, OWNING);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> owners(int p) {
        return owners(p, AffinityTopologyVersion.NONE);
    }

    /** {@inheritDoc} */
    @Override public List<ClusterNode> moving(int p) {
        if (!grp.rebalanceEnabled())
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

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public boolean update(
        @Nullable AffinityTopologyVersion exchangeVer,
        GridDhtPartitionFullMap partMap,
        @Nullable Map<Integer, T2<Long, Long>> cntrMap
    ) {
        if (log.isDebugEnabled())
            log.debug("Updating full partition map [exchVer=" + exchangeVer + ", parts=" + fullMapString() + ']');

        assert partMap != null;

        lock.writeLock().lock();

        try {
            if (stopping)
                return false;

            if (cntrMap != null) {
                // update local map partition counters
                for (Map.Entry<Integer, T2<Long, Long>> e : cntrMap.entrySet()) {
                    T2<Long, Long> cntr = this.cntrMap.get(e.getKey());

                    if (cntr == null || cntr.get2() < e.getValue().get2())
                        this.cntrMap.put(e.getKey(), e.getValue());
                }

                // update local counters in partitions
                for (int i = 0; i < locParts.length(); i++) {
                    GridDhtLocalPartition part = locParts.get(i);

                    if (part == null)
                        continue;

                    T2<Long, Long> cntr = cntrMap.get(part.id());

                    if (cntr != null)
                        part.updateCounter(cntr.get2());
                }
            }

            if (exchangeVer != null && lastExchangeVer != null && lastExchangeVer.compareTo(exchangeVer) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for full partition map update (will ignore) [lastExch=" +
                        lastExchangeVer + ", exch=" + exchangeVer + ']');

                return false;
            }

            if (node2part != null && node2part.compareTo(partMap) >= 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale partition map for full partition map update (will ignore) [lastExch=" +
                        lastExchangeVer + ", exch=" + exchangeVer + ", curMap=" + node2part + ", newMap=" + partMap + ']');

                return false;
            }

            long updateSeq = this.updateSeq.incrementAndGet();

            if (exchangeVer != null)
                lastExchangeVer = exchangeVer;

            if (node2part != null) {
                for (GridDhtPartitionMap part : node2part.values()) {
                    GridDhtPartitionMap newPart = partMap.get(part.nodeId());

                    // If for some nodes current partition has a newer map,
                    // then we keep the newer value.
                    if (newPart != null &&
                        (newPart.updateSequence() < part.updateSequence() ||
                        (grp.localStartVersion().compareTo(newPart.topologyVersion()) > 0))
                        ) {
                        if (log.isDebugEnabled())
                            log.debug("Overriding partition map in full update map [exch=" + exchangeVer +
                                ", curPart=" + mapString(part) + ", newPart=" + mapString(newPart) + ']');

                        partMap.put(part.nodeId(), part);
                    }
                }

                // Remove entry if node left.
                for (Iterator<UUID> it = partMap.keySet().iterator(); it.hasNext(); ) {
                    UUID nodeId = it.next();

                    if (!ctx.discovery().alive(nodeId)) {
                        if (log.isDebugEnabled())
                            log.debug("Removing left node from full map update [nodeId=" + nodeId + ", partMap=" +
                                partMap + ']');

                        it.remove();
                    }
                }
            }

            node2part = partMap;

            Map<Integer, Set<UUID>> p2n = new HashMap<>(grp.affinity().partitions(), 1.0f);

            for (Map.Entry<UUID, GridDhtPartitionMap> e : partMap.entrySet()) {
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

            AffinityTopologyVersion affVer = grp.affinity().lastVersion();

            GridDhtPartitionMap nodeMap = partMap.get(ctx.localNodeId());

            if (nodeMap != null && ctx.database().persistenceEnabled()) {
                for (Map.Entry<Integer, GridDhtPartitionState> e : nodeMap.entrySet()) {
                    int p = e.getKey();
                    GridDhtPartitionState state = e.getValue();

                   if (state == MOVING) {
                        GridDhtLocalPartition locPart = locParts.get(p);

                        assert locPart != null;

                        if (locPart.state() == OWNING) {
                            locPart.moving();

                            changed = true;
                        }

                        if (cntrMap != null) {
                            T2<Long, Long> cntr = cntrMap.get(p);

                            if (cntr != null && cntr.get2() > locPart.updateCounter())
                                locPart.updateCounter(cntr.get2());
                        }
                    }
                }
            }

            if (!affVer.equals(AffinityTopologyVersion.NONE) && affVer.compareTo(topVer) >= 0) {
                List<List<ClusterNode>> aff = grp.affinity().assignments(topVer);

                changed |= checkEvictions(updateSeq, aff);

                updateRebalanceVersion(aff);
            }

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after full update: " + fullMapString());

            if (changed)
                ctx.exchange().scheduleResendPartitions();

            return changed;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void applyUpdateCounters(Map<Integer, T2<Long, Long>> cntrMap) {
        assert cntrMap != null;

        lock.writeLock().lock();

        try {
            if (stopping)
                return;

            for (Map.Entry<Integer, T2<Long, Long>> e : cntrMap.entrySet()) {
                T2<Long, Long> cntr = this.cntrMap.get(e.getKey());

                if (cntr == null || cntr.get2() < e.getValue().get2())
                    this.cntrMap.put(e.getKey(), e.getValue());
            }

            for (int i = 0; i < locParts.length(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part == null)
                    continue;

                T2<Long, Long> cntr = cntrMap.get(part.id());

                if (cntr != null && cntr.get2() > part.updateCounter())
                    part.updateCounter(cntr.get2());
            }
        }
        finally {
            lock.writeLock().unlock();

        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    @Override public boolean update(
        @Nullable GridDhtPartitionExchangeId exchId,
        GridDhtPartitionMap parts
    ) {
        if (log.isDebugEnabled())
            log.debug("Updating single partition map [exchId=" + exchId + ", parts=" + mapString(parts) + ']');

        if (!ctx.discovery().alive(parts.nodeId())) {
            if (log.isDebugEnabled())
                log.debug("Received partition update for non-existing node (will ignore) [exchId=" + exchId +
                    ", parts=" + parts + ']');

            return false;
        }

        lock.writeLock().lock();

        try {
            if (stopping)
                return false;

            if (lastExchangeVer != null && exchId != null && lastExchangeVer.compareTo(exchId.topologyVersion()) > 0) {
                if (log.isDebugEnabled())
                    log.debug("Stale exchange id for single partition map update (will ignore) [lastExch=" +
                        lastExchangeVer + ", exch=" + exchId.topologyVersion() + ']');

                return false;
            }

            if (exchId != null)
                lastExchangeVer = exchId.topologyVersion();

            if (node2part == null)
                // Create invalid partition map.
                node2part = new GridDhtPartitionFullMap();

            GridDhtPartitionMap cur = node2part.get(parts.nodeId());

            if (cur != null && cur.updateSequence() >= parts.updateSequence()) {
                if (log.isDebugEnabled())
                    log.debug("Stale update sequence for single partition map update (will ignore) [exchId=" + exchId +
                        ", curSeq=" + cur.updateSequence() + ", newSeq=" + parts.updateSequence() + ']');

                return false;
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

            AffinityTopologyVersion affVer = grp.affinity().lastVersion();

            if (!affVer.equals(AffinityTopologyVersion.NONE) && affVer.compareTo(topVer) >= 0) {
                List<List<ClusterNode>> aff = grp.affinity().assignments(topVer);

                changed |= checkEvictions(updateSeq, aff);

                updateRebalanceVersion(aff);
            }

            consistencyCheck();

            if (log.isDebugEnabled())
                log.debug("Partition map after single update: " + fullMapString());

            if (changed)
                ctx.exchange().scheduleResendPartitions();

            return changed;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean detectLostPartitions(DiscoveryEvent discoEvt) {
        lock.writeLock().lock();

        try {
            int parts = grp.affinity().partitions();

            Collection<Integer> lost = null;

            for (int p = 0; p < parts; p++) {
                boolean foundOwner = false;

                Set<UUID> nodeIds = part2node.get(p);

                if (nodeIds != null) {
                    for (UUID nodeId : nodeIds) {
                        GridDhtPartitionMap partMap = node2part.get(nodeId);

                        GridDhtPartitionState state = partMap.get(p);

                        if (state == OWNING) {
                            foundOwner = true;

                            break;
                        }
                    }
                }

                if (!foundOwner) {
                    if (lost == null)
                        lost = new HashSet<>(parts - p, 1.0f);

                    lost.add(p);
                }
            }

            boolean changed = false;

            if (lost != null) {
                PartitionLossPolicy plc = grp.config().getPartitionLossPolicy();

                assert plc != null;

                // Update partition state on all nodes.
                for (Integer part : lost) {
                    long updSeq = updateSeq.incrementAndGet();

                    GridDhtLocalPartition locPart = localPartition(part, topVer, false);

                    if (locPart != null) {
                        boolean marked = plc == PartitionLossPolicy.IGNORE ? locPart.own() : locPart.markLost();

                        if (marked)
                            updateLocal(locPart.id(), locPart.state(), updSeq);

                        changed |= marked;
                    }
                    // Update map for remote node.
                    else if (plc != PartitionLossPolicy.IGNORE) {
                        Set<UUID> nodeIds = part2node.get(part);

                        if (nodeIds != null) {
                            for (UUID nodeId : nodeIds) {
                                GridDhtPartitionMap nodeMap = node2part.get(nodeId);

                                if (nodeMap.get(part) != EVICTED)
                                    nodeMap.put(part, LOST);
                            }
                        }
                    }

                    if (grp.eventRecordable(EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST)) {
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

            return changed;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void resetLostPartitions() {
        lock.writeLock().lock();

        try {
            int parts = grp.affinity().partitions();
            long updSeq = updateSeq.incrementAndGet();

            for (int part = 0; part < parts; part++) {
                Set<UUID> nodeIds = part2node.get(part);

                if (nodeIds != null) {
                    boolean lost = false;

                    for (UUID node : nodeIds) {
                        GridDhtPartitionMap map = node2part.get(node);

                        if (map.get(part) == LOST) {
                            lost = true;

                            break;
                        }
                    }

                    if (lost) {
                        GridDhtLocalPartition locPart = localPartition(part, topVer, false);

                        if (locPart != null) {
                            boolean marked = locPart.own();

                            if (marked)
                                updateLocal(locPart.id(), locPart.state(), updSeq);
                        }

                        for (UUID nodeId : nodeIds) {
                            GridDhtPartitionMap nodeMap = node2part.get(nodeId);

                            if (nodeMap.get(part) == LOST)
                                nodeMap.put(part, OWNING);
                        }
                    }
                }
            }

            checkEvictions(updSeq, grp.affinity().assignments(topVer));

            grp.needsRecovery(false);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> lostPartitions() {
        lock.readLock().lock();

        try {
            Collection<Integer> res = null;

            int parts = grp.affinity().partitions();

            for (int part = 0; part < parts; part++) {
                Set<UUID> nodeIds = part2node.get(part);

                if (nodeIds != null) {
                    for (UUID node : nodeIds) {
                        GridDhtPartitionMap map = node2part.get(node);

                        if (map.get(part) == LOST) {
                            if (res == null)
                                res = new ArrayList<>(parts - part);

                            res.add(part);

                            break;
                        }
                    }
                }
            }

            return res == null ? Collections.<Integer>emptyList() : res;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void setOwners(int p, Set<UUID> owners, boolean updateSeq) {
        lock.writeLock().lock();

        try {

            GridDhtLocalPartition locPart = locParts.get(p);

            if (locPart != null) {
                if (locPart.state() == OWNING && !owners.contains(ctx.localNodeId()))
                    locPart.moving();
            }

            for (Map.Entry<UUID, GridDhtPartitionMap> e : node2part.entrySet()) {
                if (!e.getValue().containsKey(p))
                    continue;

                if (e.getValue().get(p) == OWNING && !owners.contains(e.getKey()))
                    e.getValue().put(p, MOVING);
            }

            if (updateSeq)
                node2part = new GridDhtPartitionFullMap(node2part, this.updateSeq.incrementAndGet());
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * @param updateSeq Update sequence.
     * @return {@code True} if state changed.
     */
    private boolean checkEvictions(long updateSeq) {
        AffinityTopologyVersion affVer = grp.affinity().lastVersion();

        boolean changed = false;

        if (!affVer.equals(AffinityTopologyVersion.NONE) && affVer.compareTo(topVer) >= 0) {
            List<List<ClusterNode>> aff = grp.affinity().assignments(topVer);

            changed = checkEvictions(updateSeq, aff);

            updateRebalanceVersion(aff);
        }

        return changed;
    }

    /** {@inheritDoc} */
    @Override public void checkEvictions() {
        lock.writeLock().lock();

        try {
            long updateSeq = this.updateSeq.incrementAndGet();

            node2part.newUpdateSequence(updateSeq);

            checkEvictions(updateSeq);
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
        if (!ctx.kernalContext().state().active())
            return false;

        boolean changed = false;

        UUID locId = ctx.localNodeId();

        for (int p = 0; p < locParts.length(); p++) {
            GridDhtLocalPartition part = locParts.get(p);

            if (part == null)
                continue;

            GridDhtPartitionState state = part.state();

            if (state.active()) {
                List<ClusterNode> affNodes = aff.get(p);

                if (!affNodes.contains(ctx.localNode())) {
                    List<ClusterNode> nodes = nodes(p, topVer, OWNING);
                    Collection<UUID> nodeIds = F.nodeIds(nodes);

                    // If all affinity nodes are owners, then evict partition from local node.
                    if (nodeIds.containsAll(F.nodeIds(affNodes))) {
                        part.rent(false);

                        updateSeq = updateLocal(part.id(), part.state(), updateSeq);

                        changed = true;

                        if (log.isDebugEnabled())
                            log.debug("Evicted local partition (all affinity nodes are owners): " + part);
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
                                    part.rent(false);

                                    updateSeq = updateLocal(part.id(), part.state(), updateSeq);

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
     * @param state State.
     * @param updateSeq Update sequence.
     * @return Update sequence.
     */
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
    private long updateLocal(int p, GridDhtPartitionState state, long updateSeq) {
        ClusterNode oldest = discoCache.oldestAliveServerNodeWithCache();

        assert oldest != null || ctx.kernalContext().clientNode();

        // If this node became the oldest node.
        if (ctx.localNode().equals(oldest)) {
            long seq = node2part.updateSequence();

            if (seq != updateSeq) {
                if (seq > updateSeq) {
                    long seq0 = this.updateSeq.get();

                    if (seq0 < seq) {
                        // Update global counter if necessary.
                        boolean b = this.updateSeq.compareAndSet(seq0, seq + 1);

                        assert b : "Invalid update sequence [updateSeq=" + updateSeq +
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
                    topVer,
                    Collections.<Integer, GridDhtPartitionState>emptyMap(),
                    false);

                node2part.put(locNodeId, map);
            }

            map.updateSequence(updateSeq, topVer);

            map.put(p, state);

            Set<UUID> ids = part2node.get(p);

            if (ids == null)
                part2node.put(p, ids = U.newHashSet(3));

            ids.add(locNodeId);
        }

        return updateSeq;
    }

    /**
     * @param nodeId Node to remove.
     */
    private void removeNode(UUID nodeId) {
        assert nodeId != null;

        ClusterNode oldest = discoCache.oldestAliveServerNode();

        assert oldest != null || ctx.kernalContext().clientNode();

        ClusterNode loc = ctx.localNode();

        if (node2part != null) {
            if (loc.equals(oldest) && !node2part.nodeId().equals(loc.id())) {
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
        lock.writeLock().lock();

        try {
            if (part.own()) {
                updateLocal(part.id(), part.state(), updateSeq.incrementAndGet());

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

            updateLocal(part.id(), part.state(), seq);

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
    @Override public Map<Integer, T2<Long, Long>> updateCounters(boolean skipZeros) {
        lock.readLock().lock();

        try {
            Map<Integer, T2<Long, Long>> res;

            if (skipZeros) {
                res = U.newHashMap(cntrMap.size());

                for (Map.Entry<Integer, T2<Long, Long>> e : cntrMap.entrySet()) {
                    Long cntr = e.getValue().get2();

                    if (ZERO.equals(cntr))
                        continue;

                    res.put(e.getKey(), e.getValue());
                }
            }
            else
                res = new HashMap<>(cntrMap);

            for (int i = 0; i < locParts.length(); i++) {
                GridDhtLocalPartition part = locParts.get(i);

                if (part == null)
                    continue;

                T2<Long, Long> cntr0 = res.get(part.id());
                Long initCntr = part.initialUpdateCounter();

                if (cntr0 == null || initCntr >= cntr0.get1()) {
                    if (skipZeros && initCntr == 0L && part.updateCounter() == 0L)
                        continue;

                    res.put(part.id(), new T2<>(initCntr, part.updateCounter()));
                }
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
    @Override public boolean hasMovingPartitions() {
        lock.readLock().lock();

        try {
            assert node2part != null && node2part.valid() : "Invalid node2part [node2part: " + node2part +
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

                int size = part.dataStore().fullSize();

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
        if (!rebalancedTopVer.equals(topVer)) {
            if (node2part == null || !node2part.valid())
                return;

            for (int i = 0; i < grp.affinity().partitions(); i++) {
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
                log.debug("Updated rebalanced version [cache=" + grp.cacheOrGroupName() + ", ver=" + rebalancedTopVer + ']');
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
        if (CONSISTENCY_CHECK) {
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
