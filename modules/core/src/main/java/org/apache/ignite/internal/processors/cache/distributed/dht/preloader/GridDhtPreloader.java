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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCachePreloaderAdapter;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemander.RebalanceFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DISABLE_REBALANCING_CANCELLATION_OPTIMIZATION;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.MOVING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.RENTING;

/**
 * DHT cache preloader.
 */
public class GridDhtPreloader extends GridCachePreloaderAdapter {
    /** Default preload resend timeout. */
    public static final long DFLT_PRELOAD_RESEND_TIMEOUT = 1500;

    /** Disable rebalancing cancellation optimization. */
    private final boolean disableRebalancingCancellationOptimization = IgniteSystemProperties.getBoolean(
        IGNITE_DISABLE_REBALANCING_CANCELLATION_OPTIMIZATION);

    /** */
    private GridDhtPartitionTopology top;

    /** Partition suppliers. */
    private GridDhtPartitionSupplier supplier;

    /** Partition demanders. */
    private GridDhtPartitionDemander demander;

    /** Start future. */
    private GridFutureAdapter<Object> startFut;

    /** Busy lock to prevent activities from accessing exchanger while it's stopping. */
    private final ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /** */
    private boolean stopped;

    /**
     * @param grp Cache group.
     */
    public GridDhtPreloader(CacheGroupContext grp) {
        super(grp);

        top = grp.topology();

        startFut = new GridFutureAdapter<>();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        if (log.isDebugEnabled())
            log.debug("Starting DHT rebalancer...");

        supplier = new GridDhtPartitionSupplier(grp);
        demander = new GridDhtPartitionDemander(grp);

        demander.start();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        if (log.isDebugEnabled())
            log.debug("DHT rebalancer onKernalStop callback.");

        pause();

        try {
            if (supplier != null)
                supplier.stop();

            if (demander != null)
                demander.stop();

            top = null;

            stopped = true;
        }
        finally {
            resume();
        }
    }

    /**
     * @return Node stop exception.
     */
    private IgniteCheckedException stopError() {
        return new NodeStoppingException("Operation has been cancelled (cache or node is stopping).");
    }

    /**
     * @return Rebalance cancellation optimization flag.
     */
    public boolean disableRebalancingCancellationOptimization() {
        return disableRebalancingCancellationOptimization;
    }

    /** {@inheritDoc} */
    @Override public void onInitialExchangeComplete(@Nullable Throwable err) {
        if (err == null)
            startFut.onDone();
        else
            startFut.onDone(err);
    }

    /** {@inheritDoc} */
    @Override public void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {
        supplier.onTopologyChanged();

        demander.onTopologyChanged(lastFut);
    }

    /** {@inheritDoc} */
    @Override public boolean rebalanceRequired(GridDhtPartitionsExchangeFuture exchFut) {
        if (ctx.kernalContext().clientNode())
            return false; // No-op.

        AffinityTopologyVersion lastAffChangeTopVer =
            ctx.exchange().lastAffinityChangedTopologyVersion(exchFut.topologyVersion());

        return lastAffChangeTopVer.equals(exchFut.topologyVersion());
    }

    /** {@inheritDoc} */
    @Override public GridDhtPreloaderAssignments generateAssignments(
        GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture exchFut
    ) {
        assert exchFut == null || exchFut.isDone();

        // No assignments for disabled preloader.
        GridDhtPartitionTopology top = grp.topology();

        if (!grp.rebalanceEnabled())
            return new GridDhtPreloaderAssignments(exchId, top.readyTopologyVersion(), false);

        int partitions = grp.affinity().partitions();

        AffinityTopologyVersion topVer = top.readyTopologyVersion();

        assert exchFut == null || exchFut.context().events().topologyVersion().equals(top.readyTopologyVersion()) :
            "Topology version mismatch [exchId=" + exchId +
                ", grp=" + grp.name() +
                ", topVer=" + top.readyTopologyVersion() + ']';

        GridDhtPreloaderAssignments assignments = new GridDhtPreloaderAssignments(exchId, topVer,
            exchFut != null && exchFut.affinityReassign());

        AffinityAssignment aff = grp.affinity().cachedAffinity(topVer);

        CachePartitionFullCountersMap countersMap = grp.topology().fullUpdateCounters();

        for (int p = 0; p < partitions; p++) {
            if (ctx.exchange().hasPendingServerExchange()) {
                if (log.isDebugEnabled())
                    log.debug("Skipping assignments creation, exchange worker has pending assignments: " +
                        exchId);

                assignments.cancelled(true);

                return assignments;
            }

            // If partition belongs to local node.
            if (aff.get(p).contains(ctx.localNode())) {
                GridDhtLocalPartition part = top.localPartition(p);

                assert part != null;
                assert part.id() == p;

                // Do not rebalance OWNING or LOST partitions.
                if (part.state() == OWNING || part.state() == LOST)
                    continue;

                // If partition is currently rented prevent destroy and start clearing process.
                if (part.state() == RENTING) {
                    if (part.reserve()) {
                        part.moving();

                        part.clearAsync();

                        part.release();
                    }
                }

                // If partition was destroyed recreate it.
                if (part.state() == EVICTED) {
                    part.awaitDestroy();

                    part = top.localPartition(p, topVer, true);

                    assert part != null : "Partition was not created [grp=" + grp.name() + ", topVer=" + topVer + ", p=" + p + ']';

                    part.resetUpdateCounter();
                }

                if (part.state() != MOVING && part.state() != OWNING) {
                    throw new AssertionError("Partition has invalid state for rebalance "
                        + aff.topologyVersion() + " " + part);
                }

                ClusterNode histSupplier = null;

                if (grp.persistenceEnabled() && exchFut != null) {
                    List<UUID> nodeIds = exchFut.partitionHistorySupplier(grp.groupId(), p, part.initialUpdateCounter());

                    if (!F.isEmpty(nodeIds))
                        histSupplier = ctx.discovery().node(nodeIds.get(p % nodeIds.size()));
                }

                if (histSupplier != null && !exchFut.isClearingPartition(grp, p)) {
                    assert grp.persistenceEnabled();
                    assert remoteOwners(p, topVer).contains(histSupplier) : remoteOwners(p, topVer);

                    GridDhtPartitionDemandMessage msg = assignments.get(histSupplier);

                    if (msg == null) {
                        assignments.put(histSupplier, msg = new GridDhtPartitionDemandMessage(
                            top.updateSequence(),
                            assignments.topologyVersion(),
                            grp.groupId())
                        );
                    }

                    // TODO FIXME https://issues.apache.org/jira/browse/IGNITE-11790
                    msg.partitions().
                        addHistorical(p, part.initialUpdateCounter(), countersMap.updateCounter(p), partitions);
                }
                else {
                    List<ClusterNode> picked = remoteOwners(p, topVer);

                    if (!picked.isEmpty()) {
                        ClusterNode n = picked.get(p % picked.size());

                        GridDhtPartitionDemandMessage msg = assignments.get(n);

                        if (msg == null) {
                            assignments.put(n, msg = new GridDhtPartitionDemandMessage(
                                top.updateSequence(),
                                assignments.topologyVersion(),
                                grp.groupId()));
                        }

                        msg.partitions().addFull(p);
                    }
                }
            }
        }

        if (!assignments.isEmpty()) {
            ctx.database().lastCheckpointInapplicableForWalRebalance(grp.groupId());

            assert exchFut == null || !exchFut.rebalanced() :
                "Unexpected rebalance on rebalanced cluster " +
                    "[top=" + topVer + ", grp=" + grp.groupId() + ", assignments=" + assignments + "]";
        }

        return assignments;
    }

    /** {@inheritDoc} */
    @Override public void onReconnected() {
        startFut = new GridFutureAdapter<>();
    }

    /**
     * Returns remote owners (excluding local node) for specified partition {@code p}.
     *
     * @param p Partition.
     * @param topVer Topology version.
     * @return Nodes owning this partition.
     */
    private List<ClusterNode> remoteOwners(int p, AffinityTopologyVersion topVer) {
        List<ClusterNode> owners = grp.topology().owners(p, topVer);

        List<ClusterNode> res = new ArrayList<>(owners.size());

        for (ClusterNode owner : owners) {
            if (!owner.id().equals(ctx.localNodeId()))
                res.add(owner);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void handleSupplyMessage(UUID nodeId, final GridDhtPartitionSupplyMessage s) {
        demander.registerSupplyMessage(nodeId, s, () -> {
            if (!enterBusy())
                return;

            try {
                demander.handleSupplyMessage(nodeId, s);
            }
            finally {
                leaveBusy();
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void handleDemandMessage(int idx, UUID nodeId, GridDhtPartitionDemandMessage d) {
        ctx.kernalContext().getStripedRebalanceExecutorService().execute(() -> {
            if (!enterBusy())
                return;

            try {
                supplier.handleDemandMessage(idx, nodeId, d);
            }
            finally {
                leaveBusy();
            }
        }, Math.abs(nodeId.hashCode()));
    }

    /** {@inheritDoc} */
    @Override public RebalanceFuture addAssignments(
        GridDhtPreloaderAssignments assignments,
        boolean forceRebalance,
        long rebalanceId,
        final RebalanceFuture next,
        @Nullable GridCompoundFuture<Boolean, Boolean> forcedRebFut,
        GridCompoundFuture<Boolean, Boolean> compatibleRebFut
    ) {
        return demander.addAssignments(assignments, forceRebalance, rebalanceId, next, forcedRebFut, compatibleRebFut);
    }

    /**
     * @return Start future.
     */
    @Override public IgniteInternalFuture<Object> startFuture() {
        return startFut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> syncFuture() {
        return ctx.kernalContext().clientNode() ? startFut : demander.syncFuture();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> rebalanceFuture() {
        return ctx.kernalContext().clientNode() ? new GridFinishedFuture<>(true) : demander.rebalanceFuture();
    }

    /**
     * @return {@code true} if entered to busy state.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    private boolean enterBusy() {
        busyLock.readLock().lock();

        if (stopped) {
            busyLock.readLock().unlock();

            return false;
        }

        return true;
    }

    /**
     *
     */
    private void leaveBusy() {
        busyLock.readLock().unlock();
    }

    /**
     * Resends partitions on partition evict within configured timeout.
     *
     * @param part Evicted partition.
     * @param updateSeq Update sequence.
     */
    public void onPartitionEvicted(GridDhtLocalPartition part, boolean updateSeq) {
        if (!enterBusy())
            return;

        try {
            top.onEvicted(part, updateSeq);

            if (grp.eventRecordable(EVT_CACHE_REBALANCE_PART_UNLOADED))
                grp.addUnloadEvent(part.id());

            if (updateSeq) {
                if (log.isDebugEnabled())
                    log.debug("Partitions have been scheduled to resend [reason=" +
                        "Eviction [grp" + grp.cacheOrGroupName() + " " + part.id() + "]");

                ctx.exchange().scheduleResendPartitions();
            }
        }
        finally {
            leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean needForceKeys() {
        // Do not use force key request with enabled MVCC.
        if (grp.mvccEnabled())
            return false;

        if (grp.rebalanceEnabled()) {
            IgniteInternalFuture<Boolean> rebalanceFut = rebalanceFuture();

            if (rebalanceFut.isDone() && Boolean.TRUE.equals(rebalanceFut.result()))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public GridDhtFuture<Object> request(GridCacheContext cctx,
        GridNearAtomicAbstractUpdateRequest req,
        AffinityTopologyVersion topVer) {
        if (!needForceKeys())
            return null;

        return request0(cctx, req.keys(), topVer);
    }

    /**
     * @param keys Keys to request.
     * @return Future for request.
     */
    @Override public GridDhtFuture<Object> request(GridCacheContext cctx,
        Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer) {
        if (!needForceKeys())
            return null;

        return request0(cctx, keys, topVer);
    }

    /**
     * @param cctx Cache context.
     * @param keys Keys to request.
     * @param topVer Topology version.
     * @return Future for request.
     */
    @SuppressWarnings({"unchecked", "RedundantCast"})
    private GridDhtFuture<Object> request0(GridCacheContext cctx, Collection<KeyCacheObject> keys,
        AffinityTopologyVersion topVer) {
        if (cctx.isNear())
            cctx = cctx.near().dht().context();

        final GridDhtForceKeysFuture<?, ?> fut = new GridDhtForceKeysFuture<>(cctx, topVer, keys);

        IgniteInternalFuture<?> topReadyFut = cctx.affinity().affinityReadyFuturex(topVer);

        if (startFut.isDone() && topReadyFut == null)
            fut.init();
        else {
            if (topReadyFut == null)
                startFut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> syncFut) {
                        ctx.kernalContext().closure().runLocalSafe(
                            new GridPlainRunnable() {
                                @Override public void run() {
                                    fut.init();
                                }
                            });
                    }
                });
            else {
                GridCompoundFuture<Object, Object> compound = new GridCompoundFuture<>();

                compound.add((IgniteInternalFuture<Object>)startFut);
                compound.add((IgniteInternalFuture<Object>)topReadyFut);

                compound.markInitialized();

                compound.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> syncFut) {
                        fut.init();
                    }
                });
            }
        }

        return (GridDhtFuture)fut;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> forceRebalance() {
        return demander.forceRebalance();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override public void pause() {
        busyLock.writeLock().lock();
    }

    /** {@inheritDoc} */
    @Override public void resume() {
        busyLock.writeLock().unlock();
    }
}
