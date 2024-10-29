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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridReservable;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeAware;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.LOST;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PARTITIONS_RESERVE;

/**
 * Class responsible for partition reservation for queries executed on local node. Prevents partitions from being
 * evicted from node during query execution.
 */
public class PartitionReservationManager implements PartitionsExchangeAware {
    /** Special instance of reservable object for REPLICATED caches. */
    private static final ReplicatedReservable REPLICATED_RESERVABLE = new ReplicatedReservable();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /**
     * Group reservations cache. When affinity version is not changed and all primary partitions must be reserved we get
     * group reservation from this map instead of create new reservation group.
     */
    private final ConcurrentMap<PartitionReservationKey, GridReservable> reservations = new ConcurrentHashMap<>();

    /** Logger. */
    private final IgniteLogger log;

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public PartitionReservationManager(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(PartitionReservationManager.class);

        ctx.cache().context().exchange().registerExchangeAwareComponent(this);
    }

    /**
     * @param top Partition topology.
     * @param p Partition ID.
     * @return Partition.
     */
    private static GridDhtLocalPartition partition(GridDhtPartitionTopology top, int p) {
        return top.localPartition(p, NONE, false);
    }

    /**
     * @param cacheIds Cache IDs.
     * @param reqTopVer Topology version from request.
     * @param explicitParts Explicit partitions list.
     * @param nodeId Node ID.
     * @param reqId Request ID.
     * @return PartitionReservation instance with reservation result.
     * @throws IgniteCheckedException If failed.
     */
    public PartitionReservation reservePartitions(
        @Nullable List<Integer> cacheIds,
        AffinityTopologyVersion reqTopVer,
        final int[] explicitParts,
        UUID nodeId,
        long reqId
    ) throws IgniteCheckedException {
        try (TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_PARTITIONS_RESERVE, MTC.span()))) {
            assert reqTopVer != null;

            AffinityTopologyVersion topVer = ctx.cache().context().exchange().lastAffinityChangedTopologyVersion(reqTopVer);

            if (F.isEmpty(cacheIds))
                return new PartitionReservation(Collections.emptyList());

            Collection<Integer> partIds = partsToCollection(explicitParts);

            List<GridReservable> reserved = new ArrayList<>();

            for (int i = 0; i < cacheIds.size(); i++) {
                GridCacheContext<?, ?> cctx = ctx.cache().context().cacheContext(cacheIds.get(i));

                // Cache was not found, probably was not deployed yet.
                if (cctx == null) {
                    return new PartitionReservation(reserved,
                        String.format("Failed to reserve partitions for query (cache is not " +
                                "found on local node) [localNodeId=%s, rmtNodeId=%s, reqId=%s, affTopVer=%s, cacheId=%s]",
                            ctx.localNodeId(), nodeId, reqId, topVer, cacheIds.get(i)));
                }

                if (!cctx.rebalanceEnabled())
                    continue;

                String err = reservePartitions(reserved, cctx, partIds, topVer, nodeId, "reqId=" + reqId);

                if (err != null)
                    return new PartitionReservation(reserved, err);
            }

            return new PartitionReservation(reserved);
        }
    }

    /**
     * @param cctx Cache context.
     * @param reqTopVer Topology version from request.
     * @param explicitParts Explicit partitions list.
     * @param nodeId Node ID.
     * @param qryInfo Query info.
     * @return PartitionReservation instance with reservation result.
     * @throws IgniteCheckedException If failed.
     */
    public PartitionReservation reservePartitions(
        GridCacheContext<?, ?> cctx,
        AffinityTopologyVersion reqTopVer,
        final int[] explicitParts,
        UUID nodeId,
        String qryInfo
    ) throws IgniteCheckedException {
        try (TraceSurroundings ignored = MTC.support(ctx.tracing().create(SQL_PARTITIONS_RESERVE, MTC.span()))) {
            assert reqTopVer != null;

            AffinityTopologyVersion topVer = ctx.cache().context().exchange().lastAffinityChangedTopologyVersion(reqTopVer);

            Collection<Integer> partIds = partsToCollection(explicitParts);

            List<GridReservable> reserved = new ArrayList<>();

            String err = reservePartitions(reserved, cctx, partIds, topVer, nodeId, qryInfo);

            return new PartitionReservation(reserved, err);
        }
    }

    /**
     * @return Error message or {@code null}.
     */
    private @Nullable String reservePartitions(
        List<GridReservable> reserved,
        GridCacheContext<?, ?> cctx,
        Collection<Integer> partIds,
        AffinityTopologyVersion topVer,
        UUID nodeId,
        String qryInfo
    ) throws IgniteCheckedException {
        // For replicated cache topology version does not make sense.
        final PartitionReservationKey grpKey = new PartitionReservationKey(cctx.name(), cctx.isReplicated() ? null : topVer);

        GridReservable r = reservations.get(grpKey);

        if (partIds == null && r != null)  // Try to reserve group partition if any and no explicits.
            return groupPartitionReservation(reserved, r, cctx, topVer, nodeId, qryInfo);
        else { // Try to reserve partitions one by one.
            int partsCnt = cctx.affinity().partitions();

            if (cctx.isReplicated()) { // Check all the partitions are in owning state for replicated cache.
                if (r == null) { // Check only once.
                    GridDhtPartitionTopology top = cctx.topology();

                    top.readLock();

                    try {
                        for (int p = 0; p < partsCnt; p++) {
                            GridDhtLocalPartition part = partition(top, p);

                            // We don't need to reserve partitions because they will not be evicted in replicated caches.
                            GridDhtPartitionState partState = part != null ? part.state() : null;

                            if (partState != OWNING) {
                                return String.format("Failed to reserve partitions for " +
                                    "query (partition of REPLICATED cache is not in OWNING state) [" +
                                    "localNodeId=%s, rmtNodeId=%s, %s, affTopVer=%s, cacheId=%s, " +
                                    "cacheName=%s, part=%s, partFound=%s, partState=%s]",
                                    ctx.localNodeId(),
                                    nodeId,
                                    qryInfo,
                                    topVer,
                                    cctx.cacheId(),
                                    cctx.name(),
                                    p,
                                    (part != null),
                                    partState
                                );
                            }
                        }
                    }
                    finally {
                        top.readUnlock();
                    }

                    // Mark that we checked this replicated cache.
                    reservations.putIfAbsent(grpKey, REPLICATED_RESERVABLE);

                    MTC.span().addLog(() -> "Cache partitions were reserved [cache=" + cctx.name() +
                        ", partitions=[0.." + partsCnt + ']');
                }
            }
            else { // Reserve primary partitions for partitioned cache (if no explicit given).
                Collection<Integer> partIds0 = partIds != null ? partIds
                    : cctx.affinity().primaryPartitions(ctx.localNodeId(), topVer);

                int reservedCnt = 0;

                GridDhtPartitionTopology top = cctx.topology();

                top.readLock();

                try {
                    for (int partId : partIds0) {
                        GridDhtLocalPartition part = partition(top, partId);

                        GridDhtPartitionState partState = part != null ? part.state() : null;

                        if (partState != OWNING) {
                            if (partState == LOST) {
                                reserved.forEach(GridReservable::release);

                                failQueryOnLostData(cctx, part);
                            }
                            else {
                                return String.format("Failed to reserve partitions " +
                                    "for query (partition of PARTITIONED cache is not found or not in OWNING " +
                                    "state) [localNodeId=%s, rmtNodeId=%s, %s, affTopVer=%s, cacheId=%s, " +
                                    "cacheName=%s, part=%s, partFound=%s, partState=%s]",
                                    ctx.localNodeId(),
                                    nodeId,
                                    qryInfo,
                                    topVer,
                                    cctx.cacheId(),
                                    cctx.name(),
                                    partId,
                                    (part != null),
                                    partState
                                );
                            }
                        }

                        if (!part.reserve()) {
                            return String.format("Failed to reserve partitions for query " +
                                "(partition of PARTITIONED cache cannot be reserved) [" +
                                "localNodeId=%s, rmtNodeId=%s, %s, affTopVer=%s, cacheId=%s, " +
                                "cacheName=%s, part=%s, partFound=%s, partState=%s]",
                                ctx.localNodeId(),
                                nodeId,
                                qryInfo,
                                topVer,
                                cctx.cacheId(),
                                cctx.name(),
                                partId,
                                true,
                                partState
                            );
                        }

                        reserved.add(part);

                        reservedCnt++;
                    }
                }
                finally {
                    top.readUnlock();
                }

                MTC.span().addLog(() -> "Cache partitions were reserved [cache=" + cctx.name() +
                    ", partitions=" + partIds0 + ", topology=" + topVer + ']');

                if (partIds == null && reservedCnt > 0) {
                    // We reserved all the primary partitions for cache, attempt to add group reservation.
                    GridDhtPartitionsReservation grp = new GridDhtPartitionsReservation(topVer, cctx, "SQL");

                    synchronized (this) {
                        // Double check under lock.
                        GridReservable grpReservation = reservations.get(grpKey);

                        if (grpReservation != null)
                            return groupPartitionReservation(reserved, grpReservation, cctx, topVer, nodeId, qryInfo);
                        else {
                            if (grp.register(reserved.subList(reserved.size() - reservedCnt, reserved.size()))) {
                                reservations.put(grpKey, grp);

                                grp.onPublish(new CI1<>() {
                                    @Override public void apply(GridDhtPartitionsReservation r) {
                                        reservations.remove(grpKey, r);
                                    }
                                });
                            }
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * @param cacheName Cache name.
     */
    public void onCacheStop(String cacheName) {
        // Drop group reservations.
        for (PartitionReservationKey grpKey : reservations.keySet()) {
            if (F.eq(grpKey.cacheName(), cacheName))
                reservations.remove(grpKey);
        }
    }

    /**
     * @param cctx Cache context.
     * @param part Partition.
     */
    private static void failQueryOnLostData(GridCacheContext<?, ?> cctx, GridDhtLocalPartition part)
        throws IgniteCheckedException {
        throw new CacheInvalidStateException("Failed to execute query because cache partition has been " +
            "lost [cacheName=" + cctx.name() + ", part=" + part + ']');
    }

    /**
     * Cleanup group reservations cache on change affinity version.
     */
    @Override public void onDoneAfterTopologyUnlock(final GridDhtPartitionsExchangeFuture fut) {
        try {
            // Must not do anything at the exchange thread. Dispatch to the management thread pool.
            ctx.closure().runLocal(
                new GridPlainRunnable() {
                    @Override public void run() {
                        AffinityTopologyVersion topVer = ctx.cache().context().exchange()
                            .lastAffinityChangedTopologyVersion(fut.topologyVersion());

                        reservations.forEach((key, r) -> {
                            if (r != REPLICATED_RESERVABLE && !F.eq(key.topologyVersion(), topVer)) {
                                assert r instanceof GridDhtPartitionsReservation;

                                ((GridDhtPartitionsReservation)r).invalidate();
                            }
                        });
                    }
                },
                GridIoPolicy.MANAGEMENT_POOL);
        }
        catch (Throwable e) {
            log.error("Unexpected exception on start reservations cleanup.");
            ctx.failure().process(new FailureContext(CRITICAL_ERROR, e));
        }
    }

    /** */
    private static Collection<Integer> partsToCollection(int[] explicitParts) {
        if (explicitParts == null)
            return null;
        else if (explicitParts.length == 0)
            return Collections.emptyList();
        else {
            List<Integer> partIds = new ArrayList<>(explicitParts.length);

            for (int explicitPart : explicitParts)
                partIds.add(explicitPart);

            return partIds;
        }
    }

    /** */
    private String groupPartitionReservation(
        List<GridReservable> reserved,
        GridReservable grpReservation,
        GridCacheContext<?, ?> cctx,
        AffinityTopologyVersion topVer,
        UUID nodeId,
        String qryInfo
    ) {
        if (grpReservation != REPLICATED_RESERVABLE) {
            if (!grpReservation.reserve()) {
                return String.format("Failed to reserve partitions for query (group " +
                    "reservation failed) [localNodeId=%s, rmtNodeId=%s, %s, affTopVer=%s, cacheId=%s, " +
                    "cacheName=%s]", ctx.localNodeId(), nodeId, qryInfo, topVer, cctx.cacheId(), cctx.name());
            }

            reserved.add(grpReservation);

            MTC.span().addLog(() -> "Cache partitions were reserved " + grpReservation);
        }

        return null;
    }

    /**
     * Mapper fake reservation object for replicated caches.
     */
    private static class ReplicatedReservable implements GridReservable {
        /** {@inheritDoc} */
        @Override public boolean reserve() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override public void release() {
            throw new IllegalStateException();
        }
    }
}
