/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.management.cache.IdleVerifyResultV2;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.CHECK_SNAPSHOT_PARTS;

/** Distributed process of snapshot checking (with the partition hashes). */
public class SnapshotCheckProcess {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext kctx;

    /** Operation contexts by id. */
    private final Map<String, SnapshotCheckContext> contexts = new ConcurrentHashMap<>();

    /** Cluster-wide operation futures per snapshot called from current node. */
    private final Map<UUID, GridFutureAdapter<SnapshotPartitionsVerifyTaskResult>> clusterOpFuts = new ConcurrentHashMap<>();

    /** Check metas first phase subprocess. */
    private final DistributedProcess<SnapshotCheckProcessRequest, SnapshotCheckResponse> phase1CheckMetas;

    /** Partition hashes second phase subprocess.  */
    private final DistributedProcess<SnapshotCheckProcessRequest, SnapshotCheckResponse> phase2PartsHashes;

    /** Stop node lock. */
    private boolean nodeStopping;

    /** */
    public SnapshotCheckProcess(GridKernalContext kctx) {
        this.kctx = kctx;

        log = kctx.log(getClass());

        phase1CheckMetas = new DistributedProcess<>(kctx, CHECK_SNAPSHOT_METAS, this::prepareAndCheckMetas,
            this::reducePreparationAndMetasCheck);

        phase2PartsHashes = new DistributedProcess<>(kctx, CHECK_SNAPSHOT_PARTS, this::validateParts,
            this::reduceValidatePartsAndFinish);

        kctx.event().addLocalEventListener(evt -> {
            UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

            Throwable err = new ClusterTopologyCheckedException("Snapshot validation stopped. A required node left " +
                "the cluster [nodeId=" + nodeId + ']');

            contexts.values().forEach(ctx -> {
                if (ctx.req.nodes().contains(nodeId)) {
                    ctx.locProcFut.onDone(err);

                    // We have no a guaranty that a node-left-event is processed strictly before the 1st phase reduce which
                    // can handle this error.
                    GridFutureAdapter<?> clusterFut = clusterOpFuts.get(ctx.req.requestId());

                    if (clusterFut != null)
                        clusterFut.onDone(err);
                }
            });
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /**
     * Stops all the processes with the passed exception.
     *
     * @param err The interrupt reason.
     */
    void interrupt(Throwable err) {
        // Prevents starting new processes in #prepareAndCheckMetas.
        synchronized (contexts) {
            nodeStopping = true;
        }

        contexts.forEach((snpName, ctx) -> ctx.locProcFut.onDone(err));

        clusterOpFuts.forEach((reqId, fut) -> fut.onDone(err));
    }

    /** Phase 2 and process finish. */
    private IgniteInternalFuture<?> reduceValidatePartsAndFinish(
        UUID reqId,
        Map<UUID, SnapshotCheckResponse> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckContext ctx = context(null, reqId);

        if (ctx == null)
            return new GridFinishedFuture<>();

        contexts.remove(contextId(ctx.req));

        if (log.isInfoEnabled())
            log.info("Finished snapshot validation [req=" + ctx.req + ']');

        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.get(reqId);

        if (clusterOpFut == null)
            return new GridFinishedFuture<>();

        assert results.values().stream().noneMatch(res -> res != null && res.metas != null);

        SnapshotChecker checker = kctx.cache().context().snapshotMgr().checker();

        if (ctx.req.incrementalIdx() > 0) {
            IdleVerifyResultV2 chkRes = checker.reduceIncrementalResults(
                mapResults(results, ctx.req.nodes(), SnapshotCheckResponse::incrementalResult),
                mapErrors(errors)
            );

            clusterOpFut.onDone(new SnapshotPartitionsVerifyTaskResult(ctx.clusterMetas, chkRes));
        }
        else if (ctx.req.allRestoreHandlers()) {
            try {
                if (!errors.isEmpty())
                    throw F.firstValue(errors);

                Map<ClusterNode, Map<String, SnapshotHandlerResult<?>>> cstRes = mapResults(results, ctx.req.nodes(),
                    SnapshotCheckResponse::customHandlersResults);

                checker.checkCustomHandlersResults(ctx.req.snapshotName(), cstRes);

                clusterOpFut.onDone(new SnapshotPartitionsVerifyTaskResult(ctx.clusterMetas, null));
            }
            catch (Throwable err) {
                clusterOpFut.onDone(err);
            }
        }
        else {
            Map<ClusterNode, Exception> errors0 = mapErrors(errors);

            if (!results.isEmpty()) {
                Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> results0 = mapResults(results, ctx.req.nodes(),
                    SnapshotCheckResponse::partsHashes);

                IdleVerifyResultV2 chkRes = SnapshotChecker.reduceHashesResults(results0, errors0);

                clusterOpFut.onDone(new SnapshotPartitionsVerifyTaskResult(ctx.clusterMetas, chkRes));
            }
            else
                clusterOpFut.onDone(new IgniteSnapshotVerifyException(errors0));
        }

        return new GridFinishedFuture<>();
    }

    /** Phase 2 beginning.  */
    private IgniteInternalFuture<SnapshotCheckResponse> validateParts(SnapshotCheckProcessRequest req) {
        if (!req.nodes().contains(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        SnapshotCheckContext ctx = context(contextId(req), req.requestId());

        assert ctx != null;

        if (ctx.locMeta == null)
            return new GridFinishedFuture<>();

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        GridFutureAdapter<SnapshotCheckResponse> phaseFut = ctx.phaseFuture();

        // Might be already finished by asynchronous leave of a required node.
        if (!phaseFut.isDone()) {
            CompletableFuture<?> workingFut;

            if (req.incrementalIdx() > 0) {
                assert !req.allRestoreHandlers() : "Snapshot handlers aren't supported for incremental snapshot.";

                workingFut = snpMgr.checker().checkIncrementalSnapshot(req.snapshotName(), req.snapshotPath(), req.incrementalIdx());
            }
            else {
                workingFut = req.allRestoreHandlers()
                    ? snpMgr.checker().invokeCustomHandlers(ctx.locMeta, req.snapshotPath(), req.groups(), true)
                    : snpMgr.checker().checkPartitions(ctx.locMeta, snpMgr.snapshotLocalDir(req.snapshotName(), req.snapshotPath()),
                    req.groups(), false, true, false);
            }

            workingFut.whenComplete((res, err) -> {
                if (err != null)
                    phaseFut.onDone(err);
                else {
                    if (req.incrementalIdx() > 0)
                        phaseFut.onDone(new SnapshotCheckResponse((SnapshotChecker.IncrementalSnapshotResult)res));
                    else
                        phaseFut.onDone(new SnapshotCheckResponse((Map<?, ?>)res));
                }
            });
        }

        return phaseFut;
    }

    /** */
    private Map<ClusterNode, Exception> mapErrors(Map<UUID, Throwable> errors) {
        return errors.entrySet().stream()
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()),
                e -> e.getValue() instanceof Exception ? (Exception)e.getValue() : new IgniteException(e.getValue())));
    }

    /** */
    private <T> Map<ClusterNode, T> mapResults(
        Map<UUID, SnapshotCheckResponse> results,
        Set<UUID> requiredNodes,
        Function<SnapshotCheckResponse, T> resExtractor
    ) {
        return results.entrySet().stream()
            .filter(e -> requiredNodes.contains(e.getKey()) && e.getValue() != null)
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> resExtractor.apply(e.getValue())));
    }

    /**
     * @param ctxId Context id. If {@code null}, ignored.
     * @param reqId If {@code ctxId} is {@code null}, is used to find the operation context.
     * @return Current snapshot checking context by {@code ctxId} or {@code reqId}.
     */
    private @Nullable SnapshotCheckContext context(@Nullable String ctxId, UUID reqId) {
        return ctxId == null
            ? contexts.values().stream().filter(ctx0 -> ctx0.req.requestId().equals(reqId)).findFirst().orElse(null)
            : contexts.get(ctxId);
    }

    /** Phase 1 beginning: prepare, collect and check local metas. */
    private IgniteInternalFuture<SnapshotCheckResponse> prepareAndCheckMetas(SnapshotCheckProcessRequest req) {
        if (!req.nodes().contains(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        SnapshotCheckContext ctx;

        // Sync. with stopping in #interrupt.
        synchronized (contexts) {
            if (nodeStopping)
                return new GridFinishedFuture<>(new NodeStoppingException("The node is stopping: " + kctx.localNodeId()));

            ctx = contexts.computeIfAbsent(contextId(req), snpName -> new SnapshotCheckContext(req));
        }

        if (!ctx.req.requestId().equals(req.requestId())) {
            return new GridFinishedFuture<>(new IllegalStateException("Validation of snapshot '" + req.snapshotName()
                + "' has already started. Request=" + ctx + '.'));
        }

        // Excludes non-baseline initiator.
        if (!baseline(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        Collection<Integer> grpIds = F.isEmpty(req.groups()) ? null : F.viewReadOnly(req.groups(), CU::cacheId);

        GridFutureAdapter<SnapshotCheckResponse> phaseFut = ctx.phaseFuture();

        // Might be already finished by asynchronous leave of a required node.
        if (!phaseFut.isDone()) {
            snpMgr.checker().checkLocalMetas(
                snpMgr.snapshotLocalDir(req.snapshotName(), req.snapshotPath()),
                req.incrementalIdx(),
                grpIds,
                kctx.cluster().get().localNode().consistentId()
            ).whenComplete((locMetas, err) -> {
                if (err != null)
                    phaseFut.onDone(err);
                else
                    phaseFut.onDone(new SnapshotCheckResponse(locMetas));
            });
        }

        return phaseFut;
    }

    /** Phase 1 end. */
    private void reducePreparationAndMetasCheck(
        UUID reqId,
        Map<UUID, SnapshotCheckResponse> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckContext ctx = context(null, reqId);

        // The context is not stored in the case of concurrent check of the same snapshot but the operation future is registered.
        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.get(reqId);

        try {
            if (!errors.isEmpty())
                throw new IgniteSnapshotVerifyException(mapErrors(errors));

            if (ctx == null) {
                assert clusterOpFut == null;

                return;
            }

            if (ctx.locProcFut.error() != null)
                throw ctx.locProcFut.error();

            Map<ClusterNode, List<SnapshotMetadata>> metas = new HashMap<>();

            results.forEach((nodeId, nodeRes) -> {
                // A node might be not required. It gives null result. But a required node might have invalid empty result
                // which must be validated.
                if (ctx.req.nodes().contains(nodeId) && baseline(nodeId)) {
                    assert nodeRes != null && nodeRes.partsResults == null;

                    metas.put(kctx.cluster().get().node(nodeId), nodeRes.metas);
                }
            });

            Map<ClusterNode, Exception> metasCheck = SnapshotChecker.reduceMetasResults(ctx.req.snapshotName(), ctx.req.snapshotPath(),
                metas, null, kctx.cluster().get().localNode().consistentId());

            if (!metasCheck.isEmpty())
                throw new IgniteSnapshotVerifyException(metasCheck);

            List<SnapshotMetadata> locMetas = metas.get(kctx.cluster().get().localNode());

            ctx.locMeta = F.isEmpty(locMetas) ? null : locMetas.get(0);

            if (clusterOpFut != null)
                ctx.clusterMetas = metas;

            if (U.isLocalNodeCoordinator(kctx.discovery()))
                phase2PartsHashes.start(reqId, ctx.req);
        }
        catch (Throwable th) {
            if (ctx != null) {
                contexts.remove(contextId(ctx.req));

                if (log.isInfoEnabled())
                    log.info("Finished snapshot validation [req=" + ctx.req + ']');
            }

            if (clusterOpFut != null)
                clusterOpFut.onDone(th);
        }
    }

    /** @return Context id depending on request type and snapshot name. */
    private static String contextId(SnapshotCheckProcessRequest req) {
        return req.incrementalIdx() > 0 ? req.snapshotName() + "_inc_" + req.incrementalIdx() : req.snapshotName();
    }

    /**
     * Starts the snapshot validation process.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grpNames List of cache group names.
     * @param incIdx Incremental snapshot index.
     * @param allRestoreHandlers If {@code true}, all the registered {@link IgniteSnapshotManager#handlers()} of type
     *                    {@link SnapshotHandlerType#RESTORE} are invoked. Otherwise, only snapshot metadatas and partition
     *                    hashes are validated.
     */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> start(
        String snpName,
        @Nullable String snpPath,
        @Nullable Collection<String> grpNames,
        int incIdx,
        boolean allRestoreHandlers
    ) {
        assert !F.isEmpty(snpName);

        UUID reqId = UUID.randomUUID();

        Set<UUID> requiredNodes = new HashSet<>(F.viewReadOnly(kctx.discovery().discoCache().aliveBaselineNodes(), F.node2id()));

        // Initiator is also a required node. It collects the final oparation result.
        requiredNodes.add(kctx.localNodeId());

        SnapshotCheckProcessRequest req = new SnapshotCheckProcessRequest(
            reqId,
            requiredNodes,
            snpName,
            snpPath,
            grpNames,
            incIdx,
            allRestoreHandlers
        );

        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = new GridFutureAdapter<>();

        clusterOpFut.listen(fut -> {
            clusterOpFuts.remove(reqId);

            if (log.isInfoEnabled())
                log.info("Finished snapshot validation process [req=" + req + ']');
        });

        clusterOpFuts.put(reqId, clusterOpFut);

        phase1CheckMetas.start(req.requestId(), req);

        return clusterOpFut;
    }

    /** @return {@code True} if node with the provided id is in the cluster and is a baseline node. {@code False} otherwise. */
    private boolean baseline(UUID nodeId) {
        ClusterNode node = kctx.cluster().get().node(nodeId);

        return node != null && CU.baselineNode(node, kctx.state().clusterState());
    }

    /** Operation context. */
    private static final class SnapshotCheckContext {
        /** Request. */
        private final SnapshotCheckProcessRequest req;

        /** Current process' future. Listens error, stop requests, etc. */
        private final GridFutureAdapter<SnapshotCheckResponse> locProcFut = new GridFutureAdapter<>();

        /** Local snapshot metadata. */
        @Nullable private SnapshotMetadata locMeta;

        /** All the snapshot metadatas. */
        @Nullable private Map<ClusterNode, List<SnapshotMetadata>> clusterMetas;

        /** Creates operation context. */
        private SnapshotCheckContext(SnapshotCheckProcessRequest req) {
            this.req = req;
        }

        /** Gives a future for current process phase. The future can be stopped by asynchronous leave of a required node. */
        private <T> GridFutureAdapter<T> phaseFuture() {
            GridFutureAdapter<T> fut = new GridFutureAdapter<>();

            locProcFut.listen(f -> fut.onDone(f.error()));

            return fut;
        }
    }

    /** A DTO used to transfer nodes' results for the both phases. */
    private static final class SnapshotCheckResponse implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Metas for the phase 1. Is always {@code null} for the phase 2. */
        @Nullable private final List<SnapshotMetadata> metas;

        /**
         * Node's partition hashes for the phase 2. Is always {@code null} for the phase 1 or in case of incremental
         * snapshot.
         *
         * @see #partsHashes()
         * @see #customHandlersResults()
         */
        @Nullable private final Map<?, ?> partsResults;

        /**
         * Incremental snapshot result for the phase 2. Is always {@code null} for the phase 1 or in case of normal snapshot.
         *
         * @see #incrementalResult()
         */
        @Nullable private final SnapshotChecker.IncrementalSnapshotResult incRes;

        /** Ctor for the phase 1. */
        private SnapshotCheckResponse(List<SnapshotMetadata> metas) {
            this.metas = metas;
            this.partsResults = null;
            this.incRes = null;
        }

        /** Ctor for the phase 2 for normal snapshot. */
        private SnapshotCheckResponse(Map<?, ?> partsResults) {
            this.metas = null;
            this.partsResults = partsResults;
            this.incRes = null;
        }

        /** Ctor for the phase 2 for incremental snapshot. */
        private SnapshotCheckResponse(SnapshotChecker.IncrementalSnapshotResult incRes) {
            this.metas = null;
            this.partsResults = null;
            this.incRes = incRes;
        }

        /** */
        private @Nullable Map<PartitionKeyV2, PartitionHashRecordV2> partsHashes() {
            return (Map<PartitionKeyV2, PartitionHashRecordV2>)partsResults;
        }

        /** */
        private @Nullable Map<String, SnapshotHandlerResult<?>> customHandlersResults() {
            return (Map<String, SnapshotHandlerResult<?>>)partsResults;
        }

        /** */
        private @Nullable SnapshotChecker.IncrementalSnapshotResult incrementalResult() {
            return incRes;
        }
    }
}
