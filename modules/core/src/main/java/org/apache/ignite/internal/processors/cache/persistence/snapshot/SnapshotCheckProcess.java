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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
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

    /** Operation contexts by name. */
    private final Map<String, SnapshotCheckContext> contexts = new ConcurrentHashMap<>();

    /** Cluster-wide operation futures per snapshot called from current node. */
    private final Map<UUID, GridFutureAdapter<SnapshotPartitionsVerifyResult>> clusterOpFuts = new ConcurrentHashMap<>();

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

        contexts.remove(ctx.req.snapshotName());

        if (log.isInfoEnabled())
            log.info("Finished snapshot validation [req=" + ctx.req + ']');

        GridFutureAdapter<SnapshotPartitionsVerifyResult> clusterOpFut = clusterOpFuts.get(reqId);

        if (clusterOpFut == null)
            return new GridFinishedFuture<>();

        assert results.values().stream().noneMatch(res -> res != null && res.metas != null);

        SnapshotChecker checker = kctx.cache().context().snapshotMgr().checker();

        if (ctx.req.incrementalIndex() > 0) {
            IdleVerifyResultV2 chkRes = checker.reduceIncrementalResults(
                mapResults(results, ctx.req.nodes(), SnapshotCheckResponse::incrementalResult),
                mapErrors(errors)
            );

            clusterOpFut.onDone(new SnapshotPartitionsVerifyResult(ctx.clusterMetas, chkRes));
        }
        else if (ctx.req.allRestoreHandlers()) {
            try {
                if (!errors.isEmpty())
                    throw F.firstValue(errors);

                // Check responses: node -> consistentId -> handler name -> handler result.
                Map<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> cstRes = new HashMap<>();

                for (Map.Entry<UUID, SnapshotCheckResponse> respE : results.entrySet()) {
                    SnapshotCheckResponse resp = respE.getValue();

                    if (resp == null)
                        break;

                    if (!F.isEmpty(resp.exceptions()))
                        throw F.firstValue(resp.exceptions());

                    resp.customHandlersResults().forEach((consId, hndResMap) -> {
                        Map<Object, Map<String, SnapshotHandlerResult<?>>> nodePerConsIdRes
                            = cstRes.computeIfAbsent(kctx.cluster().get().localNode(), n -> new HashMap<>());

                        hndResMap.forEach((hndId, hndRes) ->
                            nodePerConsIdRes.computeIfAbsent(consId, cstId -> new HashMap<>()).put(hndId, hndRes));
                    });
                }

                checker.checkCustomHandlersResults(ctx.req.snapshotName(), cstRes);

                clusterOpFut.onDone(new SnapshotPartitionsVerifyResult(ctx.clusterMetas, null));
            }
            catch (Throwable err) {
                clusterOpFut.onDone(err);
            }
        }
        else {
            Map<ClusterNode, Exception> errors0 = mapErrors(errors);

            if (!results.isEmpty()) {
                Map<ClusterNode, Map<PartitionKeyV2, List<PartitionHashRecordV2>>> results0 = new HashMap<>();

                for (Map.Entry<UUID, SnapshotCheckResponse> respE : results.entrySet()) {
                    UUID nodeId = respE.getKey();
                    SnapshotCheckResponse resp = respE.getValue();

                    if (resp == null)
                        break;

                    if (!F.isEmpty(resp.exceptions()))
                        errors0.putIfAbsent(kctx.cluster().get().node(nodeId), asException(F.firstValue(resp.exceptions())));

                    resp.partsHashes().forEach((consId, partsRes) -> {
                        Map<PartitionKeyV2, List<PartitionHashRecordV2>> partsHashes
                            = results0.computeIfAbsent(kctx.cluster().get().localNode(), map -> new HashMap<>());

                        partsRes.forEach((partKey, partHash) -> partsHashes.computeIfAbsent(partKey, k -> new ArrayList<>())
                            .add(partHash));
                    });
                }

                IdleVerifyResultV2 chkRes = SnapshotChecker.reduceHashesResults(results0, errors0);

                clusterOpFut.onDone(new SnapshotPartitionsVerifyResult(ctx.clusterMetas, chkRes));
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

        SnapshotCheckContext ctx = context(req.snapshotName(), req.requestId());

        assert ctx != null;

        if (F.isEmpty(ctx.metasToProc))
            return new GridFinishedFuture<>();

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        GridFutureAdapter<SnapshotCheckResponse> phaseFut = ctx.phaseFuture();

        // Might be already finished by asynchronous leave of a required node.
        if (!phaseFut.isDone()) {
            CompletableFuture workingFut;

            if (req.incrementalIndex() > 0) {
                assert !req.allRestoreHandlers() : "Snapshot handlers aren't supported for incremental snapshot.";

                workingFut = snpMgr.checker().checkIncrementalSnapshot(req.snapshotName(), req.snapshotPath(), req.incrementalIndex());
            }
            else
                workingFut = validatePartitionsFuture(ctx);

            workingFut.whenComplete((res, err) -> {
                if (err != null)
                    phaseFut.onDone((Throwable)err);
                else {
                    if (req.incrementalIndex() > 0)
                        phaseFut.onDone(new SnapshotCheckResponse((IncrementalSnapshotCheckResult)res));
                    else
                        phaseFut.onDone(new SnapshotCheckResponse((Map)res, req.incrementalIndex() > 0));
                }
            });
        }

        return phaseFut;
    }

    /** @return Composed partitions validating future regarding {@link SnapshotCheckContext#metasToProc}. */
    private CompletableFuture<Map<String, Object>> validatePartitionsFuture(SnapshotCheckContext ctx) {
        if (F.isEmpty(ctx.metasToProc))
            return CompletableFuture.completedFuture(null);

        // Per metas result: consistent id -> check result or an exception.
        Map<String, Object> perMetaResults = new ConcurrentHashMap<>(ctx.metasToProc.size(), 1.0f);

        CompletableFuture<Map<String, Object>> composedFut = new CompletableFuture<>();
        AtomicInteger metasProcessed = new AtomicInteger(ctx.metasToProc.size());

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();
        SnapshotCheckProcessRequest req = ctx.req;

        for (SnapshotMetadata locMeta : ctx.metasToProc) {
            CompletableFuture<?> metaFut;

            if (req.allRestoreHandlers())
                metaFut = snpMgr.checker().invokeCustomHandlers(locMeta, req.snapshotPath(), req.groups(), true);
            else {
                metaFut = snpMgr.checker().checkPartitions(
                    locMeta,
                    snpMgr.snapshotLocalDir(req.snapshotName(), req.snapshotPath()),
                    req.groups(),
                    false,
                    req.fullCheck(),
                    false
                );
            }

            metaFut.whenComplete((res, err) -> {
                if (err != null)
                    perMetaResults.put(locMeta.consistentId(), err);
                else if (req.allRestoreHandlers()) {
                    Map<String, SnapshotHandlerResult<Object>> hndRes = (Map<String, SnapshotHandlerResult<Object>>)res;

                    if (!F.isEmpty(hndRes))
                        perMetaResults.put(F.first(hndRes.values()).node().consistentId().toString(), hndRes);
                }
                else {
                    Map<PartitionKeyV2, PartitionHashRecordV2> partRes = (Map<PartitionKeyV2, PartitionHashRecordV2>)res;

                    if (!F.isEmpty(partRes))
                        perMetaResults.putIfAbsent(F.first(partRes.values()).consistentId().toString(), partRes);
                }

                if (metasProcessed.decrementAndGet() == 0)
                    composedFut.complete(perMetaResults);
            });
        }

        return composedFut;
    }

    /** */
    private Map<ClusterNode, Exception> mapErrors(Map<UUID, Throwable> errors) {
        return errors.entrySet().stream()
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> asException(e.getValue())));
    }

    /** */
    private static Exception asException(Throwable th) {
        return th instanceof Exception ? (Exception)th : new IgniteException(th);
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
     * @param snpName Snapshot name. If {@code null}, ignored.
     * @param reqId If {@code ctxId} is {@code null}, is used to find the operation context.
     * @return Current snapshot checking context by {@code ctxId} or {@code reqId}.
     */
    private @Nullable SnapshotCheckContext context(@Nullable String snpName, UUID reqId) {
        return snpName == null
            ? contexts.values().stream().filter(ctx0 -> ctx0.req.requestId().equals(reqId)).findFirst().orElse(null)
            : contexts.get(snpName);
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

            ctx = contexts.computeIfAbsent(req.snapshotName(), snpName -> new SnapshotCheckContext(req));
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
                req.incrementalIndex(),
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
        GridFutureAdapter<SnapshotPartitionsVerifyResult> clusterOpFut = clusterOpFuts.get(reqId);

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
                if (ctx.req.nodes().contains(nodeId) && baseline(nodeId) && !F.isEmpty(nodeRes.metas)) {
                    assert nodeRes != null && nodeRes.partsResults == null;

                    metas.put(kctx.cluster().get().node(nodeId), nodeRes.metas);
                }
            });

            Map<ClusterNode, Exception> metasCheck = SnapshotChecker.reduceMetasResults(ctx.req.snapshotName(), ctx.req.snapshotPath(),
                metas, null, kctx.cluster().get().localNode().consistentId());

            if (!metasCheck.isEmpty())
                throw new IgniteSnapshotVerifyException(metasCheck);

            // If the topology is lesser that the snapshot's, we have to check partitions not only of current node.
            ctx.metasToProc = assingMetasToWork(metas);

            if (clusterOpFut != null)
                ctx.clusterMetas = metas;

            if (U.isLocalNodeCoordinator(kctx.discovery()))
                phase2PartsHashes.start(reqId, ctx.req);
        }
        catch (Throwable th) {
            if (ctx != null) {
                contexts.remove(ctx.req.snapshotName());

                if (log.isInfoEnabled())
                    log.info("Finished snapshot validation [req=" + ctx.req + ']');
            }

            if (clusterOpFut != null)
                clusterOpFut.onDone(th);
        }
    }

    /** */
    private List<SnapshotMetadata> assingMetasToWork(Map<ClusterNode, List<SnapshotMetadata>> clusterMetas) {
        List<SnapshotMetadata> locMetas = clusterMetas.get(kctx.cluster().get().localNode());

        if (F.isEmpty(locMetas))
            return null;

        UUID minOrderDataNodeId = clusterMetas.keySet().stream().sorted(new Comparator<>() {
            @Override public int compare(ClusterNode o1, ClusterNode o2) {
                return Long.compare(o1.order(), o2.order());
            }
        }).map(ClusterNode::id).findFirst().get();

        if (minOrderDataNodeId.equals(kctx.localNodeId())) {
            Collection<String> onlineDataNodesIds = clusterMetas.keySet().stream().map(node -> node.consistentId().toString())
                .collect(Collectors.toSet());

            locMetas.removeIf(meta -> !meta.consistentId().equals(kctx.cluster().get().localNode().consistentId())
                && onlineDataNodesIds.contains(meta.consistentId()));
        }
        else
            locMetas = Collections.singletonList(F.first(locMetas));

        return locMetas;
    }

    /**
     * Starts the snapshot validation process.
     *
     * @param snpName Snapshot name.
     * @param snpPath Snapshot directory path.
     * @param grpNames List of cache group names.
     * @param fullCheck If {@code true}, additionally calculates partition hashes. Otherwise, checks only snapshot integrity
     *                  and partition counters.
     * @param incIdx Incremental snapshot index. If not positive, snapshot is not considered as incremental.
     * @param allRestoreHandlers If {@code true}, all the registered {@link IgniteSnapshotManager#handlers()} of type
     *                    {@link SnapshotHandlerType#RESTORE} are invoked. Otherwise, only snapshot metadatas and partition
     *                    hashes are validated.
     */
    public IgniteInternalFuture<SnapshotPartitionsVerifyResult> start(
        String snpName,
        @Nullable String snpPath,
        @Nullable Collection<String> grpNames,
        boolean fullCheck,
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
            fullCheck,
            incIdx,
            allRestoreHandlers
        );

        GridFutureAdapter<SnapshotPartitionsVerifyResult> clusterOpFut = new GridFutureAdapter<>();

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

        /**
         * Metadatas to process on this node. Also indicates the snapshot part to validate on this node.
         * @see #validatePartitionsFuture(SnapshotCheckContext)
         */
        @Nullable private List<SnapshotMetadata> metasToProc;

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

        /** @see #metas() */
        @Nullable private final List<SnapshotMetadata> metas;

        /**
         * @see #partsHashes()
         * @see #customHandlersResults()
         * @see #exceptions()
         */
        @Nullable private final Map<String, ? extends Map<?, ?>> partsResults;

        /** @see #exceptions() */
        @Nullable private final Map<String, Throwable> exceptions;

        /** @see #incrementalResult() */
        @Nullable private final IncrementalSnapshotCheckResult incRes;

        /** Ctor for the phase 1. */
        private SnapshotCheckResponse(@Nullable List<SnapshotMetadata> metas) {
            this.metas = metas;
            this.partsResults = null;
            this.incRes = null;
            this.exceptions = null;
        }

        /**
         * Ctor for the phase 2 for normal snapshot.
         *
         * @param resultsPerConsId Partitions check result by node's consistent id as string: consistend id -> map of
         *                     partition checks results by a partition id or an exception for current consistent id.
         * @param allHandlers All handlers result flag.
         */
        private SnapshotCheckResponse(Map<String, Object> resultsPerConsId, boolean allHandlers) {
            this.metas = null;
            this.incRes = null;

            Map<String, Throwable> exceptions = new HashMap<>();

            if (allHandlers) {
                Map<String, Map<String, SnapshotHandlerResult<Object>>> allHndResults = new HashMap<>();

                resultsPerConsId.forEach((consId, consIdRes) -> {
                    assert consIdRes instanceof Throwable || consIdRes instanceof Map;

                    if (consIdRes instanceof Throwable)
                        exceptions.put(consId, (Throwable)consIdRes);
                    else
                        allHndResults.put(consId, (Map<String, SnapshotHandlerResult<Object>>)consIdRes);
                });

                this.partsResults = allHndResults;
            }
            else {
                Map<String, Map<PartitionKeyV2, PartitionHashRecordV2>> partsHashesResults = new HashMap<>();

                resultsPerConsId.forEach((consId, consIdRes) -> {
                    assert consIdRes instanceof Throwable || consIdRes instanceof Map;

                    if (consIdRes instanceof Throwable)
                        exceptions.put(consId, (Throwable)consIdRes);
                    else
                        partsHashesResults.put(consId, (Map<PartitionKeyV2, PartitionHashRecordV2>)consIdRes);
                });

                this.partsResults = partsHashesResults;
            }

            this.exceptions = exceptions;
        }

        /** Ctor for the phase 2 for incremental snapshot. */
        private SnapshotCheckResponse(IncrementalSnapshotCheckResult incRes) {
            this.metas = null;
            this.partsResults = null;
            this.exceptions = null;
            this.incRes = incRes;
        }

        /** Metas for the phase 1. Is always {@code null} for the phase 2. */
        @Nullable private List<SnapshotMetadata> metas() {
            return metas;
        }

        /** Exceptions found on phase 2 per consistent id. Is always {@code null} for the phase 1. */
        @Nullable private Map<String, Throwable> exceptions() {
            return exceptions;
        }

        /**
         * Node's partition hashes per consistent id for the phase 2. Is always {@code null} for the phase 1 or in case of
         * incremental snapshot.
         */
        private @Nullable Map<String, Map<PartitionKeyV2, PartitionHashRecordV2>> partsHashes() {
            return (Map<String, Map<PartitionKeyV2, PartitionHashRecordV2>>)partsResults;
        }

        /**
         * Results of the custom handlers per consistent id for the phase 2. Is always {@code null} for the phase 1 or in case of
         * incremental snapshot.
         *
         * @see IgniteSnapshotManager#handlers()
         */
        private @Nullable Map<String, Map<String, SnapshotHandlerResult<Object>>> customHandlersResults() {
            return (Map<String, Map<String, SnapshotHandlerResult<Object>>>)partsResults;
        }

        /** Incremental snapshot result for the phase 2. Is always {@code null} for the phase 1 or in case of normal snapshot. */
        private @Nullable IncrementalSnapshotCheckResult incrementalResult() {
            return incRes;
        }
    }
}
