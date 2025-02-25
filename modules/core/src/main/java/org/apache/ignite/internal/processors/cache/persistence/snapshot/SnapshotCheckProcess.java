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
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
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

        if (ctx.req.incrementalIndex() > 0)
            reduceIncrementalResults(ctx.req.nodes(), ctx.clusterMetas, results, errors, clusterOpFut);
        else if (ctx.req.allRestoreHandlers())
            reduceCustomHandlersResults(ctx, results, errors, clusterOpFut);
        else
            reducePartitionsHashesResults(ctx.clusterMetas, results, errors, clusterOpFut);

        return new GridFinishedFuture<>();
    }

    /** */
    private void reduceIncrementalResults(
        Set<UUID> requiredNodes,
        Map<ClusterNode, List<SnapshotMetadata>> clusterMetas,
        Map<UUID, SnapshotCheckResponse> results,
        Map<UUID, Throwable> errors,
        GridFutureAdapter<SnapshotPartitionsVerifyResult> fut
    ) {
        Map<ClusterNode, IncrementalSnapshotCheckResult> perNodeResults = new HashMap<>();

        for (Map.Entry<UUID, SnapshotCheckResponse> resEntry : results.entrySet()) {
            UUID nodeId = resEntry.getKey();

            SnapshotCheckResponse incResp = resEntry.getValue();

            if (incResp == null || !requiredNodes.contains(nodeId))
                continue;

            perNodeResults.put(kctx.cluster().get().node(nodeId), incResp.result());

            if (F.isEmpty(incResp.exceptions()))
                continue;

            errors.putIfAbsent(nodeId, asException(F.firstValue(incResp.exceptions())));
        }

        IdleVerifyResult chkRes = kctx.cache().context().snapshotMgr().checker()
            .reduceIncrementalResults(perNodeResults, mapErrors(errors));

        fut.onDone(new SnapshotPartitionsVerifyResult(clusterMetas, chkRes));
    }

    /** */
    private void reduceCustomHandlersResults(
        SnapshotCheckContext ctx,
        Map<UUID, SnapshotCheckResponse> results,
        Map<UUID, Throwable> errors,
        GridFutureAdapter<SnapshotPartitionsVerifyResult> fut
    ) {
        try {
            if (!errors.isEmpty())
                throw F.firstValue(errors);

            SnapshotChecker snpChecker = kctx.cache().context().snapshotMgr().checker();

            // Check responses: checking node -> snapshot part's consistent id -> handler name -> handler result.
            Map<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> reduced = new HashMap<>();

            for (Map.Entry<UUID, SnapshotCheckResponse> respEntry : results.entrySet()) {
                SnapshotCheckResponse nodeResp = respEntry.getValue();

                if (nodeResp == null)
                    continue;

                if (!F.isEmpty(nodeResp.exceptions()))
                    throw F.firstValue(nodeResp.exceptions());

                UUID nodeId = respEntry.getKey();

                Map<String, Map<String, SnapshotHandlerResult<Object>>> cstHndRes = nodeResp.result();

                cstHndRes.forEach((consId, respPerConsIdMap) -> {
                    // Reduced map of the handlers results per snapshot part's consistent id for certain node.
                    Map<Object, Map<String, SnapshotHandlerResult<?>>> nodePerConsIdResultMap
                        = reduced.computeIfAbsent(kctx.cluster().get().node(nodeId), n -> new HashMap<>());

                    respPerConsIdMap.forEach((hndId, hndRes) ->
                        nodePerConsIdResultMap.computeIfAbsent(consId, cstId -> new HashMap<>()).put(hndId, hndRes));
                });
            }

            snpChecker.checkCustomHandlersResults(ctx.req.snapshotName(), reduced);

            fut.onDone(new SnapshotPartitionsVerifyResult(ctx.clusterMetas, null));
        }
        catch (Throwable err) {
            fut.onDone(err);
        }
    }

    /** */
    private void reducePartitionsHashesResults(
        Map<ClusterNode, List<SnapshotMetadata>> clusterMetas,
        Map<UUID, SnapshotCheckResponse> results,
        Map<UUID, Throwable> errors,
        GridFutureAdapter<SnapshotPartitionsVerifyResult> fut
    ) {
        IdleVerifyResult.Builder bldr = IdleVerifyResult.builder();

        Map<ClusterNode, Exception> errors0 = mapErrors(errors);

        if (!results.isEmpty()) {
            if (!errors0.isEmpty())
                bldr.exceptions(errors0);

            for (Map.Entry<UUID, SnapshotCheckResponse> respEntry : results.entrySet()) {
                SnapshotCheckResponse resp = respEntry.getValue();

                if (resp == null)
                    continue;

                if (!F.isEmpty(resp.exceptions())) {
                    ClusterNode node = kctx.cluster().get().node(respEntry.getKey());

                    bldr.addException(node, asException(F.firstValue(resp.exceptions())));
                }

                Map<String, Map<PartitionKey, PartitionHashRecord>> partsHashesRes = resp.result();

                partsHashesRes.forEach((consId, partsPerConsId) -> bldr.addPartitionHashes(partsPerConsId));
            }

            fut.onDone(new SnapshotPartitionsVerifyResult(clusterMetas, bldr.build()));
        }
        else
            fut.onDone(new IgniteSnapshotVerifyException(errors0));
    }

    /** Phase 2 beginning.  */
    private IgniteInternalFuture<SnapshotCheckResponse> validateParts(SnapshotCheckProcessRequest req) {
        if (!req.nodes().contains(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        SnapshotCheckContext ctx = context(req.snapshotName(), req.requestId());

        assert ctx != null;

        if (F.isEmpty(ctx.metas))
            return new GridFinishedFuture<>();

        GridFutureAdapter<SnapshotCheckResponse> phaseFut = ctx.phaseFuture();

        // Might be already finished by asynchronous leave of a required node.
        if (!phaseFut.isDone()) {
            CompletableFuture<SnapshotCheckResponse> workingFut;

            if (req.incrementalIndex() > 0) {
                assert !req.allRestoreHandlers() : "Snapshot handlers aren't supported for incremental snapshot.";

                workingFut = incrementalFuture(ctx);
            }
            else if (req.allRestoreHandlers())
                workingFut = allHandlersFuture(ctx);
            else
                workingFut = partitionsHashesFuture(ctx);

            workingFut.whenComplete((res, err) -> {
                if (err != null)
                    phaseFut.onDone(err);
                else
                    phaseFut.onDone(res);
            });
        }

        return phaseFut;
    }

    /** @return A composed future of increment checks for each consistent id regarding {@link SnapshotCheckContext#metas}. */
    private CompletableFuture<SnapshotCheckResponse> incrementalFuture(SnapshotCheckContext ctx) {
        SnapshotChecker snpChecker = kctx.cache().context().snapshotMgr().checker();

        assert ctx.metas.size() == 1 : "Incremental snapshots do not support checking on other topology.";

        SnapshotMetadata meta = ctx.metas.get(0);

        CompletableFuture<SnapshotCheckResponse> resFut = new CompletableFuture<>();

        CompletableFuture<IncrementalSnapshotCheckResult> workingFut = snpChecker.checkIncrementalSnapshot(
            ctx.locFileTree.get(meta.consistentId()), ctx.req.incrementalIndex());

        workingFut.whenComplete((res, err) -> {
            if (err != null)
                resFut.completeExceptionally(err);
            else
                resFut.complete(new SnapshotCheckResponse(res, null));
        });

        return resFut;
    }

    /** @return A composed future of partitions checks for each consistent id regarding {@link SnapshotCheckContext#metas}. */
    private CompletableFuture<SnapshotCheckResponse> partitionsHashesFuture(SnapshotCheckContext ctx) {
        // Per metas result: consistent id -> check results per partition key.
        Map<String, Map<PartitionKey, PartitionHashRecord>> perMetaResults = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        // Per consistent id.
        Map<String, Throwable> exceptions = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        CompletableFuture<SnapshotCheckResponse> composedFut = new CompletableFuture<>();
        AtomicInteger metasProcessed = new AtomicInteger(ctx.metas.size());
        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        for (SnapshotMetadata meta : ctx.metas) {
            CompletableFuture<Map<PartitionKey, PartitionHashRecord>> metaFut = snpMgr.checker().checkPartitions(
                meta,
                ctx.locFileTree.get(meta.consistentId()),
                ctx.req.groups(),
                false,
                ctx.req.fullCheck(),
                false
            );

            metaFut.whenComplete((res, err) -> {
                if (err != null)
                    exceptions.put(meta.consistentId(), err);
                else if (!F.isEmpty(res))
                    perMetaResults.put(meta.consistentId(), res);

                if (metasProcessed.decrementAndGet() == 0)
                    composedFut.complete(new SnapshotCheckResponse(perMetaResults, exceptions));
            });
        }

        return composedFut;
    }

    /**
     * @return A composed future of all the snapshot handlers for each consistent id regarding {@link SnapshotCheckContext#metas}.
     * @see IgniteSnapshotManager#handlers()
     */
    private CompletableFuture<SnapshotCheckResponse> allHandlersFuture(SnapshotCheckContext ctx) {
        SnapshotChecker snpChecker = kctx.cache().context().snapshotMgr().checker();
        // Per metas result: snapshot part's consistent id -> check result per handler name.
        Map<String, Map<String, SnapshotHandlerResult<Object>>> perMetaResults = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        // Per consistent id.
        Map<String, Throwable> exceptions = new ConcurrentHashMap<>(ctx.metas.size(), 1.0f);
        CompletableFuture<SnapshotCheckResponse> composedFut = new CompletableFuture<>();
        AtomicInteger metasProcessed = new AtomicInteger(ctx.metas.size());

        for (SnapshotMetadata meta : ctx.metas) {
            CompletableFuture<Map<String, SnapshotHandlerResult<Object>>> metaFut = snpChecker.invokeCustomHandlers(meta,
                ctx.locFileTree.get(meta.consistentId()), ctx.req.groups(), true);

            metaFut.whenComplete((res, err) -> {
                if (err != null)
                    exceptions.put(meta.consistentId(), err);
                else if (!F.isEmpty(res))
                    perMetaResults.put(meta.consistentId(), res);

                if (metasProcessed.decrementAndGet() == 0)
                    composedFut.complete(new SnapshotCheckResponse(perMetaResults, exceptions));
            });
        }

        return composedFut;
    }

    /** */
    private Map<ClusterNode, Exception> mapErrors(Map<UUID, Throwable> errors) {
        return errors.entrySet().stream().collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()),
            e -> asException(e.getValue())));
    }

    /** */
    private static Exception asException(Throwable th) {
        return th instanceof Exception ? (Exception)th : new IgniteException(th);
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
                new SnapshotFileTree(kctx, req.snapshotName(), req.snapshotPath()),
                req.incrementalIndex(),
                grpIds,
                kctx.cluster().get().localNode().consistentId()
            ).whenComplete((locMetas, err) -> {
                if (err != null)
                    phaseFut.onDone(err);
                else
                    phaseFut.onDone(new SnapshotCheckResponse(locMetas, null));
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
                if (ctx.req.nodes().contains(nodeId) && baseline(nodeId) && !F.isEmpty((Collection<?>)nodeRes.result())) {
                    assert nodeRes != null;

                    metas.put(kctx.cluster().get().node(nodeId), nodeRes.result());
                }
            });

            Map<ClusterNode, Exception> metasCheck = SnapshotChecker.reduceMetasResults(ctx.req.snapshotName(), ctx.req.snapshotPath(),
                metas, null, kctx.cluster().get().localNode().consistentId());

            if (!metasCheck.isEmpty())
                throw new IgniteSnapshotVerifyException(metasCheck);

            // If the topology is lesser that the snapshot's, we have to check another partitions parts.
            ctx.metas = assingMetas(metas);

            if (!F.isEmpty(ctx.metas)) {
                if (ctx.metas.size() > 1 && ctx.req.incrementalIndex() > 0) {
                    throw new IllegalStateException("Found several snapshot metadatas to process on current node. " +
                        "Incremental snapshots do not support checking/restoring on other topology.");
                }

                ctx.locFileTree = new HashMap<>(ctx.metas.size(), 1.0f);

                for (SnapshotMetadata metaToProc : ctx.metas) {
                    SnapshotFileTree sft = new SnapshotFileTree(kctx, ctx.req.snapshotName(), ctx.req.snapshotPath(),
                        metaToProc.folderName(), metaToProc.consistentId());

                    ctx.locFileTree.put(metaToProc.consistentId(), sft);
                }
            }

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

    /**
     * Assigns snapshot metadatas to process. A snapshot can be checked on a smaller topology compared to the original one.
     * In this case, some node has to check not only own meta and partitions.
     *
     * @return Metadatas to process on current node.
     */
    private @Nullable List<SnapshotMetadata> assingMetas(Map<ClusterNode, List<SnapshotMetadata>> clusterMetas) {
        ClusterNode locNode = kctx.cluster().get().localNode();
        List<SnapshotMetadata> locMetas = clusterMetas.get(locNode);

        if (F.isEmpty(locMetas))
            return null;

        Set<String> onlineNodesConstIdsStr = new HashSet<>(clusterMetas.size());
        // The nodes are sorted with lesser order.
        Map<String, Collection<ClusterNode>> metasPerRespondedNodes = new HashMap<>();

        clusterMetas.forEach((node, nodeMetas) -> {
            if (!F.isEmpty(nodeMetas)) {
                onlineNodesConstIdsStr.add(node.consistentId().toString());

                nodeMetas.forEach(nodeMeta -> metasPerRespondedNodes.computeIfAbsent(nodeMeta.consistentId(),
                    m -> new TreeSet<>(Comparator.comparingLong(ClusterNode::order))).add(node));
            }
        });

        String locNodeConsIdStr = locNode.consistentId().toString();
        List<SnapshotMetadata> metasToProc = new ArrayList<>();

        for (SnapshotMetadata meta : locMetas) {
            if (meta.consistentId().equals(locNodeConsIdStr)) {
                assert !metasToProc.contains(meta) : "Local snapshot metadata is already assigned to process";

                metasToProc.add(meta);

                continue;
            }

            if (!onlineNodesConstIdsStr.contains(meta.consistentId())
                && F.first(metasPerRespondedNodes.get(meta.consistentId())).id().equals(kctx.localNodeId()))
                metasToProc.add(meta);
        }

        return metasToProc;
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

        // Initiator is also a required node. It collects the final operation result.
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
         * Metadatas to process on this node. Also indicates the snapshot parts to check on this node.
         * @see #partitionsHashesFuture(SnapshotCheckContext)
         */
        @Nullable private List<SnapshotMetadata> metas;

        /** Map of snapshot pathes per consistent id for {@link #metas}. */
        @Nullable private Map<String, SnapshotFileTree> locFileTree;

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

    /** A DTO to transfer node's results for the both phases. */
    private static final class SnapshotCheckResponse implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** The result. Is usually a collection or a map of hashes, metast, etc. */
        private final Object result;

        /** Exceptions per snapshot part's consistent id. */
        @Nullable private final Map<String, Throwable> exceptions;

        /** */
        private SnapshotCheckResponse(Object result, @Nullable Map<String, Throwable> exceptions) {
            assert result instanceof Serializable : "Snapshot check result is not serializable.";
            assert exceptions == null || exceptions instanceof Serializable : "Snapshot check exceptions aren't serializable.";

            this.result = result;
            this.exceptions = exceptions == null ? null : Collections.unmodifiableMap(exceptions);
        }

        /** @return Exceptions per snapshot part's consistent id. */
        private @Nullable Map<String, Throwable> exceptions() {
            return exceptions;
        }

        /** @return Certain phase's and process' result. */
        private <T> T result() {
            return (T)result;
        }
    }
}
