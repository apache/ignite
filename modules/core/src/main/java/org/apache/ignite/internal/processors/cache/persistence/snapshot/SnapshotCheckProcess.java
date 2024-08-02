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

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
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
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.SNAPSHOT_CHECK_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.SNAPSHOT_VALIDATE_PARTS;

/** Distributed process of snapshot checking (with the partition hashes). */
public class SnapshotCheckProcess {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext kctx;

    /** Operation contexts by name. */
    private final Map<String, SnapshotCheckContext> contexts = new ConcurrentHashMap<>();

    /** Cluster-wide operation futures per snapshot called from current node. */
    private final Map<UUID, GridFutureAdapter<SnapshotPartitionsVerifyTaskResult>> clusterOpFuts = new ConcurrentHashMap<>();

    /** Check metas first phase subprocess. */
    private final DistributedProcess<SnapshotCheckProcessRequest, CheckResultDTO> phase1CheckMetas;

    /** Partition hashes second phase subprocess.  */
    private final DistributedProcess<SnapshotCheckProcessRequest, CheckResultDTO> phase2PartsHashes;

    /** */
    public SnapshotCheckProcess(GridKernalContext kctx) {
        this.kctx = kctx;

        log = kctx.log(getClass());

        phase1CheckMetas = new DistributedProcess<>(kctx, SNAPSHOT_CHECK_METAS, this::prepareAndCheckMetas,
            this::reducePreparationAndMetasCheck);

        phase2PartsHashes = new DistributedProcess<>(kctx, SNAPSHOT_VALIDATE_PARTS, this::validateParts,
            this::reduceValidatePartsAndFinish);

        kctx.event().addLocalEventListener(evt -> onNodeLeft(((DiscoveryEvent)evt).eventNode().id()), EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** Expected to run in a discovery-managed thread. */
    private void onNodeLeft(UUID nodeId) {
        Throwable err = new ClusterTopologyCheckedException("Snapshot checking stopped. " +
            "A required node or the initiator node left the cluster [nodeId=" + nodeId + ']');

        Iterator<Map.Entry<String, SnapshotCheckContext>> it = contexts.entrySet().iterator();

        while (it.hasNext()) {
            SnapshotCheckContext ctx = it.next().getValue();

            if (!ctx.req.nodes().contains(nodeId))
                continue;

            if (ctx.fut != null)
                ctx.fut.onDone(err);

            it.remove();

            GridFutureAdapter<?> fut = clusterOpFuts.get(ctx.req.reqId);

            if (fut != null)
                fut.onDone(err);
        }
    }

    /** */
    Map<String, SnapshotCheckContext> requests() {
        return Collections.unmodifiableMap(contexts);
    }

    /**
     * Stops all the processes with the passed exception.
     *
     * @param err The interrupt reason.
     */
    void interrupt(Throwable err) {
        contexts.forEach((snpNane, ctx) -> {
            if (ctx.fut != null)
                ctx.fut.onDone(err);
        });

        clusterOpFuts.forEach((reqId, fut) -> fut.onDone(err));
    }

    /** Phase 2 and process finish. */
    private IgniteInternalFuture<?> reduceValidatePartsAndFinish(
        UUID reqId,
        Map<UUID, CheckResultDTO> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckContext ctx = context(null, reqId);

        if (ctx != null) {
            contexts.remove(ctx.req.snapshotName());

            if (log.isInfoEnabled())
                log.info("Finished snapshot local validation [req=" + ctx.req + ']');

            GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.get(reqId);

            // Operation node is the initiator collecting the final check operation result.
            if (clusterOpFut != null) {
                Map<ClusterNode, Exception> errors0 = mapErrors(errors);

                if (!F.isEmpty(results)) {
                    assert results.values().stream().noneMatch(res -> res != null && res.metas != null);

                    Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> results0 = mapPartsHashes(results, ctx.req.nodes());

                    IdleVerifyResultV2 chkRes = SnapshotChecker.reduceHashesResults(results0, errors0);

                    clusterOpFut.onDone(new SnapshotPartitionsVerifyTaskResult(ctx.clusterMetas, chkRes));
                }
                else
                    finishClusterFutureWithErr(clusterOpFut, null, errors0);
            }
        }

        return new GridFinishedFuture<>();
    }

    /** Phase 2 beginning.  */
    private IgniteInternalFuture<CheckResultDTO> validateParts(SnapshotCheckProcessRequest req) {
        GridFutureAdapter<?> clusterOpFut = clusterOpFuts.get(req.requestId());

        if (clusterOpFut == null && !req.nodes().contains(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        if (req.error() != null)
            return new GridFinishedFuture<>(req.error());

        SnapshotCheckContext ctx = context(null, req.requestId());

        assert ctx != null;
        assert ctx.req.reqId.equals(req.reqId);

        ctx.fut = new GridFutureAdapter<>();

        // Store metas on the initiator node to form the process result (SnapshotPartitionsVerifyTaskResult) at the end.
        if (clusterOpFut != null)
            ctx.clusterMetas = req.clusterMetas();

        // Excludes non-baseline initiator (opNode).
        if (!baseline(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        List<SnapshotMetadata> locMetas = req.clusterMetas().get(kctx.cluster().get().localNode());

        // Local meta might be null if current node started after the snapshot creation or placement.
        if (F.isEmpty(locMetas))
            ctx.fut.onDone();
        else {
            File snpDir = kctx.cache().context().snapshotMgr().snapshotLocalDir(req.snapshotName(), req.snapshotPath());

            kctx.cache().context().snapshotMgr().checker().checkPartitions(locMetas.get(0), snpDir, req.groups(), false, true, false)
                .whenComplete((res, err) -> {
                    if (err != null)
                        ctx.fut.onDone(err);
                    else
                        ctx.fut.onDone(new CheckResultDTO(res));
                });
        }

        return ctx.fut;
    }
    
    /** */
    private Map<ClusterNode, Exception> mapErrors(@Nullable Map<UUID, Throwable> errors) {
        if (F.isEmpty(errors))
            return Collections.emptyMap();

        return errors.entrySet().stream()
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> asException(e.getValue())));
    }

    /** */
    private Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> mapPartsHashes(
        @Nullable Map<UUID, CheckResultDTO> results,
        Collection<UUID> requiredNodes
    ) {
        if (F.isEmpty(results))
            return Collections.emptyMap();

        // A not required node can leave the cluster and its result can be null.
        return results.entrySet().stream()
            .filter(e -> requiredNodes.contains(e.getKey()) && e.getValue() != null)
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> e.getValue().partsHashes));
    }

    /**
     * @param snpName Snapshot name of the validation process. If {@code null}, ignored.
     * @param reqId  If {@code snpName} is {@code null}, is used to find the operation request.
     * @return Current snapshot checking context by {@code snpName} or {@code reqId}.
     */
    private @Nullable SnapshotCheckContext context(@Nullable String snpName, UUID reqId) {
        return snpName == null
            ? contexts.values().stream().filter(ctx -> ctx.req.reqId.equals(reqId)).findFirst().orElse(null)
            : contexts.get(snpName);
    }

    /** Phase 1 beginning: prepare, collect and check local metas. */
    private IgniteInternalFuture<CheckResultDTO> prepareAndCheckMetas(SnapshotCheckProcessRequest req) {
        if (!req.nodes().contains(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        SnapshotCheckContext ctx = contexts.computeIfAbsent(req.snapshotName(), snpName -> new SnapshotCheckContext(req));

        if (!ctx.req.equals(req)) {
            return new GridFinishedFuture<>(new IllegalStateException("Validation of snapshot '" + req.snapshotName()
                + "' has already started. Request=" + ctx + '.'));
        }

        // Excludes non-baseline initiator (opNode).
        if (!baseline(kctx.localNodeId()))
            return new GridFinishedFuture<>();

        if (log.isDebugEnabled())
            log.debug("Checking local snapshot metadatas [req=" + ctx.req + ']');

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        Collection<Integer> grpIds = F.isEmpty(req.groups()) ? null : F.viewReadOnly(req.groups(), CU::cacheId);

        ctx.fut = new GridFutureAdapter<>();

        snpMgr.checker().checkLocalMetas(
            snpMgr.snapshotLocalDir(req.snapshotName(), req.snapshotPath()),
            grpIds,
            kctx.cluster().get().localNode().consistentId()
        ).whenComplete((locMetas, err) -> {
            if (err != null)
                ctx.fut.onDone(err);
            else
                ctx.fut.onDone(new CheckResultDTO(locMetas));
        });

        return ctx.fut;
    }

    /** Phase 1 end. */
    private void reducePreparationAndMetasCheck(
        UUID reqId,
        Map<UUID, CheckResultDTO> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckContext ctx = context(snpName(results), reqId);

        // The context is not stored in the case of concurrent check of the same snapshot but the operation future is registered.
        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.get(reqId);

        if (!F.isEmpty(errors)) {
            if (ctx != null) {
                assert ctx.req.reqId.equals(reqId);

                contexts.remove(ctx.req.snapshotName());

                if (log.isInfoEnabled())
                    log.info("Finished snapshot local validation [req=" + ctx.req + ']');
            }

            if (clusterOpFut != null) {
                Map<ClusterNode, Exception> errors0 = mapErrors(errors);

                finishClusterFutureWithErr(clusterOpFut, null, errors0);
            }

            return;
        }

        if (ctx == null || !U.isLocalNodeCoordinator(kctx.discovery()))
            return;

        Map<ClusterNode, List<SnapshotMetadata>> metas = new HashMap<>();

        Throwable metasValidationErr;

        try {
            results.forEach((nodeId, nodeRes) -> {
                // A node might be not required. It gives null result. But a required node might have invalid empty result
                // which must be validated.
                if (ctx.req.nodes().contains(nodeId) && baseline(nodeId)) {
                    assert nodeRes != null && nodeRes.partsHashes == null;
                    assert kctx.cluster().get().node(nodeId) != null;

                    metas.put(kctx.cluster().get().node(nodeId), nodeRes.metas);
                }
            });

            SnapshotMetadataVerificationTaskResult metasRes = new SnapshotMetadataVerificationTaskResult(
                metas,
                SnapshotChecker.reduceMetasResults(ctx.req.snapshotName(), ctx.req.snapshotPath(), metas, null,
                    kctx.cluster().get().localNode().consistentId())
            );

            if (!F.isEmpty(metasRes.exceptions()))
                throw new IgniteSnapshotVerifyException(metasRes.exceptions());

            metasValidationErr = null;
        }
        catch (Throwable err) {
            metasValidationErr = err;
        }

        phase2PartsHashes.start(reqId, new SnapshotCheckProcessRequest(ctx.req, metasValidationErr, metas));

        if (log.isDebugEnabled())
            log.debug("Started partitions validation as part of the snapshot checking [req=" + ctx.req + ']');
    }

    /** Finds current snapshot name from the metas. */
    private @Nullable String snpName(@Nullable Map<UUID, CheckResultDTO> results) {
        if (F.isEmpty(results))
            return null;

        for (CheckResultDTO nodeRes : results.values()) {
            if (nodeRes == null || F.isEmpty(nodeRes.metas))
                continue;

            assert nodeRes.metas.get(0) != null : "Empty snapshot metadata in the results";
            assert !F.isEmpty(nodeRes.metas.get(0).snapshotName()) : "Empty snapshot name in a snapshot metadata.";

            return nodeRes.metas.get(0).snapshotName();
        }

        return null;
    }

    /** Starts the snapshot full validation. */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> start(
        String snpName,
        @Nullable String snpPath,
        @Nullable Collection<String> grpNames,
        boolean inclCstHndlrs
    ) {
        assert !F.isEmpty(snpName);

        UUID reqId = UUID.randomUUID();

        List<UUID> requiredNodes = new ArrayList<>(F.viewReadOnly(kctx.discovery().discoCache().aliveBaselineNodes(), F.node2id()));

        // Initiator is also a required node. It collects the final oparation result.
        requiredNodes.add(kctx.localNodeId());

        SnapshotCheckProcessRequest req = new SnapshotCheckProcessRequest(
            reqId,
            requiredNodes,
            snpName,
            snpPath,
            grpNames,
            0,
            inclCstHndlrs,
            null
        );

        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = new GridFutureAdapter<>();

        clusterOpFut.listen(fut -> {
            clusterOpFuts.remove(reqId);

            if (log.isInfoEnabled())
                log.info("Finished snapshot checking process [req=" + req + ']');
        });

        clusterOpFuts.put(reqId, clusterOpFut);

        phase1CheckMetas.start(req.requestId(), req);

        return clusterOpFut;
    }

    /** Properly sets errror to the cluster operation future. */
    static boolean finishClusterFutureWithErr(
        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut,
        Throwable propogatedError,
        Map<ClusterNode, Exception> nodeErrors
    ) {
        assert propogatedError != null || !F.isEmpty(nodeErrors);

        if (propogatedError == null)
            return clusterOpFut.onDone(new IgniteSnapshotVerifyException(nodeErrors));
        else if (propogatedError instanceof IgniteSnapshotVerifyException)
            return clusterOpFut.onDone(new SnapshotPartitionsVerifyTaskResult(null,
                new IdleVerifyResultV2(((IgniteSnapshotVerifyException)propogatedError).exceptions())));
        else
            return clusterOpFut.onDone(propogatedError);
    }

    /** @return {@code True} if the provided node id is id of a baseline node. */
    private boolean baseline(UUID nodeId) {
        return CU.baselineNode(kctx.cluster().get().node(nodeId), kctx.state().clusterState());
    }

    /** Converts failure to an exception if it is not. */
    private static Exception asException(Throwable th) {
        return th instanceof Exception ? (Exception)th : new IgniteException(th);
    }

    /** Operation context. */
    private static final class SnapshotCheckContext {
        /** Request. */
        private final SnapshotCheckProcessRequest req;

        /** Working future. Expected to be set/read in different threads but not concurrently. */
        private volatile GridFutureAdapter<CheckResultDTO> fut;

        /** Collected cluster metas. */
        @Nullable private Map<ClusterNode, List<SnapshotMetadata>> clusterMetas;

        /** Creates operation context. */
        private SnapshotCheckContext(SnapshotCheckProcessRequest req) {
            this.req = req;
        }
    }

    /** A DTO used to transfer nodes' results for the both phases. */
    private static final class CheckResultDTO implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Metas for the pahse 1. Is always {@code null} for the phase 2. */
        @Nullable private final List<SnapshotMetadata> metas;

        /** Node's partition hashes for the phase 2. Is always {@code null} for the phase 1. */
        @Nullable private final Map<PartitionKeyV2, PartitionHashRecordV2> partsHashes;

        /** Ctor for the phase 1. */
        private CheckResultDTO(@Nullable List<SnapshotMetadata> metas) {
            this.metas = metas;
            this.partsHashes = null;
        }

        /** Ctor for the phase 2. */
        private CheckResultDTO(@Nullable Map<PartitionKeyV2, PartitionHashRecordV2> partsHashes) {
            this.metas = null;
            this.partsHashes = partsHashes;
        }
    }
}
