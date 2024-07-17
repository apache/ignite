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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
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

    /** Snapshot check requests per snapshot on every node. */
    private final Map<String, SnapshotCheckProcessRequest> requests = new ConcurrentHashMap<>();

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

        kctx.event().addLocalEventListener((evt) -> {
            DiscoveryEvent devt = (DiscoveryEvent)evt;

            if (devt.eventNode().isClient() || requests.isEmpty())
                return;

            interrupt(
                new ClusterTopologyCheckedException("Snapshot checking stopped. A node left the cluster: " + devt.eventNode() + '.'),
                req -> req.nodes().contains(devt.eventNode().id())
            );
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** */
    Map<String, SnapshotCheckProcessRequest> requests() {
        return Collections.unmodifiableMap(requests);
    }

    /**
     * Stops the process with the passed exception.
     *
     * @param th The interrupt reason.
     * @param rqFilter If not {@code null}, used to filter which requests/process to stop. If {@code null}, stops all the validations.
     */
    void interrupt(Throwable th, @Nullable Function<SnapshotCheckProcessRequest, Boolean> rqFilter) {
        requests.values().forEach(req -> {
            if (rqFilter == null || rqFilter.apply(req)) {
                req.error(th);

                clean(req.requestId(), th, null, null);
            }
        });
    }

    /** Phase 2 and process finish. */
    private IgniteInternalFuture<?> reduceValidatePartsAndFinish(
        UUID procId,
        Map<UUID, CheckResultDTO> results,
        Map<UUID, Throwable> errors
    ) {
        clean(procId, null, results, errors);

        return new GridFinishedFuture<>();
    }

    /** Phase 2 beginning.  */
    private IgniteInternalFuture<CheckResultDTO> validateParts(SnapshotCheckProcessRequest req) {
        if (req.error() != null)
            return new GridFinishedFuture<>(req.error());

        SnapshotCheckProcessRequest locReq = currentRequest(null, req.requestId());

        if (locReq == null)
            return new GridFinishedFuture<>();

        assert locReq.equals(req);

        // Store metas on the originator to redulte relusts later.
        if (req.operationalNodeId().equals(kctx.localNodeId()))
            locReq.metas = req.metas;

        // Local meta might be null if current node started after the snapshot creation or placement.
        if (!req.nodes.contains(kctx.localNodeId()) || locReq.meta() == null)
            return new GridFinishedFuture<>();

        GridFutureAdapter<CheckResultDTO> locPartsChkFut = new GridFutureAdapter<>();

        stopFutureOnAnyFailure(locPartsChkFut, () -> {
            locReq.fut(locPartsChkFut);

            if (!locPartsChkFut.isDone()) {
                File snpDir = kctx.cache().context().snapshotMgr().snapshotLocalDir(locReq.snapshotName(), locReq.snapshotPath());

                kctx.cache().context().snapshotMgr().checker().checkPartitions(locReq.meta(), snpDir, locReq.groups(), false, true, false)
                    .whenComplete((res, err) -> {
                        if (err != null)
                            locPartsChkFut.onDone(err);
                        else
                            locPartsChkFut.onDone(new CheckResultDTO(res));
                    });
            }
        });

        return locPartsChkFut;
    }

    /** Cleans certain snapshot validation. */
    private void clean(
        UUID reqId,
        @Nullable Throwable opErr,
        @Nullable Map<UUID, CheckResultDTO> results,
        @Nullable Map<UUID, Throwable> errors
    ) {
        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.remove(reqId);

        stopFutureOnAnyFailure(clusterOpFut, () -> {
            SnapshotCheckProcessRequest locReq = currentRequest(null, reqId);

            Throwable err = opErr;

            if (locReq != null) {
                if (err == null && locReq.error() != null)
                    err = locReq.error();

                GridFutureAdapter<?> locWorkingFut = locReq.fut();

                boolean finished = false;

                // Try to stop local working future ASAP.
                if (locWorkingFut != null)
                    finished = err == null ? locWorkingFut.onDone() : locWorkingFut.onDone(err);

                requests.remove(locReq.snapshotName());

                if (finished && log.isInfoEnabled())
                    log.info("Finished snapshot local validation, req: " + locReq + '.');
            }

            if (clusterOpFut == null || clusterOpFut.isDone())
                return;

            boolean finished;

            Map<ClusterNode, Exception> errors0 = collectErrors(errors, locReq != null ? locReq.nodes() : null);

            if (err == null && !F.isEmpty(results)) {
                assert results.values().stream().noneMatch(res -> res != null && res.metas != null);
                assert locReq != null;

                Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> results0 = collectPartsHashes(results,
                    locReq != null ? locReq.nodes() : null);

                IdleVerifyResultV2 chkRes = SnapshotChecker.reduceHashesResults(results0, errors0);

                finished = clusterOpFut.onDone(new SnapshotPartitionsVerifyTaskResult(locReq.metas, chkRes));
            }
            else
                finished = finishClusterFutureWithErr(clusterOpFut, err, errors0);

            if (finished && log.isInfoEnabled())
                log.info("Snapshot validation process finished, reqId: " + reqId + '.');
        });
    }
    
    /** */
    private Map<ClusterNode, Exception> collectErrors(@Nullable Map<UUID, Throwable> errors, @Nullable Set<UUID> requiredNodes) {
        if (F.isEmpty(errors))
            return Collections.emptyMap();

        return errors.entrySet().stream()
            .filter(e -> (requiredNodes == null || requiredNodes.contains(e.getKey())) && e.getValue() != null)
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> asException(e.getValue())));
    }

    /** */
    private Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> collectPartsHashes(
        @Nullable Map<UUID, CheckResultDTO> results,
        Collection<UUID> requiredNodes
    ) {
        if (F.isEmpty(results))
            return Collections.emptyMap();

        return results.entrySet().stream()
            .filter(e -> requiredNodes.contains(e.getKey()) && e.getValue() != null)
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> e.getValue().partsHashes));
    }

    /**
     * @param snpName Snapshot name of the validation process. If {@code null}, ignored.
     * @param procId  If {@code snpName} is {@code null}, is used to find the operation request.
     * @return Current snapshot checking request by {@code snpName} or {@code procId}.
     */
    private @Nullable SnapshotCheckProcessRequest currentRequest(@Nullable String snpName, UUID procId) {
        return snpName == null
            ? requests.values().stream().filter(req -> req.requestId().equals(procId)).findFirst().orElse(null)
            : requests.get(snpName);
    }

    /** Phase 1 beginning: prepare, collect and check local metas. */
    private IgniteInternalFuture<CheckResultDTO> prepareAndCheckMetas(SnapshotCheckProcessRequest req) {
        SnapshotCheckProcessRequest locReq = requests.computeIfAbsent(req.snapshotName(), snpName -> req);

        if (!locReq.equals(req)) {
            return new GridFinishedFuture<>(new IllegalStateException("Validation of snapshot '" + req.snapshotName()
                + "' has already started. Request=" + locReq + '.'));
        }

        if (!req.nodes.contains(kctx.localNodeId())) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping snapshot local metadatas collecting for snapshot validation, request=" + req
                    + ". Current node is not required.");
            }

            return new GridFinishedFuture<>();
        }

        GridFutureAdapter<CheckResultDTO> locMetasChkFut = new GridFutureAdapter<>();

        stopFutureOnAnyFailure(locMetasChkFut, () -> {
            assert locReq.fut() == null;

            locReq.fut(locMetasChkFut);

            IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

            if (log.isDebugEnabled())
                log.debug("Checking local snapshot metadatas. Request=" + locReq + '.');

            Collection<Integer> grpIds = F.isEmpty(locReq.groups()) ? null : F.viewReadOnly(locReq.groups(), CU::cacheId);

            if (!locMetasChkFut.isDone()) {
                snpMgr.checker().checkLocalMetas(
                    snpMgr.snapshotLocalDir(locReq.snapshotName(), locReq.snapshotPath()),
                    grpIds,
                    kctx.cluster().get().localNode().consistentId()
                ).whenComplete((locMetas, err) -> {
                    if (err != null)
                        locMetasChkFut.onDone(err);
                    else {
                        if (!F.isEmpty(locMetas))
                            locReq.meta(locMetas.get(0));

                        locMetasChkFut.onDone(new CheckResultDTO(locMetas));
                    }
                });
            }
        });

        return locMetasChkFut;
    }

    /** Phase 1 end. */
    private void reducePreparationAndMetasCheck(
        UUID procId,
        Map<UUID, CheckResultDTO> results,
        Map<UUID, Throwable> errors
    ) {
        if (!F.isEmpty(errors)) {
            clean(procId, null, results, errors);

            return;
        }

        SnapshotCheckProcessRequest locReq = currentRequest(snpName(results), procId);

        if (locReq == null || !locReq.opCoordId.equals(kctx.localNodeId()))
            return;

        Throwable stopClusterProcErr = null;

        try {
            Map<ClusterNode, List<SnapshotMetadata>> metas = new HashMap<>();

            results.forEach((nodeId, nodeRes) -> {
                // A node might be non-baseline (not required).
                if (locReq.nodes.contains(nodeId)) {
                    assert nodeRes != null && nodeRes.partsHashes == null;
                    assert kctx.cluster().get().node(nodeId) != null;

                    metas.put(kctx.cluster().get().node(nodeId), nodeRes.metas);
                }
            });

            locReq.metas = metas;

            if (!locReq.opCoordId.equals(kctx.localNodeId()))
                return;

            SnapshotMetadataVerificationTaskResult metasRes = new SnapshotMetadataVerificationTaskResult(
                locReq.metas,
                SnapshotChecker.reduceMetasResults(locReq.snapshotName(), locReq.snapshotPath(), locReq.metas, null,
                    kctx.cluster().get().localNode().consistentId())
            );

            if (!F.isEmpty(metasRes.exceptions()))
                throw new IgniteSnapshotVerifyException(metasRes.exceptions());
        }
        catch (Throwable th) {
            stopClusterProcErr = th;
        }

        if (stopClusterProcErr != null)
            locReq.error(stopClusterProcErr);

        phase2PartsHashes.start(procId, locReq);

        if (log.isDebugEnabled())
            log.debug("Started partitions validation as part of the snapshot checking. Request=" + locReq + '.');
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

        SnapshotCheckProcessRequest req = new SnapshotCheckProcessRequest(
            reqId,
            kctx.localNodeId(),
            requiredNodes,
            requiredNodes.get(ThreadLocalRandom.current().nextInt(requiredNodes.size())),
            snpName,
            snpPath,
            grpNames,
            0,
            inclCstHndlrs,
            true
        );

        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = new GridFutureAdapter<>();

        clusterOpFut.listen(fut -> clusterOpFuts.remove(reqId));

        stopFutureOnAnyFailure(clusterOpFut, () -> {
            clusterOpFuts.put(reqId, clusterOpFut);

            phase1CheckMetas.start(req.requestId(), req);
        });

        return clusterOpFut;
    }

    /**
     * Ensures thta the future is stopped if any failure occures.
     *
     * @param fut Future to stop. If {@code null}, ignored.
     * @param action Related action to launch and watch.
     */
    private static void stopFutureOnAnyFailure(@Nullable GridFutureAdapter<?> fut, Runnable action) {
        try {
            action.run();
        }
        catch (Throwable th) {
            if (fut != null)
                fut.onDone(th);
        }
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

    /** Converts failure to an exception if it is not. */
    private static Exception asException(Throwable th) {
        return th instanceof Exception ? (Exception)th : new IgniteException(th);
    }

    /** A DTO used to transfer nodes' results for the both phases. Guarantees serializable connections. */
    private static final class CheckResultDTO implements Serializable {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Metas for the pahse 1. Is always {@code null} for the phase 2. */
        @Nullable private final List<SnapshotMetadata> metas;

        /** Node's partition hashes for the phase 2. Is always {@code null} for the phase 1. */
        @Nullable private final Map<PartitionKeyV2, PartitionHashRecordV2> partsHashes;

        /** Ctor for the phase 1. */
        private CheckResultDTO(@Nullable List<SnapshotMetadata> metas) {
            this.metas = metas == null || metas instanceof Serializable ? metas : new ArrayList<>(metas);
            this.partsHashes = null;
        }

        /** Ctor for the phase 2. */
        private CheckResultDTO(@Nullable Map<PartitionKeyV2, PartitionHashRecordV2> partsHashes) {
            this.metas = null;
            this.partsHashes = partsHashes == null || partsHashes instanceof Serializable ? partsHashes : new HashMap<>(partsHashes);
        }
    }
}
