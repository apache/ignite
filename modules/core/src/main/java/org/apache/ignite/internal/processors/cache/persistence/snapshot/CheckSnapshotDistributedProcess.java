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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.management.cache.IdleVerifyResultV2;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.management.cache.VerifyBackupPartitionsTaskV2;
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

/** */
public class CheckSnapshotDistributedProcess {
    /** */
    private static final IgniteInternalFuture FINISHED_FUT = new GridFinishedFuture<>();

    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext kctx;

    /** Snapshot check requests per snapshot on every node. */
    private final Map<String, SnapshotCheckOperationRequest> requests = new ConcurrentHashMap<>();

    /** Cluster-wide operation futures per snapshot called from current node. */
    private final Map<UUID, GridFutureAdapter<SnapshotPartitionsVerifyTaskResult>> clusterOpFuts = new ConcurrentHashMap<>();

    /** */
    private final DistributedProcess<SnapshotCheckOperationRequest, ArrayList<SnapshotMetadata>> phase1CheckMetas;

    /** */
    private final DistributedProcess<SnapshotCheckOperationRequest, HashMap<PartitionKeyV2, PartitionHashRecordV2>> phase2CalculateParts;

    /** */
    public CheckSnapshotDistributedProcess(GridKernalContext kctx) {
        this.kctx = kctx;

        log = kctx.log(getClass());

        phase1CheckMetas = new DistributedProcess<>(kctx, SNAPSHOT_CHECK_METAS, this::prepareAndCheckMetas,
            this::reducePreparationAndMetasCheck);

        phase2CalculateParts = new DistributedProcess<>(kctx, SNAPSHOT_VALIDATE_PARTS, this::validateParts,
            this::reduceValidatePartsAndFinishProc);

        kctx.event().addLocalEventListener((evt) -> nodeLeft(((DiscoveryEvent)evt).eventNode()), EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** */
    public void interrupt(Throwable th, @Nullable Function<SnapshotCheckOperationRequest, Boolean> rqFilter) {
        if (requests.isEmpty())
            return;

        requests.values().forEach(rq -> {
            if (rqFilter == null || rqFilter.apply(rq)) {
                rq.error(th);

                clean(rq.requestId(), th, null, null);
            }
        });
    }

    /** */
    private void nodeLeft(ClusterNode node) {
        if (node.isClient() || requests.isEmpty())
            return;

        interrupt(
            new ClusterTopologyCheckedException("Snapshot checking stopped. A node left the cluster: " + node + '.'),
            rq -> rq.nodes().contains(node.id())
        );
    }

    /** Phase 2 and process finish. */
    private IgniteInternalFuture<?> reduceValidatePartsAndFinishProc(
        UUID procId,
        Map<UUID, HashMap<PartitionKeyV2, PartitionHashRecordV2>> results,
        Map<UUID, Throwable> errors
    ) {
        clean(procId, null, results, errors);

        return FINISHED_FUT;
    }

    /** Phase 2 beginning.  */
    private IgniteInternalFuture<HashMap<PartitionKeyV2, PartitionHashRecordV2>> validateParts(SnapshotCheckOperationRequest incReq) {
        SnapshotCheckOperationRequest locReq;

        if (stopAndCleanOnError(incReq, null) || (locReq = requests.get(incReq.snapshotName())) == null)
            return FINISHED_FUT;

        assert locReq.equals(incReq);

        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.get(incReq.requestId());

        if (clusterOpFut != null)
            locReq.metas = incReq.metas;

        if (!incReq.nodes.contains(kctx.localNodeId()) || locReq.meta() == null)
            return FINISHED_FUT;

        GridFutureAdapter<HashMap<PartitionKeyV2, PartitionHashRecordV2>> locPartsChkFut = new GridFutureAdapter<>();

        locReq.fut = locPartsChkFut;

        ExecutorService executor = kctx.cache().context().snapshotMgr().snapshotExecutorService();

        executor.submit(() -> stopFutureOnAnyFailure(locPartsChkFut, () -> {
            File snpDir = kctx.cache().context().snapshotMgr().snapshotLocalDir(locReq.snapshotName(), locReq.snapshotPath());

            SnapshotHandlerContext hndCtx = new SnapshotHandlerContext(locReq.meta(), locReq.grps,
                kctx.cluster().get().localNode(), snpDir, false, true);

            try {
                Map<PartitionKeyV2, PartitionHashRecordV2> res = new SnapshotPartitionsVerifyHandler(kctx.cache().context())
                    .invoke(hndCtx);

                locPartsChkFut.onDone(res instanceof HashMap ? (HashMap<PartitionKeyV2, PartitionHashRecordV2>)res
                    : new HashMap<>(res));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to calculate snapshot partition hashes, req: " + incReq, e);
            }
        }));

        return locPartsChkFut;
    }

    /** */
    private boolean stopAndCleanOnError(SnapshotCheckOperationRequest req, @Nullable Map<UUID, Throwable> occuredErrors) {
        assert req != null;

        if (!F.isEmpty(occuredErrors))
            occuredErrors = occuredErrors.entrySet().stream()
                .filter(e -> req.nodes.contains(e.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (F.isEmpty(occuredErrors) && req.error() == null)
            return false;

        clean(req.requestId(), req.error(), null, occuredErrors);

        return true;
    }

    /** */
    private void clean(
        UUID rqId,
        @Nullable Throwable propogatedError,
        @Nullable Map<UUID, ? extends Map<PartitionKeyV2, PartitionHashRecordV2>> results,
        @Nullable Map<UUID, Throwable> errors
    ) {
        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.get(rqId);

        stopFutureOnAnyFailure(clusterOpFut, () -> {
            SnapshotCheckOperationRequest locRq = curRequest(null, rqId);

            Throwable err = propogatedError;

            if (locRq != null) {
                if (err == null && locRq.error() != null)
                    err = locRq.error();

                GridFutureAdapter<?> locWorkingFut = locRq.fut;

                if (err != null && locWorkingFut != null)
                    locWorkingFut.onDone(err);

                if (requests.remove(locRq.snapshotName()) != null && log.isInfoEnabled())
                    log.info("Finished snapshot local validation, req: " + locRq + '.');
            }

            assert err != null || results != null || !F.isEmpty(errors);

            if (clusterOpFut == null)
                return;

            boolean finished;

            if (err != null || !F.isEmpty(errors)) {
                finished = SnapshotMetadataVerificationTask.finishClusterFutureWithErr(clusterOpFut, err,
                    collectProperErrors(errors, locRq.nodes));
            }
            else {
                assert locRq != null;

                Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> results0 = collecProperResults(results, locRq.nodes());

                IdleVerifyResultV2 chkRes = VerifyBackupPartitionsTaskV2.reduce(results0, Collections.emptyMap());

                finished = clusterOpFut.onDone(new SnapshotPartitionsVerifyTaskResult(locRq.metas, chkRes));
            }

            if (finished && log.isInfoEnabled())
                log.info("Snapshot validation process finished, req: " + locRq + '.');
        });
    }

    /** */
    private Map<ClusterNode, Exception> collectProperErrors(@Nullable Map<UUID, Throwable> errors, Set<UUID> requiredNodes) {
        if (errors == null)
            return Collections.emptyMap();

        return errors.entrySet().stream().filter(e -> requiredNodes.contains(e.getKey()) && e.getValue() != null)
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> asException(e.getValue())));
    }

    /** */
    private Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> collecProperResults(
        @Nullable Map<UUID, ? extends Map<PartitionKeyV2, PartitionHashRecordV2>> results,
        Collection<UUID> requiredNodes
    ) {
        if (results == null)
            return Collections.emptyMap();

        return results.entrySet().stream()
            .filter(e -> requiredNodes.contains(e.getKey()) && e.getValue() != null)
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), Map.Entry::getValue));
    }

    /** */
    private @Nullable SnapshotCheckOperationRequest curRequest(@Nullable String snpName, UUID procId) {
        return snpName == null
            ? requests.values().stream().filter(rq -> rq.requestId().equals(procId)).findFirst().orElse(null)
            : requests.get(snpName);
    }

    /** Phase 1 beginning. */
    private IgniteInternalFuture<ArrayList<SnapshotMetadata>> prepareAndCheckMetas(SnapshotCheckOperationRequest extReq) {
        SnapshotCheckOperationRequest locReq = requests.computeIfAbsent(extReq.snapshotName(), nmae -> extReq);

        if (!locReq.equals(extReq)) {
            Throwable err = new IllegalStateException("Validation of snapshot '" + extReq.snapshotName()
                + "' has already started. Request=" + locReq + '.');

            clean(extReq.requestId(), err, null, null);

            return new GridFinishedFuture<>(err);
        }

        if (!extReq.nodes.contains(kctx.localNodeId()))
            return FINISHED_FUT;

        assert locReq.fut == null;

        GridFutureAdapter<ArrayList<SnapshotMetadata>> locMetasChkFut = new GridFutureAdapter<>();

        locReq.fut = locMetasChkFut;

        kctx.cache().context().snapshotMgr().snapshotExecutorService().submit(() -> stopFutureOnAnyFailure(locMetasChkFut, () -> {
            if (log.isDebugEnabled())
                log.debug("Checking local snapshot metadatas, request: " + locReq + '.');

            Collection<Integer> grpIds = F.isEmpty(locReq.groups()) ? null : F.viewReadOnly(locReq.groups(), CU::cacheId);

            List<SnapshotMetadata> locMetas = SnapshotMetadataVerificationTask.readAndCheckMetas(kctx, locReq.snapshotName(),
                locReq.snapshotPath(), locReq.incrementIndex(), grpIds);

            if (!F.isEmpty(locMetas))
                locReq.meta(locMetas.get(0));

            if (!(locMetas instanceof ArrayList))
                locMetas = new ArrayList<>(locMetas);

            // A node might have already gone before this. No need to proceed.
            if (locReq.error() != null)
                locMetasChkFut.onDone(locReq.error());
            else
                locMetasChkFut.onDone((ArrayList<SnapshotMetadata>)locMetas);
        }));

        return locMetasChkFut;
    }

    /** Phase 1 end. */
    private void reducePreparationAndMetasCheck(
        UUID procId,
        Map<UUID, ? extends List<SnapshotMetadata>> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckOperationRequest locReq = curRequest(snpName(results), procId);

        assert locReq == null || locReq.opCoordId != null;

        if (locReq == null || stopAndCleanOnError(locReq, errors) || !kctx.localNodeId().equals(locReq.opCoordId))
            return;

        Throwable stopClusterProcErr = null;

        try {
            assert !F.isEmpty(results) || !F.isEmpty(errors);

            if (locReq.error() != null)
                throw locReq.error();

            locReq.metas = new HashMap<>();

            Map<ClusterNode, Exception> resClusterErrors = new HashMap<>();

            results.forEach((nodeId, metas) -> {
                // A node might be non-baseline.
                if (locReq.nodes.contains(nodeId)) {
                    assert kctx.cluster().get().node(nodeId) != null;

                    locReq.metas.put(kctx.cluster().get().node(nodeId), metas);
                }
            });

            errors.forEach((nodeId, nodeErr) -> {
                // A node might be non-baseline.
                if (locReq.nodes.contains(nodeId)) {
                    assert kctx.cluster().get().node(nodeId) != null;
                    assert nodeErr != null;

                    resClusterErrors.put(kctx.cluster().get().node(nodeId), asException(nodeErr));
                }
            });

            SnapshotMetadataVerificationTaskResult metasRes = SnapshotMetadataVerificationTask.reduceClusterResults(
                locReq.metas,
                resClusterErrors,
                locReq.snapshotName(),
                locReq.snapshotPath(),
                kctx.cluster().get().localNode()
            );

            if (!F.isEmpty(metasRes.exceptions()))
                stopClusterProcErr = new IgniteSnapshotVerifyException(metasRes.exceptions());
        }
        catch (Throwable th) {
            stopClusterProcErr = th;
        }

        if (stopClusterProcErr != null)
            locReq.error(stopClusterProcErr);

        phase2CalculateParts.start(procId, locReq);

        if (log.isDebugEnabled())
            log.debug("Started partitions validation as part of the snapshot validation, req: " + locReq + '.');
    }

    /** Finds current snapshot name from the metas. */
    private @Nullable String snpName(@Nullable Map<UUID, ? extends List<SnapshotMetadata>> metas) {
        if (F.isEmpty(metas))
            return null;

        assert F.flatCollections(metas.values().stream().filter(Objects::nonNull).collect(Collectors.toList()))
            .stream().map(SnapshotMetadata::snapshotName).collect(Collectors.toSet()).size() < 2
            : "Empty or not unique snapshot names in the snapshot metadatas.";

        for (List<SnapshotMetadata> nodeMetas : metas.values()) {
            if (F.isEmpty(nodeMetas))
                continue;

            assert nodeMetas.get(0) != null : "Empty snapshot metadata in the results";
            assert !F.isEmpty(nodeMetas.get(0).snapshotName()) : "Empty snapshot name in a snapshot metadata.";

            return nodeMetas.get(0).snapshotName();
        }

        return null;
    }

    /** */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> start(
        String snpName,
        @Nullable String snpPath,
        @Nullable Collection<String> grpNames,
        boolean inclCstHndlrs
    ) {
        assert !F.isEmpty(snpName);

        UUID rqId = UUID.randomUUID();

        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = new GridFutureAdapter<>();

        clusterOpFut.listen(fut -> clusterOpFuts.remove(rqId));

        stopFutureOnAnyFailure(clusterOpFut, () -> {
            List<UUID> requiredNodes = new ArrayList<>(F.viewReadOnly(kctx.discovery().discoCache().aliveBaselineNodes(), F.node2id()));

            SnapshotCheckOperationRequest req = new SnapshotCheckOperationRequest(
                rqId,
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

            phase1CheckMetas.start(req.requestId(), req);

            clusterOpFuts.put(rqId, clusterOpFut);
        });

        return clusterOpFut;
    }

    /** */
    private static void stopFutureOnAnyFailure(@Nullable GridFutureAdapter<?> fut, Runnable action) {
        if (fut == null) {
            action.run();

            return;
        }

        try {
            action.run();
        }
        catch (Throwable th) {
            fut.onDone(th);
        }
    }

    /** */
    private static Exception asException(Throwable th) {
        return th instanceof Exception ? (Exception)th : new IgniteException(th);
    }
}
