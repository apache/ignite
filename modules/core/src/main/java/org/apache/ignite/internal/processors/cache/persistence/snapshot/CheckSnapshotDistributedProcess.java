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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
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
import static org.apache.ignite.internal.management.cache.VerifyBackupPartitionsTaskV2.asException;
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

    /** Local snapshot check requests pre snapshot on every actual node. */
    private final Map<String, SnapshotCheckOperationRequest> locRequests = new ConcurrentHashMap<>();

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

        kctx.event().addLocalEventListener((evt) -> {
            if (!CU.baselineNode(evt.node(), kctx.state().clusterState()))
                return;

            // Discovery-synchronized.
            cancelAll(new ClusterTopologyCheckedException("A baseline node left the cluster: " + evt.node() + '.'));
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** Must be discovery-synchronized.*/
    private void cancelAll(@Nullable Throwable th) {
        locRequests.values().forEach(r -> {
            r.error(th);

            if (r.clusterInitiatorFut == null)
                cleanLocalRequest(r);
            else
                cleanLocalRequestAndClusterFut(r, Collections.emptyMap(), null);
        });
    }

    /** Phase 2 and process finish. */
    private IgniteInternalFuture<?> reduceValidatePartsAndFinishProc(
        UUID procId,
        Map<UUID, HashMap<PartitionKeyV2, PartitionHashRecordV2>> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckOperationRequest locReq = skipAndClean(procId, null, errors);

        if (locReq == null)
            return FINISHED_FUT;

        if (locReq.clusterInitiatorFut == null) {
            cleanLocalRequest(locReq);

            return FINISHED_FUT;
        }

        cleanLocalRequestAndClusterFut(locReq, results, errors);

        return locReq.clusterInitiatorFut;
    }

    /** */
    private void cleanLocalRequestAndClusterFut(
        SnapshotCheckOperationRequest req,
        Map<UUID, ? extends Map<PartitionKeyV2, PartitionHashRecordV2>> results,
        @Nullable Map<UUID, Throwable> errors
    ) {
        assert req.clusterInitiatorFut != null;

        cleanLocalRequest(req);

        Throwable rqErr = req.error();
        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clFut = req.clusterInitiatorFut;

        Map<UUID, Throwable> errors0 = errors == null ? Collections.emptyMap() : errors;

        stopFutureOnAnyFailure(clFut, () -> {
            assert req.operationalNodeId().equals(kctx.localNodeId());

            if ((rqErr == null && clFut.onDone(new SnapshotPartitionsVerifyTaskResult(req.metas,
                VerifyBackupPartitionsTaskV2.reduce(results, errors0, kctx.cluster().get()))))
                || (rqErr != null && clFut.onDone(rqErr))) {
                if (log.isInfoEnabled())
                    log.info("Snapshot validation process finished, req: " + req + '.');
            }
        });
    }

    /** Phase 2 beginning. Discovery-synchronized. */
    private IgniteInternalFuture<HashMap<PartitionKeyV2, PartitionHashRecordV2>> validateParts(SnapshotCheckOperationRequest incReq) {
        SnapshotCheckOperationRequest locReq = skipAndClean(incReq.snapshotName(), incReq.error(), null);

        if (locReq == null)
            return FINISHED_FUT;

        assert locReq.equals(incReq);

        GridFutureAdapter<HashMap<PartitionKeyV2, PartitionHashRecordV2>> locPartsChkFut = new GridFutureAdapter<>();

        locReq.fut = locPartsChkFut;

        ExecutorService executor = kctx.cache().context().snapshotMgr().snapshotExecutorService();

        executor.submit(() ->
            stopFutureOnAnyFailure(locPartsChkFut, () -> {
                assert locReq.meta() != null;

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
                    throw new IgniteException("Failed to calculate snapshot partition hashes, req: " + locReq, e);
                }
            })
        );

        return locPartsChkFut;
    }

    /**  */
    private void cleanLocalRequest(SnapshotCheckOperationRequest req) {
        Throwable err = req.error();
        GridFutureAdapter<?> locFut = req.fut;

        if (err != null && locFut != null)
            locFut.onDone(err);

        locRequests.remove(req.snapshotName());

        if (log.isInfoEnabled())
            log.info("Finished local snapshot validation, req: " + req + '.');
    }

    /** Phase 1 beginning. Discovery-synchronized. */
    private IgniteInternalFuture<ArrayList<SnapshotMetadata>> prepareAndCheckMetas(SnapshotCheckOperationRequest extReq) {
        if (skip())
            return FINISHED_FUT;

        SnapshotCheckOperationRequest locReq = locRequests.computeIfAbsent(extReq.snapshotName(), nmae -> extReq);

        assert extReq == locReq;
        assert locReq.fut == null;
        assert locReq.clusterInitiatorFut == null || kctx.localNodeId().equals(locReq.operationalNodeId());

        GridFutureAdapter<ArrayList<SnapshotMetadata>> locMetasChkFut = new GridFutureAdapter<>();

        kctx.cache().context().snapshotMgr().snapshotExecutorService().submit(() -> {
            locReq.fut = locMetasChkFut;

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
        });

        return locMetasChkFut;
    }

    /** Phase 1 end. */
    private void reducePreparationAndMetasCheck(
        UUID procId,
        Map<UUID, ? extends List<SnapshotMetadata>> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckOperationRequest locReq = skipAndClean(procId, results, errors);

        if (locReq == null || locReq.clusterInitiatorFut == null)
            return;

        Throwable stopClusterProcErr = null;

        try {
            assert !F.isEmpty(results) || !F.isEmpty(errors);
            assert locReq.clusterInitiatorFut == null || locReq.operationalNodeId().equals(kctx.localNodeId());

            if (locReq.error() != null)
                throw locReq.error();

            locReq.metas = new HashMap<>();

            Map<ClusterNode, Exception> resClusterErrors = new HashMap<>();

            results.forEach((nodeId, metas) -> {
                ClusterNode clusterNode = kctx.cluster().get().node(nodeId);

                assert clusterNode != null;

                locReq.metas.put(clusterNode, metas);
            });

            if (!F.isEmpty(errors)) {
                errors.forEach((nodeId, nodeErr) -> {
                    ClusterNode clusterNode = kctx.cluster().get().node(nodeId);

                    assert clusterNode != null;
                    assert nodeErr != null;

                    resClusterErrors.put(clusterNode, asException(nodeErr));
                });
            }

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

    /** Finds current request by snapshot name from the metas or by the process id. */
    private @Nullable SnapshotCheckOperationRequest curRequest(UUID procId, @Nullable Map<UUID, ? extends List<SnapshotMetadata>> metas) {
        String snpName = null;

        if (metas != null) {
            assert F.flatCollections(metas.values()).stream().map(SnapshotMetadata::snapshotName)
                .collect(Collectors.toSet()).size() < 2 : "Empty or not unique snapshot names in the snapshot metadatas.";

            for (List<SnapshotMetadata> nodeMetas : metas.values()) {
                if (F.isEmpty(nodeMetas))
                    continue;

                assert nodeMetas.get(0) != null : "Empty snapshot metadata in the results";
                assert !F.isEmpty(nodeMetas.get(0).snapshotName()) : "Empty snapshot name in a snapshot metadata.";

                snpName = nodeMetas.get(0).snapshotName();

                break;
            }
        }

        return curRequest(snpName, procId);
    }

    /** */
    public IgniteInternalFuture<SnapshotPartitionsVerifyTaskResult> start(
        String snpName,
        @Nullable String snpPath,
        @Nullable Collection<String> grpNames,
        boolean inclCstHndlrs
    ) {
        assert !F.isEmpty(snpName);

        UUID procId = UUID.randomUUID();

        SnapshotCheckOperationRequest rq = locRequests.computeIfAbsent(snpName, name -> {
            SnapshotCheckOperationRequest req = new SnapshotCheckOperationRequest(procId, kctx.localNodeId(), snpName, snpPath,
                grpNames, 0, inclCstHndlrs, true);

            req.clusterInitiatorFut = new GridFutureAdapter<>();

            if (log.isInfoEnabled())
                log.info("Starting distributed snapshot check process, snpOpReq: " + req + '.');

            phase1CheckMetas.start(procId, req);

            return req;
        });

        if (rq.requestId().equals(procId) && rq.operationalNodeId().equals(kctx.localNodeId()))
            return rq.clusterInitiatorFut;

        throw new IllegalStateException("Snapshot validation started of snapshot '" + snpName + "' is already, request: " + rq + '.');
    }

    /** */
    private @Nullable SnapshotCheckOperationRequest curRequest(@Nullable String snpName, UUID procId) {
        return snpName == null
            ? locRequests.values().stream().filter(rq -> rq.requestId().equals(procId)).findFirst().orElse(null)
            : locRequests.get(snpName);
    }

    /** */
    private static void stopFutureOnAnyFailure(GridFutureAdapter<?> fut, Runnable action) {
        try {
            action.run();
        }
        catch (Throwable th) {
            fut.onDone(th);
        }
    }

    /**
     * @param procId Process or snapshot operation request id.
     * @param metasResults Nodes metas. If not {@code null}, is used to find the snapshot name.
     * @param errors Nodes errors. If not {@code null}, is used to find an snapsot check proces error and stop it.
     *
     * @return {@code Null} if current node must not check snapshot or if the process locally stopped and cleaned due
     * to the errors. Current check operation request otherwise.
     */
    private @Nullable SnapshotCheckOperationRequest skipAndClean(
        UUID procId,
        @Nullable Map<UUID, ? extends List<SnapshotMetadata>> metasResults,
        @Nullable Map<UUID, Throwable> errors
    ) {
        SnapshotCheckOperationRequest req = skip() ? null : curRequest(procId, metasResults);

        return req == null ? null : skipAndClean(req.snapshotName(), req.error(), errors);
    }

    /**
     * @return {@code Null} if current node must not check snapshot or if the process locally stopped and cleaned due
     * to the errors. Current check operation request otherwise.
     */
    private @Nullable SnapshotCheckOperationRequest skipAndClean(
        String snapshotName,
        @Nullable Throwable propagatedError,
        @Nullable Map<UUID, Throwable> nodeErrors
    ) {
        return skipAndClean(locRequests.get(snapshotName), propagatedError, nodeErrors);
    }

    /**
     * @return {@code Null} if current node must not check snapshot or if the process locally stopped and cleaned due
     * to the errors. Current check operation request otherwise.
     */
    private @Nullable SnapshotCheckOperationRequest skipAndClean(
        SnapshotCheckOperationRequest locReq,
        @Nullable Throwable propagatedError,
        @Nullable Map<UUID, Throwable> nodeErrors
    ) {
        if (locReq != null && (!F.isEmpty(nodeErrors) || propagatedError != null)) {
            if (propagatedError != null)
                locReq.error(propagatedError);

            if (locReq.clusterInitiatorFut == null)
                cleanLocalRequest(locReq);
            else
                cleanLocalRequestAndClusterFut(locReq, Collections.emptyMap(), nodeErrors);

            return null;
        }

        return locReq;
    }

    /** @return {@code True} if current node must not check a snapshot. */
    private boolean skip() {
        return kctx.clientNode() || !CU.baselineNode(kctx.cluster().get().localNode(), kctx.state().clusterState());
    }
}
