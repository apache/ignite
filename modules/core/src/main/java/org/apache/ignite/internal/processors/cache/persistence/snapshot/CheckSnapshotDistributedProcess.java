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
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
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

import static org.apache.ignite.internal.management.cache.VerifyBackupPartitionsTaskV2.asException;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.SNAPSHOT_CHECK_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.SNAPSHOT_VALIDATE_PARTS;

/** */
public class CheckSnapshotDistributedProcess {
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
            this::finishPreparationAndMetasCheck);

        phase2CalculateParts = new DistributedProcess<>(kctx, SNAPSHOT_VALIDATE_PARTS, this::validateParts,
            this::finishValidatePartsAndProc);
    }

    /** Phase 2 and process finish. */
    private IgniteInternalFuture<?> finishValidatePartsAndProc(
        UUID procId,
        Map<UUID, HashMap<PartitionKeyV2, PartitionHashRecordV2>> results,
        Map<UUID, Throwable> errors
    ) {
        SnapshotCheckOperationRequest locReq = curRequest(null, procId);

        if (locReq.clusterInitiatorFut == null) {
            finish(locReq);

            return new GridFinishedFuture<>();
        }

        finish(locReq, locReq.clusterInitiatorFut, results, errors);

        return locReq.clusterInitiatorFut;
    }

    /** */
    private void finish(
        SnapshotCheckOperationRequest req,
        GridFutureAdapter<IdleVerifyResultV2> clusterProcFut,
        Map<UUID, HashMap<PartitionKeyV2, PartitionHashRecordV2>> results,
        Map<UUID, Throwable> errors
    ) {
        finish(req);

        stopFutureOnAnyFailure(clusterProcFut, () -> {
            assert req.operationalNodeId().equals(kctx.localNodeId());

            IdleVerifyResultV2 res = req.error() == null
                ? VerifyBackupPartitionsTaskV2.reduce(results, errors, kctx.cluster().get())
                : new IdleVerifyResultV2(Collections.singletonMap(kctx.cluster().get().localNode(), asException(req.error())));

            clusterProcFut.onDone(res);

            if (log.isInfoEnabled())
                log.info("Snapshot validation process finished, req: " + req + '.');
        });
    }

    /** Phase 2 beginning. */
    private IgniteInternalFuture<HashMap<PartitionKeyV2, PartitionHashRecordV2>> validateParts(SnapshotCheckOperationRequest incReq) {
        SnapshotCheckOperationRequest locReq = curRequest(incReq.snapshotName(), incReq.requestId());

        assert locReq.equals(incReq);

        GridFutureAdapter<HashMap<PartitionKeyV2, PartitionHashRecordV2>> locPartsChkFut = new GridFutureAdapter<>();

        if (incReq.error() != null)
            locPartsChkFut.onDone(incReq.error());
        else {
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
        }

        return locPartsChkFut;
    }

    /** Finishes snapshot validation cluster process on local node, removes the request. */
    private void finish(SnapshotCheckOperationRequest req) {
        locRequests.remove(req.snapshotName());

        if (log.isInfoEnabled())
            log.info("Finished local snapshot validation, req: " + req + '.');
    }

    /** Phase 1 beginning. */
    private IgniteInternalFuture<ArrayList<SnapshotMetadata>> prepareAndCheckMetas(SnapshotCheckOperationRequest extReq) {
        if (kctx.clientNode())
            return new GridFinishedFuture<>();

        GridFutureAdapter<ArrayList<SnapshotMetadata>> locMetasChkFut = new GridFutureAdapter<>();

        ExecutorService executor = kctx.cache().context().snapshotMgr().snapshotExecutorService();

        withSingletoneSnpOperation(
            extReq.snapshotName(),
            extReq.reqId,
            () -> extReq,
            locReq -> executor.submit(() ->
                stopFutureOnAnyFailure(
                    locMetasChkFut,
                    () -> {
                        if (log.isDebugEnabled())
                            log.debug("Checking local snapshot metadatas, request: " + locReq + '.');

                        assert locReq.requestId().equals(extReq.requestId());

                        Collection<Integer> grpIds = F.isEmpty(locReq.groups()) ? null : F.viewReadOnly(locReq.groups(), CU::cacheId);

                        List<SnapshotMetadata> locMetas = SnapshotMetadataVerificationTask.readAndCheckMetas(kctx, locReq.snapshotName(),
                            locReq.snapshotPath(), locReq.incrementIndex(), grpIds);

                        assert !F.isEmpty(locMetas);

                        locReq.meta(locMetas.get(0));

                        if (!(locMetas instanceof ArrayList))
                            locMetas = new ArrayList<>(locMetas);

                        locMetasChkFut.onDone((ArrayList<SnapshotMetadata>)locMetas);
                    }
                )
            )
        );

        return locMetasChkFut;
    }

    /** Phase 1 end. */
    private void finishPreparationAndMetasCheck(UUID procId, Map<UUID, ArrayList<SnapshotMetadata>> results, Map<UUID, Throwable> errors) {
        SnapshotCheckOperationRequest locReq = curRequest(procId, results);

        if (!F.isEmpty(errors)) {
            if (locReq.clusterInitiatorFut == null)
                finish(locReq);
            else
                finish(locReq, locReq.clusterInitiatorFut, Collections.emptyMap(), errors);

            return;
        }

        if (locReq.clusterInitiatorFut == null)
            return;

        Throwable stopClusterProcErr = null;

        try {
            assert !F.isEmpty(results) || !F.isEmpty(errors);
            assert locReq.clusterInitiatorFut == null || locReq.operationalNodeId().equals(kctx.localNodeId());

            Map<ClusterNode, List<SnapshotMetadata>> resClusterMetas = new HashMap<>();
            Map<ClusterNode, Exception> resClusterErrors = new HashMap<>();

            if (!F.isEmpty(results)) {
                results.forEach((nodeId, metas) -> {
                    ClusterNode clusterNode = kctx.cluster().get().node(nodeId);

                    assert clusterNode != null;
                    assert !F.isEmpty(metas);

                    resClusterMetas.put(clusterNode, metas);
                });
            }

            if (!F.isEmpty(errors)) {
                errors.forEach((nodeId, nodeErr) -> {
                    ClusterNode clusterNode = kctx.cluster().get().node(nodeId);

                    assert clusterNode != null;
                    assert nodeErr != null;

                    resClusterErrors.put(clusterNode, asException(nodeErr));
                });
            }

            SnapshotMetadataVerificationTaskResult metasRes = SnapshotMetadataVerificationTask.reduceClusterResults(
                resClusterMetas,
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

        if (stopClusterProcErr != null) {
            assert locReq.error() == null;

            locReq.error(stopClusterProcErr);
        }

        phase2CalculateParts.start(procId, locReq);

        if (log.isDebugEnabled())
            log.debug("Started partitions validation as part of the snapshot validation, req: " + locReq + '.');
    }

    /** */
    private SnapshotCheckOperationRequest curRequest(UUID procId, @Nullable Map<UUID, ArrayList<SnapshotMetadata>> metasResults) {
        String snpName = null;

        if (!F.isEmpty(metasResults)) {
            assert F.flatCollections(metasResults.values()).stream().map(SnapshotMetadata::snapshotName)
                .collect(Collectors.toSet()).size() == 1 : "Empty or not unique snapshot names in the snapshot metadatas.";

            for (List<SnapshotMetadata> metas : metasResults.values()) {
                if (F.isEmpty(metas))
                    continue;

                assert metas.get(0) != null : "Empty snapshot metadata in the results";
                assert !F.isEmpty(metas.get(0).snapshotName()) : "Empty snapshot name in a snapshot metadata.";

                snpName = metas.get(0).snapshotName();

                break;
            }
        }

        return curRequest(snpName, procId);
    }

    /** */
    public IgniteInternalFuture<IdleVerifyResultV2> start(
        String snpName,
        @Nullable String snpPath,
        @Nullable Collection<String> grpNames,
        int incId,
        boolean inclCstHndlrs
    ) {
        assert !F.isEmpty(snpName);

        UUID procId = UUID.randomUUID();

        GridFutureAdapter<IdleVerifyResultV2> clusterWideOpFut = new GridFutureAdapter<>();

        withSingletoneSnpOperation(
            snpName,
            procId,
            () -> new SnapshotCheckOperationRequest(procId, kctx.localNodeId(), snpName, snpPath, grpNames, incId, inclCstHndlrs, true),
            req -> {
                if (log.isInfoEnabled())
                    log.info("Starting distributed snapshot check process, snpOpReq: " + req + '.');

                req.clusterInitiatorFut = clusterWideOpFut;

                phase1CheckMetas.start(procId, req);
            }
        );

        return clusterWideOpFut;
    }

    /** */
    private void withSingletoneSnpOperation(
        String snpName,
        UUID procId,
        Supplier<SnapshotCheckOperationRequest> reqSp,
        Consumer<SnapshotCheckOperationRequest> action
    ) {
        SnapshotCheckOperationRequest curReq = locRequests.computeIfAbsent(snpName, rq -> reqSp.get());

        synchronized (curReq) {
            if (!curReq.requestId().equals(procId))
                throw new IllegalStateException("Cluster process of snapshot checking is already started, snpOpReq: '" + curReq + '.');

            if (curReq.startTime() == 0)
                curReq.init();

            try {
                assert curReq.snapshotName().equals(snpName);

                action.accept(curReq);
            }
            catch (Throwable th) {
                locRequests.remove(snpName);
            }
        }
    }

    /** */
    private SnapshotCheckOperationRequest curRequest(@Nullable String snpName, UUID procId) {
        SnapshotCheckOperationRequest res;

        if (snpName != null) {
            res = locRequests.get(snpName);

            assert res != null : "Snapshot check process not found for snapshot '" + snpName + "'.";

            return res;
        }

        res = locRequests.values().stream().filter(rq -> rq.requestId().equals(procId)).findFirst()
            .orElse(null);

        assert res != null : "Snapshot check process not found for proceddId '" + procId + "'.";

        return res;
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
}
