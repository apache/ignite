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
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.SNAPSHOT_CHECK_METAS;
import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.SNAPSHOT_VALIDATE_PARTS;

/** Distributed process of snapshot checking (with the partition hashes). */
public class SnapshotCheckProcess {
    /** */
    private static final String METRIC_REG_NAME_PREF = metricName(SNAPSHOT_METRICS, "check");

    /** */
    static final String METRIC_NAME_TOTAL = "total";

    /** */
    static final String METRIC_NAME_PROCESSED = "processed";
    /** */

    static final String METRIC_NAME_PROGRESS = "progress";

    /** */
    static final String METRIC_NAME_START_TIME = "startTime";

    /** */
    static final String METRIC_NAME_RQ_ID = "requestId";

    /** */
    static final String METRIC_NAME_SNP_NAME = "snapshotName";

    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext kctx;

    /** Snapshot check requests per snapshot on every node. */
    private final Map<String, SnapshotCheckProcessRequest> requests = new ConcurrentHashMap<>();

    /** Cluster-wide operation futures per snapshot called from current node. */
    private final Map<UUID, GridFutureAdapter<SnapshotPartitionsVerifyTaskResult>> clusterOpFuts = new ConcurrentHashMap<>();

    /** Check metas first phase subprocess. */
    private final DistributedProcess<SnapshotCheckProcessRequest, ArrayList<SnapshotMetadata>> phase1CheckMetas;

    /** Partition hashes second phase subprocess.  */
    private final DistributedProcess<SnapshotCheckProcessRequest, HashMap<PartitionKeyV2, PartitionHashRecordV2>> phase2PartsHashes;

    /** */
    public SnapshotCheckProcess(GridKernalContext kctx) {
        this.kctx = kctx;

        log = kctx.log(getClass());

        phase1CheckMetas = new DistributedProcess<>(kctx, SNAPSHOT_CHECK_METAS, this::prepareAndCheckMetas,
            this::reducePreparationAndMetasCheck);

        phase2PartsHashes = new DistributedProcess<>(kctx, SNAPSHOT_VALIDATE_PARTS, this::validateParts,
            this::reduceValidatePartsAndFinish);

        kctx.event().addLocalEventListener((evt) -> nodeLeft(((DiscoveryEvent)evt).eventNode()), EVT_NODE_FAILED, EVT_NODE_LEFT);
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
    public void interrupt(Throwable th, @Nullable Function<SnapshotCheckProcessRequest, Boolean> rqFilter) {
        requests.values().forEach(rq -> {
            if (rqFilter == null || rqFilter.apply(rq)) {
                rq.error(th);

                clean(rq.requestId(), th, null, null);
            }
        });
    }

    /** Stops the related validation if the node is a mandatory one. */
    private void nodeLeft(ClusterNode node) {
        if (node.isClient() || requests.isEmpty())
            return;

        interrupt(
            new ClusterTopologyCheckedException("Snapshot checking stopped. A node left the cluster: " + node + '.'),
            rq -> rq.nodes().contains(node.id())
        );
    }

    /** Phase 2 and process finish. */
    private IgniteInternalFuture<?> reduceValidatePartsAndFinish(
        UUID procId,
        Map<UUID, HashMap<PartitionKeyV2, PartitionHashRecordV2>> results,
        Map<UUID, Throwable> errors
    ) {
        clean(procId, null, results, errors);

        return new GridFinishedFuture<>();
    }

    /** Phase 2 beginning.  */
    private IgniteInternalFuture<HashMap<PartitionKeyV2, PartitionHashRecordV2>> validateParts(SnapshotCheckProcessRequest req) {
        SnapshotCheckProcessRequest locReq;

        if (stopAndCleanOnError(req, null) || (locReq = requests.get(req.snapshotName())) == null)
            return new GridFinishedFuture<>();

        assert locReq.equals(req);

        // Store metas to collect cluster operation result later.
        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.get(req.requestId());

        if (clusterOpFut != null)
            locReq.metas = req.metas;

        // Local meta might be null if current node started after the snapshot creation or placement.
        if (!req.nodes.contains(kctx.localNodeId()) || locReq.meta() == null)
            return new GridFinishedFuture<>();

        GridFutureAdapter<HashMap<PartitionKeyV2, PartitionHashRecordV2>> locPartsChkFut = new GridFutureAdapter<>();

        locReq.fut(locPartsChkFut);

        ExecutorService executor = kctx.cache().context().snapshotMgr().snapshotExecutorService();

        executor.submit(() -> stopFutureOnAnyFailure(locPartsChkFut, () -> {
            // An error can occure when the local future is still null.
            if (locReq.error() != null)
                locPartsChkFut.onDone(locReq.error());

            if (locPartsChkFut.isDone())
                return;

            File snpDir = kctx.cache().context().snapshotMgr().snapshotLocalDir(locReq.snapshotName(), locReq.snapshotPath());

            MetricRegistryImpl mreg = kctx.metric().registry(metricsRegName(locReq.snapshotName()));

            try {
                Map<PartitionKeyV2, PartitionHashRecordV2> res = kctx.cache().context().snapshotMgr().checker()
                    .checkPartitions(locReq.meta(), snpDir, locReq.groups(), false, true, false,
                        mreg.findMetric(METRIC_NAME_TOTAL), mreg.findMetric(METRIC_NAME_PROCESSED));

                locPartsChkFut.onDone(res instanceof HashMap ? (HashMap<PartitionKeyV2, PartitionHashRecordV2>)res
                    : new HashMap<>(res));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to calculate snapshot partition hashes, req: " + req, e);
            }

            // No need to wait to the clean if current node is just a worker.
            if (!kctx.localNodeId().equals(locReq.opCoordId) && clusterOpFut == null)
                clean(locReq.reqId, null, null, null);
        }));

        return locPartsChkFut;
    }

    /**
     * If required, stops and clean related validation if errors occured
     *
     * @return {@code True} if the validation was stopped and cleaned. {@code False} otherwise.
     */
    private boolean stopAndCleanOnError(SnapshotCheckProcessRequest req, @Nullable Map<UUID, Throwable> occuredErrors) {
        assert req != null;

        assert F.isEmpty(occuredErrors) || req.nodes().containsAll(occuredErrors.keySet());

        if (F.isEmpty(occuredErrors) && req.error() == null)
            return false;

        clean(req.requestId(), req.error(), null, occuredErrors);

        return true;
    }

    /** Cleans certain snapshot validation. */
    private void clean(
        UUID rqId,
        @Nullable Throwable opErr,
        @Nullable Map<UUID, ? extends Map<PartitionKeyV2, PartitionHashRecordV2>> results,
        @Nullable Map<UUID, Throwable> errors
    ) {
        GridFutureAdapter<SnapshotPartitionsVerifyTaskResult> clusterOpFut = clusterOpFuts.remove(rqId);

        stopFutureOnAnyFailure(clusterOpFut, () -> {
            SnapshotCheckProcessRequest locRq = currentRequest(null, rqId);

            Throwable err = opErr;

            if (locRq != null)
                err = stopAndCleanLocRequest(locRq, err);

            if (clusterOpFut == null || clusterOpFut.isDone())
                return;

            boolean finished;

            // Nodes' results and errors reducing is available on a required node where the process request must be registered.
            assert (F.isEmpty(errors) && F.isEmpty(results)) || locRq != null;

            Map<ClusterNode, Exception> errors0 = locRq == null || F.isEmpty(errors)
                ? Collections.emptyMap()
                : collectErrors(errors, locRq.nodes());

            if (err == null && !F.isEmpty(results)) {
                Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> results0 = collectPartsHashes(results, locRq.nodes());

                IdleVerifyResultV2 chkRes = SnapshotChecker.reduceHashesResults(results0, errors0);

                finished = clusterOpFut.onDone(new SnapshotPartitionsVerifyTaskResult(locRq.metas, chkRes));
            }
            else
                finished = finishClusterFutureWithErr(clusterOpFut, err, errors0);

            if (finished && log.isInfoEnabled())
                log.info("Snapshot validation process finished, req: " + locRq + '.');
        });
    }

    /** */
    private Throwable stopAndCleanLocRequest(SnapshotCheckProcessRequest locRq, @Nullable Throwable err) {
        if (err == null && locRq.error() != null)
            err = locRq.error();

        GridFutureAdapter<?> locWorkingFut = locRq.fut();

        boolean finished = false;

        // Try to stop local working future ASAP.
        if (locWorkingFut != null)
            finished = err == null ? locWorkingFut.onDone() : locWorkingFut.onDone(err);

        kctx.metric().remove(metricsRegName(locRq.snapshotName()));

        requests.remove(locRq.snapshotName());

        if (finished && log.isInfoEnabled())
            log.info("Finished snapshot local validation, req: " + locRq + '.');

        return err;
    }

    /** */
    static String metricsRegName(String snpName) {
        return metricName(METRIC_REG_NAME_PREF, snpName);
    }

    /** */
    private Map<ClusterNode, Exception> collectErrors(Map<UUID, Throwable> errors, Set<UUID> requiredNodes) {
        if (errors.isEmpty())
            return Collections.emptyMap();

        return errors.entrySet().stream().filter(e -> requiredNodes.contains(e.getKey()) && e.getValue() != null)
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), e -> asException(e.getValue())));
    }

    /** */
    private Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> collectPartsHashes(
        @Nullable Map<UUID, ? extends Map<PartitionKeyV2, PartitionHashRecordV2>> results,
        Collection<UUID> requiredNodes
    ) {
        if (results == null)
            return Collections.emptyMap();

        return results.entrySet().stream()
            .filter(e -> requiredNodes.contains(e.getKey()) && e.getValue() != null)
            .collect(Collectors.toMap(e -> kctx.cluster().get().node(e.getKey()), Map.Entry::getValue));
    }

    /**
     * @param snpName Snapshot name of the validation process. If {@coe null}, ignored.
     * @param procId  If {@code snpName} is {@code null}, is used to find the operation request.
     * @return Current snapshot checking request by {@code snpName} or {@code procId}.
     */
    private @Nullable SnapshotCheckProcessRequest currentRequest(@Nullable String snpName, UUID procId) {
        return snpName == null
            ? requests.values().stream().filter(rq -> rq.requestId().equals(procId)).findFirst().orElse(null)
            : requests.get(snpName);
    }

    /** Phase 1 beginning: prepare, collect and check local metas. */
    private IgniteInternalFuture<ArrayList<SnapshotMetadata>> prepareAndCheckMetas(SnapshotCheckProcessRequest extReq) {
        SnapshotCheckProcessRequest locReq = requests.computeIfAbsent(extReq.snapshotName(), snpName -> extReq);

        if (!locReq.equals(extReq)) {
            Throwable err = new IllegalStateException("Validation of snapshot '" + extReq.snapshotName()
                + "' has already started. Request=" + locReq + '.');

            clean(extReq.requestId(), err, null, null);

            return new GridFinishedFuture<>(err);
        }

        if (!extReq.nodes.contains(kctx.localNodeId())) {
            if (log.isDebugEnabled()) {
                log.debug("Skipping snapshot local metadatas collecting for snapshot validation, request=" + extReq
                    + ". Current node is not required.");
            }

            return new GridFinishedFuture<>();
        }

        registerMetrics(locReq);

        GridFutureAdapter<ArrayList<SnapshotMetadata>> locMetasChkFut = new GridFutureAdapter<>();

        assert locReq.fut() == null;

        locReq.fut(locMetasChkFut);

        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        snpMgr.snapshotExecutorService().submit(() -> stopFutureOnAnyFailure(locMetasChkFut, () -> {
            if (log.isDebugEnabled())
                log.debug("Checking local snapshot metadatas. Request=" + locReq + '.');

            Collection<Integer> grpIds = F.isEmpty(locReq.groups()) ? null : F.viewReadOnly(locReq.groups(), CU::cacheId);

            // An error can occure when the local future is still null.
            if (locReq.error() != null)
                locMetasChkFut.onDone(locReq.error());

            if (locMetasChkFut.isDone())
                return;

            List<SnapshotMetadata> locMetas = snpMgr.checker().checkLocalMetas(
                snpMgr.snapshotLocalDir(locReq.snapshotName(), locReq.snapshotPath()),
                grpIds,
                kctx.cluster().get().localNode().consistentId()
            );

            if (!F.isEmpty(locMetas))
                locReq.meta(locMetas.get(0));

            if (!(locMetas instanceof ArrayList))
                locMetas = new ArrayList<>(locMetas);

            // A node might have already gone before this. No need to proceed.
            Throwable locRqErr = locReq.error();

            if (locRqErr != null)
                locMetasChkFut.onDone(locRqErr);
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
        SnapshotCheckProcessRequest locReq = currentRequest(snpName(results), procId);

        assert locReq == null || locReq.opCoordId != null;

        if (locReq == null || stopAndCleanOnError(locReq, errors) || !kctx.localNodeId().equals(locReq.opCoordId))
            return;

        Throwable stopClusterProcErr = null;

        try {
            assert !F.isEmpty(results) || !F.isEmpty(errors) || locReq.error() != null;

            if (locReq.error() != null)
                throw locReq.error();

            locReq.metas = new HashMap<>();

            Map<ClusterNode, Exception> errs = new HashMap<>();

            results.forEach((nodeId, metas) -> {
                // A node might be non-baseline (not required).
                if (locReq.nodes.contains(nodeId)) {
                    assert kctx.cluster().get().node(nodeId) != null;

                    locReq.metas.put(kctx.cluster().get().node(nodeId), metas);
                }
            });

            errors.forEach((nodeId, nodeErr) -> {
                // A node might be non-baseline (not required).
                if (locReq.nodes.contains(nodeId)) {
                    assert kctx.cluster().get().node(nodeId) != null;
                    assert nodeErr != null;

                    errs.put(kctx.cluster().get().node(nodeId), asException(nodeErr));
                }
            });

            SnapshotMetadataVerificationTaskResult metasRes = new SnapshotMetadataVerificationTaskResult(
                locReq.metas,
                SnapshotChecker.reduceMetasResults(locReq.snapshotName(), locReq.snapshotPath(), locReq.metas, errs,
                    kctx.cluster().get().localNode().consistentId())
            );

            if (!F.isEmpty(metasRes.exceptions()))
                stopClusterProcErr = new IgniteSnapshotVerifyException(metasRes.exceptions());
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
    private @Nullable String snpName(@Nullable Map<UUID, ? extends List<SnapshotMetadata>> metas) {
        if (F.isEmpty(metas))
            return null;

        // Ensure the same snapshot name or empty.
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

    /** Starts the snapshot full validation. */
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

            SnapshotCheckProcessRequest req = new SnapshotCheckProcessRequest(
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
    private void registerMetrics(SnapshotCheckProcessRequest rq) {
        MetricRegistryImpl mreg = kctx.metric().registry(metricsRegName(rq.snapshotName()));

        assert mreg.findMetric(METRIC_NAME_START_TIME) == null;
        assert mreg.findMetric(METRIC_NAME_RQ_ID) == null;

        mreg.register(METRIC_NAME_RQ_ID, rq::requestId, UUID.class, "Snapshot operation request id.");
        mreg.register(METRIC_NAME_SNP_NAME, rq::snapshotName, String.class, "Snapshot name.");
        mreg.register(METRIC_NAME_START_TIME, rq::startTime, "Snapshot check start time in milliseconds.");

        AtomicLongMetric total = mreg.longMetric(METRIC_NAME_TOTAL, "Total data amount to check in bytes.");
        AtomicLongMetric processed = mreg.longMetric(METRIC_NAME_PROCESSED, "Processed data amount in bytes.");

        mreg.register(METRIC_NAME_PROGRESS, () -> 100.0 * processed.value() / total.value(), "% of checked data amount.");
    }

    /**
     * Ensures thta the future is stopped if any failure occures.
     *
     * @param fut Future to stop. If {@code null}, ignored.
     * @param action Related action to launch and watch.
     */
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
}
