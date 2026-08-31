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

package org.apache.ignite.internal.management.snapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckProcess;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationRequest;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteCoreFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T5;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.management.snapshot.SnapshotStatusTask.SnapshotStatus;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.nodeIds;

/**
 * Task to get the status of the current snapshot operation in the cluster.
 */
@GridInternal
public class SnapshotStatusTask extends VisorMultiNodeTask<NoArg, SnapshotStatus, SnapshotStatus> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** */
    private transient @Nullable Boolean checkStatusSupported;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, SnapshotStatus> job(NoArg arg) {
        if (checkStatusSupported == null)
            resolveCheckStatusSupported();

        assert checkStatusSupported != null;

        return checkStatusSupported ? new SnapshotStatusJobV2(arg, debug) : new SnapshotStatusJob(arg, debug);
    }

    /** */
    private void resolveCheckStatusSupported() {
        var feature = new IgniteCoreFeature(SupportedFeatureRegistry.SNAPSHOT_CHECK_STATUS_FEATURE.id());

        if (!ignite.context().rollingUpgrade().features().isActive(feature)) {
            if (log.isInfoEnabled()) {
                log.info("The snapshot-check-aware status feature isn't enabled. The status is available only for " +
                    "snapshot creation and restoration.");
            }

            checkStatusSupported = false;

            return;
        }

        for (var n : ignite.cluster().nodes()) {
            if (!(n instanceof IgniteClusterNode)) {
                if (log.isInfoEnabled()) {
                    log.info(String.format(
                        "Cannot extract features of node %s. The status is available only for snapshot creation and restoration.",
                        n.id()
                    ));
                }

                checkStatusSupported = false;

                return;
            }

            if (!((IgniteClusterNode)n).features().contains(feature)) {
                if (log.isInfoEnabled()) {
                    log.info(String.format(
                        "Node %s doesn't support the snapshot-check-aware status feature. The status is available only " +
                            "for snapshot creation and restoration.",
                        n.id()
                    ));
                }

                checkStatusSupported = false;

                return;
            }
        }

        checkStatusSupported = true;
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<NoArg> arg) {
        return nodeIds(ignite.cluster().forServers().nodes());
    }

    /** {@inheritDoc} */
    @Override protected @Nullable SnapshotStatus reduce0(List<ComputeJobResult> results) {
        if (results.isEmpty())
            throw new IgniteException("Failed to get the snapshot status. Topology is empty.");

        IgniteException error = F.find(F.viewReadOnly(results, ComputeJobResult::getException,
            r -> r.getException() != null), null, F.notNull());

        if (error != null)
            throw new IgniteException("Failed to get the snapshot status.", error);

        Collection<SnapshotStatus> res0 = F.viewReadOnly(results, ComputeJobResult::getData, r -> r.getData() != null);

        // There is no snapshot operation.
        if (res0.isEmpty())
            return null;

        SnapshotStatus firstRes = F.first(res0);

        // Filter out differing requests due to concurrent updates on nodes.
        Collection<SnapshotStatus> sameRqRes = F.view(res0, s -> s.reqId.equals(firstRes.reqId));

        if (firstRes instanceof SnapshotStatusTask.SnapshotStatusV2) {
            var statusV2 = (SnapshotStatusTask.SnapshotStatusV2)firstRes;

            assert !F.isEmpty(statusV2.allCheckStatuses);

            Map<UUID, List<SnapshotStatus>> mergedAllCheckStatuses = U.newHashMap(sameRqRes.size());

            sameRqRes.forEach(s -> {
                assert s instanceof SnapshotStatusTask.SnapshotStatusV2;

                mergedAllCheckStatuses.putAll(((SnapshotStatusTask.SnapshotStatusV2)s).allCheckStatuses);
            });

            statusV2.allCheckStatuses = mergedAllCheckStatuses;

            return statusV2;
        }

        // Merge nodes progress.
        Map<UUID, T5<Long, Long, Long, Long, Long>> mergedProgress = U.newHashMap(sameRqRes.size());

        sameRqRes.forEach(s -> mergedProgress.putAll(s.progress));

        return new SnapshotStatus(firstRes.op, firstRes.name, firstRes.incIdx, firstRes.reqId, firstRes.startTime, mergedProgress);
    }

    /** */
    private static class SnapshotStatusJob extends SnapshotJob<NoArg, SnapshotStatus> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        private SnapshotStatusJob(@Nullable NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected @Nullable SnapshotStatus run(@Nullable NoArg arg) throws IgniteException {
            if (!CU.isPersistenceEnabled(ignite.context().config()))
                return null;

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            SnapshotOperationRequest req = snpMgr.currentCreateRequest();

            if (req != null) {
                T5<Long, Long, Long, Long, Long> metrics;

                if (req.incremental())
                    metrics = new T5<>(-1L, -1L, -1L, -1L, -1L);
                else {
                    MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_METRICS);

                    metrics = new T5<>(
                        mreg.<LongMetric>findMetric("CurrentSnapshotProcessedSize").value(),
                        mreg.<LongMetric>findMetric("CurrentSnapshotTotalSize").value(),
                        -1L,
                        -1L,
                        -1L
                    );
                }

                return new SnapshotStatus(
                    SnapshotOperation.CREATE,
                    req.snapshotName(),
                    req.incrementIndex(),
                    req.requestId().toString(),
                    req.startTime(),
                    F.asMap(ignite.localNode().id(), metrics)
                );
            }

            MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_RESTORE_METRICS);

            long startTime = mreg.<LongMetric>findMetric("startTime").value();

            if (startTime > mreg.<LongMetric>findMetric("endTime").value()) {
                return new SnapshotStatus(
                    SnapshotOperation.RESTORE,
                    mreg.findMetric("snapshotName").getAsString(),
                    mreg.<IntMetric>findMetric("incrementIndex").value(),
                    mreg.findMetric("requestId").getAsString(),
                    mreg.<LongMetric>findMetric("startTime").value(),
                    F.asMap(
                        ignite.localNode().id(),
                        new T5<>(
                            (long)mreg.<IntMetric>findMetric("processedPartitions").value(),
                            (long)mreg.<IntMetric>findMetric("totalPartitions").value(),
                            (long)mreg.<IntMetric>findMetric("processedWalSegments").value(),
                            (long)mreg.<IntMetric>findMetric("totalWalSegments").value(),
                            mreg.<LongMetric>findMetric("processedWalEntries").value()
                        )
                    )
                );
            }

            return null;
        }
    }

    /** Snapshot operation status. */
    static class SnapshotStatus implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Operation type. {@code Null} for other operation types. */
        private final @Nullable SnapshotOperation op;

        /** Snapshot name. */
        private final String name;

        /** Incremental snapshot index. */
        private final int incIdx;

        /** Request ID. */
        private final String reqId;

        /** Start time. */
        private final long startTime;

        /** Progress of operation on nodes. */
        private final Map<UUID, T5<Long, Long, Long, Long, Long>> progress;

        /** */
        private SnapshotStatus(
            @Nullable SnapshotOperation op,
            String name,
            int incIdx,
            String reqId,
            long startTime,
            Map<UUID, T5<Long, Long, Long, Long, Long>> progress
        ) {
            this.op = op;
            this.name = name;
            this.incIdx = incIdx;
            this.reqId = reqId;
            this.startTime = startTime;
            this.progress = Collections.unmodifiableMap(progress);
        }

        /** @return Operation type. {@code Null} for other operation types. */
        @Nullable SnapshotOperation operation() {
            return op;
        }

        /** @return Snapshot name. */
        String name() {
            return name;
        }

        /** @return Incremental snapshot index. */
        int incrementIndex() {
            return incIdx;
        }

        /** @return Request ID. */
        String requestId() {
            return reqId;
        }

        /** @return Start time. */
        long startTime() {
            return startTime;
        }

        /** @return Progress of operation on nodes. */
        Map<UUID, T5<Long, Long, Long, Long, Long>> progress() {
            return progress;
        }
    }

    /** Snapshot operation type. */
    enum SnapshotOperation {
        /** Snapshot creation. */
        CREATE,

        /** Snapshot restoration. */
        RESTORE
    }

    /** */
    private static class SnapshotStatusJobV2 extends SnapshotStatusTask.SnapshotStatusJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private SnapshotStatusJobV2(@Nullable NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected @Nullable SnapshotStatusV2 run(@Nullable NoArg arg) throws IgniteException {
            var res1 = super.run(arg);

            if (res1 != null)
                return new SnapshotStatusV2(res1);

            List<SnapshotStatus> checkStatuses = null;

            for (var snpCheckMReg : ignite.context().metric()) {
                if (!snpCheckMReg.name().startsWith(SnapshotCheckProcess.SNAPSHOT_CHECK_METRIC))
                    continue;

                if (checkStatuses == null)
                    checkStatuses = new ArrayList<>();

                int incIdx = snpCheckMReg.findMetric("incrementIndex") == null
                    ? 0
                    : ((IntMetric)snpCheckMReg.findMetric("incrementIndex")).value();

                T5<Long, Long, Long, Long, Long> metrics;

                if (incIdx > 0) {
                    metrics = new T5<>(
                        (long)snpCheckMReg.<IntMetric>findMetric("processedWalSegments").value(),
                        (long)snpCheckMReg.<IntMetric>findMetric("totalWalSegments").value(),
                        -1L,
                        -1L,
                        -1L
                    );
                }
                else {
                    metrics = new T5<>(
                        snpCheckMReg.<BooleanMetric>findMetric("checkPartitions").value() ? 1L : 0L,
                        (long)snpCheckMReg.<IntMetric>findMetric("processedPartitions").value(),
                        (long)snpCheckMReg.<IntMetric>findMetric("processedSnapshotParts").value(),
                        (long)snpCheckMReg.<IntMetric>findMetric("processedSnapshotParts").value(),
                        (long)snpCheckMReg.<IntMetric>findMetric("snapshotPartsToProcess").value()
                    );
                }

                checkStatuses.add(new SnapshotStatus(
                    null,
                    MetricUtils.fromFullName(snpCheckMReg.name()).get2(),
                    incIdx,
                    snpCheckMReg.findMetric("requestId").getAsString(),
                    ((LongMetric)snpCheckMReg.findMetric("startTime")).value(),
                    GridFunc.asMap(ignite.localNode().id(), metrics)
                ));
            }

            return checkStatuses == null ? null : new SnapshotStatusV2(Collections.singletonMap(ignite.localNode().id(), checkStatuses));
        }
    }

    /** Supports snapsho status. */
    private static class SnapshotStatusV2 extends SnapshotStatusTask.SnapshotStatus {
        /** */
        private static final long serialVersionUID = 0L;

        /** Statuses of snapshot check operations per nodeID. */
        private @Nullable Map<UUID, List<SnapshotStatus>> allCheckStatuses;

        /** */
        private SnapshotStatusV2(SnapshotStatus s1) {
            super(s1.op, s1.name, s1.incIdx, s1.reqId, s1.startTime, s1.progress);
        }

        /** */
        private SnapshotStatusV2(Map<UUID, List<SnapshotStatus>> allCheckStatuses) {
            // Single, V1 status holds first found check status.
            super(
                null,
                F.first(allCheckStatuses.values()).get(0).name,
                F.first(allCheckStatuses.values()).get(0).incIdx,
                F.first(allCheckStatuses.values()).get(0).reqId,
                F.first(allCheckStatuses.values()).get(0).startTime,
                F.first(allCheckStatuses.values()).get(0).progress
            );

            this.allCheckStatuses = allCheckStatuses;
        }
    }
}
