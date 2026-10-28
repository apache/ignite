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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckProcess;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationRequest;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.rollingupgrade.feature.CoreFeatureRegistry;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteCoreFeatureSet;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T5;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;
import static org.apache.ignite.internal.processors.rollingupgrade.feature.CoreFeatureRegistry.SNAPSHOT_CHECK_STATUS_FEATURE;
import static org.apache.ignite.internal.util.lang.ClusterNodeFunc.nodeIds;

/**
 * Task to get the statuses of the current snapshot operations in the cluster.
 * <p>Before {@link CoreFeatureRegistry#SNAPSHOT_CHECK_STATUS_FEATURE}, returns single {@link SnapshotStatus} for single
 * snapshot creation or snapshot restoration operation.
 * <p>With {@link CoreFeatureRegistry#SNAPSHOT_CHECK_STATUS_FEATURE}, returns collection of {@link SnapshotOperationStatus}
 * for possible several snapshot checks operations or parallel creation/restoration and checks operations. May still
 * contain a single status.
 */
@GridInternal
public class SnapshotStatusTask extends VisorMultiNodeTask<NoArg, Serializable, Serializable> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Command initiator's features. */
    private transient IgniteCoreFeatureSet cmdInitiatorFeatures;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, Serializable> job(NoArg arg) {
        return new SnapshotStatusJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<NoArg> arg) {
        cmdInitiatorFeatures = arg.initiatorFeatures();

        return nodeIds(ignite.cluster().forServers().nodes());
    }

    /** {@inheritDoc} */
    @Override protected @Nullable Serializable reduce0(List<ComputeJobResult> results) {
        if (results.isEmpty())
            throw new IgniteException("Failed to get the snapshot status. Topology is empty.");

        IgniteException error = F.find(F.viewReadOnly(results, ComputeJobResult::getException,
            r -> r.getException() != null), null, F.notNull());

        if (error != null)
            throw new IgniteException("Failed to get the snapshot status.", error);

        assert cmdInitiatorFeatures != null : "Command initiator features isn't set";

        boolean featureEnabled = ignite.context().rollingUpgrade().features().isActive(SNAPSHOT_CHECK_STATUS_FEATURE);
        boolean initiatorSupports = cmdInitiatorFeatures.features().contains(SNAPSHOT_CHECK_STATUS_FEATURE.id());

        // All the jobs' statuses combined to one flat collection.
        Collection<SnapshotOperationStatus> allStatuses = new ArrayList<>(results.size());

        for (ComputeJobResult jobRes : results) {
            if (jobRes.getData() == null)
                continue;

            /** Old single {@link SnapshotStatus} of creation or restoration status or new several results as a collection. */
            Serializable rawResult = jobRes.getData();

            assert rawResult instanceof SnapshotStatus || rawResult instanceof Collection<?> :
                "Unsupported job result type: " + rawResult.getClass().getSimpleName();
            assert !(rawResult instanceof Collection<?> cll) || cll.isEmpty() || (F.first(cll) instanceof SnapshotOperationStatus) :
                "Statuses collection is empty or its first status is of unsupported type";

            if (rawResult instanceof SnapshotStatus singleJobStatus) {
                assert !featureEnabled :
                    "Single snapshot status result is allowed only if the snapshot check status feature is not enabled";
                assert singleJobStatus.operation() != SnapshotOperation.CHECK :
                    "Status of the check operation is allowed only within the new result list, not as a single result";

                allStatuses.add(new SnapshotOperationStatus(singleJobStatus));
            }
            else {
                assert featureEnabled :
                    "Many snapshot status results are allowed only if the snapshot check status feature is enabled";

                Collection<SnapshotOperationStatus> jobMultipleStatuses = (Collection<SnapshotOperationStatus>)rawResult;

                // Skip the check status if cluster supports, but not a thin client of previous version.
                if (!initiatorSupports) {
                    jobMultipleStatuses = jobMultipleStatuses.stream().filter(s -> s.operation() != SnapshotOperation.CHECK).toList();

                    assert jobMultipleStatuses.isEmpty() || jobMultipleStatuses.size() == 1 :
                        "Snapshot creation or restoration are mutually exclusive, only one of them allowed at the same time.";
                }

                allStatuses.addAll(jobMultipleStatuses);
            }
        }

        // There is no snapshot operation or the check operation in not supported yet in the result.
        if (allStatuses.isEmpty())
            return null;

        ArrayList<SnapshotOperationStatus> taskResult = null;

        // First, find create or restore operation.
        Collection<SnapshotOperationStatus> filtered = F.viewReadOnly(allStatuses, s -> s, s -> s.operation() != SnapshotOperation.CHECK);

        // Found create or restore statuses.
        if (!filtered.isEmpty()) {
            // Merged create or restore.
            SnapshotOperationStatus merged = F.first(filtered);

            String mergedRqId = merged.reqId;

            // Filter out differing requests due to concurrent updates on nodes.
            filtered = F.view(filtered, s -> s.reqId.equals(mergedRqId));

            // Merge nodes progress.
            Map<UUID, T5<Long, Long, Long, Long, Long>> progress = new HashMap<>();

            filtered.forEach(s -> progress.putAll(s.progress));

            merged = new SnapshotOperationStatus(merged.op, merged.name, merged.incIdx, merged.reqId, merged.startTime, progress);

            // If the initiator like thin client of 'control.sh' doesn't support the feature yet, we have to rewrap
            // the first single result back to the old format.
            if (!initiatorSupports)
                return new SnapshotStatus(merged.op, merged.name, merged.incIdx, merged.reqId, merged.startTime, progress);

            if (taskResult == null)
                taskResult = new ArrayList<>();

            taskResult.add(merged);
        }

        // Check operation statuses go last.
        filtered = F.viewReadOnly(allStatuses, s -> s, s -> s.operation() == SnapshotOperation.CHECK);

        // Found check statuses.
        if (!filtered.isEmpty()) {
            // Merged check statuses per snapshot.
            Map<String, SnapshotOperationStatus> mergedCheckStatuses = U.newHashMap(filtered.size());

            filtered.forEach(chkStatus -> {
                SnapshotOperationStatus chkStatus0 = mergedCheckStatuses.computeIfAbsent(chkStatus.name, (snpName -> chkStatus));

                if (chkStatus0 != chkStatus)
                    chkStatus0.progress.putAll(chkStatus.progress);
            });

            if (taskResult == null)
                taskResult = new ArrayList<>();

            taskResult.addAll(mergedCheckStatuses.values());
        }

        // Empty list notifies that this is the new result but is empty. Old empty is always null.
        return taskResult == null ? (Serializable)Collections.emptyList() : taskResult;
    }

    /**
     * A job to get the statuses of the current snapshot operations on current node.
     * <p>Before {@link CoreFeatureRegistry#SNAPSHOT_CHECK_STATUS_FEATURE}, returns single {@link SnapshotStatus} for single
     * snapshot creation or snapshot restoration operation.
     * <p>With {@link CoreFeatureRegistry#SNAPSHOT_CHECK_STATUS_FEATURE}, returns collection of {@link SnapshotOperationStatus}
     * for possible several snapshot checks operations or parallel creation/restoration and checks operations. May still
     * contain a single status.
     */
    private static class SnapshotStatusJob extends SnapshotJob<NoArg, Serializable> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @LoggerResource
        private transient IgniteLogger log;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected SnapshotStatusJob(@Nullable NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected @Nullable Serializable run(@Nullable NoArg arg) throws IgniteException {
            if (!CU.isPersistenceEnabled(ignite.context().config()))
                return null;

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();
            SnapshotOperationRequest req = snpMgr.currentCreateRequest();

            // Only one operation like creation or restoration is allowed at the same time.
            SnapshotStatus createOrRestoreStatus = null;

            if (req != null) {
                T5<Long, Long, Long, Long, Long> metrics;

                if (req.incremental())
                    metrics = new T5<>(-1L, -1L, -1L, -1L, -1L);
                else {
                    MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_METRICS);

                    metrics = new T5<>(
                        mreg.<LongMetric>findMetric("CurrentSnapshotProcessedSize").value(),
                        mreg.<LongMetric>findMetric("CurrentSnapshotTotalSize").value(),
                        -1L, -1L, -1L);
                }

                createOrRestoreStatus = new SnapshotStatus(
                    SnapshotOperation.CREATE,
                    req.snapshotName(),
                    req.incrementIndex(),
                    req.requestId().toString(),
                    req.startTime(),
                    F.asMap(ignite.localNode().id(), metrics)
                );
            }
            else {
                MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_RESTORE_METRICS);

                long startTime = mreg.<LongMetric>findMetric("startTime").value();

                if (startTime > mreg.<LongMetric>findMetric("endTime").value()) {
                    createOrRestoreStatus = new SnapshotStatus(
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
            }

            /** Return old single result. */
            if (!ignite.context().rollingUpgrade().features().isActive(SNAPSHOT_CHECK_STATUS_FEATURE)) {
                log.warning("The snapshot check status feature isn't activated yet. The status is available only " +
                    "for snapshot creation and restoration.");

                return createOrRestoreStatus;
            }

            // New nany results.
            ArrayList<SnapshotOperationStatus> snpOpStatuses = null;

            if (createOrRestoreStatus != null) {
                snpOpStatuses = new ArrayList<>();

                // Status of creation or restoration operation always goes firts. This is important for old command initiators (control.sh).
                snpOpStatuses.add(new SnapshotOperationStatus(createOrRestoreStatus));
            }

            for (ReadOnlyMetricRegistry snpCheckMReg : ignite.context().metric()) {
                if (!snpCheckMReg.name().startsWith(SnapshotCheckProcess.SNAPSHOT_CHECK_METRIC))
                    continue;

                Metric rqIdMetric = snpCheckMReg.findMetric("requestId");

                // The requestId metric is registered last.
                if (rqIdMetric == null)
                    continue;

                int incIdx = snpCheckMReg.findMetric("incrementIndex") == null
                    ? 0
                    : snpCheckMReg.<IntMetric>findMetric("incrementIndex").value();

                T5<Long, Long, Long, Long, Long> metrics;

                if (incIdx > 0) {
                    metrics = new T5<>(
                        (long)snpCheckMReg.<IntMetric>findMetric("processedWalSegments").value(),
                        (long)snpCheckMReg.<IntMetric>findMetric("totalWalSegments").value(),
                        -1L, -1L, -1L
                    );
                }
                else {
                    metrics = new T5<>(
                        (long)snpCheckMReg.<IntMetric>findMetric("processedPartitions").value(),
                        (long)snpCheckMReg.<IntMetric>findMetric("totalPartitions").value(),
                        -1L, -1L, -1L
                    );
                }

                SnapshotOperationStatus snpOpStatus = new SnapshotOperationStatus(
                    SnapshotOperation.CHECK,
                    MetricUtils.fromFullName(snpCheckMReg.name()).get2(),
                    incIdx,
                    rqIdMetric.getAsString(),
                    ((LongMetric)snpCheckMReg.findMetric("startTime")).value(),
                    F.asMap(ignite.localNode().id(), metrics)
                );

                if (snpOpStatuses == null)
                    snpOpStatuses = new ArrayList<>();

                snpOpStatuses.add(snpOpStatus);
            }

            return snpOpStatuses;
        }
    }

    /** Snapshot operation status. */
    public static class SnapshotStatus implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Operation type. */
        private final SnapshotOperation op;

        /** Snapshot name. */
        private final String name;

        /** Incremental snapshot index. */
        private final int incIdx;

        /** Request ID. */
        private final String requestId;

        /** Start time. */
        private final long startTime;

        /** Progress of operation on nodes. */
        Map<UUID, T5<Long, Long, Long, Long, Long>> progress;

        /** */
        private SnapshotStatus(
            SnapshotOperation op,
            String name,
            int incIdx,
            String requestId,
            long startTime,
            Map<UUID, T5<Long, Long, Long, Long, Long>> progress
        ) {
            this.op = op;
            this.name = name;
            this.incIdx = incIdx;
            this.requestId = requestId;
            this.startTime = startTime;
            this.progress = progress;
        }

        /** @return Operation type. */
        public SnapshotOperation operation() {
            return op;
        }

        /** @return Snapshot name. */
        public String name() {
            return name;
        }

        /** @return Incremental snapshot index. */
        public int incrementIndex() {
            return incIdx;
        }

        /** @return Request ID. */
        public String requestId() {
            return requestId;
        }

        /** @return Start time. */
        public long startTime() {
            return startTime;
        }

        /** @return Progress of operation on nodes. */
        public Map<UUID, T5<Long, Long, Long, Long, Long>> progress() {
            return Collections.unmodifiableMap(progress);
        }
    }

    /**
     * Snapshot operation status after {@link CoreFeatureRegistry#SNAPSHOT_CHECK_STATUS_FEATURE}.
     * Is now a {@linkIgniteDataTransferObject}.
     */
    public static class SnapshotOperationStatus extends IgniteDataTransferObject {
        /** */
        private static final long serialVersionUID = 0L;

        /** Operation type. */
        @Order(0)
        SnapshotOperation op;

        /** Snapshot name. */
        @Order(1)
        String name;

        /** Incremental snapshot index. */
        @Order(2)
        int incIdx;

        /** Request ID. */
        @Order(3)
        String reqId;

        /** Start time. */
        @Order(4)
        long startTime;

        /** Progress of operation on nodes. */
        @Order(5)
        Map<UUID, T5<Long, Long, Long, Long, Long>> progress;

        /** Empty constructor for serialization purposes. */
        public SnapshotOperationStatus() {
            // No-op.
        }

        /** */
        private SnapshotOperationStatus(
            SnapshotOperation op,
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
            this.progress = progress;
        }

        /** */
        public SnapshotOperationStatus(SnapshotStatus s) {
            this(s.op, s.name, s.incIdx, s.requestId, s.startTime, s.progress);
        }

        /** @return Operation type. */
        public SnapshotOperation operation() {
            return op;
        }

        /** @return Snapshot name. */
        public String name() {
            return name;
        }

        /** @return Incremental snapshot index. */
        public int incrementIndex() {
            return incIdx;
        }

        /** @return Request ID. */
        public String requestId() {
            return reqId;
        }

        /** @return Start time. */
        public long startTime() {
            return startTime;
        }

        /** @return Progress of operation on nodes. */
        public Map<UUID, T5<Long, Long, Long, Long, Long>> progress() {
            return Collections.unmodifiableMap(progress);
        }
    }

    /** Snapshot operation type. */
    public enum SnapshotOperation {
        /** Create snapshot. */
        CREATE,

        /** Restore snapshot. */
        RESTORE,

        /**
         * Check snapshot.
         *
         * @see CoreFeatureRegistry#SNAPSHOT_CHECK_STATUS_FEATURE
         */
        CHECK
    }
}
