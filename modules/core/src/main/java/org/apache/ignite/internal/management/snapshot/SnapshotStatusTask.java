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
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
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

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, SnapshotStatus> job(NoArg arg) {
        return new SnapshotStatusJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<NoArg> arg) {
        return nodeIds(ignite.cluster().forServers().nodes());
    }

    /** {@inheritDoc} */
    @Nullable @Override protected SnapshotStatus reduce0(List<ComputeJobResult> results) {
        if (results.isEmpty())
            throw new IgniteException("Failed to get the snapshot status. Topology is empty.");

        IgniteException error = F.find(F.viewReadOnly(results, ComputeJobResult::getException,
            r -> r.getException() != null), null, F.notNull());

        if (error != null)
            throw new IgniteException("Failed to get the snapshot status.", error);

        // First, find create or restore operation. Check sattuses, if are, contains any status.
        Collection<SnapshotStatus> res = F.viewReadOnly(results, ComputeJobResult::getData,
            r -> r.getData() != null && ((SnapshotStatus)r.getData()).operation() != SnapshotOperation.CHECK);

        if (res.isEmpty())
            res = F.viewReadOnly(results, ComputeJobResult::getData, r -> r.getData() != null);

        // There is no snapshot operation.
        if (res.isEmpty())
            return null;

        SnapshotStatus mergerRes = F.first(res);

        // Try to find check statutes.
        Map<String, SnapshotStatus> chkStatuses = null;

        for (SnapshotStatus s : res) {
            if (s.checkStatuses == null) {
                assert s.op != SnapshotOperation.CHECK;

                continue;
            }

            if (chkStatuses == null)
                chkStatuses = U.newHashMap(res.size());

            for (SnapshotStatus s0 : s.checkStatuses) {
                var prev = chkStatuses.putIfAbsent(s0.name(), s0);

                if (prev == null)
                    continue;

                // Merge nodes progress.
                prev.progress.putAll(s0.progress());
            }
        }

        var rqId = mergerRes.requestId;

        // Filter out differing requests due to concurrent updates on nodes.
        res = F.view(res, s -> s.requestId.equals(rqId));

        // Create of restore.
        if (mergerRes.operation() != SnapshotOperation.CHECK) {
            // Merge nodes progress.
            Map<UUID, T5<Long, Long, Long, Long, Long>> progress = new HashMap<>();

            res.forEach(s -> progress.putAll(s.progress));

            mergerRes = new SnapshotStatus(mergerRes.op, mergerRes.name, mergerRes.incIdx, mergerRes.requestId,
                mergerRes.startTime, progress);

            if (chkStatuses != null)
                mergerRes.checkStatuses = new ArrayList<>(chkStatuses.values());
        }

        assert !F.isEmpty(mergerRes.progress);

        return mergerRes;
    }

    /** */
    protected static class SnapshotStatusJob extends SnapshotJob<NoArg, SnapshotStatus> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        @LoggerResource
        private transient IgniteLogger log;

        /** */
        private boolean clusterSupportsSnapshotCheckStatus() {
            var feature = new IgniteCoreFeature(SupportedFeatureRegistry.SNAPSHOT_CHECK_STATUS_FEATURE.id());

            if (!ignite.context().rollingUpgrade().features().isActive(feature)) {
                log.warning("The snapshot-check-aware status feature isn't enabled. The status is available only for " +
                    "snapshot creation and restoration.");

                return false;
            }

            for (var n : ignite.cluster().nodes()) {
                if (!(n instanceof IgniteClusterNode cn) || !cn.features().contains(feature)) {
                    log.warning(String.format("Node %s doesn't support the snapshot check status feature. The status " +
                        "is available only for snapshot creation and restoration.", n.id()));

                    return false;
                }
            }

            return true;
        }

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected SnapshotStatusJob(@Nullable NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected @Nullable SnapshotStatus run(@Nullable NoArg arg) throws IgniteException {
            if (!CU.isPersistenceEnabled(ignite.context().config()))
                return null;

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();
            SnapshotOperationRequest req = snpMgr.currentCreateRequest();
            SnapshotStatus res = null;

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

                res = new SnapshotStatus(
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
                    res = new SnapshotStatus(
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

            if (!clusterSupportsSnapshotCheckStatus())
                return res;

            List<SnapshotStatus> checkStatuses = null;

            for (var snpCheckMReg : ignite.context().metric()) {
                if (!snpCheckMReg.name().startsWith(SnapshotCheckProcess.SNAPSHOT_CHECK_METRIC))
                    continue;

                Metric rqIdMetric = snpCheckMReg.findMetric("requestId");

                // The requestId metric is registered last.
                if (rqIdMetric == null)
                    continue;

                if (checkStatuses == null)
                    checkStatuses = new ArrayList<>();

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

                var status = new SnapshotStatus(
                    null,
                    MetricUtils.fromFullName(snpCheckMReg.name()).get2(),
                    incIdx,
                    rqIdMetric.getAsString(),
                    ((LongMetric)snpCheckMReg.findMetric("startTime")).value(),
                    GridFunc.asMap(ignite.localNode().id(), metrics)
                );

                checkStatuses.add(status);
            }

            if (checkStatuses != null) {
                if (res == null)
                    res = new SnapshotStatus(checkStatuses);
                else
                    res.checkStatuses = checkStatuses;
            }

            return res;
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
        private final Map<UUID, T5<Long, Long, Long, Long, Long>> progress;

        /** Nodes' statuses of all snapshot check operations. */
        private @Nullable List<SnapshotStatus> checkStatuses;

        /** */
        SnapshotStatus(
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

        /** */
        private SnapshotStatus(List<SnapshotStatus> checkStatuses) {
            // Single, V1 status holds first check status.
            this(
                SnapshotOperation.CHECK,
                checkStatuses.get(0).name(),
                checkStatuses.get(0).incrementIndex(),
                checkStatuses.get(0).requestId(),
                checkStatuses.get(0).startTime(),
                checkStatuses.get(0).progress()
            );

            assert !F.isEmpty(checkStatuses);

            this.checkStatuses = checkStatuses;
        }

        /** @return Operation type. */
        SnapshotOperation operation() {
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
            return requestId;
        }

        /** @return Start time. */
        long startTime() {
            return startTime;
        }

        /** @return Progress of operation on nodes. */
        Map<UUID, T5<Long, Long, Long, Long, Long>> progress() {
            return Collections.unmodifiableMap(progress);
        }

        /** @return Statuses of parallel check. */
        @Nullable List<SnapshotStatus> checkStatuses() {
            return checkStatuses;
        }
    }

    /** Snapshot operation type. */
    public enum SnapshotOperation {
        /** Create snapshot. */
        CREATE,

        /** Restore snapshot. */
        RESTORE,

        /** Check snapshot. */
        CHECK
    }
}
