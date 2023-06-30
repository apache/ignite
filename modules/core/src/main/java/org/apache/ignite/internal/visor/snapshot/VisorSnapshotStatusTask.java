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

package org.apache.ignite.internal.visor.snapshot;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationRequest;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T5;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METRICS;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;
import static org.apache.ignite.internal.visor.snapshot.VisorSnapshotStatusTask.SnapshotStatus;

/**
 * Task to get the status of the current snapshot operation in the cluster.
 */
@GridInternal
public class VisorSnapshotStatusTask extends VisorMultiNodeTask<NoArg, VisorSnapshotTaskResult, List<SnapshotStatus>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<NoArg, List<SnapshotStatus>> job(NoArg arg) {
        return new VisorSnapshotStatusJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<NoArg> arg) {
        return F.nodeIds(ignite.cluster().forServers().nodes());
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorSnapshotTaskResult reduce0(List<ComputeJobResult> results) {
        if (results.isEmpty())
            return new VisorSnapshotTaskResult(null, new IgniteException("Failed to get the snapshot status. Topology is empty."));

        // Check for errors.
        IgniteException error = F.find(F.viewReadOnly(results, ComputeJobResult::getException,
            r -> r.getException() != null), null, F.notNull());

        if (error != null)
            return new VisorSnapshotTaskResult(null, new IgniteException("Failed to get the snapshot status.", error));

        Collection<List<SnapshotStatus>> jobsRes = F.viewReadOnly(results, ComputeJobResult::getData);

        // Group by request ID.
        Map<String, List<SnapshotStatus>> groupedByReqId = F.flatCollections(jobsRes).stream()
            .collect(Collectors.groupingBy(status -> status.requestId));

        // Merge nodes progress.
        Collection<SnapshotStatus> ops = F.viewReadOnly(groupedByReqId, (opStatuses) -> {
            SnapshotStatus s0 = F.first(opStatuses);

            Map<UUID, T5<Long, Long, Long, Long, Long>> progress = new HashMap<>();

            opStatuses.forEach(s -> progress.putAll(s.progress));

            return new SnapshotStatus(s0.op, s0.name, s0.incIdx, s0.requestId, s0.startTime, progress);
        }).values();

        return new VisorSnapshotTaskResult(new ArrayList<>(ops), null);
    }

    /** */
    private static class VisorSnapshotStatusJob extends VisorSnapshotJob<NoArg, List<SnapshotStatus>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStatusJob(@Nullable NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected List<SnapshotStatus> run(@Nullable NoArg arg) throws IgniteException {
            List<SnapshotStatus> res = new ArrayList<>();

            if (!CU.isPersistenceEnabled(ignite.context().config()))
                return res;

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            // Snapshot check operations.
            res.addAll(F.viewReadOnly(snpMgr.checkOperationsStatus(), status -> new SnapshotStatus(
                SnapshotOperation.CHECK,
                status.metadata().snapshotName(),
                -1,
                status.requestId().toString(),
                status.startTime(),
                F.asMap(ignite.localNode().id(),
                    new T5<>(status.processedPartitions().longValue(), (long)status.totalPartitions(), -1L, -1L, -1L))
            )));

            // Snapshot create operation.
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
                        -1L, -1L, -1L);
                }

                res.add(new SnapshotStatus(
                    SnapshotOperation.CREATE,
                    req.snapshotName(),
                    req.incrementIndex(),
                    req.requestId().toString(),
                    req.startTime(),
                    F.asMap(ignite.localNode().id(), metrics)
                ));
            }

            // Snapshot restore operation.
            MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_RESTORE_METRICS);

            long startTime = mreg.<LongMetric>findMetric("startTime").value();

            if (startTime > mreg.<LongMetric>findMetric("endTime").value()) {
                res.add(new SnapshotStatus(
                    SnapshotOperation.RESTORE,
                    mreg.findMetric("snapshotName").getAsString(),
                    mreg.<IntMetric>findMetric("incrementIndex").value(),
                    mreg.findMetric("requestId").getAsString(),
                    startTime,
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
                ));
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

        /** */
        public SnapshotStatus(
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
            this.progress = Collections.unmodifiableMap(progress);
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
            return progress;
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
