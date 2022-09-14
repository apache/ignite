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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotOperationRequest;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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
public class VisorSnapshotStatusTask extends VisorMultiNodeTask<Void, VisorSnapshotTaskResult, SnapshotStatus> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<Void, SnapshotStatus> job(Void arg) {
        return new VisorSnapshotStatusJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Override protected Collection<UUID> jobNodes(VisorTaskArgument<Void> arg) {
        return F.nodeIds(ignite.cluster().forServers().nodes());
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorSnapshotTaskResult reduce0(List<ComputeJobResult> results) {
        if (results.isEmpty())
            return new VisorSnapshotTaskResult(null, new IgniteException("Failed to get the snapshot status. Topology is empty."));

        IgniteException error = F.find(F.viewReadOnly(results, ComputeJobResult::getException,
            r -> r.getException() != null), null, F.notNull());

        if (error != null)
            return new VisorSnapshotTaskResult(null, new IgniteException("Failed to get the snapshot status.", error));

        Collection<SnapshotStatus> res = F.viewReadOnly(results, ComputeJobResult::getData, r -> r.getData() != null);

        // There is no snapshot operation.
        if (res.isEmpty())
            return new VisorSnapshotTaskResult(null, null);

        SnapshotStatus s0 = F.first(res);

        // Filter out differing requests due to concurrent updates on nodes.
        res = F.view(res, s -> s.requestId.equals(s0.requestId));

        // Merge nodes progress.
        Map<UUID, T2<Long, Long>> progress = new HashMap<>();

        res.forEach(s -> progress.putAll(s.progress));

        return new VisorSnapshotTaskResult(new SnapshotStatus(s0.op, s0.name, s0.requestId, s0.startTime, progress), null);
    }

    /** */
    private static class VisorSnapshotStatusJob extends VisorJob<Void, SnapshotStatus> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorSnapshotStatusJob(@Nullable Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected SnapshotStatus run(@Nullable Void arg) throws IgniteException {
            if (!CU.isPersistenceEnabled(ignite.context().config()))
                return null;

            IgniteSnapshotManager snpMgr = ignite.context().cache().context().snapshotMgr();

            SnapshotOperationRequest req = snpMgr.currentCreateRequest();

            if (req != null) {
                MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_METRICS);

                return new SnapshotStatus(
                    SnapshotOperation.CREATE,
                    req.snapshotName(),
                    req.requestId().toString(),
                    req.startTime(),
                    F.asMap(
                        ignite.localNode().id(),
                        new T2<>(
                            mreg.<LongMetric>findMetric("CurrentSnapshotProcessedSize").value(),
                            mreg.<LongMetric>findMetric("CurrentSnapshotTotalSize").value()
                        )
                    )
                );
            }

            MetricRegistry mreg = ignite.context().metric().registry(SNAPSHOT_RESTORE_METRICS);

            long startTime = mreg.<LongMetric>findMetric("startTime").value();

            if (startTime > mreg.<LongMetric>findMetric("endTime").value()) {
                return new SnapshotStatus(
                    SnapshotOperation.RESTORE,
                    mreg.findMetric("snapshotName").getAsString(),
                    mreg.findMetric("requestId").getAsString(),
                    mreg.<LongMetric>findMetric("startTime").value(),
                    F.asMap(
                        ignite.localNode().id(),
                        new T2<>(
                            (long)mreg.<IntMetric>findMetric("processedPartitions").value(),
                            (long)mreg.<IntMetric>findMetric("totalPartitions").value()
                        )
                    )
                );
            }

            return null;
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

        /** Request ID. */
        private final String requestId;

        /** Start time. */
        private final long startTime;

        /** Progress of operation on nodes. */
        private final Map<UUID, T2<Long, Long>> progress;

        /** */
        public SnapshotStatus(SnapshotOperation op, String name, String requestId, long startTime,
            Map<UUID, T2<Long, Long>> progress) {
            this.op = op;
            this.name = name;
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

        /** @return Request ID. */
        public String requestId() {
            return requestId;
        }

        /** @return Start time. */
        public long startTime() {
            return startTime;
        }

        /** @return Progress of operation on nodes. */
        public Map<UUID, T2<Long, Long>> progress() {
            return progress;
        }
    }

    /** Snapshot operation type. */
    public enum SnapshotOperation {
        /** Create snapshot. */
        CREATE,

        /** Restore snapshot. */
        RESTORE
    }
}
