/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.mxbean.SnapshotMXBean;
import org.apache.ignite.spi.metric.IntMetric;

import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DFLT_CHECK_CRC_ON_RESTORE;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotRestoreProcess.SNAPSHOT_RESTORE_METRICS;

/**
 * Snapshot MBean features.
 */
public class SnapshotMXBeanImpl implements SnapshotMXBean {
    /** Instance of snapshot cache shared manager. */
    private final IgniteSnapshotManager mgr;

    /** Instance of metric manager. */
    private final GridMetricManager metricMgr;

    /**
     * @param ctx Kernal context.
     */
    public SnapshotMXBeanImpl(GridKernalContext ctx) {
        mgr = ctx.cache().context().snapshotMgr();
        metricMgr = ctx.metric();
    }

    /** {@inheritDoc} */
    @Override public void createSnapshot(String snpName, String snpPath) {
        IgniteFuture<Void> fut = mgr.createSnapshot(snpName, F.isEmpty(snpPath) ? null : snpPath, false);

        if (fut.isDone())
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void createIncrementalSnapshot(String fullSnapshot, String fullSnapshotPath) {
        IgniteFuture<Void> fut = mgr.createSnapshot(fullSnapshot, F.isEmpty(fullSnapshotPath) ? null : fullSnapshotPath, true);

        if (fut.isDone())
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void cancelSnapshot(String snpName) {
        mgr.cancelSnapshot(snpName).get();
    }

    /** {@inheritDoc} */
    @Override public void cancelSnapshotOperation(String reqId) {
        mgr.cancelSnapshotOperation(UUID.fromString(reqId)).get();
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(String name, String path, String grpNames) {
        Set<String> grpNamesSet = F.isEmpty(grpNames) ? null :
            Arrays.stream(grpNames.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());

        IgniteFuture<Void> fut = mgr.restoreSnapshot(name, F.isEmpty(path) ? null : path, grpNamesSet);

        if (fut.isDone())
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(String name, String path, String grpNames, int incIdx) {
        Set<String> grpNamesSet = F.isEmpty(grpNames) ? null :
            Arrays.stream(grpNames.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toSet());

        IgniteFuture<Void> fut = mgr.restoreSnapshot(
            name,
            F.isEmpty(path) ? null : path,
            grpNamesSet,
            incIdx,
            DFLT_CHECK_CRC_ON_RESTORE
        );

        if (fut.isDone())
            fut.get();
    }

    /** {@inheritDoc} */
    @Override public void cancelSnapshotRestore(String name) {
        mgr.cancelSnapshotRestore(name).get();
    }

    /** {@inheritDoc} */
    @Override public String status() {
        SnapshotOperationRequest req = mgr.currentCreateRequest();

        if (req != null) {
            return "Create snapshot operation is in progress [name=" + req.snapshotName() +
                ", incremental=" + req.incremental() +
                (req.incremental() ? (", incrementIndex=" + req.incrementIndex()) : "") +
                ", id=" + req.requestId() + ']';
        }

        if (mgr.isRestoring()) {
            MetricRegistry mreg = metricMgr.registry(SNAPSHOT_RESTORE_METRICS);

            String name = mreg.findMetric("snapshotName").getAsString();
            int incIdx = mreg.<IntMetric>findMetric("incrementIndex").value();
            String id = mreg.findMetric("requestId").getAsString();

            boolean incremental = incIdx > 0;

            return "Restore snapshot operation is in progress [name=" + name + ", incremental=" + incremental +
                (incremental ? ", incrementIndex=" + incIdx : "") + ", id=" + id + ']';
        }

        return "There is no create or restore snapshot operation in progress.";
    }
}
