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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotCheckProcess;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteCoreFeature;
import org.apache.ignite.internal.processors.rollingupgrade.feature.SupportedFeatureRegistry;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.lang.GridFunc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T5;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.metric.BooleanMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.jetbrains.annotations.Nullable;

/** V2 of {@link SnapshotStatusTask} with the support of snapshot check status. */
@GridInternal
public class SnapshotStatusTaskV2 extends SnapshotStatusTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** */
    private transient @Nullable Boolean checkStatusSupported;

    /** @inheritDoc}  */
    @Override protected VisorJob<NoArg, SnapshotStatus> job(NoArg arg) {
        if (checkStatusSupported == null)
            resolveCheckStatusSupported();

        return checkStatusSupported ? new SnapshotStatusJobV2(arg, debug) : new SnapshotStatusJob(arg, debug);
    }

    /** */
    private void resolveCheckStatusSupported() {
        var feature = new IgniteCoreFeature(SupportedFeatureRegistry.SNAPSHOT_CHECK_STATUS_FEATURE.id());

        if (!ignite.context().rollingUpgrade().features().isActive(feature)) {
            log.warning("The snapshot-check-aware status feature isn't enabled. The status is available only for " +
                "snapshot creation and restoration.");

            checkStatusSupported = false;

            return;
        }

        for (var n : ignite.cluster().nodes()) {
            if (!(n instanceof IgniteClusterNode cn) || !cn.features().contains(feature)) {
                log.warning(String.format(
                    "Node %s doesn't support the snapshot-check-aware status feature. The status is available only " +
                        "for snapshot creation and restoration.",
                    n.id()
                ));

                checkStatusSupported = false;

                return;
            }
        }

        checkStatusSupported = true;
    }

    /** {@inheritDoc} */
    @Override protected @Nullable SnapshotStatus reduce0(List<ComputeJobResult> results) {
        SnapshotStatus res0 = super.reduce0(results);

        // No results received at all.
        if (res0 == null)
            return null;

        // Found crate or restore result.
        if (res0.operation() != null)
            return res0;

        Collection<SnapshotStatus> sameRqRes = F.viewReadOnly(results, ComputeJobResult::getData,
            r -> r.getData() != null && ((SnapshotStatus)r.getData()).requestId().equals(res0.requestId()));

        assert !F.isEmpty(sameRqRes);

        SnapshotStatus firstRes = F.first(sameRqRes);

        assert firstRes instanceof SnapshotStatusV2 : "Expected V2 snapshot status result";

        SnapshotStatusV2 firstResV2 = (SnapshotStatusV2)firstRes;

        // Check status: snpName, per node collection.
        Map<String, SnapshotStatus> statusesMap = U.newHashMap(sameRqRes.size());

        sameRqRes.forEach(s -> {
            assert s instanceof SnapshotStatusV2;

            for (SnapshotStatus s0 : ((SnapshotStatusV2)s).allCheckStatuses) {
                var prev = statusesMap.putIfAbsent(s0.name(), s0);

                if (prev == null)
                    continue;

                // Merge nodes progress.
                prev.progress().putAll(s0.progress());
            }

            firstResV2.allCheckStatuses = new ArrayList<>(statusesMap.values());
        });

        return firstResV2;
    }

    /** V2 of {@link SnapshotStatusJob} with the support of snapshot check status. */
    private static class SnapshotStatusJobV2 extends SnapshotStatusTask.SnapshotStatusJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private SnapshotStatusJobV2(@Nullable NoArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected @Nullable SnapshotStatus run(@Nullable NoArg arg) throws IgniteException {
            var res1 = super.run(arg);

            // Create or restore status detected.
            if (res1 != null)
                return res1;

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
                        (long)snpCheckMReg.<IntMetric>findMetric("totalPartitions").value(),
                        (long)snpCheckMReg.<IntMetric>findMetric("processedSnapshotParts").value(),
                        (long)snpCheckMReg.<IntMetric>findMetric("snapshotPartsToProcess").value()
                    );
                }

                var status = new SnapshotStatus(
                    null,
                    MetricUtils.fromFullName(snpCheckMReg.name()).get2(),
                    incIdx,
                    snpCheckMReg.findMetric("requestId").getAsString(),
                    ((LongMetric)snpCheckMReg.findMetric("startTime")).value(),
                    GridFunc.asMap(ignite.localNode().id(), metrics)
                );

                checkStatuses.add(status);
            }

            return checkStatuses == null ? null : new SnapshotStatusV2(checkStatuses);
        }
    }

    /** V2 of {@link SnapshotStatus} with the support of snapshot check status. */

    public static class SnapshotStatusV2 extends SnapshotStatusTask.SnapshotStatus {
        /** */
        private static final long serialVersionUID = 0L;

        /** Nodes' statuses of all snapshot check operations. */
        @Nullable List<SnapshotStatus> allCheckStatuses;

        /** */
        private SnapshotStatusV2(SnapshotStatus s1) {
            super(s1.operation(), s1.name(), s1.incrementIndex(), s1.requestId(), s1.startTime(), s1.progress());
        }

        /** */
        private SnapshotStatusV2(List<SnapshotStatus> allCheckStatuses) {
            // Single, V1 status holds first found check status.
            super(
                null,
                allCheckStatuses.get(0).name(),
                allCheckStatuses.get(0).incrementIndex(),
                allCheckStatuses.get(0).requestId(),
                allCheckStatuses.get(0).startTime(),
                allCheckStatuses.get(0).progress()
            );

            this.allCheckStatuses = allCheckStatuses;
        }
    }
}
