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

package org.apache.ignite.internal.visor.baseline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cluster.baseline.autoadjust.BaselineAutoAdjustStatus;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task that will collect information about baseline topology and can change its state.
 */
@GridInternal
@GridVisorManagementTask
public class VisorBaselineTask extends VisorOneNodeTask<VisorBaselineTaskArg, VisorBaselineTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorBaselineJob job(VisorBaselineTaskArg arg) {
        return new VisorBaselineJob(arg, debug);
    }

    /**
     * Job that will collect baseline topology information.
     */
    private static class VisorBaselineJob extends VisorJob<VisorBaselineTaskArg, VisorBaselineTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorBaselineJob(VisorBaselineTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * Collect baseline and server nodes.
         *
         * @return Baseline descriptor.
         */
        private VisorBaselineTaskResult collect() {
            IgniteClusterEx cluster = ignite.cluster();

            Collection<? extends BaselineNode> baseline = cluster.currentBaselineTopology();

            Collection<? extends BaselineNode> srvrs = cluster.forServers().nodes();

            VisorBaselineAutoAdjustSettings autoAdjustSettings = new VisorBaselineAutoAdjustSettings(
                cluster.isBaselineAutoAdjustEnabled(),
                cluster.baselineAutoAdjustTimeout()
            );

            BaselineAutoAdjustStatus adjustStatus = cluster.baselineAutoAdjustStatus();

            return new VisorBaselineTaskResult(
                ignite.cluster().active(),
                cluster.topologyVersion(),
                F.isEmpty(baseline) ? null : baseline,
                srvrs,
                autoAdjustSettings,
                adjustStatus.getTimeUntilAutoAdjust(),
                adjustStatus.getTaskState() == BaselineAutoAdjustStatus.TaskState.IN_PROGRESS
            );
        }

        /**
         * Set new baseline.
         *
         * @param baselineTop Collection of baseline node.
         * @return Baseline descriptor.
         */
        private VisorBaselineTaskResult set0(Collection<BaselineNode> baselineTop) {
            ignite.cluster().setBaselineTopology(baselineTop);

            return collect();
        }

        /**
         * @return Current baseline.
         */
        private Map<String, BaselineNode> currentBaseLine() {
            Map<String, BaselineNode> nodes = new HashMap<>();

            Collection<BaselineNode> baseline = ignite.cluster().currentBaselineTopology();

            if (!F.isEmpty(baseline)) {
                for (BaselineNode node : baseline)
                    nodes.put(node.consistentId().toString(), node);
            }

            return nodes;
        }

        /**
         * @return Current server nodes.
         */
        private Map<String, BaselineNode> currentServers() {
            Map<String, BaselineNode> nodes = new HashMap<>();

            for (ClusterNode node : ignite.cluster().forServers().nodes())
                nodes.put(node.consistentId().toString(), node);

            return nodes;
        }

        /**
         * Set new baseline.
         *
         * @param consistentIds Collection of consistent IDs to set.
         * @return New baseline.
         */
        private VisorBaselineTaskResult set(List<String> consistentIds) {
            Map<String, BaselineNode> baseline = currentBaseLine();
            Map<String, BaselineNode> srvrs = currentServers();

            Collection<BaselineNode> baselineTop = new ArrayList<>();

            for (String consistentId : consistentIds) {
                if (srvrs.containsKey(consistentId))
                    baselineTop.add(srvrs.get(consistentId));

                else if (baseline.containsKey(consistentId))
                    baselineTop.add(baseline.get(consistentId));

                else
                    throw new IllegalArgumentException(
                        "Check arguments. Node not found for consistent ID: " + consistentId
                    );
            }

            return set0(baselineTop);
        }

        /**
         * Add new nodes to baseline.
         *
         * @param consistentIds Collection of consistent IDs to add.
         * @return New baseline.
         */
        private VisorBaselineTaskResult add(List<String> consistentIds) {
            Map<String, BaselineNode> baseline = currentBaseLine();
            Map<String, BaselineNode> srvrs = currentServers();

            for (String consistentId : consistentIds) {
                BaselineNode node = srvrs.get(consistentId);

                if (node == null)
                    throw new IllegalArgumentException("Node not found for consistent ID: " + consistentId);

                baseline.put(consistentId, node);
            }

            return set0(baseline.values());
        }

        /**
         * Remove nodes from baseline.
         *
         * @param consistentIds Collection of consistent IDs to remove.
         * @return New baseline.
         */
        private VisorBaselineTaskResult remove(List<String> consistentIds) {
            Map<String, BaselineNode> baseline = currentBaseLine();

            if (F.isEmpty(baseline))
                return set0(Collections.emptyList());

            for (String consistentId : consistentIds) {
                BaselineNode node = baseline.remove(consistentId);

                if (node == null)
                    throw new IllegalArgumentException("Node not found for consistent ID: " + consistentId);
            }

            return set0(baseline.values());
        }

        /**
         * Set baseline by topology version.
         *
         * @param targetVer Target topology version.
         * @return New baseline.
         */
        private VisorBaselineTaskResult version(long targetVer) {
            IgniteClusterEx cluster = ignite.cluster();

            if (targetVer > cluster.topologyVersion())
                throw new IllegalArgumentException("Topology version is ahead of time: " + targetVer);

            cluster.setBaselineTopology(targetVer);

            return collect();
        }

        /**
         * Update baseline autoAdjustment settings.
         *
         * @param settings Baseline autoAdjustment settings.
         * @return New baseline.
         */
        private VisorBaselineTaskResult updateAutoAdjustmentSettings(VisorBaselineAutoAdjustSettings settings) {
            if (settings.getSoftTimeout() != null)
                ignite.cluster().baselineAutoAdjustTimeout(settings.getSoftTimeout());

            if (settings.getEnabled() != null)
                ignite.cluster().baselineAutoAdjustEnabled(settings.getEnabled());

            return collect();
        }

        /** {@inheritDoc} */
        @Override protected VisorBaselineTaskResult run(@Nullable VisorBaselineTaskArg arg) throws IgniteException {
            switch (arg.getOperation()) {
                case ADD:
                    return add(arg.getConsistentIds());

                case REMOVE:
                    return remove(arg.getConsistentIds());

                case SET:
                    return set(arg.getConsistentIds());

                case VERSION:
                    return version(arg.getTopologyVersion());

                case AUTOADJUST:
                    return updateAutoAdjustmentSettings(arg.getAutoAdjustSettings());

                default:
                    return collect();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorBaselineJob.class, this);
        }
    }
}
