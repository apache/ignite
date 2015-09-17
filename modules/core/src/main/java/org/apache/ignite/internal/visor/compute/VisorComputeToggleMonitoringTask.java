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

package org.apache.ignite.internal.visor.compute;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.compute.VisorComputeMonitoringHolder.COMPUTE_MONITORING_HOLDER_KEY;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.VISOR_TASK_EVTS;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.checkExplicitTaskMonitoring;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorComputeToggleMonitoringTask extends
    VisorMultiNodeTask<IgniteBiTuple<String, Boolean>, Boolean, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override protected Boolean reduce0(List<ComputeJobResult> results) {
        Collection<Boolean> toggles = new HashSet<>();

        for (ComputeJobResult res : results)
            toggles.add(res.<Boolean>getData());

        // If all nodes return same result.
        return toggles.size() == 1;
    }

    /** {@inheritDoc} */
    @Override protected VisorComputeToggleMonitoringJob job(IgniteBiTuple<String, Boolean> arg) {
        return new VisorComputeToggleMonitoringJob(arg, debug);
    }

    /**
     * Job to toggle task monitoring on node.
     */
    private static class VisorComputeToggleMonitoringJob extends VisorJob<IgniteBiTuple<String, Boolean>, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Visor ID key and monitoring state flag.
         * @param debug Debug flag.
         */
        private VisorComputeToggleMonitoringJob(IgniteBiTuple<String, Boolean> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Boolean run(IgniteBiTuple<String, Boolean> arg) {
            if (checkExplicitTaskMonitoring(ignite))
                return true;
            else {
                ConcurrentMap<String, VisorComputeMonitoringHolder> storage = ignite.cluster().nodeLocalMap();

                VisorComputeMonitoringHolder holder = storage.get(COMPUTE_MONITORING_HOLDER_KEY);

                if (holder == null) {
                    VisorComputeMonitoringHolder holderNew = new VisorComputeMonitoringHolder();

                    VisorComputeMonitoringHolder holderOld =
                        storage.putIfAbsent(COMPUTE_MONITORING_HOLDER_KEY, holderNew);

                    holder = holderOld == null ? holderNew : holderOld;
                }

                String visorKey = arg.get1();

                boolean state = arg.get2();

                // Set task monitoring state.
                if (state)
                    holder.startCollect(ignite, visorKey);
                else
                    holder.stopCollect(ignite, visorKey);

                // Return actual state. It could stay the same if events explicitly enabled in configuration.
                return ignite.allEventsUserRecordable(VISOR_TASK_EVTS);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeToggleMonitoringJob.class, this);
        }
    }
}