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

package org.gridgain.grid.kernal.visor.compute;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.kernal.visor.compute.VisorComputeMonitoringHolder.*;
import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Task to run gc on nodes.
 */
@GridInternal
public class VisorComputeToggleMonitoringTask extends
    VisorMultiNodeTask<IgniteBiTuple<String, Boolean>, Boolean, Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override protected Boolean reduce0(List<ComputeJobResult> results) throws IgniteCheckedException {
        Collection<Boolean> toggles = new HashSet<>();

        for (ComputeJobResult res: results)
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
        @Override protected Boolean run(IgniteBiTuple<String, Boolean> arg) throws IgniteCheckedException {
            if (checkExplicitTaskMonitoring(g))
                return true;
            else {
                ClusterNodeLocalMap<String, VisorComputeMonitoringHolder> storage = g.nodeLocalMap();

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
                    holder.startCollect(g, visorKey);
                else
                    holder.stopCollect(g, visorKey);

                // Return actual state. It could stay the same if events explicitly enabled in configuration.
                return g.allEventsUserRecordable(VISOR_TASK_EVTS);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeToggleMonitoringJob.class, this);
        }
    }
}
