/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.baseline;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.processors.cluster.baseline.autoadjust.BaselineAutoAdjustStatus;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.jetbrains.annotations.Nullable;

/**
 * Task that will collect information about baseline topology.
 */
@GridInternal
public class VisorBaselineViewTask extends VisorOneNodeTask<Void, VisorBaselineTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorBaselineViewJob job(Void arg) {
        return new VisorBaselineViewJob(arg, debug);
    }

    /**
     * Job that will collect baseline topology information.
     */
    private static class VisorBaselineViewJob extends VisorJob<Void, VisorBaselineTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorBaselineViewJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorBaselineTaskResult run(@Nullable Void arg) throws IgniteException {
            IgniteClusterEx cluster = ignite.cluster();

            BaselineAutoAdjustStatus adjustStatus = cluster.baselineAutoAdjustStatus();

            return new VisorBaselineTaskResult(
                ignite.cluster().active(),
                cluster.topologyVersion(),
                cluster.currentBaselineTopology(),
                cluster.forServers().nodes(),
                cluster.isBaselineAutoAdjustEnabled(),
                cluster.baselineAutoAdjustTimeout(),
                adjustStatus.getTimeUntilAutoAdjust(),
                adjustStatus.getTaskState() == BaselineAutoAdjustStatus.TaskState.IN_PROGRESS
            );
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorBaselineViewJob.class, this);
        }
    }
}
