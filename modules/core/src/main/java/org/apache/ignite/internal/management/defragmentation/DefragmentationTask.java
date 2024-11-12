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

package org.apache.ignite.internal.management.defragmentation;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.management.defragmentation.DefragmentationCommand.DefragmentationCancelCommandArg;
import org.apache.ignite.internal.management.defragmentation.DefragmentationCommand.DefragmentationStatusCommandArg;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.IgniteDefragmentation;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** */
@GridInternal
public class DefragmentationTask extends VisorMultiNodeTask
    <DefragmentationStatusCommandArg, DefragmentationTaskResult, DefragmentationTaskResult> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<DefragmentationStatusCommandArg, DefragmentationTaskResult> job(
        DefragmentationStatusCommandArg arg
    ) {
        return new DefragmentationJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected DefragmentationTaskResult reduce0(List<ComputeJobResult> results) {
        if (taskArg instanceof DefragmentationScheduleCommandArg) {
            StringBuilder msg = new StringBuilder();

            for (ComputeJobResult res : results) {
                msg.append(res.getNode().consistentId()).append(":\n");

                if (res.getData() == null)
                    msg.append("    err=").append(res.getException()).append('\n');
                else {
                    DefragmentationTaskResult data = res.getData();

                    msg.append("    success=").append(data.isSuccess()).append('\n');
                    msg.append("    msg=").append(data.getMessage()).append('\n');
                }
            }

            return new DefragmentationTaskResult(true, msg.toString());
        }

        assert results.size() == 1;

        ComputeJobResult res = results.get(0);

        if (res.getException() == null)
            return res.getData();

        throw res.getException();
    }

    /** */
    private static class DefragmentationJob extends VisorJob<DefragmentationStatusCommandArg, DefragmentationTaskResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected DefragmentationJob(@Nullable DefragmentationStatusCommandArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected DefragmentationTaskResult run(
            @Nullable DefragmentationStatusCommandArg arg
        ) throws IgniteException {
            if (arg instanceof DefragmentationScheduleCommandArg)
                return runSchedule((DefragmentationScheduleCommandArg)arg);

            if (arg instanceof DefragmentationCancelCommandArg)
                return runCancel();

            if (arg instanceof DefragmentationStatusCommandArg)
                return runStatus();

            throw new IllegalArgumentException("Operation: " + arg);
        }

        /** */
        private DefragmentationTaskResult runSchedule(DefragmentationScheduleCommandArg arg) {
            final IgniteDefragmentation defragmentation = ignite.context().defragmentation();

            final IgniteDefragmentation.ScheduleResult scheduleResult;

            try {
                scheduleResult = defragmentation.schedule(F.isEmpty(arg.caches()) ? null : Arrays.asList(arg.caches()));
            }
            catch (IgniteCheckedException e) {
                return new DefragmentationTaskResult(false, e.getMessage());
            }

            String msg;

            switch (scheduleResult) {
                case SUCCESS_SUPERSEDED_PREVIOUS:
                    msg = "Scheduling completed successfully. Previously scheduled task has been removed.";
                    break;
                case SUCCESS:
                default:
                    msg = "Scheduling completed successfully.";
                    break;
            }

            return new DefragmentationTaskResult(true, msg);
        }

        /** */
        private DefragmentationTaskResult runStatus() {
            final IgniteDefragmentation defragmentation = ignite.context().defragmentation();

            try {
                return new DefragmentationTaskResult(true, defragmentation.status().toString());
            }
            catch (IgniteCheckedException e) {
                return new DefragmentationTaskResult(false, e.getMessage());
            }
        }

        /** */
        private DefragmentationTaskResult runCancel() {
            final IgniteDefragmentation defragmentation = ignite.context().defragmentation();

            try {
                final IgniteDefragmentation.CancelResult cancelResult = defragmentation.cancel();

                String msg;

                switch (cancelResult) {
                    case SCHEDULED_NOT_FOUND:
                        msg = "Scheduled defragmentation task is not found.";
                        break;
                    case CANCELLED:
                        msg = "Defragmentation cancelled successfully.";
                        break;
                    case COMPLETED_OR_CANCELLED:
                        msg = "Defragmentation is already completed or has been cancelled previously.";
                        break;
                    case CANCELLED_SCHEDULED:
                    default:
                        msg = "Scheduled defragmentation task cancelled successfully.";
                        break;
                }

                return new DefragmentationTaskResult(true, msg);
            }
            catch (IgniteCheckedException e) {
                return new DefragmentationTaskResult(false, e.getMessage());
            }
        }
    }
}
