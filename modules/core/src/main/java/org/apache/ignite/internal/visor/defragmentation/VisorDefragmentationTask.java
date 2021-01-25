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

package org.apache.ignite.internal.visor.defragmentation;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.IgniteDefragmentation;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

/** */
@GridInternal
@GridVisorManagementTask
public class VisorDefragmentationTask extends VisorMultiNodeTask
    <VisorDefragmentationTaskArg, VisorDefragmentationTaskResult, VisorDefragmentationTaskResult>
{
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorJob<VisorDefragmentationTaskArg, VisorDefragmentationTaskResult> job(
        VisorDefragmentationTaskArg arg
    ) {
        return new VisorDefragmentationJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorDefragmentationTaskResult reduce0(List<ComputeJobResult> results) {
        if (taskArg.operation() == VisorDefragmentationOperation.SCHEDULE) {
            StringBuilder msg = new StringBuilder();

            for (ComputeJobResult res : results) {
                msg.append(res.getNode().consistentId()).append(":\n");

                if (res.getData() == null)
                    msg.append("    err=").append(res.getException()).append('\n');
                else {
                    VisorDefragmentationTaskResult data = res.getData();

                    msg.append("    success=").append(data.isSuccess()).append('\n');
                    msg.append("    msg=").append(data.getMessage()).append('\n');
                }
            }

            return new VisorDefragmentationTaskResult(true, msg.toString());
        }

        assert results.size() == 1;

        ComputeJobResult res = results.get(0);

        if (res.getException() == null)
            return res.getData();

        throw res.getException();
    }

    /** */
    private static class VisorDefragmentationJob extends VisorJob<VisorDefragmentationTaskArg, VisorDefragmentationTaskResult> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         * @param debug Flag indicating whether debug information should be printed into node log.
         */
        protected VisorDefragmentationJob(@Nullable VisorDefragmentationTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorDefragmentationTaskResult run(
            @Nullable VisorDefragmentationTaskArg arg
        ) throws IgniteException {
            switch (arg.operation()) {
                case SCHEDULE:
                    return runSchedule(arg);

                case STATUS:
                    return runStatus(arg);

                case CANCEL:
                    return runCancel(arg);
            }

            throw new IllegalArgumentException("Operation: " + arg.operation());
        }

        /** */
        private VisorDefragmentationTaskResult runSchedule(VisorDefragmentationTaskArg arg) {
            final IgniteDefragmentation defragmentation = ignite.context().defragmentation();

            final IgniteDefragmentation.ScheduleResult scheduleResult;

            try {
                scheduleResult = defragmentation.schedule(arg.cacheNames());
            }
            catch (IgniteCheckedException e) {
                return new VisorDefragmentationTaskResult(false, e.getMessage());
            }

            String message;

            switch (scheduleResult) {
                case SUCCESS_SUPERSEDED_PREVIOUS:
                    message = "Scheduling completed successfully. Previously scheduled task has been removed.";
                    break;
                case SUCCESS:
                default:
                    message = "Scheduling completed successfully.";
                    break;
            }

            return new VisorDefragmentationTaskResult(true, message);
        }

        /** */
        private VisorDefragmentationTaskResult runStatus(VisorDefragmentationTaskArg arg) {
            final IgniteDefragmentation defragmentation = ignite.context().defragmentation();

            try {
                return new VisorDefragmentationTaskResult(true, defragmentation.status().toString());
            } catch (IgniteCheckedException e) {
                return new VisorDefragmentationTaskResult(false, e.getMessage());
            }
        }

        /** */
        private VisorDefragmentationTaskResult runCancel(VisorDefragmentationTaskArg arg) {
            final IgniteDefragmentation defragmentation = ignite.context().defragmentation();

            try {
                final IgniteDefragmentation.CancelResult cancelResult = defragmentation.cancel();

                String message;

                switch (cancelResult) {
                    case SCHEDULED_NOT_FOUND:
                        message = "Scheduled defragmentation task is not found.";
                        break;
                    case CANCELLED:
                        message = "Defragmentation cancelled successfully.";
                        break;
                    case COMPLETED_OR_CANCELLED:
                        message = "Defragmentation is already completed or has been cancelled previously.";
                        break;
                    case CANCELLED_SCHEDULED:
                    default:
                        message = "Scheduled defragmentation task cancelled successfully.";
                        break;
                }

                return new VisorDefragmentationTaskResult(true, message);
            } catch (IgniteCheckedException e) {
                return new VisorDefragmentationTaskResult(false, e.getMessage());
            }
        }
    }
}
