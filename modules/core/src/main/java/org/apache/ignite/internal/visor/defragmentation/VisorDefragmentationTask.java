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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.DEFRAGMENTATION_MNTC_TASK_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters.toStore;

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
            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

            MaintenanceTask oldTask;

            try {
                List<String> cacheNames = arg.cacheNames();

                oldTask = mntcReg.registerMaintenanceTask(toStore(cacheNames == null ? Collections.emptyList() : cacheNames));
            }
            catch (IgniteCheckedException e) {
                return new VisorDefragmentationTaskResult(false, "Scheduling failed: " + e.getMessage());
            }

            return new VisorDefragmentationTaskResult(
                true,
                "Scheduling completed successfully." +
                (oldTask == null ? "" : " Previously scheduled task has been removed.")
            );
        }

        /** */
        private VisorDefragmentationTaskResult runStatus(VisorDefragmentationTaskArg arg) {
            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

            if (!mntcReg.isMaintenanceMode())
                return new VisorDefragmentationTaskResult(false, "Node is not in maintenance node.");

            IgniteCacheDatabaseSharedManager dbMgr = ignite.context().cache().context().database();

            assert dbMgr instanceof GridCacheDatabaseSharedManager;

            CachePartitionDefragmentationManager defrgMgr = ((GridCacheDatabaseSharedManager)dbMgr)
                .defragmentationManager();

            if (defrgMgr == null)
                return new VisorDefragmentationTaskResult(true, "There's no active defragmentation process on the node.");

            return new VisorDefragmentationTaskResult(true, defrgMgr.status());
        }

        /** */
        private VisorDefragmentationTaskResult runCancel(VisorDefragmentationTaskArg arg) {
            assert arg.cacheNames() == null : "Cancelling specific caches is not yet implemented";

            MaintenanceRegistry mntcReg = ignite.context().maintenanceRegistry();

            if (!mntcReg.isMaintenanceMode()) {
                boolean deleted = mntcReg.unregisterMaintenanceTask(DEFRAGMENTATION_MNTC_TASK_NAME);

                String msg = deleted
                    ? "Scheduled defragmentation task cancelled successfully."
                    : "Scheduled defragmentation task is not found.";

                return new VisorDefragmentationTaskResult(true, msg);
            }
            else {
                List<MaintenanceAction<?>> actions;

                try {
                    actions = mntcReg.actionsForMaintenanceTask(DEFRAGMENTATION_MNTC_TASK_NAME);
                }
                catch (IgniteException e) {
                    return new VisorDefragmentationTaskResult(true, "Defragmentation is already completed or has been cancelled previously.");
                }

                Optional<MaintenanceAction<?>> stopAct = actions.stream().filter(a -> "stop".equals(a.name())).findAny();

                assert stopAct.isPresent();

                try {
                    Object res = stopAct.get().execute();

                    assert res instanceof Boolean;

                    boolean cancelled = (Boolean)res;

                    String msg = cancelled
                        ? "Defragmentation cancelled successfully."
                        : "Defragmnentation is already completed or has been cancelled previously.";

                    return new VisorDefragmentationTaskResult(true, msg);
                }
                catch (Exception e) {
                    return new VisorDefragmentationTaskResult(false, "Exception occurred: " + e.getMessage());
                }
            }
        }
    }
}
