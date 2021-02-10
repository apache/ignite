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

package org.apache.ignite.internal.processors.cache.persistence.defragmentation;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.Status;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceTask;

import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.CachePartitionDefragmentationManager.DEFRAGMENTATION_MNTC_TASK_NAME;
import static org.apache.ignite.internal.processors.cache.persistence.defragmentation.maintenance.DefragmentationParameters.toStore;

/**
 * Defragmentation operation service implementation.
 */
public class IgniteDefragmentationImpl implements IgniteDefragmentation {
    /** Kernal context. */
    private final GridKernalContext ctx;

    public IgniteDefragmentationImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public ScheduleResult schedule(List<String> cacheNames) throws IgniteCheckedException {
        final MaintenanceRegistry maintenanceRegistry = ctx.maintenanceRegistry();

        MaintenanceTask oldTask;

        try {
            oldTask = maintenanceRegistry.registerMaintenanceTask(toStore(cacheNames != null ? cacheNames : Collections.emptyList()));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteCheckedException("Scheduling failed: " + e.getMessage());
        }

        return oldTask != null ? ScheduleResult.SUCCESS_SUPERSEDED_PREVIOUS : ScheduleResult.SUCCESS;
    }

    /** {@inheritDoc} */
    @Override public CancelResult cancel() throws IgniteCheckedException {
        final MaintenanceRegistry maintenanceRegistry = ctx.maintenanceRegistry();

        if (!maintenanceRegistry.isMaintenanceMode()) {
            boolean deleted = maintenanceRegistry.unregisterMaintenanceTask(DEFRAGMENTATION_MNTC_TASK_NAME);

            return deleted ? CancelResult.CANCELLED_SCHEDULED : CancelResult.SCHEDULED_NOT_FOUND;
        }
        else {
            List<MaintenanceAction<?>> actions;

            try {
                actions = maintenanceRegistry.actionsForMaintenanceTask(DEFRAGMENTATION_MNTC_TASK_NAME);
            }
            catch (IgniteException e) {
                return CancelResult.COMPLETED_OR_CANCELLED;
            }

            Optional<MaintenanceAction<?>> stopAct = actions.stream().filter(a -> "stop".equals(a.name())).findAny();

            assert stopAct.isPresent();

            try {
                Object res = stopAct.get().execute();

                assert res instanceof Boolean;

                boolean cancelled = (Boolean)res;

                return cancelled ? CancelResult.CANCELLED : CancelResult.COMPLETED_OR_CANCELLED;
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Exception occurred: " + e.getMessage(), e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public DefragmentationStatus status() throws IgniteCheckedException {
        final MaintenanceRegistry maintenanceRegistry = ctx.maintenanceRegistry();

        if (!maintenanceRegistry.isMaintenanceMode())
            throw new IgniteCheckedException("Node is not in maintenance mode.");

        IgniteCacheDatabaseSharedManager dbMgr = ctx.cache().context().database();

        assert dbMgr instanceof GridCacheDatabaseSharedManager;

        CachePartitionDefragmentationManager defrgMgr = ((GridCacheDatabaseSharedManager)dbMgr)
                .defragmentationManager();

        if (defrgMgr == null)
            throw new IgniteCheckedException("There's no active defragmentation process on the node.");

        final Status status = defrgMgr.status();

        final long startTs = status.getStartTs();
        final long finishTs = status.getFinishTs();
        final long elapsedTime = finishTs != 0 ? finishTs - startTs : System.currentTimeMillis() - startTs;

        Map<String, CompletedDefragmentationInfo> completedCaches = new HashMap<>();
        Map<String, InProgressDefragmentationInfo> progressCaches = new HashMap<>();

        status.getFinishedGroups().forEach((context, progress) -> {
            final String name = context.cacheOrGroupName();

            final long oldSize = progress.getOldSize();
            final long newSize = progress.getNewSize();
            final long cgElapsedTime = progress.getFinishTs() - progress.getStartTs();

            final CompletedDefragmentationInfo info = new CompletedDefragmentationInfo(cgElapsedTime, oldSize, newSize);
            completedCaches.put(name, info);
        });

        status.getProgressGroups().forEach((context, progress) -> {
            final String name = context.cacheOrGroupName();

            final long cgElapsedTime = System.currentTimeMillis() - progress.getStartTs();
            final int partsTotal = progress.getPartsTotal();
            final int partsCompleted = progress.getPartsCompleted();

            final InProgressDefragmentationInfo info = new InProgressDefragmentationInfo(cgElapsedTime, partsCompleted, partsTotal);
            progressCaches.put(name, info);
        });

        return new DefragmentationStatus(
            completedCaches,
            progressCaches,
            status.getScheduledGroups(),
            status.getSkippedGroups(),
            status.getTotalPartitionCount(),
            status.getDefragmentedPartitionCount(),
            startTs,
            elapsedTime
        );
    }

    /** {@inheritDoc} */
    @Override public boolean inProgress() {
        final Status status = getStatus();

        return status != null && status.getFinishTs() == 0;
    }

    /** {@inheritDoc} */
    @Override public int processedPartitions() {
        final Status status = getStatus();

        if (status == null)
            return 0;

        return status.getDefragmentedPartitionCount();
    }

    /** {@inheritDoc} */
    @Override public int totalPartitions() {
        final CachePartitionDefragmentationManager.Status status = getStatus();

        if (status == null)
            return 0;

        return status.getTotalPartitionCount();
    }

    /** {@inheritDoc} */
    @Override public long startTime() {
        final CachePartitionDefragmentationManager.Status status = getStatus();

        if (status == null)
            return 0;

        return status.getStartTs();
    }

    /**
     * Get defragmentation status.
     * @return Defragmentation status or {@code null} if there is no ongoing defragmentation.
     */
    private Status getStatus() {
        final MaintenanceRegistry maintenanceRegistry = ctx.maintenanceRegistry();

        if (!maintenanceRegistry.isMaintenanceMode())
            return null;

        IgniteCacheDatabaseSharedManager dbMgr = ctx.cache().context().database();

        assert dbMgr instanceof GridCacheDatabaseSharedManager;

        CachePartitionDefragmentationManager defrgMgr = ((GridCacheDatabaseSharedManager) dbMgr)
                .defragmentationManager();

        if (defrgMgr == null)
            return null;

        return defrgMgr.status();
    }

}
