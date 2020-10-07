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

package org.apache.ignite.internal.maintenance;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRecord;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** */
public class MaintenanceProcessor extends GridProcessorAdapter implements MaintenanceRegistry {
    /** */
    private static final String IN_MEMORY_MODE_ERR_MSG = "Maintenance Mode is not supported for in-memory clusters";

    /**
     * Active {@link MaintenanceRecord}s are the ones that were read from disk when node entered Maintenance Mode.
     */
    private final Map<UUID, MaintenanceRecord> activeRecords = new ConcurrentHashMap<>();

    /**
     * Requested {@link MaintenanceRecord}s are collection of records requested by user
     * or other components when node operates normally (not in Maintenance Mode).
     */
    private final Map<UUID, MaintenanceRecord> requestedRecords = new ConcurrentHashMap<>();

    /** */
    private final Map<UUID, MaintenanceWorkflowCallback> workflowCallbacks = new ConcurrentHashMap<>();

    /** */
    private final MaintenanceFileStorage fileStorage;

    /** */
    private final boolean inMemoryMode;

    /**
     * @param ctx Kernal context.
     */
    public MaintenanceProcessor(GridKernalContext ctx) {
        super(ctx);

        inMemoryMode = !CU.isPersistenceEnabled(ctx.config());

        if (inMemoryMode) {
            fileStorage = new MaintenanceFileStorage(true,
                null,
                null,
                null);

            return;
        }

        fileStorage = new MaintenanceFileStorage(false,
            ctx.pdsFolderResolver(),
            ctx.config().getDataStorageConfiguration().getFileIOFactory(),
            log);
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceRecord registerMaintenanceRecord(MaintenanceRecord rec) throws IgniteCheckedException {
        if (inMemoryMode)
            throw new IgniteCheckedException(IN_MEMORY_MODE_ERR_MSG);

        if (isMaintenanceMode())
            throw new IgniteCheckedException("Node is already in Maintenance Mode, " +
                "registering additional maintenance record is not allowed in Maintenance Mode.");

        MaintenanceRecord oldRec = requestedRecords.put(rec.id(), rec);

        if (oldRec != null) {
            log.info(
                "Maintenance Record for id " + rec.id() +
                    " is already registered" +
                    oldRec.parameters() != null ? " with parameters " + oldRec.parameters() : "" + "." +
                    " It will be replaced with new record" +
                    rec.parameters() != null ? " with parameters " + rec.parameters() : "" + "."
            );
        }

        try {
            fileStorage.writeMaintenanceRecord(rec);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to register maintenance record " + rec, e);
        }

        return oldRec;
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        try {
            fileStorage.stop();
        }
        catch (IOException e) {
            log.warning("Failed to free maintenance file resources", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (inMemoryMode)
            return;

        try {
            fileStorage.init();

            activeRecords.putAll(fileStorage.getAllRecords());
        }
        catch (Throwable t) {
            log.warning("Caught exception when starting MaintenanceProcessor," +
                " maintenance mode won't be entered", t);

            activeRecords.clear();

            fileStorage.clear();
        }
    }

    /** {@inheritDoc} */
    @Override public void prepareAndExecuteMaintenance() {
        if (isMaintenanceMode()) {
            workflowCallbacks.entrySet().removeIf(cbE ->
                {
                    if (!cbE.getValue().proceedWithMaintenance()) {
                        unregisterMaintenanceRecord(cbE.getKey());

                        return true;
                    }

                    return false;
                }
            );
        }

        if (!workflowCallbacks.isEmpty())
            proceedWithMaintenance();
        else {
            if (log.isInfoEnabled())
                log.info("All maintenance records are fixed, no need to enter maintenance mode. " +
                    "Restart the node to get it back to normal operations.");
        }
    }

    /** {@inheritDoc} */
    @Override public void proceedWithMaintenance() {
        for (Map.Entry<UUID, MaintenanceWorkflowCallback> cbE : workflowCallbacks.entrySet()) {
            MaintenanceAction mntcAction = cbE.getValue().automaticAction();

            if (mntcAction != null) {
                try {
                    mntcAction.execute();
                }
                catch (Throwable t) {
                    log.warning("Failed to execute automatic action for maintenance record: " +
                        activeRecords.get(cbE.getKey()), t);

                    throw t;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceRecord activeMaintenanceRecord(UUID maitenanceId) {
        return activeRecords.get(maitenanceId);
    }

    /** {@inheritDoc} */
    @Override public boolean isMaintenanceMode() {
        return !activeRecords.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public void unregisterMaintenanceRecord(UUID mntcId) {
        if (inMemoryMode)
            return;

        if (isMaintenanceMode())
            activeRecords.remove(mntcId);
        else
            requestedRecords.remove(mntcId);

        try {
            fileStorage.deleteMaintenanceRecord(mntcId);
        }
        catch (IOException e) {
            log.warning("Failed to clear maintenance record with id "
                + mntcId
                + " from file, whole file will be deleted", e
            );

            fileStorage.clear();
        }
    }

    /** {@inheritDoc} */
    @Override public void registerWorkflowCallback(@NotNull UUID mntcId, @NotNull MaintenanceWorkflowCallback cb) {
        if (inMemoryMode)
            throw new IgniteException(IN_MEMORY_MODE_ERR_MSG);

        List<MaintenanceAction> actions = cb.allActions();

        if (actions == null || actions.isEmpty())
            throw new IgniteException("Maintenance workflow callback should provide at least one mainetance action");

        int size = actions.size();
        long distinctSize = actions.stream().map(a -> a.name()).distinct().count();

        if (distinctSize < size)
            throw new IgniteException("All actions of a single workflow should have unique names: " +
                actions.stream().map(a -> a.name()).collect(Collectors.joining(", ")));

        Optional<String> wrongActionName = actions
            .stream()
            .map(MaintenanceAction::name)
            .filter(name -> !U.alphanumericUnderscore(name))
            .findFirst();

        if (wrongActionName.isPresent())
            throw new IgniteException(
                "All actions' names should contain only alphanumeric and underscore symbols: "
                    + wrongActionName.get());

        workflowCallbacks.put(mntcId, cb);
    }

    /** {@inheritDoc} */
    @Override public List<MaintenanceAction> actionsForMaintenanceRecord(UUID maintenanceId) {
        if (inMemoryMode)
            throw new IgniteException(IN_MEMORY_MODE_ERR_MSG);

        if (!activeRecords.containsKey(maintenanceId))
            throw new IgniteException("Maintenance workflow callback for given ID not found, " +
                "cannot retrieve maintenance actions for it: " + maintenanceId);

        return workflowCallbacks.get(maintenanceId).allActions();
    }
}
