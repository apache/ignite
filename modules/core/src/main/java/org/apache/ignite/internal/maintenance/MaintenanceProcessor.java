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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRecord;
import org.apache.ignite.maintenance.MaintenanceRegistry;
import org.apache.ignite.maintenance.MaintenanceWorkflowCallback;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** */
public class MaintenanceProcessor extends GridProcessorAdapter implements MaintenanceRegistry {
    /** */
    private static final String MAINTENANCE_FILE_NAME = "maintenance_records.mntc";

    /** */
    private static final String DELIMITER = "\t";

    /** Maintenance record consists of three parts: ID, description (user-readable part)
     * and information to execute maintenance action. */
    private static final int MNTC_RECORD_PARTS_COUNT = 3;

    /** */
    private static final String IN_MEMORY_MODE_ERR_MSG = "Maintenance Mode is not supported for in-memory clusters";

    /** */
    private final Map<UUID, MaintenanceRecord> registeredRecords = new ConcurrentHashMap<>();

    /** */
    private final Map<UUID, MaintenanceWorkflowCallback> workflowCallbacks = new ConcurrentHashMap<>();

    /** */
    private volatile File mntcRecordsFile;

    /** */
    private final boolean inMemoryMode;

    /**
     * @param ctx Kernal context.
     */
    public MaintenanceProcessor(GridKernalContext ctx) {
        super(ctx);

        inMemoryMode = !CU.isPersistenceEnabled(ctx.config());
    }

    /** {@inheritDoc} */
    @Override public void registerMaintenanceRecord(MaintenanceRecord rec) throws IgniteCheckedException {
        if (inMemoryMode)
            throw new IgniteCheckedException(IN_MEMORY_MODE_ERR_MSG);

        if (mntcRecordsFile == null) {
            log.warning("Maintenance records file not found, record won't be stored: "
                + rec.description());

            return;
        }

        try (FileOutputStream out = new FileOutputStream(mntcRecordsFile, true)) {
            try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                writeMaintenanceRecord(rec, writer);
            }
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to register maintenance record "
                + rec
                , e);
        }
    }

    /** */
    private void writeMaintenanceRecord(MaintenanceRecord rec, Writer writer) throws IOException {
        writer.write(rec.id().toString() + DELIMITER);
        writer.write(rec.description() + DELIMITER);
        writer.write(rec.actionParameters());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (inMemoryMode)
            return;

        try {
            PdsFolderSettings folderSettings = ctx.pdsFolderResolver().resolveFolders();

            File storeDir = new File(folderSettings.persistentStoreRootPath(), folderSettings.folderName());

            U.ensureDirectory(storeDir, "store directory for node persistent data", log);

            mntcRecordsFile = new File(storeDir, MAINTENANCE_FILE_NAME);

            if (!mntcRecordsFile.exists()) {
                mntcRecordsFile.createNewFile();

                return;
            }

            try (FileInputStream in = new FileInputStream(mntcRecordsFile)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    reader.lines().forEach(s -> {
                        String[] subStrs = s.split(DELIMITER);

                        if (subStrs.length != MNTC_RECORD_PARTS_COUNT) {
                            log.info("Corrupted maintenance record found, skipping: " + s);

                            return;
                        }

                        UUID id = UUID.fromString(subStrs[0]);
                        MaintenanceRecord rec = new MaintenanceRecord(id, subStrs[1], subStrs[2]);

                        registeredRecords.put(id, rec);
                    });
                }
            }
        }
        catch (Throwable t) {
            log.warning("Caught exception when starting MaintenanceProcessor," +
                " maintenance mode won't be entered", t);

            registeredRecords.clear();

            if (mntcRecordsFile != null)
                mntcRecordsFile.delete();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean prepareMaintenance() {
        workflowCallbacks.values().removeIf(cb ->
            {
                if (!cb.proceedWithMaintenance()) {
                    clearMaintenanceRecord(cb.maintenanceId());

                    return true;
                }

                return false;
            }
        );

        return !workflowCallbacks.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public void proceedWithMaintenance() {
        for (MaintenanceWorkflowCallback cb : workflowCallbacks.values()) {
            MaintenanceAction mntcAction = cb.automaticAction();

            if (mntcAction != null) {
                try {
                    mntcAction.execute();
                }
                catch (Throwable t) {
                    log.warning("Failed to execute automatic action for maintenance record: " +
                        registeredRecords.get(cb.maintenanceId()), t);

                    throw t;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable MaintenanceRecord maintenanceRecord(UUID maitenanceId) {
        return registeredRecords.get(maitenanceId);
    }

    /** {@inheritDoc} */
    @Override public boolean isMaintenanceMode() {
        return !registeredRecords.isEmpty();
    }

    /** {@inheritDoc} */
    @Override public void clearMaintenanceRecord(UUID mntcId) {
        registeredRecords.remove(mntcId);

        if (mntcRecordsFile.exists()) {
            try (FileOutputStream out = new FileOutputStream(mntcRecordsFile, false)) {
                try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    for (MaintenanceRecord rec : registeredRecords.values()) {
                        writeMaintenanceRecord(rec, writer);
                    }
                }
            }
            catch (IOException e) {
                log.warning("Failed to clear maintenance record with id "
                    + mntcId
                    + " from file, whole file will be deleted", e
                );

                mntcRecordsFile.delete();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void registerWorkflowCallback(@NotNull MaintenanceWorkflowCallback cb) {
        if (inMemoryMode)
            throw new IgniteException(IN_MEMORY_MODE_ERR_MSG);

        UUID mntcId = cb.maintenanceId();

        if (!registeredRecords.containsKey(mntcId))
            throw new IgniteException("Maintenance record for given ID not found," +
                " maintenance workflow callback for non-existing record won't be registered: " + mntcId);

        List<MaintenanceAction> actions = cb.allActions();

        if (actions == null || actions.isEmpty())
            throw new IgniteException("Maintenance workflow callback should provide at least one mainetance action");

        int size = actions.size();
        long distinctSize = actions.stream().map(a -> a.name()).distinct().count();

        if (distinctSize < size)
            throw new IgniteException("All actions of a single workflow should have unique names: " +
                actions.stream().map(a -> a.name()).collect(Collectors.joining(", ")));

        workflowCallbacks.put(mntcId, cb);
    }

    /** {@inheritDoc} */
    @Override public List<MaintenanceAction> actionsForMaintenanceRecord(UUID maintenanceId) {
        if (inMemoryMode)
            throw new IgniteException(IN_MEMORY_MODE_ERR_MSG);

        if (!registeredRecords.containsKey(maintenanceId))
            throw new IgniteException("Maintenance workflow callback for given ID not found, " +
                "cannot retrieve maintenance actions for it: " + maintenanceId);

        return workflowCallbacks.get(maintenanceId).allActions();
    }
}
