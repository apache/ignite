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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.maintenance.MaintenanceAction;
import org.apache.ignite.maintenance.MaintenanceRecord;
import org.apache.ignite.maintenance.MaintenanceRegistry;
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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** */
public class MaintenanceProcessor extends GridProcessorAdapter implements MaintenanceRegistry {
    /** */
    private static final String MAINTENANCE_FILE_NAME = "maintenance_records.mntc";

    /** */
    private final Map<UUID, MaintenanceRecord> registeredRecords = new ConcurrentHashMap<>();

    /** */
    private final Map<UUID, MaintenanceAction> registeredActions = new ConcurrentHashMap<>();

    /** */
    private volatile File mntcRecordsFile;

    /**
     * @param ctx Kernal context.
     */
    public MaintenanceProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void registerMaintenanceRecord(MaintenanceRecord rec) throws IgniteCheckedException {
        if (mntcRecordsFile == null) {
            log.warning("Maintenance records file not found, record won't be stored: "
                + rec.description());

            return;
        }

        try {
            try (FileOutputStream out = new FileOutputStream(mntcRecordsFile)) {
                try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    writer.write(rec.id().toString() + '\t');
                    writer.write(rec.description() + '\t');
                    writer.write(rec.actionParameters());
                }
            }
        }
        catch (IOException ioE) {
            throw new IgniteCheckedException("Failed to register maintenance record", ioE);
        }
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        try {
            PdsFolderSettings folderSettings = ctx.pdsFolderResolver().resolveFolders();

            File storeDir = new File(folderSettings.persistentStoreRootPath(), folderSettings.folderName());

            if (!storeDir.exists())
                return;

            mntcRecordsFile = new File(storeDir, MAINTENANCE_FILE_NAME);

            if (!mntcRecordsFile.exists()) {
                mntcRecordsFile.createNewFile();

                return;
            }

            try (FileInputStream in = new FileInputStream(mntcRecordsFile)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    reader.lines().forEach(s -> {
                        String[] subStrs = s.split("\t");

                        UUID id = UUID.fromString(subStrs[0]);
                        MaintenanceRecord rec = new MaintenanceRecord(id, subStrs[1], subStrs[2]);

                        registeredRecords.put(id, rec);
                    });
                }

            }
        }
        catch (Throwable t) {
            // TODO: handle anything so that maintenance mode won't be activated
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
        //TODO cleanup record from file by recreating the file without this record
    }

    /** {@inheritDoc} */
    @Override public void registerMaintenanceAction(@NotNull UUID mntcId, @NotNull MaintenanceAction action) {
        registeredActions.put(mntcId, action);
    }
}
