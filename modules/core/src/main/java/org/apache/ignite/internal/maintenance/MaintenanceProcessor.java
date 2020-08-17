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
import org.apache.ignite.maintenance.MaintenanceRecordBuilder;
import org.apache.ignite.maintenance.MaintenanceRegistry;
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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/** */
public class MaintenanceProcessor extends GridProcessorAdapter implements MaintenanceRegistry {
    /** */
    private static final String MAINTENANCE_FILE_NAME = "maintenance.txt";

    /** */
    private final Map<UUID, String> registeredRecords = new HashMap<>();

    /**
     * @param ctx Kernal context.
     */
    public MaintenanceProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void registerMaintenanceRecord(MaintenanceRecordBuilder bldr) throws IgniteCheckedException {
        try {
            File mntcFile = new File(MAINTENANCE_FILE_NAME);

            try (FileOutputStream out = new FileOutputStream(mntcFile)) {
                try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                    writer.write(bldr.maintenanceTypeId().toString() + System.lineSeparator());
                    writer.write(bldr.maintenanceDescription() + System.lineSeparator());
                    writer.write(bldr.getMaintenanceRecord());
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
            File mntcFile = new File(MAINTENANCE_FILE_NAME);

            try (FileInputStream in = new FileInputStream(mntcFile)) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                    reader.lines().forEach(s -> {
                        String[] subStrs = s.split(System.lineSeparator());

                        registeredRecords.put(UUID.fromString(subStrs[0]), subStrs[subStrs.length - 1]);
                    });
                }

            }
        }
        catch (IOException ioE) {
            // No-op.
        }
    }

    /** {@inheritDoc} */
    @Override public @Nullable String maintenanceRecord(UUID maitenanceId) {
        return registeredRecords.get(maitenanceId);
    }
}
