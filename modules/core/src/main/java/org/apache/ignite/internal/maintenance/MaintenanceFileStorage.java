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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFolderSettings;
import org.apache.ignite.internal.processors.cache.persistence.filename.PdsFoldersResolver;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.maintenance.MaintenanceRecord;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Provides API for durable storage of {@link MaintenanceRecord}s and hides implementation details from higher levels.
 *
 * Human-readable storage format is rigid but simple.
 * <ol>
 *     <li>
 *         Maintenance file with recordsis stored in work directory of node
 *         under persistent store root defined by consistentId of node.
 *     </li>
 *     <li>
 *         Each record is written to disk as a {@link String} on a separate line.
 *     </li>
 *     <li>
 *         Record consists of two or three parts: record UUID, record description and optional parameters.
 *     </li>
 * </ol>
 */
public class MaintenanceFileStorage {
    /** */
    private static final String MAINTENANCE_FILE_NAME = "maintenance_records.mntc";

    /** */
    private static final String RECORDS_SEPARATOR = System.lineSeparator();

    /** */
    private static final String REC_PARTS_SEPARATOR = "\t";

    /** Maintenance record consists of two or three parts: ID, description (user-readable part)
     * and optional record parameters. */
    private static final int MAX_MNTC_RECORD_PARTS_COUNT = 3;

    /** */
    private final boolean inMemoryMode;

    /** */
    private final PdsFoldersResolver pdsFoldersResolver;

    /** */
    private volatile File mntcRecordsFile;

    /** */
    private volatile FileIO mntcRecordsFileIO;

    /** */
    private final FileIOFactory ioFactory;

    /** */
    private final Map<UUID, MaintenanceRecord> recordsInSync = new ConcurrentHashMap<>();

    /** */
    private final IgniteLogger log;

    /** */
    public MaintenanceFileStorage(boolean inMemoryMode,
                                  PdsFoldersResolver pdsFoldersResolver,
                                  FileIOFactory ioFactory,
                                  IgniteLogger log) {
        this.inMemoryMode = inMemoryMode;
        this.pdsFoldersResolver = pdsFoldersResolver;
        this.ioFactory = ioFactory;
        this.log = log;
    }

    /** */
    public void init() throws IgniteCheckedException, IOException {
        if (inMemoryMode)
            return;

        PdsFolderSettings folderSettings = pdsFoldersResolver.resolveFolders();
        File storeDir = new File(folderSettings.persistentStoreRootPath(), folderSettings.folderName());
        U.ensureDirectory(storeDir, "store directory for node persistent data", log);

        mntcRecordsFile = new File(storeDir, MAINTENANCE_FILE_NAME);

        if (!mntcRecordsFile.exists())
            mntcRecordsFile.createNewFile();

        mntcRecordsFileIO = ioFactory.create(mntcRecordsFile);

        readRecordsFromFile();
    }

    /**
     * Deletes file with maintenance records.
     */
    public void clear() {
        if (mntcRecordsFile != null)
            mntcRecordsFile.delete();
    }

    /**
     * Stops
     */
    public void stop() throws IOException {
        if (inMemoryMode)
            return;

        if (mntcRecordsFileIO != null)
            mntcRecordsFileIO.close();
    }

    /** */
    private void readRecordsFromFile() throws IOException {
        int len = (int) mntcRecordsFileIO.size();

        if (len == 0)
            return;

        byte[] allBytes = new byte[len];

        mntcRecordsFileIO.read(allBytes, 0, len);

        String[] allRecords = new String(allBytes).split(RECORDS_SEPARATOR);

        for (String recStr : allRecords) {
            String[] subStrs = recStr.split(REC_PARTS_SEPARATOR);

            int partsNum = subStrs.length;

            if (partsNum < MAX_MNTC_RECORD_PARTS_COUNT - 1) {
                log.info("Corrupted maintenance record found and will be skipped, " +
                    "mandatory parts are missing: " + recStr);

                continue;
            }

            if (partsNum > MAX_MNTC_RECORD_PARTS_COUNT) {
                log.info("Corrupted maintenance record found and will be skipped, " +
                    "too many parts in record: " + recStr);

                continue;
            }

            UUID id;

            try {
                id = UUID.fromString(subStrs[0]);
            }
            catch (IllegalArgumentException e) {
                log.info("Corrupted maintenance record found and will be skipped, " +
                    "record id is unreadable: " + recStr);

                continue;
            }

            MaintenanceRecord rec = new MaintenanceRecord(id, subStrs[1], partsNum == 3 ? subStrs[2] : null);

            recordsInSync.put(id, rec);
        }
    }

    /** */
    private void writeRecordsToFile() throws IOException {
        mntcRecordsFileIO.clear();

        String allRecs = recordsInSync.values().stream()
            .map(
                rec -> rec.id().toString() +
                    REC_PARTS_SEPARATOR +
                    rec.description() +
                    REC_PARTS_SEPARATOR +
                    (rec.parameters() != null ? rec.parameters() : "")
            )
            .collect(Collectors.joining(System.lineSeparator()));

        byte[] allRecsBytes = allRecs.getBytes();

        int left = allRecsBytes.length;
        int len = allRecsBytes.length;

        while ((left -= mntcRecordsFileIO.writeFully(allRecsBytes, len - left, left)) > 0)
            ;

        mntcRecordsFileIO.force();
    }

    /** */
    public Map<UUID, MaintenanceRecord> getAllRecords() {
        if (inMemoryMode)
            return null;

        return Collections.unmodifiableMap(recordsInSync);
    }

    /** */
    public void writeMaintenanceRecord(MaintenanceRecord rec) throws IOException {
        if (inMemoryMode)
            return;

        recordsInSync.put(rec.id(), rec);

        writeRecordsToFile();
    }

    /** */
    public void deleteMaintenanceRecord(UUID recId) throws IOException {
        if (inMemoryMode)
            return;

        recordsInSync.remove(recId);

        writeRecordsToFile();
    }
}
