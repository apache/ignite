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

package org.apache.ignite.internal.processors.cache.persistence.wal.link;


import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Cached wrapper of {@link DataRecordPayloadLinker} with possibility to cache {@link DataRecord} records and tracking cache misses.
 */
public class CachedPayloadLinker {
    /** Default cache size of {@link DataRecord} records in megabytes. */
    private static final long DEFAULT_CACHE_SIZE_MB = 128;

    /** WAL manager. */
    private final IgniteWriteAheadLogManager wal;

    /** {@link DataRecordPayloadLinker} cache needed for link payload. */
    private final LinkedHashMap<WALPointer, DataRecordPayloadLinker> dataRecordsCache;

    /** The number of direct WAL lookups of {@link DataRecord} records. */
    private int walLookups;

    /**
     * Create an instance of linker with given {@code wal}.
     *
     * @param wal WAL manager to lookup {@link DataRecord} records.
     */
    public CachedPayloadLinker(@NotNull IgniteWriteAheadLogManager wal) {
        this.wal = wal;

        // Extract DataRecords cache size from system properties.
        final long dataRecordsCacheSize = IgniteSystemProperties.getLong(
                IgniteSystemProperties.IGNITE_WAL_DATA_RECORDS_CACHE_SIZE_MB,
                DEFAULT_CACHE_SIZE_MB) * 1024 * 1024;

        // DataRecords size bounded cache.
        dataRecordsCache = new LinkedHashMap<WALPointer, DataRecordPayloadLinker>() {
            private final long MAX_RECORDS_SIZE = dataRecordsCacheSize;

            private long recordsTotalSize = 0;

            @Override
            protected boolean removeEldestEntry(Map.Entry<WALPointer, DataRecordPayloadLinker> eldest) {
                if (recordsTotalSize > MAX_RECORDS_SIZE && size() > 1) {
                    recordsTotalSize -= eldest.getValue().entrySize();
                    return true;
                }
                return false;
            }

            @Override
            public DataRecordPayloadLinker put(WALPointer key, DataRecordPayloadLinker value) {
                recordsTotalSize += value.entrySize();
                return super.put(key, value);
            }
        };
    }

    /**
     * Link {@code byte[]} payload from {@link DataRecord} entry to {@link WALReferenceAwareRecord} record.
     *
     * @param record WAL record.
     * @param pointer WAL pointer.
     * @throws IgniteCheckedException If unable to link payload to record.
     */
    public void linkPayload(WALRecord record, WALPointer pointer) throws IgniteCheckedException {
        if (record instanceof DataRecord) {
            DataRecord dataRecord = (DataRecord) record;

            // Create and cache linker with new DataRecord in case of CREATE or UPDATE operations.
            if (dataRecord.operation() == GridCacheOperation.CREATE
                    || dataRecord.operation() == GridCacheOperation.UPDATE) {
                DataRecordPayloadLinker linker = new DataRecordPayloadLinker(dataRecord);
                dataRecordsCache.put(pointer, linker);
            }
        }
        else if (record instanceof WALReferenceAwareRecord) {
            WALReferenceAwareRecord referenceRecord = (WALReferenceAwareRecord) record;

            WALPointer lookupPointer = referenceRecord.reference();

            DataRecordPayloadLinker linker = lookupLinker(lookupPointer);

            linker.linkPayload(referenceRecord);
        }
    }

    /**
     * Lookup {@link DataRecord} and associated linker from cache or WAL with given {@code lookupPointer}.
     *
     * @param lookupPointer Possible WAL reference to {@link DataRecord}.
     * @return {@link DataRecordPayloadLinker} associated with given {@code lookupPointer}.
     * @throws IgniteCheckedException If unable to lookup {@link DataRecord}.
     */
    private DataRecordPayloadLinker lookupLinker(WALPointer lookupPointer) throws IgniteCheckedException {
        // Try to find existed linker in cache.
        DataRecordPayloadLinker linker = dataRecordsCache.get(lookupPointer);
        if (linker != null)
            return linker;

        walLookups++;

        try {
            // Try to find record in WAL.
            WALIterator iterator = wal.replay(lookupPointer);
            IgniteBiTuple<WALPointer, WALRecord> tuple = iterator.next();

            if (!(tuple.getValue() instanceof DataRecord))
                throw new IllegalStateException("Found unexpected WAL record " + tuple.getValue());

            if (!lookupPointer.equals(tuple.getKey()))
                throw new IllegalStateException("DataRecord pointer is invalid " + tuple.getKey());

            DataRecord record = (DataRecord) tuple.getValue();

            if (!(record.operation() == GridCacheOperation.CREATE || record.operation() == GridCacheOperation.UPDATE))
                throw new IllegalStateException("DataRecord operation is invalid " + record.operation());

            linker = new DataRecordPayloadLinker(record);

            dataRecordsCache.put(lookupPointer, linker);

            return linker;
        }
        catch (Exception e) {
            throw new IgniteCheckedException("Unable to lookup DataRecord by " + lookupPointer, e);
        }
    }

    /**
     * The number of WAL lookups (cache misses) during linkage process.
     */
    public int walLookups() {
        return walLookups;
    }
}
