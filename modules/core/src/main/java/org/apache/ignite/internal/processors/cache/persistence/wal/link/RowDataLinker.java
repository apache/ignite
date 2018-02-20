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
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Cached wrapper of {@link RowDataHolder} with possibility to cache {@link DataRecord} records and tracking cache misses.
 */
public class RowDataLinker {
    /** Default cache size of {@link DataRecord} records in megabytes. */
    private static final long DEFAULT_CACHE_SIZE_MB = 128;

    /** WAL manager. */
    private final IgniteWriteAheadLogManager wal;

    /** Class to convert {@link DataRecord} or {@link MetastoreDataRecord} records to {@link RowDataHolder} entities. */
    private final RowDataConverter converter;

    /** {@link RowDataHolder} cache needed for link payload. */
    private final LinkedHashMap<WALPointer, RowDataHolder> rowDataCache;

    /** The number of explicit WAL lookups for {@link DataRecord} or {@link MetastoreDataRecord} records. */
    private int walLookups;

    /**
     * Create an instance of linker with given {@code wal}.
     *
     * @param sharedCtx Shared context.
     */
    public RowDataLinker(@NotNull GridCacheSharedContext sharedCtx) {
        this.wal = sharedCtx.wal();
        this.converter = new RowDataConverter(sharedCtx);

        // Extract DataRecords cache size from system properties.
        final long dataRecordsCacheSize = IgniteSystemProperties.getLong(
                IgniteSystemProperties.IGNITE_WAL_DATA_RECORDS_CACHE_SIZE_MB,
                DEFAULT_CACHE_SIZE_MB) * 1024 * 1024;

        // DataRecords size bounded cache.
        rowDataCache = new LinkedHashMap<WALPointer, RowDataHolder>() {
            private final long MAX_RECORDS_SIZE = dataRecordsCacheSize;

            private long recordsTotalSize = 0;

            @Override
            protected boolean removeEldestEntry(Map.Entry<WALPointer, RowDataHolder> eldest) {
                if (recordsTotalSize > MAX_RECORDS_SIZE && size() > 1) {
                    recordsTotalSize -= eldest.getValue().rowSize();
                    return true;
                }
                return false;
            }

            @Override
            public RowDataHolder put(WALPointer key, RowDataHolder value) {
                recordsTotalSize += value.rowSize();
                return super.put(key, value);
            }
        };
    }

    public RowDataHolder addDataRecord(DataRecord record, WALPointer pointer) throws IgniteCheckedException {
        // Create and cache linker with new DataRecord in case of CREATE or UPDATE operations.
        if (record.writeEntries().size() == 1
                && (record.operation() == GridCacheOperation.CREATE || record.operation() == GridCacheOperation.UPDATE)) {
            RowDataHolder holder = converter.convertFrom(record);

            rowDataCache.put(pointer, holder);

            return holder;
        }

        return null;
    }

    public RowDataHolder addMetastorageDataRecord(MetastoreDataRecord record, WALPointer pointer) throws IgniteCheckedException {
        if (record.value() != null) {
            RowDataHolder holder = converter.convertFrom(record);

            rowDataCache.put(pointer, holder);

            return holder;
        }

        return null;
    }

    /**
     * Link {@code byte[]} payload from {@link DataRecord} entry to {@link WALReferenceAwareRecord} record.
     *
     * @param record WAL record.
     * @throws IgniteCheckedException If unable to link payload to record.
     */
    public void linkRow(WALReferenceAwareRecord record) throws IgniteCheckedException {
        WALPointer lookupPointer = record.reference();

        RowDataHolder holder = lookupRowData(lookupPointer);

        holder.linkRow(record);
    }

    /**
     * Lookup {@link RowDataHolder} from cache or WAL associated with given {@code lookupPointer}.
     *
     * @param lookupPointer Possible WAL reference to {@link DataRecord}.
     * @return {@link RowDataHolder} associated with given {@code lookupPointer}.
     * @throws IgniteCheckedException If unable to lookup {@link DataRecord}.
     */
    private RowDataHolder lookupRowData(WALPointer lookupPointer) throws IgniteCheckedException {
        // Try to find existed data in cache.
        RowDataHolder holder = rowDataCache.get(lookupPointer);
        if (holder != null)
            return holder;

        walLookups++;

        try {
            // Try to find record in WAL.
            // TODO: Maybe we can replay over several tuples and populate cache with then in batch mode?
            WALIterator iterator = wal.replay(lookupPointer, false);
            IgniteBiTuple<WALPointer, WALRecord> tuple = iterator.next();

            if (tuple.getValue() instanceof DataRecord) {
                DataRecord record = (DataRecord) tuple.getValue();

                holder = addDataRecord(record, lookupPointer);
            }
            else if (tuple.getValue() instanceof MetastoreDataRecord) {
                MetastoreDataRecord record = (MetastoreDataRecord) tuple.getValue();

                holder = addMetastorageDataRecord(record, lookupPointer);
            }

            if (holder == null)
                throw new IllegalStateException("Invalid record " + tuple.getValue());

            return holder;
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
