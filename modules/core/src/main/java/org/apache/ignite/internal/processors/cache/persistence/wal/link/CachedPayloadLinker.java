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
 * Wrapper of {@link DataRecordPayloadLinker} with possibility to cache {@link DataRecord} records and tracking cache misses.
 */
public class CachedPayloadLinker {
    /** Default cache size of {@link DataRecord} records in megabytes. */
    private static final long DEFAULT_CACHE_SIZE_MB = 128;

    /** WAL manager. */
    private final IgniteWriteAheadLogManager wal;

    /** Class to link {@link DataRecord) entries payload to {@link WALReferenceAwareRecord} records. */
    private final DataRecordPayloadLinker linker = new DataRecordPayloadLinker();

    /** {@link DataRecord} records cache needed for {@code linker}. */
    private final LinkedHashMap<WALPointer, DataRecord> dataRecordsCache;

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
        dataRecordsCache = new LinkedHashMap<WALPointer, DataRecord>() {
            private final long MAX_RECORDS_SIZE = dataRecordsCacheSize;

            private long recordsTotalSize = 0;

            @Override
            protected boolean removeEldestEntry(Map.Entry<WALPointer, DataRecord> eldest) {
                if (recordsTotalSize > MAX_RECORDS_SIZE && size() > 1) {
                    recordsTotalSize -= eldest.getValue().size();
                    return true;
                }
                return false;
            }

            @Override
            public DataRecord put(WALPointer key, DataRecord value) {
                recordsTotalSize += value.size();
                return super.put(key, value);
            }
        };
    }

    /**
     * Link {@code byte[]} payload from {@link DataRecord} entries to {@link WALReferenceAwareRecord} record.
     *
     * @param record WAL record.
     * @param pointer WAL pointer.
     * @throws IgniteCheckedException If unable to link payload to record.
     */
    public void linkPayload(WALRecord record, WALPointer pointer) throws IgniteCheckedException {
        if (record instanceof DataRecord) {
            DataRecord dataRecord = (DataRecord) record;

            // Re-initialize linker with new DataRecord in case of CREATE or UPDATE operations.
            if (dataRecord.operation() == GridCacheOperation.CREATE
                    || dataRecord.operation() == GridCacheOperation.UPDATE) {
                // Cache DataRecord.
                dataRecordsCache.put(pointer, (DataRecord) record);

                linker.init(dataRecord, pointer);
            }
        }
        else if (record instanceof WALReferenceAwareRecord) {
            WALReferenceAwareRecord referenceRecord = (WALReferenceAwareRecord) record;

            // There is no available payload in linker, try to lookup new DataRecord.
            if (!linker.hasPayload() || !linker.pointer().equals(referenceRecord.reference())) {
                WALPointer lookupPointer = referenceRecord.reference();

                DataRecord dataRecord = lookupDataRecord(lookupPointer);

                linker.init(dataRecord, lookupPointer);
            }

            linker.linkPayload(referenceRecord);
        }
    }

    /**
     * Lookup {@link DataRecord} from records cache or WAL with given {@code lookupPointer}.
     *
     * @param lookupPointer Possible WAL reference to {@link DataRecord}.
     * @return {@link DataRecord} associated with given {@code lookupPointer}.
     * @throws IgniteCheckedException If unable to lookup {@link DataRecord}.
     */
    private DataRecord lookupDataRecord(WALPointer lookupPointer) throws IgniteCheckedException {
        // Try to find record in cache.
        DataRecord dataRecord = dataRecordsCache.get(lookupPointer);
        if (dataRecord != null)
            return dataRecord;

        walLookups++;

        try {
            // Try to find record in WAL.
            WALIterator iterator = wal.replay(lookupPointer);
            IgniteBiTuple<WALPointer, WALRecord> tuple = iterator.next();

            if (!(tuple.getValue() instanceof DataRecord))
                throw new IllegalStateException("Unexpected WAL record " + tuple.getValue());

            return (DataRecord) tuple.getValue();
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
