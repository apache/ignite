package org.apache.ignite.internal.processors.cache.persistence.wal.link;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Base functionality to link {@link DataRecord} entries to {@link WALReferenceAwareRecord} records.
 */
public class DataRecordLinker {
    /** Array with length of entries to be linked contained in DataRecord. */
    private int[] entrySizes;

    /** Current linker position. */
    private Position position = Position.NONE;

    /**
     * Initialize linker with {@link DataRecord} entries.
     *
     * @param record DataRecord
     * @throws IgniteCheckedException If linker state is not valid.
     */
    public void init(DataRecord record) throws IgniteCheckedException {
        if (position != Position.NONE)
            throw new IgniteCheckedException("Linker previous state is not finished properly: {"
                    + "numOfDataEntries=" + entrySizes.length
                    + ", curDataEntrySize=" + entrySizes[position.index]
                    + ", curPosition=" + position
                    + "}");

        this.position = new Position(0,0);
        this.entrySizes = new int[record.writeEntries().size()];

        for (int i = 0; i < entrySizes.length; i++) {
            DataEntry entry = record.writeEntries().get(i);
            CacheDataRow dataRow = wrap(entry);

            entrySizes[i] = FreeListImpl.getRowSize(dataRow, entry.storeCacheId());
        }
    }

    /**
     * Link reference record with DataRecord entry.
     * Linker {@code position} will be changed in this case.
     *
     * @param record Linking record.
     * @throws IgniteCheckedException If it's impossible to link record.
     */
    public void link(WALReferenceAwareRecord record) throws IgniteCheckedException {
        if (position == Position.NONE)
            throw new IgniteCheckedException("Linker is not initialized: {"
                    + "referenceRecord=" + record + "}");

        feed(record);
        advance();
        tryFinish();
    }

    /**
     * @return Linker current position.
     */
    public Position position() {
        return position;
    }

    /**
     * Shift Linker {@code position} offset on linking {@code record} requested payload size.
     *
     * @param record Linking record.
     * @throws IgniteCheckedException If it's impossible to link record.
     */
    private void feed(WALReferenceAwareRecord record) throws IgniteCheckedException {
        if (position.offset + record.payloadSize() > entrySizes[position.index])
            throw new IgniteCheckedException("Linking record size is more than remaining space in current DataRecord entry: {"
                    + "numOfDataEntries=" + entrySizes.length
                    + ", curDataEntrySize=" + entrySizes[position.index]
                    + ", curPosition=" + position
                    + ", referenceRecord=" + record
                    + "}");

        position.offset += record.payloadSize();
    }

    /**
     * Shift Linker {@code position} index if current entry is fully linked.
     */
    private void advance() {
        if (position.offset == entrySizes[position.index]) {
            position.index++;
            position.offset = 0;
        }
    }

    /**
     * Invalidate Linker {@code position} if all entries are fully linked.
     */
    private void tryFinish() {
        if (position.index == entrySizes.length)
            position = Position.NONE;
    }

    /**
     * Linker position.
     */
    public static class Position {
        /** Undefined position. */
        public static final Position NONE = new Position(-1, 0);

        /** Current linking entry index. */
        public int index;

        /** Number of linked bytes in current entry. */
        public int offset;

        /**
         * Initialize Position with given {@code index} and {@code offset}
         *
         * @param index Entry index.
         * @param offset Number of linked bytes in current entry.
         */
        public Position(int index, int offset) {
            this.index = index;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "Position{" +
                    "index=" + index +
                    ", offset=" + offset +
                    '}';
        }
    }

    /**
     * Create CacheDataRow adapter to calculate entry row size and extract byte payload from it.
     *
     * @param entry WAL {@link DataRecord} entry.
     * @return CacheDataRow.
     */
    public static CacheDataRow wrap(DataEntry entry) {
        if (entry instanceof LazyDataEntry) {
            LazyDataEntry lazyEntry = (LazyDataEntry) entry;

            return new CacheDataRowAdapter(
                    new KeyCacheObjectImpl(null, lazyEntry.rawKey(), entry.partitionId()),
                    new CacheObjectImpl(null, lazyEntry.rawValue()),
                    lazyEntry.writeVersion(),
                    lazyEntry.expireTime(),
                    lazyEntry.cacheId()
            );
        }

        return new CacheDataRowAdapter(
                entry.key(),
                entry.value(),
                entry.writeVersion(),
                entry.expireTime(),
                entry.cacheId()
        );
    }

    /**
     * CacheDataRow adapter can be created both from {@link DataEntry} and {@link LazyDataEntry}.
     * Class is needed for calculating row size and extracting key/value {@code byte[]} payloads.
     */
    public static class CacheDataRowAdapter implements CacheDataRow {

        private final KeyCacheObject key;
        private final CacheObject value;
        private final GridCacheVersion version;
        private final long expireTime;
        private final int cacheId;

        public CacheDataRowAdapter(KeyCacheObject key, CacheObject value, GridCacheVersion version, long expireTime, int cacheId) {
            this.key = key;
            this.value = value;
            this.version = version;
            this.expireTime = expireTime;
            this.cacheId = cacheId;
        }

        @Override
        public KeyCacheObject key() {
            return key;
        }

        @Override
        public long link() {
            return 0;
        }

        @Override
        public int hash() {
            return 0;
        }

        @Override
        public int cacheId() {
            return cacheId;
        }

        @Override
        public CacheObject value() {
            return value;
        }

        @Override
        public GridCacheVersion version() {
            return version;
        }

        @Override
        public long expireTime() {
            return expireTime;
        }

        @Override
        public int partition() {
            return 0;
        }

        @Override
        public void link(long link) {

        }

        @Override
        public void key(KeyCacheObject key) {

        }

        @Override
        public WALPointer reference() {
            return null;
        }
    }
}
