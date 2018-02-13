package org.apache.ignite.internal.processors.cache.persistence.wal.link;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.MetastoreDataRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetastorageDataRow;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.SimpleDataPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

public class RowDataConverter {

    private final GridCacheSharedContext sharedCtx;

    public RowDataConverter(GridCacheSharedContext sharedCtx) {
        this.sharedCtx = sharedCtx;
    }

    public RowDataHolder convertFrom(DataRecord record) throws IgniteCheckedException {
        assert record.writeEntries().size() == 1;

        DataEntry writeEntry = record.writeEntries().get(0);

        boolean storeCacheId = false;
        if (sharedCtx.cacheContext(writeEntry.cacheId()) != null)
            storeCacheId = sharedCtx.cacheContext(writeEntry.cacheId()).group().storeCacheIdInDataPage();

        CacheDataRow row = wrap(writeEntry, storeCacheId);

        return new RowDataHolder(row, DataPageIO.VERSIONS.latest().getRowSize(row));
    }

    public RowDataHolder convertFrom(MetastoreDataRecord record) throws IgniteCheckedException {
        assert record.value() != null;

        MetastorageDataRow row = new MetastorageDataRow(record.key(), record.value());

        return new RowDataHolder(row, SimpleDataPageIO.VERSIONS.latest().getRowSize(row));
    }

    /**
     * Create CacheDataRow adapter to calculate row row size and extract byte payload from it.
     *
     * @param entry WAL {@link DataRecord} row.
     * @return CacheDataRow.
     */
    public static CacheDataRow wrap(DataEntry entry, boolean storeCacheId) {
        if (entry instanceof LazyDataEntry) {
            LazyDataEntry lazyEntry = (LazyDataEntry) entry;

            // Create key from raw bytes.
            KeyCacheObject key;
            switch (lazyEntry.getKeyType()) {
                case CacheObject.TYPE_REGULAR: {
                    key = new KeyCacheObjectImpl(null, lazyEntry.getKeyBytes(), lazyEntry.partitionId());
                    break;
                }
                case CacheObject.TYPE_BINARY: {
                    key = new BinaryObjectImpl(null, lazyEntry.getKeyBytes(), 0);
                    break;
                }
                default:
                    throw new IllegalStateException("Undefined key type: " + lazyEntry.getKeyType());
            }

            // Create value from raw bytes.
            CacheObject value;
            switch (lazyEntry.getValType()) {
                case CacheObject.TYPE_REGULAR: {
                    value = new CacheObjectImpl(null, lazyEntry.getValBytes());
                    break;
                }
                case CacheObject.TYPE_BINARY: {
                    value = new BinaryObjectImpl(null, lazyEntry.getValBytes(), 0);
                    break;
                }
                case CacheObject.TYPE_BYTE_ARR: {
                    value = new CacheObjectByteArrayImpl(lazyEntry.getValBytes());
                    break;
                }
                case CacheObject.TYPE_BINARY_ENUM: {
                    value = new BinaryEnumObjectImpl(null, lazyEntry.getValBytes());
                    break;
                }
                default:
                    throw new IllegalStateException("Undefined value type: " + lazyEntry.getValType());
            }

            return new CacheDataRowAdapter(
                    key,
                    value,
                    lazyEntry.writeVersion(),
                    lazyEntry.expireTime(),
                    storeCacheId ? lazyEntry.cacheId() : 0
            );
        }

        return new CacheDataRowAdapter(
                entry.key(),
                entry.value(),
                entry.writeVersion(),
                entry.expireTime(),
                storeCacheId ? entry.cacheId() : 0
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
        private long link;

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
            return link;
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
            this.link = link;
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
