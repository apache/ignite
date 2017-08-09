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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.LazyDataEntry;
import org.apache.ignite.internal.pagemem.wal.record.WALReferenceAwareRecord;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectByteArrayImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.freelist.FreeListImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;

/**
 * Class to extract and link payload from {@link DataRecord} entry to {@link WALReferenceAwareRecord} records.
 */
public class DataRecordPayloadLinker {
    /** Linking entry size. */
    private final int entrySize;

    /** Linking entry. */
    private final CacheDataRow entry;

    /**
     * Create linker with given {@code record}.
     *
     * @param record Data record.
     * @throws IgniteCheckedException If it's impossible to create linker.
     */
    public DataRecordPayloadLinker(DataRecord record) throws IgniteCheckedException {
        this.entry = wrap(record.writeEntry());
        this.entrySize = FreeListImpl.getRowSize(entry, record.writeEntry().storeCacheId());
    }

    /**
     * Link {@link DataRecord} current entry {@code byte[]} payload to given {@code record}.
     *
     * @param record WAL record.
     * @throws IgniteCheckedException If it's impossible to link payload to given {@code record}.
     */
    public void linkPayload(WALReferenceAwareRecord record) throws IgniteCheckedException {
        // Initialize byte buffer for entry payload.
        ByteBuffer payloadBuffer = ByteBuffer.allocate(record.payloadSize());
        payloadBuffer.order(ByteOrder.nativeOrder());

        // Write data entry payload to buffer.
        if (record.offset() != -1)
            DataPageIO.writeFragmentData(entry, payloadBuffer, entrySize - record.offset() - record.payloadSize(), record.payloadSize());
        else
            DataPageIO.writeRowData(entry, payloadBuffer, record.payloadSize());

        record.payload(payloadBuffer.array());
    }

    /**
     * @return Linking entry size.
     */
    public int entrySize() {
        return entrySize;
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

            // Create key from raw bytes.
            KeyCacheObject key;
            switch (lazyEntry.keyType()) {
                case CacheObject.TYPE_REGULAR: {
                    key = new KeyCacheObjectImpl(null, lazyEntry.rawKey(), lazyEntry.partitionId());
                    break;
                }
                case CacheObject.TYPE_BINARY: {
                    key = new BinaryObjectImpl(null, lazyEntry.rawKey(), 0);
                    break;
                }
                default:
                    throw new IllegalStateException("Undefined key type: " + lazyEntry.keyType());
            }

            // Create value from raw bytes.
            CacheObject value;
            switch (lazyEntry.valueType()) {
                case CacheObject.TYPE_REGULAR: {
                    value = new CacheObjectImpl(null, lazyEntry.rawValue());
                    break;
                }
                case CacheObject.TYPE_BINARY: {
                    value = new BinaryObjectImpl(null, lazyEntry.rawValue(), 0);
                    break;
                }
                case CacheObject.TYPE_BYTE_ARR: {
                    value = new CacheObjectByteArrayImpl(lazyEntry.rawValue());
                    break;
                }
                case CacheObject.TYPE_BINARY_ENUM: {
                    value = new BinaryEnumObjectImpl(null, lazyEntry.rawValue());
                    break;
                }
                default:
                    throw new IllegalStateException("Undefined value type: " + lazyEntry.valueType());
            }

            return new CacheDataRowAdapter(
                    key,
                    value,
                    lazyEntry.writeVersion(),
                    lazyEntry.expireTime(),
                    lazyEntry.storeCacheId() ? lazyEntry.cacheId() : 0
            );
        }

        return new CacheDataRowAdapter(
                entry.key(),
                entry.value(),
                entry.writeVersion(),
                entry.expireTime(),
                entry.storeCacheId() ? entry.cacheId() : 0
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
