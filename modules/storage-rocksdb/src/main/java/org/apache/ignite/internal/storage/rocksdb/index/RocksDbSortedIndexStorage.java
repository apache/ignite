/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.rocksdb.index;

import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksIteratorAdapter;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.IndexRowDeserializer;
import org.apache.ignite.internal.storage.index.IndexRowFactory;
import org.apache.ignite.internal.storage.index.IndexRowPrefix;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * {@link SortedIndexStorage} implementation based on RocksDB.
 */
public class RocksDbSortedIndexStorage implements SortedIndexStorage {
    private final ColumnFamily indexCf;

    private final SortedIndexDescriptor descriptor;

    private final IndexRowFactory indexRowFactory;

    private final IndexRowDeserializer indexRowDeserializer;

    /**
     * Creates a new Index storage.
     *
     * @param indexCf Column Family for storing the data.
     * @param descriptor Index descriptor.
     */
    public RocksDbSortedIndexStorage(ColumnFamily indexCf, SortedIndexDescriptor descriptor) {
        this.indexCf = indexCf;
        this.descriptor = descriptor;
        this.indexRowFactory = new BinaryIndexRowFactory(descriptor);
        this.indexRowDeserializer = new BinaryIndexRowDeserializer(descriptor);
    }

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public IndexRowFactory indexRowFactory() {
        return indexRowFactory;
    }

    @Override
    public IndexRowDeserializer indexRowDeserializer() {
        return indexRowDeserializer;
    }

    @Override
    public void put(IndexRow row) {
        assert row.rowBytes().length > 0;
        assert row.primaryKey().keyBytes().length > 0;

        try {
            indexCf.put(row.rowBytes(), row.primaryKey().keyBytes());
        } catch (RocksDBException e) {
            throw new StorageException("Error while adding data to Rocks DB", e);
        }
    }

    @Override
    public void remove(IndexRow key) {
        try {
            indexCf.delete(key.rowBytes());
        } catch (RocksDBException e) {
            throw new StorageException("Error while removing data from Rocks DB", e);
        }
    }

    @Override
    public Cursor<IndexRow> range(IndexRowPrefix lowerBound, IndexRowPrefix upperBound) {
        RocksIterator iter = indexCf.newIterator();

        iter.seekToFirst();

        return new RocksIteratorAdapter<>(iter) {
            @Nullable
            private PrefixComparator lowerBoundComparator = new PrefixComparator(descriptor, lowerBound);

            private final PrefixComparator upperBoundComparator = new PrefixComparator(descriptor, upperBound);

            @Override
            public boolean hasNext() {
                while (super.hasNext()) {
                    var row = new ByteBufferRow(it.key());

                    if (lowerBoundComparator != null) {
                        // if lower comparator is not null, then the lower bound has not yet been reached
                        if (lowerBoundComparator.compare(row) < 0) {
                            it.next();

                            continue;
                        } else {
                            // once the lower bound is reached, we no longer need to check it
                            lowerBoundComparator = null;
                        }
                    }

                    return upperBoundComparator.compare(row) <= 0;
                }

                return false;
            }

            @Override
            protected IndexRow decodeEntry(byte[] key, byte[] value) {
                return new BinaryIndexRow(key, value);
            }
        };
    }

    @Override
    public void close() throws Exception {
        indexCf.close();
    }

    @Override
    public void destroy() {
        try {
            indexCf.destroy();
        } catch (Exception e) {
            throw new StorageException(String.format("Failed to destroy index \"%s\"", descriptor.name()), e);
        }
    }
}
