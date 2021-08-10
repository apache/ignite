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

package org.apache.ignite.internal.storage.rocksdb;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Storage implementation based on a single RocksDB instance.
 */
public class RocksDbStorage implements Storage {
    static {
        RocksDB.loadLibrary();
    }

    /** RocksDB comparator options. */
    private final ComparatorOptions comparatorOptions;

    /** RocksDB comparator. */
    private final AbstractComparator comparator;

    /** RockDB options. */
    private final Options options;

    /** RocksDb instance. */
    private final RocksDB db;

    /**
     * @param dbPath Path to the folder to store data.
     * @param comparator Keys comparator.
     * @throws StorageException If failed to create RocksDB instance.
     */
    public RocksDbStorage(Path dbPath, Comparator<ByteBuffer> comparator) throws StorageException {
        try {
            comparatorOptions = new ComparatorOptions();

            this.comparator = new AbstractComparator(comparatorOptions) {
                /** {@inheritDoc} */
                @Override public String name() {
                    return "comparator";
                }

                /** {@inheritDoc} */
                @Override public int compare(ByteBuffer a, ByteBuffer b) {
                    return comparator.compare(a, b);
                }
            };

            options = new Options();

            options.setCreateIfMissing(true);

            options.setComparator(this.comparator);

            this.db = RocksDB.open(options, dbPath.toAbsolutePath().toString());
        }
        catch (RocksDBException e) {
            try {
                close();
            }
            catch (Exception ex) {
                e.addSuppressed(ex);
            }

            throw new StorageException("Failed to start the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public DataRow read(SearchRow key) throws StorageException {
        try {
            byte[] keyBytes = key.keyBytes();

            return new SimpleDataRow(keyBytes, db.get(keyBytes));
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to read data from the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> readAll(Collection<? extends SearchRow> keys) throws StorageException {
        List<DataRow> res = new ArrayList<>();

        try {
            List<byte[]> keysList = keys.stream().map(SearchRow::keyBytes).collect(Collectors.toList());

            List<byte[]> values = db.multiGetAsList(keysList);

            assert keys.size() == values.size();

            for (int i = 0; i < keysList.size(); i++) {
                byte[] key = keysList.get(i);

                res.add(new SimpleDataRow(key, values.get(i)));
            }

            return res;
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to read data from the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void write(DataRow row) throws StorageException {
        try {
            db.put(row.keyBytes(), row.valueBytes());
        }
        catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<? extends DataRow> rows) throws StorageException {
        try (WriteBatch batch = new WriteBatch();
             WriteOptions opts = new WriteOptions()) {
            for (DataRow row : rows)
                batch.put(row.keyBytes(), row.valueBytes());

            db.write(opts, batch);
        }
        catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> insertAll(Collection<? extends DataRow> rows) throws StorageException {
        List<DataRow> cantInsert = new ArrayList<>();

        try (WriteBatch batch = new WriteBatch();
             WriteOptions opts = new WriteOptions()) {

            for (DataRow row : rows) {
                if (db.get(row.keyBytes()) == null)
                    batch.put(row.keyBytes(), row.valueBytes());
                else
                    cantInsert.add(row);
            }

            db.write(opts, batch);
        }
        catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }

        return cantInsert;
    }

    /** {@inheritDoc} */
    @Override public void remove(SearchRow key) throws StorageException {
        try {
            db.delete(key.keyBytes());
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> removeAll(Collection<? extends SearchRow> keys) {
        List<DataRow> res = new ArrayList<>();

        try (WriteBatch batch = new WriteBatch();
             WriteOptions opts = new WriteOptions()) {

            for (SearchRow key : keys) {
                byte[] keyBytes = key.keyBytes();

                byte[] value = db.get(keyBytes);

                if (value != null) {
                    res.add(new SimpleDataRow(keyBytes, value));

                    batch.delete(keyBytes);
                }
            }

            db.write(opts, batch);
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> removeAllExact(Collection<? extends DataRow> keyValues) {
        List<DataRow> res = new ArrayList<>();

        try (WriteBatch batch = new WriteBatch();
             WriteOptions opts = new WriteOptions()) {

            List<byte[]> keys = new ArrayList<>();
            List<byte[]> expectedValues = new ArrayList<>();

            for (DataRow keyValue : keyValues) {
                byte[] keyBytes = keyValue.keyBytes();
                byte[] valueBytes = keyValue.valueBytes();

                keys.add(keyBytes);
                expectedValues.add(valueBytes);
            }

            List<byte[]> values = db.multiGetAsList(keys);

            assert values.size() == expectedValues.size();

            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);
                byte[] expectedValue = expectedValues.get(i);
                byte[] value = values.get(i);

                if (Arrays.equals(value, expectedValue)) {
                    res.add(new SimpleDataRow(key, value));

                    batch.delete(key);
                }
            }

            db.write(opts, batch);
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public <T> T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException {
        try {
            byte[] keyBytes = key.keyBytes();
            byte[] existingDataBytes = db.get(keyBytes);

            clo.call(new SimpleDataRow(keyBytes, existingDataBytes));

            switch (clo.operationType()) {
                case WRITE:
                    db.put(keyBytes, clo.newRow().valueBytes());

                    break;

                case REMOVE:
                    db.delete(keyBytes);

                    break;

                case NOOP:
                    break;
            }

            return clo.result();
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to access data in the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        return new ScanCursor(db.newIterator(), filter);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        IgniteUtils.closeAll(comparatorOptions, comparator, options, db);
    }

    /** Cusror wrapper over the RocksIterator object with custom filter. */
    private static class ScanCursor implements Cursor<DataRow> {
        /** Iterator from RocksDB. */
        private final RocksIterator iter;

        /** Custom filter predicate. */
        private final Predicate<SearchRow> filter;

        private ScanCursor(RocksIterator iter, Predicate<SearchRow> filter) {
            this.iter = iter;
            this.filter = filter;

            iter.seekToFirst();
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<DataRow> iterator() {
            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            while (isValid() && !filter.test(new SimpleDataRow(iter.key(), null)))
                iter.next();

            return isValid();
        }

        /**
         * Checks iterator validity.
         *
         * @throws IgniteInternalException If iterator is not valid and {@link RocksIterator#status()} has thrown an
         *      exception.
         */
        private boolean isValid() {
            if (iter.isValid())
                return true;

            try {
                iter.status();

                return false;
            }
            catch (RocksDBException e) {
                throw new IgniteInternalException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public DataRow next() {
            if (!hasNext())
                throw new NoSuchElementException();

            var row = new SimpleDataRow(iter.key(), iter.value());

            iter.next();

            return row;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {
            iter.close();
        }
    }
}
