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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
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
import org.jetbrains.annotations.TestOnly;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.apache.ignite.internal.rocksdb.RocksUtils.createSstFile;

/**
 * Storage implementation based on a single RocksDB instance.
 */
public class RocksDbStorage implements Storage {
    /** Suffix for the temporary snapshot folder */
    private static final String TMP_SUFFIX = ".tmp";

    /** Snapshot file name. */
    private static final String COLUMN_FAMILY_NAME = "data";

    static {
        RocksDB.loadLibrary();
    }

    /** RocksDB comparator options. */
    private final ComparatorOptions comparatorOptions;

    /** RocksDB comparator. */
    private final AbstractComparator comparator;

    /** RockDB options. */
    private final DBOptions options;

    /** RocksDb instance. */
    private final RocksDB db;

    /** Data column family. */
    private final ColumnFamily data;

    /** DB path. */
    private final Path dbPath;

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor = Executors.newSingleThreadExecutor();

    /**
     * @param dbPath Path to the folder to store data.
     * @param comparator Keys comparator.
     * @throws StorageException If failed to create RocksDB instance.
     */
    public RocksDbStorage(Path dbPath, Comparator<ByteBuffer> comparator) throws StorageException {
        try {
            this.dbPath = dbPath;

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

            options = new DBOptions()
                .setCreateMissingColumnFamilies(true)
                .setCreateIfMissing(true);

            Options dataOptions = new Options().setCreateIfMissing(true).setComparator(this.comparator);

            ColumnFamilyOptions dataFamilyOptions = new ColumnFamilyOptions(dataOptions);

            List<ColumnFamilyDescriptor> descriptors = Collections.singletonList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, dataFamilyOptions)
            );

            var handles = new ArrayList<ColumnFamilyHandle>();

            db = RocksDB.open(options, dbPath.toAbsolutePath().toString(), descriptors, handles);

            data = new ColumnFamily(db, handles.get(0), COLUMN_FAMILY_NAME, dataFamilyOptions, dataOptions);
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
    @Override @Nullable public DataRow read(SearchRow key) throws StorageException {
        try {
            byte[] keyBytes = key.keyBytes();

            byte[] valueBytes = data.get(keyBytes);

            return valueBytes == null ? null : new SimpleDataRow(keyBytes, valueBytes);
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to read data from the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> readAll(Collection<? extends SearchRow> keys) throws StorageException {
        List<DataRow> res = new ArrayList<>(keys.size());

        try {
            List<byte[]> keysList = keys.stream().map(SearchRow::keyBytes).collect(Collectors.toList());

            List<byte[]> valuesList = db.multiGetAsList(keysList);

            assert keys.size() == valuesList.size();

            for (int i = 0; i < keysList.size(); i++) {
                byte[] key = keysList.get(i);

                byte[] value = valuesList.get(i);

                if (value != null)
                    res.add(new SimpleDataRow(key, value));
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
            byte[] value = row.valueBytes();

            assert value != null;

            data.put(row.keyBytes(), value);
        }
        catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeAll(Collection<? extends DataRow> rows) throws StorageException {
        try (WriteBatch batch = new WriteBatch();
             WriteOptions opts = new WriteOptions()) {
            for (DataRow row : rows) {
                byte[] value = row.valueBytes();

                assert value != null;

                data.put(batch, row.keyBytes(), value);
            }

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
                if (data.get(row.keyBytes()) == null) {
                    byte[] value = row.valueBytes();

                    assert value != null;

                    data.put(batch, row.keyBytes(), value);
                } else
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
            data.delete(key.keyBytes());
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

                byte[] value = data.get(keyBytes);

                if (value != null) {
                    res.add(new SimpleDataRow(keyBytes, value));

                    data.delete(batch, keyBytes);
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

                    data.delete(batch, key);
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

            byte[] existingDataBytes = data.get(keyBytes);

            clo.call(existingDataBytes == null ? null : new SimpleDataRow(keyBytes, existingDataBytes));

            switch (clo.operationType()) {
                case WRITE:
                    DataRow newRow = clo.newRow();

                    assert newRow != null;

                    byte[] value = newRow.valueBytes();

                    assert value != null;

                    data.put(keyBytes, value);

                    break;

                case REMOVE:
                    data.delete(keyBytes);

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
        return new ScanCursor(data.newIterator(), filter);
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> snapshot(Path snapshotPath) {
        Path tempPath = Paths.get(snapshotPath.toString() + TMP_SUFFIX);

        // Create a RocksDB point-in-time snapshot
        Snapshot snapshot = db.getSnapshot();

        return CompletableFuture.runAsync(() -> {
            // (Re)create the temporary directory
            IgniteUtils.deleteIfExists(tempPath);

            try {
                Files.createDirectories(tempPath);
            }
            catch (IOException e) {
                throw new IgniteInternalException("Failed to create directory: " + tempPath, e);
            }
        }, snapshotExecutor)
            .thenRunAsync(() -> createSstFile(data, snapshot, tempPath), snapshotExecutor)
            .whenComplete((aVoid, throwable) -> {
                // Release a snapshot
                db.releaseSnapshot(snapshot);

                // Snapshot is not actually closed here, because a Snapshot instance doesn't own a pointer, the
                // database does. Calling close to maintain the AutoCloseable semantics
                snapshot.close();

                if (throwable != null)
                    return;

                // Delete snapshot directory if it already exists
                IgniteUtils.deleteIfExists(snapshotPath);

                try {
                    // Rename the temporary directory
                    Files.move(tempPath, snapshotPath);
                }
                catch (IOException e) {
                    throw new IgniteInternalException("Failed to rename: " + tempPath + " to " + snapshotPath, e);
                }
            });
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(Path path) {
        try (IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions()) {
            Path snapshotPath = path.resolve(COLUMN_FAMILY_NAME);

            if (!Files.exists(snapshotPath))
                throw new IgniteInternalException("Snapshot not found: " + snapshotPath);

            data.ingestExternalFile(Collections.singletonList(snapshotPath.toString()), ingestOptions);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException("Fail to ingest sst file at path: " + path, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(snapshotExecutor, 10, TimeUnit.SECONDS);

        IgniteUtils.closeAll(data, db, options, comparator, comparatorOptions);
    }

    /** Cursor wrapper over the RocksIterator object with custom filter. */
    private static class ScanCursor implements Cursor<DataRow> {
        /** Iterator from RocksDB. */
        private final RocksIterator iter;

        /** Custom filter predicate. */
        private final Predicate<SearchRow> filter;

        /**
         * @param iter Iterator.
         * @param filter Filter.
         */
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
            while (isValid() && !filter.test(new SimpleDataRow(iter.key(), iter.value())))
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

    /**
     * @return Path to the database.
     */
    @TestOnly
    public Path getDbPath() {
        return dbPath;
    }
}
