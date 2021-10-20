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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static java.util.Collections.nCopies;
import static org.apache.ignite.internal.rocksdb.RocksUtils.createSstFile;

/**
 * Storage implementation based on a single RocksDB instance.
 */
public class RocksDbPartitionStorage implements PartitionStorage {
    /** Suffix for the temporary snapshot folder */
    private static final String TMP_SUFFIX = ".tmp";

    /** Partition id. */
    private final int partId;

    /** RocksDb instance. */
    private final RocksDB db;

    /** Data column family. */
    private final ColumnFamily data;

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor = Executors.newSingleThreadExecutor();

    /**
     * Constructor.
     *
     * @param partId Partition id.
     * @param db Rocks DB instance.
     * @param columnFamily Column family to be used for all storage operations.
     * @param storage
     * @throws StorageException If failed to create RocksDB instance.
     */
    public RocksDbPartitionStorage(int partId, RocksDB db, ColumnFamily columnFamily) throws StorageException {
        this.partId = partId;
        this.db = db;
        this.data = columnFamily;
    }

    /**
     * Returns the ColumnFamily instance associated with the partition.
     *
     * @return ColumnFamily instance associated with the partition.
     */
    public ColumnFamily columnFamily() {
        return data;
    }

    /** {@inheritDoc} */
    @Override public int partitionId() {
        return partId;
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
    @Override public Collection<DataRow> readAll(List<? extends SearchRow> keys) throws StorageException {
        List<DataRow> res = new ArrayList<>(keys.size());

        try {
            List<byte[]> keysList = getKeys(keys);
            List<byte[]> valuesList = db.multiGetAsList(nCopies(keys.size(), data.handle()), keysList);

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
    @Override public void writeAll(List<? extends DataRow> rows) throws StorageException {
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
    @Override public Collection<DataRow> insertAll(List<? extends DataRow> rows) throws StorageException {
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
    @Override public Collection<SearchRow> removeAll(List<? extends SearchRow> keys) {
        List<SearchRow> skippedRows = new ArrayList<>();

        try (WriteBatch batch = new WriteBatch();
             WriteOptions opts = new WriteOptions()) {

            for (SearchRow key : keys) {
                byte[] keyBytes = key.keyBytes();

                byte[] value = data.get(keyBytes);

                if (value != null)
                    data.delete(batch, keyBytes);
                else
                    skippedRows.add(key);
            }

            db.write(opts, batch);
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }

        return skippedRows;
    }

    /** {@inheritDoc} */
    @Override public Collection<DataRow> removeAllExact(List<? extends DataRow> keyValues) {
        List<DataRow> skippedRows = new ArrayList<>();

        try (WriteBatch batch = new WriteBatch();
             WriteOptions opts = new WriteOptions()) {

            List<byte[]> keys = getKeys(keyValues);
            List<byte[]> values = db.multiGetAsList(nCopies(keys.size(), data.handle()), keys);

            assert values.size() == keyValues.size();

            for (int i = 0; i < keys.size(); i++) {
                byte[] key = keys.get(i);
                byte[] expectedValue = keyValues.get(i).valueBytes();
                byte[] value = values.get(i);

                if (Arrays.equals(value, expectedValue))
                    data.delete(batch, key);
                else
                    skippedRows.add(keyValues.get(i));
            }

            db.write(opts, batch);
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }

        return skippedRows;
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
            Path snapshotPath = path.resolve(data.name());

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
     * Gets a list of key byte arrays.
     * @param keyValues Key rows.
     * @return List of keys as byte arrays.
     */
    private List<byte[]> getKeys(List<? extends SearchRow> keyValues) {
        List<byte[]> keys = new ArrayList<>(keyValues.size());

        for (SearchRow keyValue : keyValues)
            keys.add(keyValue.keyBytes());

        return keys;
    }
}
