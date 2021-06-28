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
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.rocksdb.AbstractComparator;
import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

/**
 * Storage implementation based on a single RocksDB instance.
 */
public class RocksDbStorage implements Storage, AutoCloseable {
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

    /** RW lock. */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

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
            close();

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
    @Override public void write(DataRow row) throws StorageException {
        rwLock.readLock().lock();

        try {
            db.put(row.keyBytes(), row.valueBytes());
        }
        catch (RocksDBException e) {
            throw new StorageException("Filed to write data to the storage", e);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void remove(SearchRow key) throws StorageException {
        rwLock.readLock().lock();

        try {
            db.delete(key.keyBytes());
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to remove data from the storage", e);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void invoke(SearchRow key, InvokeClosure clo) throws StorageException {
        rwLock.writeLock().lock();

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
        }
        catch (RocksDBException e) {
            throw new StorageException("Failed to access data in the storage", e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        return new ScanCursor(db.newIterator(), filter);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        try (comparatorOptions; comparator; options) {
            db.close();
        }
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
