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

package org.apache.ignite.internal.metastorage.server.persistence;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.metastorage.server.Condition;
import org.apache.ignite.internal.metastorage.server.Entry;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.Operation;
import org.apache.ignite.internal.metastorage.server.Value;
import org.apache.ignite.internal.metastorage.server.WatchEvent;
import org.apache.ignite.internal.rocksdb.ColumnFamily;
import org.apache.ignite.internal.rocksdb.RocksBiPredicate;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteInternalException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import static org.apache.ignite.internal.metastorage.server.Value.TOMBSTONE;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.appendLong;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.bytesToLong;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.bytesToValue;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.getAsLongs;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.keyToRocksKey;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.longToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.RocksStorageUtils.valueToBytes;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.DATA;
import static org.apache.ignite.internal.metastorage.server.persistence.StorageColumnFamilyType.INDEX;
import static org.apache.ignite.internal.rocksdb.RocksUtils.createSstFile;
import static org.apache.ignite.internal.rocksdb.RocksUtils.find;
import static org.apache.ignite.internal.rocksdb.RocksUtils.forEach;
import static org.apache.ignite.internal.util.ArrayUtils.LONG_EMPTY_ARRAY;

/**
 * Key-value storage based on RocksDB.
 * Keys are stored with revision.
 * Values are stored in the default column family with an update counter and a boolean flag which represents
 * whether this record is a tombstone.
 * <br>
 * Key: [8 bytes revision, N bytes key itself].
 * <br>
 * Value: [8 bytes update counter, 1 byte tombstone flag, N bytes value].
 * <br>
 * The mapping from the key to the set of the storage's revisions is stored in the "index" column family.
 * A key represents the key of an entry and the value is a {@code byte[]} that represents a {@code long[]} where every
 * item is a revision of the storage.
 */
public class RocksDBKeyValueStorage implements KeyValueStorage {
    /** Suffix for the temporary snapshot folder */
    private static final String TMP_SUFFIX = ".tmp";

    /** A revision to store with system entries. */
    private static final long SYSTEM_REVISION_MARKER_VALUE = -1;

    /** Revision key. */
    private static final byte[] REVISION_KEY = keyToRocksKey(
        SYSTEM_REVISION_MARKER_VALUE,
        "SYSTEM_REVISION_KEY".getBytes(StandardCharsets.UTF_8)
    );

    /** Update counter key. */
    private static final byte[] UPDATE_COUNTER_KEY = keyToRocksKey(
        SYSTEM_REVISION_MARKER_VALUE,
        "SYSTEM_UPDATE_COUNTER_KEY".getBytes(StandardCharsets.UTF_8)
    );

    /**
     * Special value for the revision number which means that operation should be applied
     * to the latest revision of an entry.
     */
    private static final long LATEST_REV = -1;

    /** Lexicographic order comparator. */
    static final Comparator<byte[]> CMP = Arrays::compare;

    static {
        RocksDB.loadLibrary();
    }

    /** RW lock. */
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    /** Thread-pool for snapshot operations execution. */
    private final ExecutorService snapshotExecutor = Executors.newFixedThreadPool(2);

    /** Path to the rocksdb database. */
    private final Path dbPath;

    /** RockDB options. */
    private volatile DBOptions options;

    /** RocksDb instance. */
    private volatile RocksDB db;

    /** Data column family. */
    private volatile ColumnFamily data;

    /** Index column family. */
    private volatile ColumnFamily index;

    /** Revision. Will be incremented for each single-entry or multi-entry update operation. */
    private volatile long rev;

    /** Update counter. Will be incremented for each update of any particular entry. */
    private volatile long updCntr;

    /**
     * Constructor.
     *
     * @param dbPath RocksDB path.
     */
    public RocksDBKeyValueStorage(Path dbPath) {
        this.dbPath = dbPath;
    }

    /** {@inheritDoc} */
    @Override public void start() {
        options = new DBOptions()
            .setCreateMissingColumnFamilies(true)
            .setCreateIfMissing(true);

        Options dataOptions = new Options().setCreateIfMissing(true)
            // The prefix is the revision of an entry, so prefix length is the size of a long
            .useFixedLengthPrefixExtractor(Long.BYTES);

        ColumnFamilyOptions dataFamilyOptions = new ColumnFamilyOptions(dataOptions);

        Options indexOptions = new Options().setCreateIfMissing(true);

        ColumnFamilyOptions indexFamilyOptions = new ColumnFamilyOptions(indexOptions);

        List<ColumnFamilyDescriptor> descriptors = Arrays.asList(
            new ColumnFamilyDescriptor(DATA.nameAsBytes(), dataFamilyOptions),
            new ColumnFamilyDescriptor(INDEX.nameAsBytes(), indexFamilyOptions)
        );

        var handles = new ArrayList<ColumnFamilyHandle>();

        try {
            // Delete existing data, relying on the raft's snapshot and log playback
            destroyRocksDB();

            this.db = RocksDB.open(options, dbPath.toAbsolutePath().toString(), descriptors, handles);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException("Failed to start the storage", e);
        }

        data = new ColumnFamily(db, handles.get(0), DATA.name(), dataFamilyOptions, dataOptions);

        index = new ColumnFamily(db, handles.get(1), INDEX.name(), indexFamilyOptions, indexOptions);
    }

    /**
     * Clear the RocksDB instance.
     * The major difference with directly deleting the DB directory manually is that
     * destroyDB() will take care of the case where the RocksDB database is stored
     * in multiple directories. For instance, a single DB can be configured to store
     * its data in multiple directories by specifying different paths to
     * DBOptions::db_paths, DBOptions::db_log_dir, and DBOptions::wal_dir.
     *
     * @throws RocksDBException If failed.
     */
    private void destroyRocksDB() throws RocksDBException {
        try (Options opt = new Options()) {
            RocksDB.destroyDB(dbPath.toString(), opt);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(snapshotExecutor, 10, TimeUnit.SECONDS);

        IgniteUtils.closeAll(data, index, db, options);
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public CompletableFuture<Void> snapshot(Path snapshotPath) {
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
        }, snapshotExecutor).thenCompose(aVoid ->
            // Create futures for capturing SST snapshots of the column families
            CompletableFuture.allOf(
                CompletableFuture.runAsync(() -> createSstFile(data, snapshot, tempPath), snapshotExecutor),
                CompletableFuture.runAsync(() -> createSstFile(index, snapshot, tempPath), snapshotExecutor)
            )
        ).whenComplete((aVoid, throwable) -> {
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
                IgniteUtils.atomicMoveFile(tempPath, snapshotPath, null);
            }
            catch (IOException e) {
                throw new IgniteInternalException("Failed to rename: " + tempPath + " to " + snapshotPath, e);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void restoreSnapshot(Path path) {
        rwLock.writeLock().lock();

        try (IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions()) {
            for (ColumnFamily family : Arrays.asList(data, index)) {
                Path snapshotPath = path.resolve(family.name());

                if (!Files.exists(snapshotPath))
                    throw new IgniteInternalException("Snapshot not found: " + snapshotPath);

                family.ingestExternalFile(Collections.singletonList(snapshotPath.toString()), ingestOptions);
            }

            rev = bytesToLong(data.get(REVISION_KEY));

            updCntr = bytesToLong(data.get(UPDATE_COUNTER_KEY));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException("Fail to ingest sst file at path: " + path, e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public long revision() {
        return rev;
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return updCntr;
    }

    /** {@inheritDoc} */
    @Override public void put(byte[] key, byte[] value) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            long cntr = updCntr + 1;

            addDataToBatch(batch, key, value, curRev, cntr);

            updateKeysIndex(batch, key, curRev);

            fillAndWriteBatch(batch, curRev, cntr);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Adds a revision to the keys index.
     *
     * @param batch Write batch.
     * @param key Key.
     * @param curRev New revision for key.
     */
    private void updateKeysIndex(WriteBatch batch, byte[] key, long curRev) {
        try {
            // Get the revisions current value
            byte @Nullable [] array = index.get(key);

            // Store the new value
            index.put(batch, key, appendLong(array, curRev));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }

    /**
     * Fills the batch with system values (the update counter and the revision) and writes it to the db.
     *
     * @param batch Write batch.
     * @param newRev New revision.
     * @param newCntr New update counter.
     * @throws RocksDBException If failed.
     */
    private void fillAndWriteBatch(WriteBatch batch, long newRev, long newCntr) throws RocksDBException {
        try (WriteOptions opts = new WriteOptions()) {
            data.put(batch, UPDATE_COUNTER_KEY, longToBytes(newCntr));
            data.put(batch, REVISION_KEY, longToBytes(newRev));

            db.write(opts, batch);

            rev = newRev;
            updCntr = newCntr;
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndPut(byte[] key, byte[] value) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;
            long cntr = updCntr + 1;

            long[] revs = getRevisions(key);

            long lastRev = revs.length == 0 ? 0 : lastRevision(revs);

            addDataToBatch(batch, key, value, curRev, cntr);

            updateKeysIndex(batch, key, curRev);

            fillAndWriteBatch(batch, curRev, cntr);

            // Return previous value.
            return doGetValue(key, lastRev);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void putAll(List<byte[]> keys, List<byte[]> values) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            long counter = addAllToBatch(batch, keys, values, curRev);

            for (byte[] key : keys)
                updateKeysIndex(batch, key, curRev);

            fillAndWriteBatch(batch, curRev, counter);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAndPutAll(List<byte[]> keys, List<byte[]> values) {
        Collection<Entry> res;

        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            res = doGetAll(keys, curRev);

            long counter = addAllToBatch(batch, keys, values, curRev);

            for (byte[] key : keys)
                updateKeysIndex(batch, key, curRev);

            fillAndWriteBatch(batch, curRev, counter);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }

        return res;
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry get(byte[] key) {
        rwLock.readLock().lock();

        try {
            return doGet(key, LATEST_REV, false);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry get(byte[] key, long rev) {
        rwLock.readLock().lock();

        try {
            return doGet(key, rev, true);
        }
        finally {
            rwLock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAll(List<byte[]> keys) {
        return doGetAll(keys, LATEST_REV);
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        return doGetAll(keys, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override public void remove(byte[] key) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;
            long counter = updCntr + 1;

            if (addToBatchForRemoval(batch, key, curRev, counter)) {
                updateKeysIndex(batch, key, curRev);

                fillAndWriteBatch(batch, curRev, counter);
            }
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Entry getAndRemove(byte[] key) {
        rwLock.writeLock().lock();

        try {
            Entry e = doGet(key, LATEST_REV, false);

            if (e.empty() || e.tombstone())
                return e;

            return getAndPut(key, TOMBSTONE);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void removeAll(List<byte[]> keys) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            long counter = updCntr;

            for (byte[] key : keys) {
                if (addToBatchForRemoval(batch, key, curRev, counter + 1)) {
                    existingKeys.add(key);

                    counter++;
                }
            }

            for (byte[] key : existingKeys)
                updateKeysIndex(batch, key, curRev);

            fillAndWriteBatch(batch, curRev, counter);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @NotNull
    @Override public Collection<Entry> getAndRemoveAll(List<byte[]> keys) {
        Collection<Entry> res = new ArrayList<>(keys.size());

        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            long curRev = rev + 1;

            List<byte[]> existingKeys = new ArrayList<>(keys.size());

            List<byte[]> vals = new ArrayList<>(keys.size());

            for (byte[] key : keys) {
                Entry e = doGet(key, LATEST_REV, false);

                res.add(e);

                if (e.empty() || e.tombstone())
                    continue;

                existingKeys.add(key);

                vals.add(TOMBSTONE);
            }

            long counter = addAllToBatch(batch, existingKeys, vals, curRev);

            for (byte[] key : existingKeys)
                updateKeysIndex(batch, key, curRev);

            fillAndWriteBatch(batch, curRev, counter);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean invoke(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            Entry e = get(condition.key());

            boolean branch = condition.test(e);

            Collection<Operation> ops = branch ? success : failure;

            long curRev = rev + 1;

            boolean modified = false;

            long counter = updCntr;

            List<byte[]> updatedKeys = new ArrayList<>();

            for (Operation op : ops) {
                byte[] key = op.key();

                switch (op.type()) {
                    case PUT:
                        counter++;

                        addDataToBatch(batch, key, op.value(), curRev, counter);

                        updatedKeys.add(key);

                        modified = true;

                        break;

                    case REMOVE:
                        counter++;

                        boolean removed = addToBatchForRemoval(batch, key, curRev, counter);

                        if (!removed)
                            counter--;
                        else
                            updatedKeys.add(key);

                        modified |= removed;

                        break;

                    case NO_OP:
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown operation type: " + op.type());
                }
            }

            if (modified) {
                for (byte[] key : updatedKeys)
                    updateKeysIndex(batch, key, curRev);

                fillAndWriteBatch(batch, curRev, counter);
            }

            return branch;
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo) {
        return new RangeCursor(this, keyFrom, keyTo, rev);
    }

    /** {@inheritDoc} */
    @Override public Cursor<Entry> range(byte[] keyFrom, byte[] keyTo, long revUpperBound) {
        return new RangeCursor(this, keyFrom, keyTo, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] keyFrom, byte @Nullable [] keyTo, long rev) {
        assert keyFrom != null : "keyFrom couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        return new WatchCursor(this, rev, k ->
            CMP.compare(keyFrom, k) <= 0 && (keyTo == null || CMP.compare(k, keyTo) < 0)
        );
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(byte[] key, long rev) {
        assert key != null : "key couldn't be null.";
        assert rev > 0 : "rev must be positive.";

        return new WatchCursor(this, rev, k -> CMP.compare(k, key) == 0);
    }

    /** {@inheritDoc} */
    @Override public Cursor<WatchEvent> watch(Collection<byte[]> keys, long rev) {
        assert keys != null && !keys.isEmpty() : "keys couldn't be null or empty: " + keys;
        assert rev > 0 : "rev must be positive.";

        TreeSet<byte[]> keySet = new TreeSet<>(CMP);

        keySet.addAll(keys);

        return new WatchCursor(this, rev, keySet::contains);
    }

    /** {@inheritDoc} */
    @Override public void compact() {
        rwLock.writeLock().lock();

        try (WriteBatch batch = new WriteBatch()) {
            try (RocksIterator iterator = index.newIterator()) {
                iterator.seekToFirst();

                forEach(iterator, (key, value) -> compactForKey(batch, key, getAsLongs(value)));
            }

            fillAndWriteBatch(batch, rev, updCntr);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
        finally {
            rwLock.writeLock().unlock();
        }
    }

    /**
     * Adds a key to a batch marking the value as a tombstone.
     *
     * @param batch Write batch.
     * @param key Target key.
     * @param curRev Revision.
     * @param counter Update counter.
     * @return {@code true} if an entry can be deleted.
     * @throws RocksDBException If failed.
     */
    private boolean addToBatchForRemoval(WriteBatch batch, byte[] key, long curRev, long counter) throws RocksDBException {
        Entry e = doGet(key, LATEST_REV, false);

        if (e.empty() || e.tombstone())
            return false;

        addDataToBatch(batch, key, TOMBSTONE, curRev, counter);

        return true;
    }

    /**
     * Compacts all entries by the given key, removing all previous revisions and deleting the last entry if it is
     * a tombstone.
     *
     * @param batch Write batch.
     * @param key Target key.
     * @param revs Revisions.
     * @throws RocksDBException If failed.
     */
    private void compactForKey(WriteBatch batch, byte[] key, long[] revs) throws RocksDBException {
        long lastRev = lastRevision(revs);

        for (int i = 0; i < revs.length - 1; i++)
            data.delete(batch, keyToRocksKey(revs[i], key));

        byte[] rocksKey = keyToRocksKey(lastRev, key);

        Value value = bytesToValue(data.get(rocksKey));

        if (value.tombstone()) {
            index.delete(batch, rocksKey);

            index.delete(batch, key);
        }
        else
            index.put(batch, key, longToBytes(lastRev));
    }

    /**
     * Gets all entries with given keys and a revision.
     *
     * @param keys Target keys.
     * @param rev Target revision.
     * @return Collection of entries.
     */
    @NotNull
    private Collection<Entry> doGetAll(Collection<byte[]> keys, long rev) {
        assert keys != null : "keys list can't be null.";
        assert !keys.isEmpty() : "keys list can't be empty.";
        assert rev > 0 || rev == LATEST_REV : "Revision must be positive or " + LATEST_REV + '.';

        Collection<Entry> res = new ArrayList<>(keys.size());

        rwLock.readLock().lock();

        try {
            for (byte[] key : keys)
                res.add(doGet(key, rev, false));
        }
        finally {
            rwLock.readLock().unlock();
        }

        return res;
    }

    /**
     * Gets the value by key and revision.
     *
     * @param key Target key.
     * @param rev Target revision.
     * @param exactRev {@code true} if searching for exact revision, {@code false} if rev is an upper bound (inclusive).
     * @return Value.
     */
    @NotNull
    Entry doGet(byte[] key, long rev, boolean exactRev) {
        assert rev == LATEST_REV && !exactRev || rev > LATEST_REV :
            "Invalid arguments: [rev=" + rev + ", exactRev=" + exactRev + ']';

        long[] revs;
        try {
            revs = getRevisions(key);
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }

        if (revs == null || revs.length == 0)
            return Entry.empty(key);

        long lastRev;

        if (rev == LATEST_REV)
            lastRev = lastRevision(revs);
        else
            lastRev = exactRev ? rev : maxRevision(revs, rev);

        // lastRev can be -1 if maxRevision return -1.
        if (lastRev == -1)
            return Entry.empty(key);

        return doGetValue(key, lastRev);
    }

    /**
     * Get a list of the revisions of the entry corresponding to the key.
     *
     * @param key Key.
     * @return List of the revisions.
     * @throws RocksDBException If failed to perform {@link RocksDB#get(ColumnFamilyHandle, byte[])}.
     */
    private long[] getRevisions(byte[] key) throws RocksDBException {
        byte[] revisions = index.get(key);

        if (revisions == null)
            return LONG_EMPTY_ARRAY;

        return getAsLongs(revisions);
    }

    /**
     * Returns maximum revision which must be less or equal to {@code upperBoundRev}. If there is no such revision then
     * {@code -1} will be returned.
     *
     * @param revs Revisions list.
     * @param upperBoundRev Revision upper bound.
     * @return Maximum revision or {@code -1} if there is no such revision.
     */
    static long maxRevision(long[] revs, long upperBoundRev) {
        for (int i = revs.length - 1; i >= 0; i--) {
            long rev = revs[i];

            if (rev <= upperBoundRev)
                return rev;
        }

        return -1;
    }

    /**
     * Gets the value by a key and a revision.
     *
     * @param key Target key.
     * @param revision Target revision.
     * @return Entry.
     */
    @NotNull
    Entry doGetValue(byte[] key, long revision) {
        if (revision == 0)
            return Entry.empty(key);

        byte[] valueBytes;

        try {
            valueBytes = data.get(keyToRocksKey(revision, key));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }

        if (valueBytes == null || valueBytes.length == 0)
            return Entry.empty(key);

        Value lastVal = bytesToValue(valueBytes);

        if (lastVal.tombstone())
            return Entry.tombstone(key, revision, lastVal.updateCounter());

        return new Entry(key, lastVal.bytes(), revision, lastVal.updateCounter());
    }

    /**
     * Adds an entry to the batch.
     *
     * @param batch Write batch.
     * @param key Key.
     * @param value Value.
     * @param curRev Revision.
     * @param cntr Update counter.
     * @throws RocksDBException If failed.
     */
    private void addDataToBatch(WriteBatch batch, byte[] key, byte[] value, long curRev, long cntr) throws RocksDBException {
        byte[] rocksKey = keyToRocksKey(curRev, key);

        byte[] rocksValue = valueToBytes(value, cntr);

        data.put(batch, rocksKey, rocksValue);
    }

    /**
     * Adds all entries to the batch.
     *
     * @param batch Write batch.
     * @param keys Keys.
     * @param values Values.
     * @param curRev Revision.
     * @return New update counter value.
     * @throws RocksDBException If failed.
     */
    private long addAllToBatch(WriteBatch batch, List<byte[]> keys, List<byte[]> values, long curRev) throws RocksDBException {
        long counter = this.updCntr;

        for (int i = 0; i < keys.size(); i++) {
            counter++;

            byte[] key = keys.get(i);

            byte[] bytes = values.get(i);

            addDataToBatch(batch, key, bytes, curRev, counter);
        }

        return counter;
    }

    /**
     * Gets an entry from the keys index with the least key greater than or equal to the specified key.
     *
     * @param keyFrom Key.
     * @return Higher or equal entry. Returns {@code null} if no such entry exists.
     */
    @Nullable
    Map.Entry<byte[], long[]> revisionCeilingEntry(byte[] keyFrom) {
        return higherOrCeiling(keyFrom, false);
    }

    /**
     * Gets an entry from the keys index with the least key greater than the specified key.
     *
     * @param key Key.
     * @return Higher entry or {@code null} if no such entry exists.
     */
    @Nullable
    Map.Entry<byte[], long[]> revisionHigherEntry(byte[] key) {
        return higherOrCeiling(key, true);
    }

    /**
     * Gets an entry from the keys index with the least key greater than or equal to the specified key,
     * depending on the strictlyHigher parameter.
     *
     * @param key
     * @param strictlyHigher {@code true} for a strictly higher entry, {@code false} for a ceiling one.
     * @return Entry for the least key greater than or equal to the specified key. If no such entry
     * exists returns {@code null}.
     */
    @Nullable
    private IgniteBiTuple<byte[], long[]> higherOrCeiling(byte[] key, boolean strictlyHigher) {
        try (RocksIterator iterator = index.newIterator()) {
            iterator.seek(key);

            RocksBiPredicate predicate = strictlyHigher ?
                (k, v) -> CMP.compare(k, key) > 0 :
                (k, v) -> CMP.compare(k, key) >= 0;

            boolean found = find(iterator, predicate);

            if (!found)
                return null;

            return new IgniteBiTuple<>(iterator.key(), getAsLongs(iterator.value()));
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }

    /**
     * Creates a new iterator over the {@link StorageColumnFamilyType#DATA} column family.
     *
     * @param options Read options.
     * @return Iterator.
     */
    public RocksIterator newDataIterator(ReadOptions options) {
        return data.newIterator(options);
    }

    /**
     * Gets last revision from the list.
     *
     * @param revs Revisions.
     * @return Last revision.
     */
    private static long lastRevision(long[] revs) {
        return revs[revs.length - 1];
    }

    /**
     * @return Database lock
     */
    ReadWriteLock lock() {
        return rwLock;
    }

    /**
     * @return Database.
     */
    RocksDB db() {
        return db;
    }

    /** */
    @TestOnly
    public Path getDbPath() {
        return dbPath;
    }
}
