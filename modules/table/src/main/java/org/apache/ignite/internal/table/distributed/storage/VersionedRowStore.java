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

package org.apache.ignite.internal.table.distributed.storage;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * TODO asch IGNITE-15934 use read only buffers ? replace Pair from ignite-schema
 * TODO asch IGNITE-15935 can use some sort of a cache on tx coordinator to avoid network IO.
 * TODO asch IGNITE-15934 invokes on storage not used for now, can it be changed ?
 */
public class VersionedRowStore {
    /** Storage delegate. */
    private final PartitionStorage storage;

    /** Transaction manager. */
    private TxManager txManager;

    /**
     * The constructor.
     *
     * @param storage The storage.
     * @param txManager The TX manager.
     */
    public VersionedRowStore(@NotNull PartitionStorage storage, @NotNull TxManager txManager) {
        this.storage = Objects.requireNonNull(storage);
        this.txManager = Objects.requireNonNull(txManager);
    }

    /**
     * Decodes a storage row to a pair where the first is an actual value (visible to a current transaction) and the second is an old value
     * used for rollback.
     *
     * @param row       The row.
     * @param timestamp Timestamp timestamp.
     * @return Actual value.
     */
    protected Pair<BinaryRow, BinaryRow> versionedRow(@Nullable DataRow row, Timestamp timestamp) {
        return resolve(unpack(row), timestamp);
    }

    /**
     * Gets a row.
     *
     * @param row The search row.
     * @param ts The timestamp.
     * @return The result row.
     */
    public BinaryRow get(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        SearchRow key = extractAndWrapKey(row);

        DataRow readValue = storage.read(key);

        return versionedRow(readValue, ts).getFirst();
    }

    /**
     * Gets multiple rows.
     *
     * @param keyRows Search rows.
     * @param ts The timestamp.
     * @return The result rows.
     */
    public List<BinaryRow> getAll(Collection<BinaryRow> keyRows, Timestamp ts) {
        assert keyRows != null && !keyRows.isEmpty();

        List<BinaryRow> res = new ArrayList<>(keyRows.size());

        for (BinaryRow keyRow : keyRows) {
            res.add(get(keyRow, ts));
        }

        return res;
    }

    /**
     * Upserts a row.
     *
     * @param row The row.
     * @param ts The timestamp.
     */
    public void upsert(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        Pair<BinaryRow, BinaryRow> pair = resolve(unpack(storage.read(key)), ts);

        storage.write(pack(key, new Value(row, pair.getSecond(), ts)));
    }

    /**
     * Upserts a row and returns previous value.
     *
     * @param row The row.
     * @param ts The timestamp.
     * @return Previous row.
     */
    @Nullable
    public BinaryRow getAndUpsert(@NotNull BinaryRow row, Timestamp ts) {
        assert row != null;

        BinaryRow oldRow = get(row, ts);

        upsert(row, ts);

        return oldRow != null ? oldRow : null;
    }

    /**
     * Deletes a row.
     *
     * @param row The row.
     * @param ts The timestamp.
     * @return {@code True} if was deleted.
     */
    public boolean delete(BinaryRow row, Timestamp ts) {
        assert row != null;

        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        Pair<BinaryRow, BinaryRow> pair = resolve(unpack(storage.read(key)), ts);

        if (pair.getFirst() == null) {
            return false;
        }

        // Write a tombstone.
        storage.write(pack(key, new Value(null, pair.getSecond(), ts)));

        return true;
    }

    /**
     * Upserts multiple rows.
     *
     * @param rows Search rows.
     * @param ts The timestamp.
     */
    public void upsertAll(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        for (BinaryRow row : rows) {
            upsert(row, ts);
        }
    }

    /**
     * Inserts a row.
     *
     * @param row The row.
     * @param ts The timestamp.
     * @return {@code true} if was inserted.
     */
    public boolean insert(BinaryRow row, Timestamp ts) {
        assert row != null && row.hasValue() : row;
        SimpleDataRow key = new SimpleDataRow(extractAndWrapKey(row).keyBytes(), null);

        Pair<BinaryRow, BinaryRow> pair = resolve(unpack(storage.read(key)), ts);

        if (pair.getFirst() != null) {
            return false;
        }

        storage.write(pack(key, new Value(row, null, ts)));

        return true;
    }

    /**
     * Inserts multiple rows.
     *
     * @param rows Rows.
     * @param ts The timestamp.
     * @return List of not inserted rows.
     */
    public List<BinaryRow> insertAll(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        List<BinaryRow> inserted = new ArrayList<>(rows.size());

        for (BinaryRow row : rows) {
            if (!insert(row, ts)) {
                inserted.add(row);
            }
        }

        return inserted;
    }

    /**
     * Replaces an existing row.
     *
     * @param row The row.
     * @param ts The timestamp.
     * @return {@code True} if was replaced.
     */
    public boolean replace(BinaryRow row, Timestamp ts) {
        assert row != null;

        BinaryRow oldRow = get(row, ts);

        if (oldRow != null) {
            upsert(row, ts);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Replaces a row by exact match.
     *
     * @param oldRow Old row.
     * @param newRow New row.
     * @param ts The timestamp.
     * @return {@code True} if was replaced.
     */
    public boolean replace(BinaryRow oldRow, BinaryRow newRow, Timestamp ts) {
        assert oldRow != null;
        assert newRow != null;

        BinaryRow oldRow0 = get(oldRow, ts);

        if (oldRow0 != null && equalValues(oldRow0, oldRow)) {
            upsert(newRow, ts);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Replaces existing row and returns a previous value.
     *
     * @param row The row.
     * @param ts The timestamp.
     * @return Replaced row.
     */
    public BinaryRow getAndReplace(BinaryRow row, Timestamp ts) {
        BinaryRow oldRow = get(row, ts);

        if (oldRow != null) {
            upsert(row, ts);

            return oldRow;
        } else {
            return null;
        }
    }

    /**
     * Deletes a row by exact match.
     *
     * @param row The row.
     * @param ts The timestamp.
     * @return {@code True} if was deleted.
     */
    public boolean deleteExact(BinaryRow row, Timestamp ts) {
        assert row != null;
        assert row.hasValue();

        BinaryRow oldRow = get(row, ts);

        if (oldRow != null && equalValues(oldRow, row)) {
            delete(oldRow, ts);

            return true;
        } else {
            return false;
        }
    }

    /**
     * Delets a row and returns a previous value.
     *
     * @param row The row.
     * @param ts The timestamp.
     * @return Deleted row.
     */
    public BinaryRow getAndDelete(BinaryRow row, Timestamp ts) {
        BinaryRow oldRow = get(row, ts);

        if (oldRow != null) {
            delete(oldRow, ts);

            return oldRow;
        } else {
            return null;
        }
    }

    /**
     * Deletes multiple rows.
     *
     * @param keyRows Search rows.
     * @param ts The timestamp.
     * @return Not deleted rows.
     */
    public List<BinaryRow> deleteAll(Collection<BinaryRow> keyRows, Timestamp ts) {
        var notDeleted = new ArrayList<BinaryRow>();

        for (BinaryRow keyRow : keyRows) {
            if (!delete(keyRow, ts)) {
                notDeleted.add(keyRow);
            }
        }

        return notDeleted;
    }

    /**
     * Deletes multiple rows by exact match.
     *
     * @param rows Search rows.
     * @param ts The timestamp.
     * @return Not deleted rows.
     */
    public List<BinaryRow> deleteAllExact(Collection<BinaryRow> rows, Timestamp ts) {
        assert rows != null && !rows.isEmpty();

        var notDeleted = new ArrayList<BinaryRow>(rows.size());

        for (BinaryRow row : rows) {
            if (!deleteExact(row, ts)) {
                notDeleted.add(row);
            }
        }

        return notDeleted;
    }

    /**
     * Tests row values for equality.
     *
     * @param row Row.
     * @return Extracted key.
     */
    private boolean equalValues(@NotNull BinaryRow row, @NotNull BinaryRow row2) {
        if (row.hasValue() ^ row2.hasValue()) {
            return false;
        }

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }

    /**
     * Closes a storage.
     *
     * @throws Exception If failed.
     */
    public void close() throws Exception {
        storage.close();
    }

    /**
     * Extracts a key and a value from the {@link BinaryRow} and wraps it in a {@link DataRow}.
     *
     * @param row Binary row.
     * @return Data row.
     */
    @NotNull
    private static DataRow extractAndWrapKeyValue(@NotNull BinaryRow row) {
        byte[] key = new byte[row.keySlice().capacity()];
        row.keySlice().get(key);

        return new SimpleDataRow(key, row.hasValue() ? row.bytes() : null);
    }

    /**
     * Extracts a key from the {@link BinaryRow} and wraps it in a {@link SearchRow}.
     *
     * @param row Binary row.
     * @return Search row.
     */
    @NotNull
    private static SearchRow extractAndWrapKey(@NotNull BinaryRow row) {
        // TODO asch IGNITE-15934 can reuse thread local byte buffer
        byte[] key = new byte[row.keySlice().capacity()];
        row.keySlice().get(key);

        return new SimpleDataRow(key, null);
    }

    /**
     * Unpacks a raw value into (cur, old, ts) triplet. TODO asch IGNITE-15934 not very efficient.
     *
     * @param row The row.
     * @return The value.
     */
    private static Value unpack(@Nullable DataRow row) {
        if (row == null) {
            return new Value(null, null, null);
        }

        ByteBuffer buf = row.value();

        BinaryRow newVal = null;
        BinaryRow oldVal = null;

        int l1 = buf.asIntBuffer().get();

        int pos = 4;

        buf.position(pos);

        if (l1 != 0) {
            // TODO asch IGNITE-15934 get rid of copying
            byte[] tmp = new byte[l1];

            buf.get(tmp);

            newVal = new ByteBufferRow(tmp);

            pos += l1;
        }

        buf.position(pos);

        int l2 = buf.asIntBuffer().get();

        pos += 4;

        buf.position(pos);

        if (l2 != 0) {
            // TODO asch get rid of copying
            byte[] tmp = new byte[l2];

            buf.get(tmp);

            oldVal = new ByteBufferRow(tmp);

            pos += l2;
        }

        buf.position(pos);

        long ts = buf.getLong();
        long nodeId = buf.getLong();

        return new Value(newVal, oldVal, new Timestamp(ts, nodeId));
    }

    /**
     * Packs a multi-versioned value.
     *
     * @param key The key.
     * @param value The value.
     * @return Data row.
     */
    private static DataRow pack(SearchRow key, Value value) {
        byte[] b1 = null;
        byte[] b2 = null;

        int l1 = value.newRow == null ? 0 : (b1 = value.newRow.bytes()).length;
        int l2 = value.oldRow == null ? 0 : (b2 = value.oldRow.bytes()).length;

        // TODO asch write only values.
        ByteBuffer buf = ByteBuffer.allocate(4 + l1 + 4 + l2 + 16);

        buf.asIntBuffer().put(l1);

        buf.position(4);

        if (l1 > 0) {
            buf.put(b1);
        }

        buf.asIntBuffer().put(l2);

        buf.position(buf.position() + 4);

        if (l2 > 0) {
            buf.put(b2);
        }

        buf.putLong(value.timestamp.getTimestamp());
        buf.putLong(value.timestamp.getNodeId());

        return new SimpleDataRow(key.keyBytes(), buf.array());
    }

    /**
     * Resolves a multi-versioned value depending on a viewer's timestamp.
     *
     * @param val        The value.
     * @param timestamp  The timestamp
     * @return New and old rows pair.
     * @see #versionedRow
     */
    private Pair<BinaryRow, BinaryRow> resolve(Value val, Timestamp timestamp) {
        if (val.timestamp == null) { // New or after reset.
            assert val.oldRow == null : val;

            return new Pair<>(val.newRow, null);
        }

        // Checks "inTx" condition. Will be false if this is a first transactional op.
        if (val.timestamp.equals(timestamp)) {
            return new Pair<>(val.newRow, val.oldRow);
        }

        TxState state = txManager.state(val.timestamp);

        BinaryRow cur;

        if (state == TxState.ABORTED) { // Was aborted and had written a temp value.
            cur = val.oldRow;
        } else {
            cur = val.newRow;
        }

        return new Pair<>(cur, cur);
    }

    /**
     * Takes a snapshot.
     *
     * @param path The path.
     * @return Snapshot future.
     */
    public CompletionStage<Void> snapshot(Path path) {
        return storage.snapshot(path);
    }

    /**
     * Restores a snapshot.
     *
     * @param path The path.
     */
    public void restoreSnapshot(Path path) {
        storage.restoreSnapshot(path);
    }

    /**
     * Executes a scan.
     *
     * @param pred The predicate.
     * @return The cursor.
     */
    public Cursor<BinaryRow> scan(Predicate<SearchRow> pred) {
        Cursor<DataRow> delegate = storage.scan(pred);

        // TODO asch add tx support IGNITE-15087.
        return new Cursor<BinaryRow>() {
            private @Nullable BinaryRow cur = null;

            @Override
            public void close() throws Exception {
                delegate.close();
            }

            @NotNull
            @Override
            public Iterator<BinaryRow> iterator() {
                return this;
            }

            @Override
            public boolean hasNext() {
                if (cur != null) {
                    return true;
                }

                if (delegate.hasNext()) {
                    DataRow row = delegate.next();

                    cur = versionedRow(row, null).getFirst();

                    return cur != null ? true : hasNext(); // Skip tombstones.
                }

                return false;
            }

            @Override
            public BinaryRow next() {
                BinaryRow next = cur;

                cur = null;

                assert next != null;

                return next;
            }
        };
    }

    /**
     * Versioned value.
     */
    private static class Value {
        /** Current value. */
        BinaryRow newRow;

        /** The value for rollback. */
        @Nullable BinaryRow oldRow;

        /** Transaction's timestamp. */
        Timestamp timestamp;

        /**
         * The constructor.
         *
         * @param newRow New row.
         * @param oldRow Old row.
         * @param timestamp The timestamp.
         */
        Value(@Nullable BinaryRow newRow, @Nullable BinaryRow oldRow, Timestamp timestamp) {
            this.newRow = newRow;
            this.oldRow = oldRow;
            this.timestamp = timestamp;
        }
    }

    /**
     * Wrapper provides correct byte[] comparison.
     */
    public static class KeyWrapper {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * The constructor.
         *
         * @param data Wrapped data.
         */
        public KeyWrapper(byte[] data, int hash) {
            assert data != null;

            this.data = data;
            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            KeyWrapper wrapper = (KeyWrapper) o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return hash;
        }
    }

    /**
     * Returns a storage delegate.
     *
     * @return The delegate.
     */
    public PartitionStorage delegate() {
        return storage;
    }

    /**
     * Returns a transaction manager.
     *
     * @return Transaction manager.
     */
    public TxManager txManager() {
        return txManager;
    }

    /**
     * Adapter that converts a {@link BinaryRow} into a {@link SearchRow}.
     */
    private static class BinarySearchRow implements SearchRow {
        /** Search key. */
        private final byte[] keyBytes;

        /** Source row. */
        private final BinaryRow sourceRow;

        /**
         * The constructor.
         *
         * @param row The search row.
         */
        BinarySearchRow(BinaryRow row) {
            sourceRow = row;
            keyBytes = new byte[row.keySlice().capacity()];

            row.keySlice().get(keyBytes);
        }

        /** {@inheritDoc} */
        @Override
        public byte @NotNull [] keyBytes() {
            return keyBytes;
        }

        /** {@inheritDoc} */
        @Override
        public @NotNull ByteBuffer key() {
            return ByteBuffer.wrap(keyBytes);
        }
    }
}
