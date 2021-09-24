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

package org.apache.ignite.internal.table.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.schema.definition.SchemaManagementMode;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;

/**
 * Dummy table storage implementation.
 */
public class DummyInternalTableImpl implements InternalTable {
    /** In-memory dummy store. */
    private final Map<KeyWrapper, BinaryRow> store = new ConcurrentHashMap<>();

    /**
     * Wrapper provides correct byte[] comparison.
     */
    private static class KeyWrapper {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        KeyWrapper(byte[] data, int hash) {
            assert data != null;

            this.data = data;
            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyWrapper wrapper = (KeyWrapper)o;
            return Arrays.equals(data, wrapper.data);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return hash;
        }
    }

    /** {@inheritDoc} */
    @Override public @NotNull IgniteUuid tableId() {
        return new IgniteUuid(UUID.randomUUID(), 0);
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public @NotNull SchemaManagementMode schemaMode() {
        return SchemaManagementMode.STRICT;
    }

    /** {@inheritDoc} */
    @Override public void schema(SchemaManagementMode schemaMode) {
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> get(@NotNull BinaryRow row, Transaction tx) {
        assert row != null;

        return CompletableFuture.completedFuture(store.get(extractAndWrapKey(row)));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(@NotNull BinaryRow row, Transaction tx) {
        assert row != null;

        store.put(extractAndWrapKey(row), row);

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndUpsert(@NotNull BinaryRow row,
        Transaction tx) {
        assert row != null;

        final BinaryRow old = store.put(extractAndWrapKey(row), row);

        return CompletableFuture.completedFuture(old);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> delete(BinaryRow row, Transaction tx) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);
        final BinaryRow oldVal = store.get(key);

        return CompletableFuture.completedFuture(oldVal != null && oldVal.hasValue() && store.remove(key, oldVal));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows,
        Transaction tx) {
        assert keyRows != null && !keyRows.isEmpty();

        final List<BinaryRow> res = keyRows.stream()
            .map(this::extractAndWrapKey)
            .map(store::get)
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, Transaction tx) {
        assert rows != null && !rows.isEmpty();

        rows.stream()
            .map(k -> store.put(extractAndWrapKey(k), k));

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> insert(BinaryRow row, Transaction tx) {
        assert row != null;

        return CompletableFuture.completedFuture(store.putIfAbsent(extractAndWrapKey(row), row) == null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, Transaction tx) {
        assert rows != null && !rows.isEmpty();

        final List<BinaryRow> res = rows.stream()
            .map(k -> store.putIfAbsent(extractAndWrapKey(k), k) == null ? null : k)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

        return CompletableFuture.completedFuture(res);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow row, Transaction tx) {
        assert row != null;

        final KeyWrapper key = extractAndWrapKey(row);
        final BinaryRow oldRow = store.get(key);

        if (oldRow == null || !oldRow.hasValue())
            return CompletableFuture.completedFuture(false);

        return CompletableFuture.completedFuture(store.put(key, row) == oldRow);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, Transaction tx) {
        assert oldRow != null;
        assert newRow != null;

        final KeyWrapper key = extractAndWrapKey(oldRow);
        final BinaryRow row = store.get(key);

        if (row == null)
            return CompletableFuture.completedFuture(!oldRow.hasValue() && store.put(key, newRow) == null);

        return CompletableFuture.completedFuture(equalValues(row, oldRow) && store.put(key, newRow) != null);
    }

    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> deleteExact(BinaryRow row, Transaction tx) {
        assert row != null;
        assert row.hasValue();

        final KeyWrapper key = extractAndWrapKey(row);
        final BinaryRow old = store.get(key);

        if (old == null || !old.hasValue())
            return CompletableFuture.completedFuture(false);

        return CompletableFuture.completedFuture(equalValues(row, old) && store.remove(key) != null);
    }

    @Override public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows,
        Transaction tx) {
        return null;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private DummyInternalTableImpl.KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new KeyWrapper(bytes, row.hash());
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private boolean equalValues(@NotNull BinaryRow row, @NotNull BinaryRow row2) {
        if (row.hasValue() ^ row2.hasValue())
            return false;

        return row.valueSlice().compareTo(row2.valueSlice()) == 0;
    }
}
