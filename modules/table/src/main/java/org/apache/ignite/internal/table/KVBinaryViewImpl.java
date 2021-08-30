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

package org.apache.ignite.internal.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;

/**
 * Key-value view implementation for binary user-object representation.
 */
public class KVBinaryViewImpl extends AbstractTableView implements KeyValueBinaryView {
    /** Marshaller. */
    private final TupleMarshallerImpl marsh;

    /** Table manager. */
    private final TableManager tblMgr;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     * @param schemaReg Schema registry.
     */
    public KVBinaryViewImpl(InternalTable tbl, SchemaRegistry schemaReg, TableManager tblMgr, Transaction tx) {
        super(tbl, schemaReg, tx);

        this.tblMgr = tblMgr;

        marsh = new TupleMarshallerImpl(tblMgr, tbl, schemaReg);
    }

    /** {@inheritDoc} */
    @Override public Tuple get(@NotNull Tuple key) {
        return sync(getAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        Row kRow = marshaller().marshal(key, null); // Convert to portable format to pass TX/storage layer.

        return tbl.get(kRow, tx)  // Load async.
                .thenApply(this::wrap) // Binary -> schema-aware row
                .thenApply(TableRow::valueTuple); // Narrow to value.
    }

    /** {@inheritDoc} */
    @Override public Map<Tuple, Tuple> getAll(@NotNull Collection<Tuple> keys) {
        return sync(getAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Tuple, Tuple>> getAllAsync(@NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        return tbl.getAll(keys.stream().map(k -> marsh.marshal(k, null)).collect(Collectors.toList()), tx)
            .thenApply(ts -> ts.stream().filter(Objects::nonNull).map(this::wrap).collect(Collectors.toMap(TableRow::keyTuple, TableRow::valueTuple)));
    }

    /** {@inheritDoc} */
    @Override public boolean contains(@NotNull Tuple key) {
        return get(key) != null;
    }

    /** {@inheritDoc} */
    @Override public void put(@NotNull Tuple key, Tuple val) {
        sync(putAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.upsert(row, tx);
    }

    /** {@inheritDoc} */
    @Override public void putAll(@NotNull Map<Tuple, Tuple> pairs) {
        sync(putAllAsync(pairs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<Tuple, Tuple> pairs) {
        Objects.requireNonNull(pairs);

        return tbl.upsertAll(pairs.entrySet()
            .stream()
            .map(this::marshalPair)
            .collect(Collectors.toList()), tx);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndPut(@NotNull Tuple key, Tuple val) {
        return sync(getAndPutAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndPutAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.getAndUpsert(row, tx)
            .thenApply(this::wrap) // Binary -> schema-aware row
            .thenApply(TableRow::valueTuple); // Narrow to value.
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(@NotNull Tuple key, @NotNull Tuple val) {
        return sync(putIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.insert(row, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull Tuple key) {
        return sync(removeAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, null); // Convert to portable format to pass TX/storage layer.

        return tbl.delete(row, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull Tuple key, @NotNull Tuple val) {
        return sync(removeAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull Tuple key, @NotNull Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.deleteExact(row, tx);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> removeAll(@NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        return sync(removeAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> removeAllAsync(@NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        return tbl.deleteAll(keys.stream().map(k -> marsh.marshal(k, null)).collect(Collectors.toList()), tx)
                .thenApply(t -> t.stream().filter(Objects::nonNull).map(this::wrap).map(TableRow::valueTuple).collect(Collectors.toList()));
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndRemove(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        return sync(getAndRemoveAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndRemoveAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        return tbl.getAndDelete(marsh.marshal(key, null), tx)
                .thenApply(this::wrap)
                .thenApply(TableRow::valueTuple);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple key, Tuple val) {
        return sync(replaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.replace(row, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple key, Tuple oldVal, Tuple newVal) {
        return sync(replaceAsync(key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple key, Tuple oldVal, Tuple newVal) {
        Objects.requireNonNull(key);

        Row oldRow = marshaller().marshal(key, oldVal); // Convert to portable format to pass TX/storage layer.
        Row newRow = marshaller().marshal(key, newVal); // Convert to portable format to pass TX/storage layer.

        return tbl.replace(oldRow, newRow, tx);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(@NotNull Tuple key, Tuple val) {
        return sync(getAndReplaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        return tbl.getAndReplace(marsh.marshal(key, val), tx)
            .thenApply(this::wrap)
            .thenApply(TableRow::valueTuple);
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(
        @NotNull Tuple key,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
        @NotNull Tuple key,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<Tuple, R> invokeAll(
        @NotNull Collection<Tuple> keys,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<Map<Tuple, R>> invokeAllAsync(
        @NotNull Collection<Tuple> keys,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public KVBinaryViewImpl withTransaction(Transaction tx) {
        return new KVBinaryViewImpl(tbl, schemaReg, tblMgr, tx);
    }

    /**
     * @return Marshaller.
     */
    private TupleMarshaller marshaller() {
        return marsh;
    }

    /**
     * @param row Binary row.
     * @return Row.
     */
    protected Row wrap(BinaryRow row) {
        if (row == null)
            return null;

        return schemaReg.resolve(row);
    }

    /**
     * Marshals a key-value pair into the table row.
     *
     * @param pair A map entry represents the key-value pair.
     * @return Row.
     */
    private Row marshalPair(Map.Entry<Tuple, Tuple> pair) {
        return marshaller().marshal(pair.getKey(), pair.getValue());
    }
}
