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
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * Key-value view implementation for binary user-object representation.
 *
 * @implNote Key-value {@link Tuple}s represents marshalled user-objects
 * regarding the binary object concept.
 */
public class KVBinaryViewImpl extends AbstractTableView implements KeyValueBinaryView {
    /** Marshaller. */
    private final TupleMarshallerImpl marsh;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     * @param schemaReg Schema registry.
     */
    public KVBinaryViewImpl(InternalTable tbl, SchemaRegistry schemaReg) {
        super(tbl, schemaReg);

        marsh = new TupleMarshallerImpl(schemaReg);
    }

    /** {@inheritDoc} */
    @Override public Tuple get(Tuple key) {
        return sync(getAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(Tuple key) {
        Objects.requireNonNull(key);

        Row kRow = marshaller().marshal(key, null); // Convert to portable format to pass TX/storage layer.

        return tbl.get(kRow)  // Load async.
            .thenApply(this::wrap) // Binary -> schema-aware row
            .thenApply(t -> t == null ? null : t.valueChunk()); // Narrow to value.
    }

    /** {@inheritDoc} */
    @Override public Map<Tuple, Tuple> getAll(Collection<Tuple> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Tuple, Tuple>> getAllAsync(Collection<Tuple> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean contains(Tuple key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public void put(Tuple key, Tuple val) {
        sync(putAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAsync(Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.upsert(row);
    }

    /** {@inheritDoc} */
    @Override public void putAll(Map<Tuple, Tuple> pairs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAllAsync(Map<Tuple, Tuple> pairs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndPut(Tuple key, Tuple val) {
        return sync(getAndPutAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndPutAsync(Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.getAndUpsert(row)
            .thenApply(this::wrap) // Binary -> schema-aware row
            .thenApply(t -> t == null ? null : t.valueChunk()); // Narrow to value.
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(Tuple key, Tuple val) {
        return sync(putIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.insert(row);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Tuple key) {
        return sync(removeAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(Tuple key) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, null); // Convert to portable format to pass TX/storage layer.

        return tbl.delete(row);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(Tuple key, Tuple val) {
        return sync(removeAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(Tuple key, Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.deleteExact(row);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> removeAll(Collection<Tuple> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> removeAllAsync(Collection<Tuple> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndRemove(Tuple key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndRemoveAsync(Tuple key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple key, Tuple val) {
        return sync(replaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        Row row = marshaller().marshal(key, val); // Convert to portable format to pass TX/storage layer.

        return tbl.replace(row);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple key, Tuple oldVal, Tuple newVal) {
        return sync(replaceAsync(key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(Tuple key, Tuple oldVal, Tuple newVal) {
        Objects.requireNonNull(key);

        Row oldRow = marshaller().marshal(key, oldVal); // Convert to portable format to pass TX/storage layer.
        Row newRow = marshaller().marshal(key, newVal); // Convert to portable format to pass TX/storage layer.

        return tbl.replace(oldRow, newRow);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(Tuple key, Tuple val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(Tuple key, Tuple val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(
        Tuple key,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
        Tuple key,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<Tuple, R> invokeAll(
        Collection<Tuple> keys,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<Map<Tuple, R>> invokeAllAsync(
        Collection<Tuple> keys,
        InvokeProcessor<Tuple, Tuple, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder tupleBuilder() {
        return new TupleBuilderImpl(schemaReg.schema());
    }

    /**
     * @return Marshaller.
     */
    private TupleMarshaller marshaller() {
        return marsh;
    }

    /**
     * @param row Binary row.
     * @return Table row.
     */
    protected TableRow wrap(BinaryRow row) {
        if (row == null)
            return null;

        final SchemaDescriptor schema = schemaReg.schema(row.schemaVersion());

        return new TableRow(schema, new Row(schema, row));
    }
}
