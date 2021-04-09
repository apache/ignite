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
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl extends AbstractTableView implements Table {
    /** Marshaller. */
    private final TupleMarshallerImpl marsh;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    public TableImpl(InternalTable tbl, TableSchemaManager schemaMgr) {
        super(tbl, schemaMgr);

        marsh = new TupleMarshallerImpl(schemaMgr);
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, schemaMgr, recMapper);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(tbl, schemaMgr, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView kvView() {
        return new KVBinaryViewImpl(tbl, schemaMgr);
    }

    /** {@inheritDoc} */
    @Override public Tuple get(Tuple keyRec) {
        return sync(getAsync(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshaller().marshal(keyRec, null); // Convert to portable format to pass TX/storage layer.

        return tbl.get(keyRow).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> getAll(Collection<Tuple> keyRecs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(Collection<Tuple> keyRecs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public void upsert(Tuple rec) {
        sync(upsertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAsync(Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.upsert(keyRow);
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(Collection<Tuple> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAllAsync(Collection<Tuple> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndUpsert(Tuple rec) {
        return sync(getAndUpsertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.getAndUpsert(keyRow).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public boolean insert(Tuple rec) {
        return sync(insertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insertAsync(Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.insert(keyRow);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> insertAll(Collection<Tuple> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(Collection<Tuple> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple rec) {
        return sync(replaceAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshaller().marshal(rec);

        return tbl.replace(keyRow);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(Tuple oldRec, Tuple newRec) {
        return sync(replaceAsync(oldRec, newRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(Tuple oldRec, Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        final Row oldRow = marshaller().marshal(oldRec);
        final Row newRow = marshaller().marshal(newRec);

        return tbl.replace(oldRow, newRow);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(Tuple rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(Tuple rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean delete(Tuple keyRec) {
        return sync(deleteAsync(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteAsync(Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshaller().marshal(keyRec, null);

        return tbl.delete(keyRow);
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(Tuple rec) {
        return sync(deleteExactAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExactAsync(Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshaller().marshal(rec);

        return tbl.deleteExact(row);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndDelete(Tuple rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(Tuple rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAll(Collection<Tuple> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(Collection<Tuple> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAllExact(Collection<Tuple> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(
        Collection<Tuple> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(
        Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
        Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Tuple, T> invokeAll(
        Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(
        Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public TupleBuilder tupleBuilder() {
        return new TupleBuilderImpl();
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
    private TableRow wrap(BinaryRow row) {
        if (row == null)
            return null;

        final SchemaDescriptor schema = schemaMgr.schema(row.schemaVersion());

        return new TableRow(schema, new Row(schema, row));
    }
}
