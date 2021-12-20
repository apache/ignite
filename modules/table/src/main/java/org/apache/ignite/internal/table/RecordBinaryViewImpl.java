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

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Table view implementation for binary objects.
 */
public class RecordBinaryViewImpl extends AbstractTableView implements RecordView<Tuple> {
    /** Marshaller. */
    private final TupleMarshallerImpl marsh;

    /**
     * Constructor.
     *
     * @param tbl       The table.
     * @param schemaReg Table schema registry.
     */
    public RecordBinaryViewImpl(InternalTable tbl, SchemaRegistry schemaReg) {
        super(tbl, schemaReg);

        marsh = new TupleMarshallerImpl(schemaReg);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple get(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return sync(getAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true); // Convert to portable format to pass TX/storage layer.

        return tbl.get(keyRow, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> getAll(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        return sync(getAllAsync(tx, keyRecs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return tbl.getAll(mapToBinary(keyRecs, true), (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@Nullable Transaction tx, @NotNull Tuple rec) {
        sync(upsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.upsert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        sync(upsertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.upsertAll(mapToBinary(recs, false), (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndUpsert(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(getAndUpsertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.getAndUpsert(row, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(insertAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.insert(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> insertAll(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        return sync(insertAllAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.insertAll(mapToBinary(recs, false), (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(replaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return sync(replaceAsync(tx, oldRec, newRec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.replace(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull Tuple oldRec, @NotNull Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        final Row oldRow = marshal(oldRec, false);
        final Row newRow = marshal(newRec, false);

        return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndReplace(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(getAndReplaceAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.getAndReplace(row, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return sync(deleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true);

        return tbl.delete(keyRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@Nullable Transaction tx, @NotNull Tuple rec) {
        return sync(deleteExactAsync(tx, rec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, @NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.deleteExact(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Tuple getAndDelete(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        return sync(getAndDeleteAsync(tx, keyRec));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@Nullable Transaction tx, @NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true);

        return tbl.getAndDelete(keyRow, (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> deleteAll(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        return sync(deleteAllAsync(tx, keyRecs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return tbl.deleteAll(mapToBinary(keyRecs, true), (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Tuple> deleteAllExact(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        return sync(deleteAllExactAsync(tx, recs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@Nullable Transaction tx, @NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return tbl.deleteAllExact(mapToBinary(recs, false), (InternalTransaction) tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> T invoke(
            @Nullable Transaction tx,
            @NotNull Tuple keyRec,
            InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
            @Nullable Transaction tx,
            @NotNull Tuple keyRec,
            InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> Map<Tuple, T> invokeAll(
            @Nullable Transaction tx,
            @NotNull Collection<Tuple> keyRecs,
            InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(
            @Nullable Transaction tx,
            @NotNull Collection<Tuple> keyRecs,
            InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Marshal a tuple to a row.
     *
     * @param tuple   The tuple.
     * @param keyOnly Marshal key part only if {@code true}, otherwise marshal both, key and value parts.
     * @return Row.
     * @throws IgniteException If failed to marshal tuple.
     */
    private Row marshal(@NotNull Tuple tuple, boolean keyOnly) throws IgniteException {
        try {
            if (keyOnly) {
                return marsh.marshalKey(tuple);
            } else {
                return marsh.marshal(tuple);
            }
        } catch (TupleMarshallerException ex) {
            throw convertException(ex);
        }
    }

    /**
     * Returns table row tuple.
     *
     * @param row Binary row.
     */
    private Tuple wrap(BinaryRow row) {
        if (row == null) {
            return null;
        }

        final Row wrapped = schemaReg.resolve(row);

        return TableRow.tuple(wrapped);
    }

    /**
     * Returns table rows.
     *
     * @param rows Binary rows.
     */
    private Collection<Tuple> wrap(Collection<BinaryRow> rows) {
        if (rows == null) {
            return null;
        }

        return schemaReg.resolve(rows).stream().map(TableRow::tuple).collect(toList());
    }

    /**
     * Maps a collection of tuples to binary rows.
     *
     * @param rows Tuples.
     * @param key  {@code true} to marshal only a key.
     * @return List of binary rows.
     */
    private Collection<BinaryRow> mapToBinary(Collection<Tuple> rows, boolean key) {
        Collection<BinaryRow> mapped = new ArrayList<>(rows.size());

        for (Tuple row : rows) {
            mapped.add(marshal(row, key));
        }

        return mapped;
    }
}
