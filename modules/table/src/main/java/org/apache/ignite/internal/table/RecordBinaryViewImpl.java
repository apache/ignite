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
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.distributed.TableManager;
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

    /** Table manager. */
    private final TableManager tblMgr;

    /**
     * Constructor.
     *
     * @param tbl Table.
     * @param schemaReg Table schema registry.
     * @param tblMgr Table manager.
     * @param tx The transaction.
     */
    public RecordBinaryViewImpl(InternalTable tbl, SchemaRegistry schemaReg, TableManager tblMgr, @Nullable Transaction tx) {
        super(tbl, schemaReg, tx);

        marsh = new TupleMarshallerImpl(tblMgr, tbl, schemaReg);

        this.tblMgr = tblMgr;
    }

    /** {@inheritDoc} */
    @Override public RecordBinaryViewImpl withTransaction(Transaction tx) {
        return new RecordBinaryViewImpl(tbl, schemaReg, tblMgr, tx);
    }

    /** {@inheritDoc} */
    @Override public Tuple get(@NotNull Tuple keyRec) {
        return sync(getAsync(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true); // Convert to portable format to pass TX/storage layer.

        return tbl.get(keyRow, tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> getAll(@NotNull Collection<Tuple> keyRecs) {
        return sync(getAllAsync(keyRecs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        HashSet<BinaryRow> keys = new HashSet<>(keyRecs.size());

        for (Tuple keyRec : keyRecs) {
            final Row keyRow = marshal(keyRec, true);

            keys.add(keyRow);
        }

        return tbl.getAll(keys, tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public void upsert(@NotNull Tuple rec) {
        sync(upsertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.upsert(row, tx);
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(@NotNull Collection<Tuple> recs) {
        sync(upsertAllAsync(recs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        HashSet<BinaryRow> rows = new HashSet<>(recs.size());

        for (Tuple keyRec : recs) {
            final Row row = marshal(keyRec, false);

            rows.add(row);
        }

        return tbl.upsertAll(rows, tx);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndUpsert(@NotNull Tuple rec) {
        return sync(getAndUpsertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.getAndUpsert(row, tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public boolean insert(@NotNull Tuple rec) {
        return sync(insertAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.insert(row, tx);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> insertAll(@NotNull Collection<Tuple> recs) {
        return sync(insertAllAsync(recs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        HashSet<BinaryRow> rows = new HashSet<>(recs.size());

        for (Tuple keyRec : recs) {
            final Row row = marshal(keyRec, false);

            rows.add(row);
        }

        return tbl.insertAll(rows, tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple rec) {
        return sync(replaceAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.replace(row, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return sync(replaceAsync(oldRec, newRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        final Row oldRow = marshal(oldRec, false);
        final Row newRow = marshal(newRec, false);

        return tbl.replace(oldRow, newRow, tx);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(@NotNull Tuple rec) {
        return sync(getAndReplaceAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.getAndReplace(row, tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(@NotNull Tuple keyRec) {
        return sync(deleteAsync(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        final Row keyRow = marshal(keyRec, true);

        return tbl.delete(keyRow, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(@NotNull Tuple rec) {
        return sync(deleteExactAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row row = marshal(rec, false);

        return tbl.deleteExact(row, tx);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndDelete(@NotNull Tuple rec) {
        return sync(getAndDeleteAsync(rec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        final Row keyRow = marshal(rec, true);

        return tbl.getAndDelete(keyRow, tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAll(@NotNull Collection<Tuple> recs) {
        return sync(deleteAllAsync(recs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        HashSet<BinaryRow> keys = new HashSet<>(recs.size());

        for (Tuple keyRec : recs) {
            final Row keyRow = marshal(keyRec, true);

            keys.add(keyRow);
        }

        return tbl.deleteAll(keys, tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAllExact(@NotNull Collection<Tuple> recs) {
        return sync(deleteAllExactAsync(recs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(
        @NotNull Collection<Tuple> recs
    ) {
        Objects.requireNonNull(recs);

        HashSet<BinaryRow> rows = new HashSet<>(recs.size());

        for (Tuple keyRec : recs) {
            final Row row = marshal(keyRec, false);

            rows.add(row);
        }

        return tbl.deleteAllExact(rows, tx).thenApply(this::wrap);
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(
        @NotNull Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
        @NotNull Tuple keyRec,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Tuple, T> invokeAll(
        @NotNull Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(
        @NotNull Collection<Tuple> keyRecs,
        InvokeProcessor<Tuple, Tuple, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Marshal a tuple to a row.
     *
     * @param tuple Tuple.
     * @param keyOnly Marshal key part only if {@code true}, otherwise marshal both, key and value parts.
     * @return Row.
     * @throws IgniteException If failed to marshal tuple.
     */
    private Row marshal(@NotNull Tuple tuple, boolean keyOnly) throws IgniteException {
        try {
            if (keyOnly)
                return marsh.marshalKey(tuple);
            else
                return marsh.marshal(tuple);
        } catch (TupleMarshallerException ex) {
            throw convertException(ex);
        }
    }

    /**
     * @param row Binary row.
     * @return Table row tuple.
     */
    private Tuple wrap(BinaryRow row) {
        if (row == null)
            return null;

        final Row wrapped = schemaReg.resolve(row);

        return TableRow.tuple(wrapped);
    }

    /**
     * @param rows Binary rows.
     * @return Table rows.
     */
    private Collection<Tuple> wrap(Collection<BinaryRow> rows) {
        if (rows == null)
            return null;

        return rows.stream().filter(Objects::nonNull).map(this::wrap).collect(Collectors.toSet());
    }
}
