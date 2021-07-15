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
import org.apache.ignite.internal.schema.marshaller.RecordSerializer;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Record view implementation.
 */
public class RecordViewImpl<R> extends AbstractTableView implements RecordView<R> {
    /**
     * Constructor.
     *  @param tbl Table.
     * @param schemaReg Schema registry.
     * @param mapper Record class mapper.
     * @param tx The transaction.
     */
    public RecordViewImpl(InternalTable tbl, SchemaRegistry schemaReg, RecordMapper<R> mapper, @Nullable Transaction tx) {
        super(tbl, schemaReg, tx);
    }

    /** {@inheritDoc} */
    @Override public R fill(R recObjToFill) {
        return sync(fillAsync(recObjToFill));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<R> fillAsync(R recObjToFill) {
        Objects.requireNonNull(recObjToFill);

        RecordSerializer<R> marsh = serializer();

        Row kRow = marsh.serialize(recObjToFill);  // Convert to portable format to pass TX/storage layer.

        return tbl.get(kRow, tx)  // Load async.
            .thenApply(this::wrap) // Binary -> schema-aware row
            .thenApply(r -> marsh.deserialize(r, recObjToFill)); // Deserialize and fill record.
    }

    /** {@inheritDoc} */
    @Override public R get(@NotNull R keyRec) {
        return sync(getAsync(keyRec));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<R> getAsync(@NotNull R keyRec) {
        Objects.requireNonNull(keyRec);

        RecordSerializer<R> marsh = serializer();

        Row kRow = marsh.serialize(keyRec);  // Convert to portable format to pass TX/storage layer.

        return tbl.get(kRow, tx)  // Load async.
            .thenApply(this::wrap) // Binary -> schema-aware row
            .thenApply(marsh::deserialize); // Deserialize.
    }

    /** {@inheritDoc} */
    @Override public Collection<R> getAll(@NotNull Collection<R> keyRecs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<R>> getAllAsync(@NotNull Collection<R> keyRecs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public void upsert(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAsync(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(@NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public R getAndUpsert(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<R> getAndUpsertAsync(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean insert(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<R> insertAll(@NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<R>> insertAllAsync(@NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull R oldRec, @NotNull R newRec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R oldRec, @NotNull R newRec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public R getAndReplace(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<R> getAndReplaceAsync(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean delete(@NotNull R keyRec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull R keyRec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public R getAndDelete(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<R> getAndDeleteAsync(@NotNull R rec) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<R> deleteAll(@NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<R>> deleteAllAsync(@NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<R> deleteAllExact(@NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(@NotNull Collection<R> recs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(@NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
        @NotNull R keyRec,
        InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<R, T> invokeAll(
        @NotNull Collection<R> keyRecs,
        InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(
        @NotNull Collection<R> keyRecs,
        InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public RecordViewImpl<R> withTransaction(Transaction tx) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * @return Marshaller.
     */
    private RecordSerializer<R> serializer() {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * @param row Binary row.
     * @return Schema-aware row.
     */
    private Row wrap(BinaryRow row) {
        if (row == null)
            return null;

        final SchemaDescriptor rowSchema = schemaReg.schema(row.schemaVersion()); // Get a schema for row.

        return new Row(rowSchema, row);
    }
}
