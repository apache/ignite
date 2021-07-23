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

package org.apache.ignite.client.fakes;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fake internal table.
 */
public class FakeInternalTable implements InternalTable {
    private final String tableName;

    private final UUID tableId;

    private final ConcurrentHashMap<ByteBuffer, BinaryRow> data = new ConcurrentHashMap<>();

    public FakeInternalTable(String tableName, UUID tableId) {
        this.tableName = tableName;
        this.tableId = tableId;
    }

    /** {@inheritDoc} */
    @Override public @NotNull UUID tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return tableName;
    }

    /** {@inheritDoc} */
    @Override public @NotNull SchemaMode schemaMode() {
        return SchemaMode.STRICT_SCHEMA;
    }

    /** {@inheritDoc} */
    @Override public void schema(SchemaMode schemaMode) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> get(BinaryRow keyRow, @Nullable Transaction tx) {
        return CompletableFuture.completedFuture(data.get(keyRow.keySlice()));
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsert(BinaryRow row, @Nullable Transaction tx) {
        data.put(row.keySlice(), row);

        return CompletableFuture.completedFuture(null);
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> insert(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> delete(BinaryRow keyRow, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> deleteExact(BinaryRow oldRow, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, @Nullable Transaction tx) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows, @Nullable Transaction tx) {
        return null;
    }
}
