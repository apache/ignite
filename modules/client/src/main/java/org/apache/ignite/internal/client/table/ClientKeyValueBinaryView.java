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

package org.apache.ignite.internal.client.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client key-value view implementation for binary user-object representation.
 */
public class ClientKeyValueBinaryView implements KeyValueView<Tuple, Tuple> {
    /** Underlying table. */
    private final ClientTable tbl;

    /**
     * Constructor.
     *
     * @param tbl Table.
     */
    public ClientKeyValueBinaryView(ClientTable tbl) {
        assert tbl != null;

        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public Tuple get(@NotNull Tuple key) {
        return getAsync(key).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> tbl.writeTuple(key, schema, out, true),
                ClientTable::readValueTuple);
    }

    /** {@inheritDoc} */
    @Override public Map<Tuple, Tuple> getAll(@NotNull Collection<Tuple> keys) {
        return getAllAsync(keys).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<Tuple, Tuple>> getAllAsync(@NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (s, w) -> tbl.writeTuples(keys, s, w, true),
                tbl::readKvTuples,
                Collections.emptyMap());
    }

    /** {@inheritDoc} */
    @Override public boolean contains(@NotNull Tuple key) {
        return containsAsync(key).join();
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> containsAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (schema, out) -> tbl.writeTuple(key, schema, out, true),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public void put(@NotNull Tuple key, Tuple val) {
        putAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        // TODO IGNITE-15194: Convert Tuple to a schema-order Array as a first step.
        // If it does not match the latest schema, then request latest and convert again.
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> tbl.writeKvTuple(key, val, s, w, false),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override public void putAll(@NotNull Map<Tuple, Tuple> pairs) {
        putAllAsync(pairs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<Tuple, Tuple> pairs) {
        Objects.requireNonNull(pairs);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> tbl.writeKvTuples(pairs, s, w),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndPut(@NotNull Tuple key, Tuple val) {
        return getAndPutAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndPutAsync(@NotNull Tuple key, Tuple val) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> tbl.writeKvTuple(key, val, s, w, false),
                ClientTable::readValueTuple);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(@NotNull Tuple key, @NotNull Tuple val) {
        return putIfAbsentAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull Tuple key, Tuple val) {
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> tbl.writeKvTuple(key, val, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull Tuple key) {
        return removeAsync(key).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull Tuple key) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
            ClientOp.TUPLE_DELETE,
            (s, w) -> tbl.writeTuple(key, s, w, true),
            ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull Tuple key, @NotNull Tuple val) {
        return removeAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull Tuple key, @NotNull Tuple val) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(val);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> tbl.writeKvTuple(key, val, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> removeAll(@NotNull Collection<Tuple> keys) {
        return removeAllAsync(keys).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> removeAllAsync(@NotNull Collection<Tuple> keys) {
        Objects.requireNonNull(keys);

        return tbl.doSchemaOutInOpAsync(
            ClientOp.TUPLE_DELETE_ALL,
            (s, w) -> tbl.writeTuples(keys, s, w, true),
            (schema, in) -> tbl.readTuples(schema, in, true),
            Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndRemove(@NotNull Tuple key) {
        return getAndRemoveAsync(key).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndRemoveAsync(@NotNull Tuple key) {
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> tbl.writeTuple(key, s, w, true),
                ClientTable::readValueTuple);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple key, Tuple val) {
        return replaceAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> tbl.writeKvTuple(key, val, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple key, Tuple oldVal, Tuple newVal) {
        return replaceAsync(key, oldVal, newVal).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple key, Tuple oldVal, Tuple newVal) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    tbl.writeKvTuple(key, oldVal, s, w, false);
                    tbl.writeKvTuple(key, newVal, s, w, true);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(@NotNull Tuple key, Tuple val) {
        return getAndReplaceAsync(key, val).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple key, Tuple val) {
        Objects.requireNonNull(key);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> tbl.writeKvTuple(key, val, s, w, false),
                ClientTable::readValueTuple);
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
    @Override public @Nullable Transaction transaction() {
        // TODO: Transactions IGNITE-15240
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public KeyValueView<Tuple, Tuple> withTransaction(Transaction tx) {
        // TODO: Transactions IGNITE-15240
        throw new UnsupportedOperationException("Not implemented yet.");
    }
}
