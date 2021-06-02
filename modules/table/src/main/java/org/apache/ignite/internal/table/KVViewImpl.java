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
import org.apache.ignite.internal.schema.marshaller.KVSerializer;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;

/**
 * Key-value view implementation.
 */
public class KVViewImpl<K, V> extends AbstractTableView implements KeyValueView<K, V> {
    /**
     * Constructor.
     *
     * @param tbl Table storage.
     * @param schemaReg Schema registry.
     * @param keyMapper Key class mapper.
     * @param valueMapper Value class mapper.
     */
    public KVViewImpl(InternalTable tbl, SchemaRegistry schemaReg, KeyMapper<K> keyMapper,
        ValueMapper<V> valueMapper) {
        super(tbl, schemaReg);
    }

    /** {@inheritDoc} */
    @Override public V get(@NotNull K key) {
        return sync(getAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAsync(@NotNull K key) {
        Objects.requireNonNull(key);

        final KVSerializer<K, V> marsh = marshaller();

        Row kRow = marsh.serialize(key, null); // Convert to portable format to pass TX/storage layer.

        return tbl.get(kRow)
            .thenApply(this::wrap) // Binary -> schema-aware row
            .thenApply(marsh::deserializeValue); // row -> deserialized obj.
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean contains(@NotNull K key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public void put(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAsync(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public void putAll(@NotNull Map<K, V> pairs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<K, V> pairs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndPutAsync(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull K key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull K key, @NotNull V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key, @NotNull V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public Collection<K> removeAll(@NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<K>> removeAllAsync(@NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(@NotNull K key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndRemoveAsync(@NotNull K key) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull K key, V oldVal, V newVal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V oldVal, V newVal) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndReplaceAsync(@NotNull K key, V val) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> R invoke(@NotNull K key, InvokeProcessor<K, V, R> proc, Serializable... args) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
        @NotNull K key,
        InvokeProcessor<K, V, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <R extends Serializable> Map<K, R> invokeAll(
        @NotNull Collection<K> keys,
        InvokeProcessor<K, V, R> proc,
        Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(
        @NotNull Collection<K> keys,
        InvokeProcessor<K, V, R> proc, Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * @return Marshaller.
     */
    private KVSerializer<K, V> marshaller() {
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
