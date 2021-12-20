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
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value view implementation.
 */
public class KeyValueViewImpl<K, V> extends AbstractTableView implements KeyValueView<K, V> {
    /** Marshaller factory. */
    private final Function<SchemaDescriptor, KvMarshaller<K, V>> marshallerFactory;

    /** Key-value marshaller. */
    private volatile KvMarshaller<K, V> marsh;

    /**
     * Constructor.
     *
     * @param tbl         Table storage.
     * @param schemaReg   Schema registry.
     * @param keyMapper   Key class mapper.
     * @param valueMapper Value class mapper.
     */
    public KeyValueViewImpl(
            InternalTable tbl,
            SchemaRegistry schemaReg,
            Mapper<K> keyMapper,
            Mapper<V> valueMapper
    ) {
        super(tbl, schemaReg);

        marshallerFactory = (schema) -> new KvMarshallerImpl<>(schema, keyMapper, valueMapper);
    }

    /** {@inheritDoc} */
    @Override
    public V get(@Nullable Transaction tx, @NotNull K key) {
        return sync(getAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<V> getAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(key));

        return tbl.get(keyRow, (InternalTransaction) tx)
                .thenApply(this::unmarshalValue); // row -> deserialized obj.
    }

    /** {@inheritDoc} */
    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        return sync(getAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, @NotNull K key) {
        return sync(containsAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(key));

        return tbl.get(keyRow, (InternalTransaction) tx)
                .thenApply(Objects::nonNull);
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, @NotNull K key, V val) {
        sync(putAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<Void> putAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(key), val);

        return tbl.upsert(keyRow, (InternalTransaction) tx).thenAccept(ignore -> {
        });
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, @NotNull Map<K, V> pairs) {
        sync(putAllAsync(tx, pairs));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, @NotNull Map<K, V> pairs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public V getAndPut(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(getAndPutAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(key), val);

        return tbl.getAndUpsert(keyRow, (InternalTransaction) tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, @NotNull K key, @NotNull V val) {
        return sync(putIfAbsentAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(key), val);

        return tbl.insert(keyRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, @NotNull K key) {
        return sync(removeAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(removeAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(key));
        return tbl.delete(keyRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(key), val);
        return tbl.deleteExact(keyRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        return sync(removeAllAsync(tx, keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public V getAndRemove(@Nullable Transaction tx, @NotNull K key) {
        return sync(getAndRemoveAsync(tx, key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, @NotNull K key) {
        BinaryRow keyRow = marshal(Objects.requireNonNull(key));
        return tbl.getAndDelete(keyRow, (InternalTransaction) tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(replaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal) {
        return sync(replaceAsync(tx, key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRow row = marshal(Objects.requireNonNull(key), val);
        return tbl.replace(row, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);

        BinaryRow oldRow = marshal(key, oldVal);
        BinaryRow newRow = marshal(key, newVal);
        return tbl.replace(oldRow, newRow, (InternalTransaction) tx);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndReplace(@Nullable Transaction tx, @NotNull K key, V val) {
        return sync(getAndReplaceAsync(tx, key, val));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull
    CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        BinaryRow row = marshal(key, val);

        return tbl.getAndReplace(row, (InternalTransaction) tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> R invoke(
            @Nullable Transaction tx,
            @NotNull K key,
            InvokeProcessor<K, V, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<R> invokeAsync(
            @Nullable Transaction tx,
            @NotNull K key,
            InvokeProcessor<K, V, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> Map<K, R> invokeAll(
            @Nullable Transaction tx,
            @NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <R extends Serializable> CompletableFuture<Map<K, R>> invokeAllAsync(
            @Nullable Transaction tx,
            @NotNull Collection<K> keys,
            InvokeProcessor<K, V, R> proc,
            Serializable... args
    ) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * Returns marshaller.
     *
     * @param schemaVersion Schema version.
     */
    private KvMarshaller<K, V> marshaller(int schemaVersion) {
        KvMarshaller<K, V> marsh = this.marsh;

        if (marsh != null && marsh.schemaVersion() == schemaVersion) {
            return marsh;
        }

        // TODO: Cache marshaller for schema version or upgrade row?

        return this.marsh = marshallerFactory.apply(schemaReg.schema(schemaVersion));
    }

    /**
     * Unmarshal value object from given binary row.
     *
     * @param binaryRow Binary row.
     * @return Value object.
     */
    private V unmarshalValue(BinaryRow binaryRow) {
        if (binaryRow == null || !binaryRow.hasValue()) {
            return null;
        }

        Row row = schemaReg.resolve(binaryRow);

        KvMarshaller<K, V> marshaller = marshaller(row.schemaVersion());

        try {
            return marshaller.unmarshalValue(row);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Marshal key-value pair to a row.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Binary row.
     */
    private BinaryRow marshal(@NotNull K key, V val) {
        final KvMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        try {
            return marsh.marshal(key, val);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Marshal key to a row.
     *
     * @param key Key object.
     * @return Binary row.
     */
    private BinaryRow marshal(@NotNull K key) {
        final KvMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        try {
            return marsh.marshal(key);
        } catch (MarshallerException e) {
            throw new IgniteException(e);
        }
    }
}
