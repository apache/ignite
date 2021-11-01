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
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.KVMarshaller;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.schema.marshaller.Serializer;
import org.apache.ignite.internal.schema.marshaller.SerializerFactory;
import org.apache.ignite.internal.schema.row.Row;
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
    private final SerializerFactory marshallerFactory;

    /** Key object mapper. */
    private final Mapper<K> keyMapper;

    /** Value object mapper. */
    private final Mapper<V> valueMapper;

    /** Marshaller. */
    private KVMarshallerImpl<K, V> marsh;

    /**
     * Constructor.
     *
     * @param tbl Table storage.
     * @param schemaReg Schema registry.
     * @param keyMapper Key class mapper.
     * @param valueMapper Value class mapper.
     * @param tx The transaction.
     */
    public KeyValueViewImpl(InternalTable tbl, SchemaRegistry schemaReg, Mapper<K> keyMapper,
                            Mapper<V> valueMapper, @Nullable Transaction tx) {
        super(tbl, schemaReg, tx);

        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
        marshallerFactory = SerializerFactory.createJavaSerializerFactory();
    }

    /** {@inheritDoc} */
    @Override public V get(@NotNull K key) {
        return sync(getAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAsync(@NotNull K key) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), null);

        return tbl.get(kRow, tx)
            .thenApply(this::unmarshalValue); // row -> deserialized obj.
    }

    /** {@inheritDoc} */
    @Override public Map<K, V> getAll(@NotNull Collection<K> keys) {
        return sync(getAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public boolean contains(@NotNull K key) {
        return get(key) != null;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<Boolean> containsAsync(@NotNull K key) {
        return getAsync(key).thenApply(Objects::nonNull);
    }

    /** {@inheritDoc} */
    @Override public void put(@NotNull K key, V val) {
        sync(putAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAsync(@NotNull K key, V val) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), val);

        return tbl.upsert(kRow, tx).thenAccept(ignore -> {
        });
    }

    /** {@inheritDoc} */
    @Override public void putAll(@NotNull Map<K, V> pairs) {
        sync(putAllAsync(pairs));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> putAllAsync(@NotNull Map<K, V> pairs) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public V getAndPut(@NotNull K key, V val) {
        return sync(getAndPutAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndPutAsync(@NotNull K key, V val) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), val);

        return tbl.getAndUpsert(kRow, tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override public boolean putIfAbsent(@NotNull K key, @NotNull V val) {
        return sync(putIfAbsentAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@NotNull K key, V val) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), val);

        return tbl.insert(kRow, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull K key) {
        return sync(removeAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), null);

        return tbl.delete(kRow, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(@NotNull K key, @NotNull V val) {
        return sync(removeAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> removeAsync(@NotNull K key, @NotNull V val) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), val);

        return tbl.deleteExact(kRow, tx);
    }

    /** {@inheritDoc} */
    @Override public Collection<K> removeAll(@NotNull Collection<K> keys) {
        return sync(removeAllAsync(keys));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<K>> removeAllAsync(@NotNull Collection<K> keys) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public V getAndRemove(@NotNull K key) {
        return sync(getAndRemoveAsync(key));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndRemoveAsync(@NotNull K key) {
        BinaryRow kRow = marshal(Objects.requireNonNull(key), null);

        return tbl.getAndDelete(kRow, tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull K key, V val) {
        return sync(replaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V val) {
        BinaryRow row = marshal(key, val);

        return tbl.replace(row, tx);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull K key, V oldVal, V newVal) {
        return sync(replaceAsync(key, oldVal, newVal));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull K key, V oldVal, V newVal) {
        BinaryRow oldRow = marshal(key, oldVal);
        BinaryRow newRow = marshal(key, newVal);

        return tbl.replace(oldRow, newRow, tx);
    }

    /** {@inheritDoc} */
    @Override public V getAndReplace(@NotNull K key, V val) {
        return sync(getAndReplaceAsync(key, val));
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<V> getAndReplaceAsync(@NotNull K key, V val) {
        BinaryRow row = marshal(key, val);

        return tbl.getAndReplace(row, tx).thenApply(this::unmarshalValue);
    }

    /** {@inheritDoc} */
    @Override
    public <R extends Serializable> R invoke(@NotNull K key, InvokeProcessor<K, V, R> proc, Serializable... args) {
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

    /** {@inheritDoc} */
    @Override public KeyValueViewImpl<K, V> withTransaction(Transaction tx) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /**
     * @param schemaVersion Schema version.
     * @return Marshaller.
     */
    private KVMarshaller<K, V> marshaller(int schemaVersion) {
        if (marsh == null || marsh.schemaVersion == schemaVersion) {
            // TODO: Cache marshaller for schema version or upgrade row?
            marsh = new KVMarshallerImpl<>(
                schemaVersion,
                marshallerFactory.create(
                    schemaReg.schema(schemaVersion),
                    keyMapper.getType(),
                    valueMapper.getType()
                )
            );
        }

        return marsh;
    }

    private V unmarshalValue(BinaryRow v) {
        if (v == null || !v.hasValue())
            return null;

        Row row = schemaReg.resolve(v);

        KVMarshaller<K, V> marshaller = marshaller(row.schemaVersion());

        return marshaller.unmarshalValue(row);
    }

    private BinaryRow marshal(@NotNull K key, V o) {
        final KVMarshaller<K, V> marsh = marshaller(schemaReg.lastSchemaVersion());

        return marsh.marshal(key, o);
    }

    /**
     * Marshaller wrapper for KV view.
     * Note: Serializer must be re-created if schema changed.
     *
     * @param <K> Key type.
     * @param <V> Value type.
     */
    private static class KVMarshallerImpl<K, V> implements KVMarshaller<K, V> {
        /** Schema version. */
        private final int schemaVersion;

        /** Serializer. */
        private Serializer serializer;

        /**
         * Creates KV marshaller.
         *
         * @param schemaVersion Schema version.
         * @param serializer Serializer.
         */
        KVMarshallerImpl(int schemaVersion, Serializer serializer) {
            this.schemaVersion = schemaVersion;

            this.serializer = serializer;
        }

        /** {@inheritDoc} */
        @Override public BinaryRow marshal(@NotNull K key, V val) {
            try {
                return serializer.serialize(key, val);
            } catch (SerializationException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @NotNull @Override public K unmarshalKey(@NotNull Row row) {
            try {
                return serializer.deserializeKey(row);
            } catch (SerializationException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Nullable @Override public V unmarshalValue(@NotNull Row row) {
            try {
                return serializer.deserializeValue(row);
            } catch (SerializationException e) {
                throw new IgniteException(e);
            }
        }
    }
}
