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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client key-value view implementation.
 */
public class ClientKeyValueView<K, V> implements KeyValueView<K, V> {
    /** Underlying table. */
    private final ClientTable tbl;

    /** Key serializer.  */
    private final ClientRecordSerializer<K> keySer;

    /** Value serializer.  */
    private final ClientRecordSerializer<V> valSer;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     * @param keyMapper Key mapper.
     * @param valMapper value mapper.
     */
    public ClientKeyValueView(ClientTable tbl, Mapper<K> keyMapper, Mapper<V> valMapper) {
        assert tbl != null;
        assert keyMapper != null;
        assert valMapper != null;

        this.tbl = tbl;

        keySer = new ClientRecordSerializer<>(tbl.tableId(), keyMapper);
        valSer = new ClientRecordSerializer<>(tbl.tableId(), valMapper);
    }

    /** {@inheritDoc} */
    @Override
    public V get(@Nullable Transaction tx, @NotNull K key) {
        return getAsync(tx, key).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAsync(@Nullable Transaction tx, @NotNull K key) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> keySer.writeRec(key, schema, out, TuplePart.KEY),
                (inSchema, in) -> valSer.readRec(inSchema, in, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public Map<K, V> getAll(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        return getAllAsync(tx, keys).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<K, V>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        Objects.requireNonNull(keys);
        throwUnsupportedIfTxNotNull(tx);

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyMap());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (schema, out) -> keySer.writeRecs(keys, schema, out, TuplePart.KEY),
                this::readGetAllResponse,
                Collections.emptyMap());
    }

    /** {@inheritDoc} */
    @Override
    public boolean contains(@Nullable Transaction tx, @NotNull K key) {
        return containsAsync(tx, key).join();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> containsAsync(@Nullable Transaction tx, @NotNull K key) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_CONTAINS_KEY,
                (schema, out) -> keySer.writeRec(key, schema, out, TuplePart.KEY),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public void put(@Nullable Transaction tx, @NotNull K key, V val) {
        putAsync(tx, key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> writeKeyValue(s, w, key, val),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void putAll(@Nullable Transaction tx, @NotNull Map<K, V> pairs) {
        putAllAsync(tx, pairs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAllAsync(@Nullable Transaction tx, @NotNull Map<K, V> pairs) {
        Objects.requireNonNull(pairs);
        throwUnsupportedIfTxNotNull(tx);

        if (pairs.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> {
                    w.packIgniteUuid(tbl.tableId());
                    w.packInt(s.version());
                    w.packInt(pairs.size());

                    for (Entry<K, V> e : pairs.entrySet()) {
                        keySer.writeRecRaw(e.getKey(), s, w, TuplePart.KEY);
                        valSer.writeRecRaw(e.getValue(), s, w, TuplePart.VAL);
                    }
                },
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndPut(@Nullable Transaction tx, @NotNull K key, V val) {
        return getAndPutAsync(tx, key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndPutAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> writeKeyValue(s, w, key, val),
                (s, r) -> valSer.readRec(s, r, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public boolean putIfAbsent(@Nullable Transaction tx, @NotNull K key, @NotNull V val) {
        return putIfAbsentAsync(tx, key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> putIfAbsentAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> writeKeyValue(s, w, key, val),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, @NotNull K key) {
        return removeAsync(tx, key).join();
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(@Nullable Transaction tx, @NotNull K key, V val) {
        return removeAsync(tx, key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> keySer.writeRec(key, s, w, TuplePart.KEY),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> removeAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> writeKeyValue(s, w, key, val),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<K> removeAll(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        return removeAllAsync(tx, keys).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<K>> removeAllAsync(@Nullable Transaction tx, @NotNull Collection<K> keys) {
        Objects.requireNonNull(keys);
        throwUnsupportedIfTxNotNull(tx);

        if (keys.isEmpty()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (schema, out) -> keySer.writeRecs(keys, schema, out, TuplePart.KEY),
                (inSchema, in) -> keySer.readRecs(inSchema, in, false, TuplePart.KEY),
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public V getAndRemove(@Nullable Transaction tx, @NotNull K key) {
        return getAndRemoveAsync(tx, key).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndRemoveAsync(@Nullable Transaction tx, @NotNull K key) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> keySer.writeRec(key, s, w, TuplePart.KEY),
                (s, r) -> valSer.readRec(s, r, TuplePart.VAL));
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull K key, V val) {
        return replaceAsync(tx, key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return replaceAsync(tx, key, oldVal, newVal).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> writeKeyValue(s, w, key, val),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull K key, V oldVal, V newVal) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    keySer.writeRec(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(oldVal, s, w, TuplePart.VAL);

                    keySer.writeRecRaw(key, s, w, TuplePart.KEY);
                    valSer.writeRecRaw(newVal, s, w, TuplePart.VAL);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public V getAndReplace(@Nullable Transaction tx, @NotNull K key, V val) {
        return getAndReplaceAsync(tx, key, val).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<V> getAndReplaceAsync(@Nullable Transaction tx, @NotNull K key, V val) {
        Objects.requireNonNull(key);
        throwUnsupportedIfTxNotNull(tx);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> writeKeyValue(s, w, key, val),
                (inSchema, in) -> valSer.readRec(inSchema, in, TuplePart.VAL));
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

    private void writeKeyValue(ClientSchema s, ClientMessagePacker w, @NotNull K key, V val) {
        keySer.writeRec(key, s, w, TuplePart.KEY);
        valSer.writeRecRaw(val, s, w, TuplePart.VAL);
    }

    private HashMap<K, V> readGetAllResponse(ClientSchema schema, ClientMessageUnpacker in) {
        var cnt = in.unpackInt();

        var res = new LinkedHashMap<K, V>(cnt);

        Marshaller keyMarsh = schema.getMarshaller(keySer.mapper(), TuplePart.KEY);
        Marshaller valMarsh = schema.getMarshaller(valSer.mapper(), TuplePart.VAL);

        var reader = new ClientMarshallerReader(in);

        try {
            for (int i = 0; i < cnt; i++) {
                in.unpackBoolean(); // TODO: Optimize (IGNITE-16022).
                res.put((K) keyMarsh.readObject(reader, null), (V) valMarsh.readObject(reader, null));
            }

            return res;
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    private static void throwUnsupportedIfTxNotNull(Transaction tx) {
        if (tx != null) {
            // TODO: IGNITE-15240.
            throw new UnsupportedOperationException("Not implemented yet.");
        }
    }
}
