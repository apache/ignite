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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallerUtil;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client record view implementation.
 */
public class ClientRecordView<R> implements RecordView<R> {
    /** Mapper. */
    private final Mapper<R> recMapper;

    /** Underlying table. */
    private final ClientTable tbl;

    /** Simple mapping mode: single column maps to a basic type. For example, {@code RecordView<String>}.  */
    private final boolean oneColumnMode;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     * @param recMapper Mapper.
     */
    public ClientRecordView(ClientTable tbl, Mapper<R> recMapper) {
        this.tbl = tbl;
        this.recMapper = recMapper;

        oneColumnMode = MarshallerUtil.mode(recMapper.targetType()) != null;
    }

    /** {@inheritDoc} */
    @Override
    public R get(@Nullable Transaction tx, @NotNull R keyRec) {
        return getAsync(tx, keyRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAsync(@Nullable Transaction tx, @NotNull R keyRec) {
        Objects.requireNonNull(keyRec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> writeRec(keyRec, schema, out, TuplePart.KEY),
                (inSchema, in) -> readValRec(keyRec, inSchema, in));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> getAll(@Nullable Transaction tx, @NotNull Collection<R> keyRecs) {
        return getAllAsync(tx, keyRecs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> getAllAsync(@Nullable Transaction tx, @NotNull Collection<R> keyRecs) {
        Objects.requireNonNull(keyRecs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (schema, out) -> writeRecs(keyRecs, schema, out, TuplePart.KEY),
                this::readRecsNullable,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@Nullable Transaction tx, @NotNull R rec) {
        upsertAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@Nullable Transaction tx, @NotNull R rec) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> writeRec(rec, s, w, TuplePart.KEY_AND_VAL),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        upsertAllAsync(tx, recs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        Objects.requireNonNull(recs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> writeRecs(recs, s, w, TuplePart.KEY_AND_VAL),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public R getAndUpsert(@Nullable Transaction tx, @NotNull R rec) {
        return getAndUpsertAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndUpsertAsync(@Nullable Transaction tx, @NotNull R rec) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> writeRec(rec, s, w, TuplePart.KEY_AND_VAL),
                (s, r) -> readValRec(rec, s, r));
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@Nullable Transaction tx, @NotNull R rec) {
        return insertAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@Nullable Transaction tx, @NotNull R rec) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> writeRec(rec, s, w, TuplePart.KEY_AND_VAL),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> insertAll(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        return insertAllAsync(tx, recs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> insertAllAsync(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        Objects.requireNonNull(recs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_INSERT_ALL,
                (s, w) -> writeRecs(recs, s, w, TuplePart.KEY_AND_VAL),
                this::readRecs,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull R rec) {
        return replaceAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@Nullable Transaction tx, @NotNull R oldRec, @NotNull R newRec) {
        return replaceAsync(tx, oldRec, newRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull R rec) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> writeRec(rec, s, w, TuplePart.KEY_AND_VAL),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@Nullable Transaction tx, @NotNull R oldRec, @NotNull R newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> writeRecs(oldRec, newRec, s, w, TuplePart.KEY_AND_VAL),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public R getAndReplace(@Nullable Transaction tx, @NotNull R rec) {
        return getAndReplaceAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndReplaceAsync(@Nullable Transaction tx, @NotNull R rec) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> writeRec(rec, s, w, TuplePart.KEY_AND_VAL),
                (s, r) -> readValRec(rec, s, r));
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@Nullable Transaction tx, @NotNull R keyRec) {
        return deleteAsync(tx, keyRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@Nullable Transaction tx, @NotNull R keyRec) {
        Objects.requireNonNull(keyRec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> writeRec(keyRec, s, w, TuplePart.KEY),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@Nullable Transaction tx, @NotNull R rec) {
        return deleteExactAsync(tx, rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@Nullable Transaction tx, @NotNull R rec) {
        Objects.requireNonNull(rec);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> writeRec(rec, s, w, TuplePart.KEY_AND_VAL),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override
    public R getAndDelete(@Nullable Transaction tx, @NotNull R keyRec) {
        return getAndDeleteAsync(tx, keyRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndDeleteAsync(@Nullable Transaction tx, @NotNull R keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> writeRec(keyRec, s, w, TuplePart.KEY),
                (s, r) -> readValRec(keyRec, s, r));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAll(@Nullable Transaction tx, @NotNull Collection<R> keyRecs) {
        return deleteAllAsync(tx, keyRecs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllAsync(@Nullable Transaction tx, @NotNull Collection<R> keyRecs) {
        Objects.requireNonNull(keyRecs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (s, w) -> writeRecs(keyRecs, s, w, TuplePart.KEY),
                (schema, in) -> readRecs(schema, in, false, TuplePart.KEY),
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAllExact(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        return deleteAllExactAsync(tx, recs).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(@Nullable Transaction tx, @NotNull Collection<R> recs) {
        Objects.requireNonNull(recs);
        // TODO: Transactions IGNITE-15240
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL_EXACT,
                (s, w) -> writeRecs(recs, s, w, TuplePart.KEY_AND_VAL),
                this::readRecs,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> T invoke(@Nullable Transaction tx, @NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(
            @Nullable Transaction tx,
            @NotNull R keyRec,
            InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> Map<R, T> invokeAll(
            @Nullable Transaction tx,
            @NotNull Collection<R> keyRecs,
            InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(
            @Nullable Transaction tx,
            @NotNull Collection<R> keyRecs,
            InvokeProcessor<R, R, T> proc
    ) {
        throw new UnsupportedOperationException();
    }

    private void writeRec(@NotNull R rec, ClientSchema schema, ClientMessagePacker out, TuplePart part) {
        out.packIgniteUuid(tbl.tableId());
        out.packInt(schema.version());

        Marshaller marshaller = schema.getMarshaller(recMapper, part);
        ClientMarshallerWriter writer = new ClientMarshallerWriter(out);

        try {
            marshaller.writeObject(rec, writer);
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    private void writeRecs(@NotNull R rec, @NotNull R rec2, ClientSchema schema, ClientMessagePacker out, TuplePart part) {
        out.packIgniteUuid(tbl.tableId());
        out.packInt(schema.version());

        Marshaller marshaller = schema.getMarshaller(recMapper, part);
        ClientMarshallerWriter writer = new ClientMarshallerWriter(out);

        try {
            marshaller.writeObject(rec, writer);
            marshaller.writeObject(rec2, writer);
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    private void writeRecs(@NotNull Collection<R> recs, ClientSchema schema, ClientMessagePacker out, TuplePart part) {
        out.packIgniteUuid(tbl.tableId());
        out.packInt(schema.version());
        out.packInt(recs.size());

        Marshaller marshaller = schema.getMarshaller(recMapper, part);
        ClientMarshallerWriter writer = new ClientMarshallerWriter(out);

        try {
            for (R rec : recs) {
                marshaller.writeObject(rec, writer);
            }
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    private Collection<R> readRecsNullable(ClientSchema schema, ClientMessageUnpacker in) {
        return readRecs(schema, in, true, TuplePart.KEY_AND_VAL);
    }

    private Collection<R> readRecs(ClientSchema schema, ClientMessageUnpacker in) {
        return readRecs(schema, in, false, TuplePart.KEY_AND_VAL);
    }

    private Collection<R> readRecs(ClientSchema schema, ClientMessageUnpacker in, boolean nullable, TuplePart part) {
        var cnt = in.unpackInt();
        var res = new ArrayList<R>(cnt);

        if (cnt == 0) {
            return res;
        }

        Marshaller marshaller = schema.getMarshaller(recMapper, part);
        var reader = new ClientMarshallerReader(in);

        try {
            for (int i = 0; i < cnt; i++) {
                if (nullable && !in.unpackBoolean()) {
                    res.add(null);
                } else {
                    res.add((R) marshaller.readObject(reader, null));
                }
            }
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }

        return res;
    }

    private R readValRec(@NotNull R keyRec, ClientSchema schema, ClientMessageUnpacker in) {
        if (oneColumnMode) {
            return keyRec;
        }

        Marshaller keyMarshaller = schema.getMarshaller(recMapper, TuplePart.KEY);
        Marshaller valMarshaller = schema.getMarshaller(recMapper, TuplePart.VAL);

        ClientMarshallerReader reader = new ClientMarshallerReader(in);

        try {
            var res = (R) valMarshaller.readObject(reader, null);

            keyMarshaller.copyObject(keyRec, res);

            return res;
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }
}
