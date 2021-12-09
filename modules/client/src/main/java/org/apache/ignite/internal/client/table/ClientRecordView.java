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
    public R get(@NotNull R keyRec) {
        return getAsync(keyRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAsync(@NotNull R keyRec) {
        Objects.requireNonNull(keyRec);

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> writeRec(keyRec, schema, out, TuplePart.KEY),
                (inSchema, in) -> readValRec(keyRec, inSchema, in));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> getAll(@NotNull Collection<R> keyRecs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> getAllAsync(@NotNull Collection<R> keyRecs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@NotNull R rec) {
        upsertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@NotNull R rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> writeRec(rec, s, w, TuplePart.KEY_AND_VAL),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@NotNull Collection<R> recs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<R> recs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public R getAndUpsert(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndUpsertAsync(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> insertAll(@NotNull Collection<R> recs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> insertAllAsync(@NotNull Collection<R> recs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull R oldRec, @NotNull R newRec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R oldRec, @NotNull R newRec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public R getAndReplace(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndReplaceAsync(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@NotNull R keyRec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull R keyRec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public R getAndDelete(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndDeleteAsync(@NotNull R rec) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }        // TODO: Implement all operations (IGNITE-16087).


    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAll(@NotNull Collection<R> recs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllAsync(@NotNull Collection<R> recs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAllExact(@NotNull Collection<R> recs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(@NotNull Collection<R> recs) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> T invoke(@NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(@NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> Map<R, T> invokeAll(@NotNull Collection<R> keyRecs, InvokeProcessor<R, R, T> proc) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(@NotNull Collection<R> keyRecs,
            InvokeProcessor<R, R, T> proc) {
        // TODO: Implement all operations (IGNITE-16087).
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Transaction transaction() {
        // TODO: Transactions IGNITE-15240
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override
    public RecordView<R> withTransaction(Transaction tx) {
        // TODO: Transactions IGNITE-15240
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    private void writeRec(@NotNull R rec, ClientSchema schema, ClientMessagePacker out, TuplePart part) {
        out.packIgniteUuid(tbl.tableId());
        out.packInt(schema.version());

        try {
            schema.getMarshaller(recMapper, part).writeObject(rec, new ClientMarshallerWriter(out));
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    private R readValRec(@NotNull R keyRec, ClientSchema inSchema, ClientMessageUnpacker in) {
        if (oneColumnMode) {
            return keyRec;
        }

        try {
            var res = (R) inSchema.getMarshaller(recMapper, TuplePart.VAL).readObject(new ClientMarshallerReader(in), null);

            inSchema.getMarshaller(recMapper, TuplePart.KEY).copyObject(keyRec, res);

            return res;
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }
}
