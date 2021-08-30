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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.msgpack.core.MessageFormat;

/**
 * Client table API implementation.
 */
public class ClientTable implements Table {
    /** */
    private final UUID id;

    /** */
    private final String name;

    /** */
    private final ReliableChannel ch;

    /** */
    private final ConcurrentHashMap<Integer, ClientSchema> schemas = new ConcurrentHashMap<>();

    /** */
    private volatile int latestSchemaVer = -1;

    /** */
    private final Object latestSchemaLock = new Object();

    /**
     * Constructor.
     *
     * @param ch Channel.
     * @param id Table id.
     * @param name Table name.
     */
    public ClientTable(ReliableChannel ch, UUID id, String name) {
        assert ch != null;
        assert id != null;
        assert name != null && !name.isEmpty();

        this.ch = ch;
        this.id = id;
        this.name = name;
    }

    /**
     * Gets the table id.
     *
     * @return Table id.
     */
    public UUID tableId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public KeyValueBinaryView kvView() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Table withTransaction(Transaction tx) {
        // TODO: Transactions IGNITE-15240
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public Tuple get(@NotNull Tuple keyRec) {
        return getAsync(keyRec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> writeTuple(keyRec, schema, out, true),
                this::readTuple);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> getAll(@NotNull Collection<Tuple> keyRecs) {
        return getAllAsync(keyRecs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@NotNull Collection<Tuple> keyRecs) {
        Objects.requireNonNull(keyRecs);

        return doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_ALL,
                (s, w) -> writeTuples(keyRecs, s, w, true),
                this::readTuples,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override public void upsert(@NotNull Tuple rec) {
        upsertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        // TODO IGNITE-15194: Convert Tuple to a schema-order Array as a first step.
        // If it does not match the latest schema, then request latest and convert again.
        return doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> writeTuple(rec, s, w),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override public void upsertAll(@NotNull Collection<Tuple> recs) {
        upsertAllAsync(recs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT_ALL,
                (s, w) -> writeTuples(recs, s, w, false),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndUpsert(@NotNull Tuple rec) {
        return getAndUpsertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_UPSERT,
                (s, w) -> writeTuple(rec, s, w, false),
                this::readTuple);
    }

    /** {@inheritDoc} */
    @Override public boolean insert(@NotNull Tuple rec) {
        return insertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return doSchemaOutOpAsync(
                ClientOp.TUPLE_INSERT,
                (s, w) -> writeTuple(rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> insertAll(@NotNull Collection<Tuple> recs) {
        return insertAllAsync(recs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return doSchemaOutInOpAsync(
                ClientOp.TUPLE_INSERT_ALL,
                (s, w) -> writeTuples(recs, s, w, false),
                this::readTuples,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple rec) {
        return replaceAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE,
                (s, w) -> writeTuple(rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return replaceAsync(oldRec, newRec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        Objects.requireNonNull(oldRec);
        Objects.requireNonNull(newRec);

        return doSchemaOutOpAsync(
                ClientOp.TUPLE_REPLACE_EXACT,
                (s, w) -> {
                    writeTuple(oldRec, s, w, false, false);
                    writeTuple(newRec, s, w, false, true);
                },
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndReplace(@NotNull Tuple rec) {
        return getAndReplaceAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_REPLACE,
                (s, w) -> writeTuple(rec, s, w, false),
                this::readTuple);
    }

    /** {@inheritDoc} */
    @Override public boolean delete(@NotNull Tuple keyRec) {
        return deleteAsync(keyRec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull Tuple keyRec) {
        Objects.requireNonNull(keyRec);

        return doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE,
                (s, w) -> writeTuple(keyRec, s, w, true),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public boolean deleteExact(@NotNull Tuple rec) {
        return deleteExactAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return doSchemaOutOpAsync(
                ClientOp.TUPLE_DELETE_EXACT,
                (s, w) -> writeTuple(rec, s, w, false),
                ClientMessageUnpacker::unpackBoolean);
    }

    /** {@inheritDoc} */
    @Override public Tuple getAndDelete(@NotNull Tuple rec) {
        return getAndDeleteAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@NotNull Tuple rec) {
        Objects.requireNonNull(rec);

        return doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET_AND_DELETE,
                (s, w) -> writeTuple(rec, s, w, false),
                this::readTuple);
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAll(@NotNull Collection<Tuple> recs) {
        return deleteAllAsync(recs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL,
                (s, w) -> writeTuples(recs, s, w, true),
                (schema, in) -> readTuples(schema, in, true),
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override public Collection<Tuple> deleteAllExact(@NotNull Collection<Tuple> recs) {
        return deleteAllExactAsync(recs).join();
    }

    /** {@inheritDoc} */
    @Override public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@NotNull Collection<Tuple> recs) {
        Objects.requireNonNull(recs);

        return doSchemaOutInOpAsync(
                ClientOp.TUPLE_DELETE_ALL_EXACT,
                (s, w) -> writeTuples(recs, s, w, false),
                this::readTuples,
                Collections.emptyList());
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T invoke(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> Map<Tuple, T> invokeAll(@NotNull Collection<Tuple> keyRecs, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(@NotNull Collection<Tuple> keyRecs, InvokeProcessor<Tuple, Tuple, T> proc) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public @Nullable Transaction transaction() {
        // TODO: Transactions IGNITE-15240
        throw new UnsupportedOperationException();
    }

    private CompletableFuture<ClientSchema> getLatestSchema() {
        if (latestSchemaVer >= 0)
            return CompletableFuture.completedFuture(schemas.get(latestSchemaVer));

        return loadSchema(null);
    }

    private CompletableFuture<ClientSchema> getSchema(int ver) {
        var schema = schemas.get(ver);

        if (schema != null)
            return CompletableFuture.completedFuture(schema);

        return loadSchema(ver);
    }

    private CompletableFuture<ClientSchema> loadSchema(Integer ver) {
        return ch.serviceAsync(ClientOp.SCHEMAS_GET, w -> {
            w.out().packUuid(id);

            if (ver == null)
                w.out().packNil();
            else {
                w.out().packArrayHeader(1);
                w.out().packInt(ver);
            }
        }, r -> {
            int schemaCnt = r.in().unpackMapHeader();

            if (schemaCnt == 0)
                throw new IgniteClientException("Schema not found: " + ver);

            ClientSchema last = null;

            for (var i = 0; i < schemaCnt; i++)
                last = readSchema(r.in());

            return last;
        });
    }

    private ClientSchema readSchema(ClientMessageUnpacker in) {
        var schemaVer = in.unpackInt();
        var colCnt = in.unpackArrayHeader();

        var columns = new ClientColumn[colCnt];

        for (int i = 0; i < colCnt; i++) {
            var propCnt = in.unpackArrayHeader();

            assert propCnt >= 4;

            var name = in.unpackString();
            var type = in.unpackInt();
            var isKey = in.unpackBoolean();
            var isNullable = in.unpackBoolean();

            // Skip unknown extra properties, if any.
            in.skipValue(propCnt - 4);

            var column = new ClientColumn(name, type, isNullable, isKey, i);
            columns[i] = column;
        }

        var schema = new ClientSchema(schemaVer, columns);

        schemas.put(schemaVer, schema);

        synchronized (latestSchemaLock) {
            if (schemaVer > latestSchemaVer) {
                latestSchemaVer = schemaVer;
            }
        }

        return schema;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return IgniteToStringBuilder.toString(ClientTable.class, this);
    }

    private void writeTuple(
            @NotNull Tuple tuple,
            ClientSchema schema,
            ClientMessagePacker out
    ) {
        writeTuple(tuple, schema, out, false, false);
    }

    private void writeTuple(
            @NotNull Tuple tuple,
            ClientSchema schema,
            ClientMessagePacker out,
            boolean keyOnly
    ) {
        writeTuple(tuple, schema, out, keyOnly, false);
    }

    private void writeTuple(
            @NotNull Tuple tuple,
            ClientSchema schema,
            ClientMessagePacker out,
            boolean keyOnly,
            boolean skipHeader
    ) {
        // TODO: Special case for ClientTupleBuilder - it has columns in order
        var vals = new Object[keyOnly ? schema.keyColumnCount() : schema.columns().length];
        var tupleSize = tuple.columnCount();

        for (var i = 0; i < tupleSize; i++) {
            var colName = tuple.columnName(i);
            var col = schema.column(colName);

            if (keyOnly && !col.key())
                continue;

            vals[col.schemaIndex()] = tuple.value(i);
        }

        if (!skipHeader) {
            out.packUuid(id);
            out.packInt(schema.version());
        }

        for (var val : vals)
            out.packObject(val);
    }

    private void writeTuples(
            @NotNull Collection<Tuple> tuples,
            ClientSchema schema,
            ClientMessagePacker out,
            boolean keyOnly
    ) {
        out.packUuid(id);
        out.packInt(schema.version());
        out.packInt(tuples.size());

        for (var tuple : tuples)
            writeTuple(tuple, schema, out, keyOnly, true);
    }

    private Tuple readTuple(ClientSchema schema, ClientMessageUnpacker in) {
        return readTuple(schema, in, false);
    }

    private Tuple readTuple(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var tuple = new ClientTuple(schema);

        var colCnt = keyOnly ? schema.keyColumnCount() : schema.columns().length;

        for (var i = 0; i < colCnt; i++)
            tuple.setInternal(i, in.unpackObject(schema.columns()[i].type()));

        return tuple;
    }

    private Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in) {
        return readTuples(schema, in, false);
    }

    private Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var cnt = in.unpackInt();
        var res = new ArrayList<Tuple>(cnt);

        for (int i = 0; i < cnt; i++)
            res.add(readTuple(schema, in, keyOnly));

        return res;
    }

    private <T> CompletableFuture<T> doSchemaOutInOpAsync(
            int opCode,
            BiConsumer<ClientSchema, ClientMessagePacker> writer,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> reader
    ) {
        return doSchemaOutInOpAsync(opCode, writer, reader, null);
    }

    private <T> CompletableFuture<T> doSchemaOutInOpAsync(
            int opCode,
            BiConsumer<ClientSchema, ClientMessagePacker> writer,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> reader,
            T defaultValue
    ) {
        return getLatestSchema()
                .thenCompose(schema ->
                        ch.serviceAsync(opCode,
                                w -> writer.accept(schema, w.out()),
                                r -> readSchemaAndReadData(schema, r.in(), reader, defaultValue)))
                .thenCompose(t -> loadSchemaAndReadData(t, reader));
    }

    private <T> CompletableFuture<T> doSchemaOutOpAsync(
            int opCode,
            BiConsumer<ClientSchema, ClientMessagePacker> writer,
            Function<ClientMessageUnpacker, T> reader) {
        return getLatestSchema()
                .thenCompose(schema ->
                        ch.serviceAsync(opCode,
                                w -> writer.accept(schema, w.out()),
                                r -> reader.apply(r.in())));
    }

    private <T> Object readSchemaAndReadData(
            ClientSchema knownSchema,
            ClientMessageUnpacker in,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> fn,
            T defaultValue
    ) {
        if (in.getNextFormat() == MessageFormat.NIL)
            return defaultValue;

        var schemaVer = in.unpackInt();

        var resSchema = schemaVer == knownSchema.version() ? knownSchema : schemas.get(schemaVer);

        if (resSchema != null)
            return fn.apply(knownSchema, in);

        // Schema is not yet known - request.
        // Retain unpacker - normally it is closed when this method exits.
        return new IgniteBiTuple<>(in.retain(), schemaVer);
    }

    private <T> CompletionStage<T> loadSchemaAndReadData(
            Object data,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> fn
    ) {
        if (!(data instanceof IgniteBiTuple))
            return CompletableFuture.completedFuture((T) data);

        var biTuple = (IgniteBiTuple<ClientMessageUnpacker, Integer>) data;

        var in = biTuple.getKey();
        var schemaId = biTuple.getValue();

        assert in != null;
        assert schemaId != null;

        var resFut = getSchema(schemaId).thenApply(schema -> fn.apply(schema, in));

        // Close unpacker.
        resFut.handle((tuple, err) -> {
            in.close();
            return null;
        });

        return resFut;
    }
}
