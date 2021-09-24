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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.msgpack.core.MessageFormat;

/**
 * Client table API implementation.
 */
public class ClientTable implements Table {
    /** */
    private final IgniteUuid id;

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
    public ClientTable(ReliableChannel ch, IgniteUuid id, String name) {
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
    public IgniteUuid tableId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String tableName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    /** {@inheritDoc} */
    @Override public <K, V> KeyValueView<K, V> keyValueView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        throw new UnsupportedOperationException("Not implemented yet.");
    }

    @Override public RecordView<Tuple> recordView() {
        return new ClientRecordBinaryView(this);
    }

    /** {@inheritDoc} */
    @Override public KeyValueView<Tuple, Tuple> keyValueView() {
        return new ClientKeyValueBinaryView(this);
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
            w.out().packIgniteUuid(id);

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

    public void writeTuple(
            @NotNull Tuple tuple,
            ClientSchema schema,
            ClientMessagePacker out
    ) {
        writeTuple(tuple, schema, out, false, false);
    }

    public void writeTuple(
            @NotNull Tuple tuple,
            ClientSchema schema,
            ClientMessagePacker out,
            boolean keyOnly
    ) {
        writeTuple(tuple, schema, out, keyOnly, false);
    }

    public void writeTuple(
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
            out.packIgniteUuid(id);
            out.packInt(schema.version());
        }

        for (var val : vals)
            out.packObject(val);
    }

    public void writeKvTuple(
            @NotNull Tuple key,
            @Nullable Tuple val,
            ClientSchema schema,
            ClientMessagePacker out,
            boolean skipHeader
    ) {
        var vals = new Object[schema.columns().length];

        for (var i = 0; i < key.columnCount(); i++) {
            var colName = key.columnName(i);
            var col = schema.column(colName);

            if (!col.key())
                continue;

            vals[col.schemaIndex()] = key.value(i);
        }

        if (val != null) {
            for (var i = 0; i < val.columnCount(); i++) {
                var colName = val.columnName(i);
                var col = schema.column(colName);

                if (col.key())
                    continue;

                vals[col.schemaIndex()] = val.value(i);
            }
        }

        if (!skipHeader) {
            out.packIgniteUuid(id);
            out.packInt(schema.version());
        }

        for (var v : vals)
            out.packObject(v);
    }

    public void writeKvTuples(Map<Tuple, Tuple> pairs, ClientSchema schema, ClientMessagePacker out) {
        out.packIgniteUuid(id);
        out.packInt(schema.version());
        out.packInt(pairs.size());

        for (Map.Entry<Tuple, Tuple> pair : pairs.entrySet())
            writeKvTuple(pair.getKey(), pair.getValue(), schema, out, true);
    }

    public void writeTuples(
            @NotNull Collection<Tuple> tuples,
            ClientSchema schema,
            ClientMessagePacker out,
            boolean keyOnly
    ) {
        out.packIgniteUuid(id);
        out.packInt(schema.version());
        out.packInt(tuples.size());

        for (var tuple : tuples)
            writeTuple(tuple, schema, out, keyOnly, true);
    }

    private static Tuple readTuple(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var tuple = new ClientTuple(schema);

        var colCnt = keyOnly ? schema.keyColumnCount() : schema.columns().length;

        for (var i = 0; i < colCnt; i++)
            tuple.setInternal(i, in.unpackObject(schema.columns()[i].type()));

        return tuple;
    }

    static Tuple readValueTuple(ClientSchema schema, ClientMessageUnpacker in, Tuple keyTuple) {
        var tuple = new ClientTuple(schema);

        for (var i = 0; i < schema.columns().length; i++) {
            ClientColumn col = schema.columns()[i];

            Object value = i < schema.keyColumnCount()
                    ? keyTuple.value(col.name())
                    : in.unpackObject(schema.columns()[i].type());

            tuple.setInternal(i, value);
        }

        return tuple;
    }

    static Tuple readValueTuple(ClientSchema schema, ClientMessageUnpacker in) {
        var keyColCnt = schema.keyColumnCount();
        var colCnt = schema.columns().length;

        var valTuple = new ClientTuple(schema, keyColCnt, schema.columns().length - 1);

        for (var i = keyColCnt; i < colCnt; i++) {
            ClientColumn col = schema.columns()[i];
            Object val = in.unpackObject(col.type());

            valTuple.setInternal(i - keyColCnt, val);
        }

        return valTuple;
    }

    static IgniteBiTuple<Tuple, Tuple> readKvTuple(ClientSchema schema, ClientMessageUnpacker in) {
        var keyColCnt = schema.keyColumnCount();
        var colCnt = schema.columns().length;

        var keyTuple = new ClientTuple(schema, 0, keyColCnt - 1);
        var valTuple = new ClientTuple(schema, keyColCnt, schema.columns().length - 1);

        for (var i = 0; i < colCnt; i++) {
            ClientColumn col = schema.columns()[i];
            Object val = in.unpackObject(col.type());

            if (i < keyColCnt)
                keyTuple.setInternal(i, val);
            else
                valTuple.setInternal(i - keyColCnt, val);
        }

        return new IgniteBiTuple<>(keyTuple, valTuple);
    }

    public Map<Tuple, Tuple> readKvTuples(ClientSchema schema, ClientMessageUnpacker in) {
        var cnt = in.unpackInt();
        Map<Tuple, Tuple> res = new HashMap<>(cnt);

        for (int i = 0; i < cnt; i++) {
            var pair = readKvTuple(schema, in);
            res.put(pair.get1(), pair.get2());
        }

        return res;
    }

    Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in) {
        return readTuples(schema, in, false);
    }

    Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var cnt = in.unpackInt();
        var res = new ArrayList<Tuple>(cnt);

        for (int i = 0; i < cnt; i++)
            res.add(readTuple(schema, in, keyOnly));

        return res;
    }

    <T> CompletableFuture<T> doSchemaOutInOpAsync(
            int opCode,
            BiConsumer<ClientSchema, ClientMessagePacker> writer,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> reader
    ) {
        return doSchemaOutInOpAsync(opCode, writer, reader, null);
    }

    <T> CompletableFuture<T> doSchemaOutInOpAsync(
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

    <T> CompletableFuture<T> doSchemaOutOpAsync(
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
