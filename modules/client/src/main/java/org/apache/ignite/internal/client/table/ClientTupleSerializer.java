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

import static org.apache.ignite.internal.client.proto.ClientMessageCommon.NO_VALUE;
import static org.apache.ignite.internal.client.table.ClientTable.writeTx;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Tuple serializer.
 */
class ClientTupleSerializer {
    /** Table ID. */
    private final IgniteUuid tableId;

    /**
     * Constructor.
     *
     * @param tableId Table id.
     */
    ClientTupleSerializer(IgniteUuid tableId) {
        this.tableId = tableId;
    }

    /**
     * Writes {@link Tuple}.
     *
     * @param tuple Tuple.
     * @param schema Schema.
     * @param out Out.
     */
    public void writeTuple(
            @Nullable Transaction tx,
            @NotNull Tuple tuple,
            ClientSchema schema,
            PayloadOutputChannel out
    ) {
        writeTuple(tx, tuple, schema, out, false, false);
    }

    /**
     * Writes {@link Tuple}.
     *
     * @param tuple Tuple.
     * @param schema Schema.
     * @param out Out.
     * @param keyOnly Key only.
     */
    public void writeTuple(
            @Nullable Transaction tx,
            @NotNull Tuple tuple,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean keyOnly
    ) {
        writeTuple(tx, tuple, schema, out, keyOnly, false);
    }

    /**
     * Writes {@link Tuple}.
     *
     * @param tuple Tuple.
     * @param schema Schema.
     * @param out Out.
     * @param keyOnly Key only.
     * @param skipHeader Skip header.
     */
    public void writeTuple(
            @Nullable Transaction tx,
            @NotNull Tuple tuple,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean keyOnly,
            boolean skipHeader
    ) {
        if (!skipHeader) {
            out.out().packIgniteUuid(tableId);
            writeTx(tx, out);
            out.out().packInt(schema.version());
        }

        var columns = schema.columns();
        var count = keyOnly ? schema.keyColumnCount() : columns.length;

        for (var i = 0; i < count; i++) {
            var col = columns[i];

            Object v = tuple.valueOrDefault(col.name(), NO_VALUE);

            out.out().packObject(v);
        }
    }

    /**
     * Writes key and value {@link Tuple}.
     *
     * @param key Key tuple.
     * @param val Value tuple.
     * @param schema Schema.
     * @param out Out.
     * @param skipHeader Skip header.
     */
    public void writeKvTuple(
            @Nullable Transaction tx,
            @NotNull Tuple key,
            @Nullable Tuple val,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean skipHeader
    ) {
        if (!skipHeader) {
            out.out().packIgniteUuid(tableId);
            writeTx(tx, out);
            out.out().packInt(schema.version());
        }

        var columns = schema.columns();

        for (var i = 0; i < columns.length; i++) {
            var col = columns[i];

            Object v = col.key()
                    ? key.valueOrDefault(col.name(), NO_VALUE)
                    : val != null
                            ? val.valueOrDefault(col.name(), NO_VALUE)
                            : NO_VALUE;

            out.out().packObject(v);
        }
    }

    /**
     * Writes pairs {@link Tuple}.
     *
     * @param pairs Key tuple.
     * @param schema Schema.
     * @param out Out.
     */
    public void writeKvTuples(@Nullable Transaction tx, Map<Tuple, Tuple> pairs, ClientSchema schema, PayloadOutputChannel out) {
        out.out().packIgniteUuid(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());
        out.out().packInt(pairs.size());

        for (Map.Entry<Tuple, Tuple> pair : pairs.entrySet()) {
            writeKvTuple(tx, pair.getKey(), pair.getValue(), schema, out, true);
        }
    }

    /**
     * Writes {@link Tuple}'s.
     *
     * @param tuples Tuples.
     * @param schema Schema.
     * @param out Out.
     * @param keyOnly Key only.
     */
    public void writeTuples(
            @Nullable Transaction tx,
            @NotNull Collection<Tuple> tuples,
            ClientSchema schema,
            PayloadOutputChannel out,
            boolean keyOnly
    ) {
        out.out().packIgniteUuid(tableId);
        writeTx(tx, out);
        out.out().packInt(schema.version());
        out.out().packInt(tuples.size());

        for (var tuple : tuples) {
            writeTuple(tx, tuple, schema, out, keyOnly, true);
        }
    }

    public static Tuple readTuple(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var tuple = new ClientTuple(schema);

        var colCnt = keyOnly ? schema.keyColumnCount() : schema.columns().length;

        for (var i = 0; i < colCnt; i++) {
            tuple.setInternal(i, in.unpackObject(schema.columns()[i].type()));
        }

        return tuple;
    }

    public static Tuple readValueTuple(ClientSchema schema, ClientMessageUnpacker in, Tuple keyTuple) {
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

    public static Tuple readValueTuple(ClientSchema schema, ClientMessageUnpacker in) {
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

    public static IgniteBiTuple<Tuple, Tuple> readKvTuple(ClientSchema schema, ClientMessageUnpacker in) {
        var keyColCnt = schema.keyColumnCount();
        var colCnt = schema.columns().length;

        var keyTuple = new ClientTuple(schema, 0, keyColCnt - 1);
        var valTuple = new ClientTuple(schema, keyColCnt, schema.columns().length - 1);

        for (var i = 0; i < colCnt; i++) {
            ClientColumn col = schema.columns()[i];
            Object val = in.unpackObject(col.type());

            if (i < keyColCnt) {
                keyTuple.setInternal(i, val);
            } else {
                valTuple.setInternal(i - keyColCnt, val);
            }
        }

        return new IgniteBiTuple<>(keyTuple, valTuple);
    }

    /**
     * Reads {@link Tuple} pairs.
     *
     * @param schema Schema.
     * @param in In.
     * @return Tuple pairs.
     */
    public static Map<Tuple, Tuple> readKvTuplesNullable(ClientSchema schema, ClientMessageUnpacker in) {
        var cnt = in.unpackInt();
        Map<Tuple, Tuple> res = new HashMap<>(cnt);

        for (int i = 0; i < cnt; i++) {
            var hasValue = in.unpackBoolean();

            if (hasValue) {
                var pair = readKvTuple(schema, in);

                res.put(pair.get1(), pair.get2());
            }
        }

        return res;
    }

    public static Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in) {
        return readTuples(schema, in, false);
    }

    public static Collection<Tuple> readTuples(ClientSchema schema, ClientMessageUnpacker in, boolean keyOnly) {
        var cnt = in.unpackInt();
        var res = new ArrayList<Tuple>(cnt);

        for (int i = 0; i < cnt; i++) {
            res.add(readTuple(schema, in, keyOnly));
        }

        return res;
    }

    public static Collection<Tuple> readTuplesNullable(ClientSchema schema, ClientMessageUnpacker in) {
        var cnt = in.unpackInt();
        var res = new ArrayList<Tuple>(cnt);

        for (int i = 0; i < cnt; i++) {
            var tuple = in.unpackBoolean()
                    ? readTuple(schema, in, false)
                    : null;

            res.add(tuple);
        }

        return res;
    }
}
