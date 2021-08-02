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

package org.apache.ignite.client.handler.requests.table;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.client.proto.ClientDataType;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.apache.ignite.table.manager.IgniteTables;
import org.msgpack.core.MessageFormat;

/**
 * Common table functionality.
 */
class ClientTableCommon {
    /**
     * Writes a schema.
     *
     * @param packer Packer.
     * @param schemaVer Schema version.
     * @param schema Schema.
     * @throws IOException When serialization fails.
     */
    public static void writeSchema(
            ClientMessagePacker packer,
            int schemaVer,
            SchemaDescriptor schema
    ) throws IOException {
        packer.packInt(schemaVer);

        if (schema == null) {
            packer.packNil();

            return;
        }

        var colCnt = schema.columnNames().size();
        packer.packArrayHeader(colCnt);

        for (var colIdx = 0; colIdx < colCnt; colIdx++) {
            var col = schema.column(colIdx);

            packer.packArrayHeader(4);
            packer.packString(col.name());
            packer.packInt(getClientDataType(col.type().spec()));
            packer.packBoolean(schema.isKeyColumn(colIdx));
            packer.packBoolean(col.nullable());
        }
    }

    /**
     * Writes a tuple.
     *
     * @param packer Packer.
     * @param tuple Tuple.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuple(ClientMessagePacker packer, Tuple tuple) {
        try {
            if (tuple == null) {
                packer.packNil();

                return;
            }

            var schema = ((SchemaAware) tuple).schema();

            packer.packInt(schema.version());

            for (var col : schema.keyColumns().columns())
                writeColumnValue(packer, tuple, col);

            for (var col : schema.valueColumns().columns())
                writeColumnValue(packer, tuple, col);
        }
        catch (Throwable t) {
            throw new IgniteException("Failed to serialize tuple", t);
        }
    }

    /**
     * Reads a tuple.
     *
     * @param unpacker Unpacker.
     * @param table Table.
     * @param keyOnly Whether only key fields are expected.
     * @return Tuple.
     * @throws IOException When deserialization fails.
     */
    public static Tuple readTuple(ClientMessageUnpacker unpacker, TableImpl table, boolean keyOnly) throws IOException {
        var schemaId = unpacker.unpackInt();
        var schema = table.schemaView().schema(schemaId);
        var builder = table.tupleBuilder();

        var cnt = keyOnly ? schema.keyColumns().length() : schema.length();

        for (int i = 0; i < cnt; i++) {
            if (unpacker.getNextFormat() == MessageFormat.NIL) {
                unpacker.skipValue();
                continue;
            }

            readAndSetColumnValue(unpacker, builder, schema.column(i));
        }

        return builder.build();
    }

    /**
     * Reads a tuple as a map, without schema.
     *
     * @param unpacker Unpacker.
     * @param table Table.
     * @return Tuple.
     * @throws IOException When deserialization fails.
     */
    public static Tuple readTupleSchemaless(ClientMessageUnpacker unpacker, TableImpl table) throws IOException {
        var cnt = unpacker.unpackMapHeader();
        var builder = table.tupleBuilder();

        for (int i = 0; i < cnt; i++) {
            var colName = unpacker.unpackString();

            builder.set(colName, unpacker.unpackValue());
        }

        return builder.build();
    }

    /**
     * Reads a table.
     *
     * @param unpacker Unpacker.
     * @param tables Ignite tables.
     * @return Table.
     * @throws IOException When deserialization fails.
     */
    public static TableImpl readTable(ClientMessageUnpacker unpacker, IgniteTables tables) throws IOException {
        var tableId = unpacker.unpackUuid();

        return ((IgniteTablesInternal)tables).table(tableId);
    }

    private static void readAndSetColumnValue(ClientMessageUnpacker unpacker, TupleBuilder builder, Column col)
            throws IOException {
        builder.set(col.name(), unpacker.unpackObject(getClientDataType(col.type().spec())));
    }

    private static int getClientDataType(NativeTypeSpec spec) {
        switch (spec) {
            case INT8:
                return ClientDataType.INT8;

            case INT16:
                return ClientDataType.INT16;

            case INT32:
                return ClientDataType.INT32;

            case INT64:
                return ClientDataType.INT64;

            case FLOAT:
                return ClientDataType.FLOAT;

            case DOUBLE:
                return ClientDataType.DOUBLE;

            case DECIMAL:
                return ClientDataType.DECIMAL;

            case UUID:
                return ClientDataType.UUID;

            case STRING:
                return ClientDataType.STRING;

            case BYTES:
                return ClientDataType.BYTES;

            case BITMASK:
                return ClientDataType.BITMASK;
        }

        throw new IgniteException("Unsupported native type: " + spec);
    }

    private static void writeColumnValue(ClientMessagePacker packer, Tuple tuple, Column col) throws IOException {
        var val = tuple.value(col.name());

        if (val == null) {
            packer.packNil();
            return;
        }

        switch (col.type().spec()) {
            case INT8:
                packer.packByte((byte) val);
                break;

            case INT16:
                packer.packShort((short) val);
                break;

            case INT32:
                packer.packInt((int) val);
                break;

            case INT64:
                packer.packLong((long) val);
                break;

            case FLOAT:
                packer.packFloat((float) val);
                break;

            case DOUBLE:
                packer.packDouble((double) val);
                break;

            case DECIMAL:
                packer.packDecimal((BigDecimal) val);
                break;

            case UUID:
                packer.packUuid((UUID) val);
                break;

            case STRING:
                packer.packString((String) val);
                break;

            case BYTES:
                byte[] bytes = (byte[]) val;
                packer.packBinaryHeader(bytes.length);
                packer.writePayload(bytes);
                break;

            case BITMASK:
                packer.packBitSet((BitSet)val);
                break;

            default:
                throw new IgniteException("Data type not supported: " + col.type());
        }
    }
}
