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

package org.apache.ignite.client.handler;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.proto.ClientDataType;
import org.apache.ignite.client.proto.ClientErrorCode;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.client.proto.ProtocolVersion;
import org.apache.ignite.client.proto.ServerMessageType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TupleBuilderImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.buffer.ByteBufferInput;
import org.slf4j.Logger;

/**
 * Handles messages from thin clients.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientInboundMessageHandler extends ChannelInboundHandlerAdapter {
    /** Logger. */
    private final Logger log;

    /** API entry point. */
    private final Ignite ignite;

    /** Context. */
    private ClientContext clientContext;

    /**
     * Constructor.
     *
     * @param ignite Ignite API entry point.
     * @param log Logger.
     */
    public ClientInboundMessageHandler(Ignite ignite, Logger log) {
        assert ignite != null;
        assert log != null;

        this.ignite = ignite;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) throws IOException {
        var buf = (ByteBuffer) msg;

        var unpacker = getUnpacker(buf);
        var packer = getPacker();

        if (clientContext == null)
            handshake(ctx, unpacker, packer);
        else
            processOperation(ctx, unpacker, packer);
    }

    private void handshake(ChannelHandlerContext ctx, ClientMessageUnpacker unpacker, ClientMessagePacker packer)
            throws IOException {
        try {
            var clientVer = ProtocolVersion.unpack(unpacker);

            if (!clientVer.equals(ProtocolVersion.LATEST_VER))
                throw new IgniteException("Unsupported version: " +
                        clientVer.major() + "." + clientVer.minor() + "." + clientVer.patch());

            var clientCode = unpacker.unpackInt();
            var featuresLen = unpacker.unpackBinaryHeader();
            var features = BitSet.valueOf(unpacker.readPayload(featuresLen));

            clientContext = new ClientContext(clientVer, clientCode, features);

            log.debug("Handshake: " + clientContext);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValue(extensionsLen);

            // Response.
            ProtocolVersion.LATEST_VER.pack(packer);

            packer.packInt(ClientErrorCode.SUCCESS)
                    .packBinaryHeader(0) // Features.
                    .packMapHeader(0); // Extensions.

            write(packer, ctx);
        }
        catch (Throwable t) {
            packer = getPacker();

            ProtocolVersion.LATEST_VER.pack(packer);
            packer.packInt(ClientErrorCode.FAILED).packString(t.getMessage());

            write(packer, ctx);
        }
    }

    private void write(ClientMessagePacker packer, ChannelHandlerContext ctx) {
        var buf = packer.toMessageBuffer().sliceAsByteBuffer();

        ctx.writeAndFlush(buf);
    }

    private void writeError(int requestId, Throwable err, ChannelHandlerContext ctx) {
        try {
            assert err != null;

            ClientMessagePacker packer = getPacker();
            packer.packInt(ServerMessageType.RESPONSE);
            packer.packInt(requestId);
            packer.packInt(ClientErrorCode.FAILED);

            String msg = err.getMessage();

            if (msg == null)
                msg = err.getClass().getName();

            packer.packString(msg);

            write(packer, ctx);
        }
        catch (Throwable t) {
            exceptionCaught(ctx, t);
        }
    }

    private ClientMessagePacker getPacker() {
        return new ClientMessagePacker();
    }

    private ClientMessageUnpacker getUnpacker(ByteBuffer buf) {
        return new ClientMessageUnpacker(new ByteBufferInput(buf));
    }

    private void processOperation(ChannelHandlerContext ctx, ClientMessageUnpacker unpacker, ClientMessagePacker packer) throws IOException {
        var opCode = unpacker.unpackInt();
        var requestId = unpacker.unpackInt();

        packer.packInt(ServerMessageType.RESPONSE)
                .packInt(requestId)
                .packInt(ClientErrorCode.SUCCESS);

        try {
            var fut = processOperation(unpacker, packer, opCode);

            if (fut == null) {
                // Operation completed synchronously.
                write(packer, ctx);
            }
            else {
                fut.whenComplete((Object res, Object err) -> {
                    if (err != null)
                        writeError(requestId, (Throwable) err, ctx);
                    else
                        write(packer, ctx);
                });
            }

        }
        catch (Throwable t) {
            writeError(requestId, t, ctx);
        }
    }

    private CompletableFuture processOperation(ClientMessageUnpacker unpacker, ClientMessagePacker packer, int opCode)
            throws IOException {
        // TODO: Handle all operations asynchronously (add async table API).
        switch (opCode) {
            case ClientOp.TABLE_DROP: {
                var tableName = unpacker.unpackString();

                ignite.tables().dropTable(tableName);

                break;
            }

            case ClientOp.TABLES_GET: {
                List<Table> tables = ignite.tables().tables();

                packer.packMapHeader(tables.size());

                for (var table : tables) {
                    var tableImpl = (TableImpl) table;

                    packer.packUuid(tableImpl.tableId());
                    packer.packString(table.tableName());
                }

                break;
            }

            case ClientOp.SCHEMAS_GET: {
                var table = readTable(unpacker);

                if (unpacker.getNextFormat() == MessageFormat.NIL) {
                    // Return the latest schema.
                    packer.packMapHeader(1);

                    var schema = table.schemaView().schema();

                    if (schema == null)
                        throw new IgniteException("Schema registry is not initialized.");

                    writeSchema(packer, schema.version(), schema);
                }
                else {
                    var cnt = unpacker.unpackArrayHeader();
                    packer.packMapHeader(cnt);

                    for (var i = 0; i < cnt; i++) {
                        var schemaVer = unpacker.unpackInt();
                        var schema = table.schemaView().schema(schemaVer);
                        writeSchema(packer, schemaVer, schema);
                    }
                }

                break;
            }

            case ClientOp.TABLE_GET: {
                String tableName = unpacker.unpackString();
                Table table = ignite.tables().table(tableName);

                if (table == null)
                    packer.packNil();
                else
                    packer.packUuid(((TableImpl) table).tableId());

                break;
            }

            case ClientOp.TUPLE_UPSERT: {
                var table = readTable(unpacker);
                var tuple = readTuple(unpacker, table, false);

                return table.upsertAsync(tuple);
            }

            case ClientOp.TUPLE_UPSERT_SCHEMALESS: {
                var table = readTable(unpacker);
                var tuple = readTupleSchemaless(unpacker, table);

                return table.upsertAsync(tuple);
            }

            case ClientOp.TUPLE_GET: {
                var table = readTable(unpacker);
                var keyTuple = readTuple(unpacker, table, true);

                return table.getAsync(keyTuple).thenAccept(t -> writeTuple(packer, t));
            }

            default:
                throw new IgniteException("Unexpected operation code: " + opCode);
        }

        return null;
    }

    private void writeSchema(ClientMessagePacker packer, int schemaVer, SchemaDescriptor schema) throws IOException {
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

    private void writeTuple(ClientMessagePacker packer, Tuple tuple) {
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

    private Tuple readTuple(ClientMessageUnpacker unpacker, TableImpl table, boolean keyOnly) throws IOException {
        var schemaId = unpacker.unpackInt();
        var schema = table.schemaView().schema(schemaId);
        var builder = (TupleBuilderImpl) table.tupleBuilder();

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

    private Tuple readTupleSchemaless(ClientMessageUnpacker unpacker, TableImpl table) throws IOException {
        var cnt = unpacker.unpackMapHeader();
        var builder = table.tupleBuilder();

        for (int i = 0; i < cnt; i++) {
            var colName = unpacker.unpackString();

            builder.set(colName, unpacker.unpackValue());
        }

        return builder.build();
    }

    private TableImpl readTable(ClientMessageUnpacker unpacker) throws IOException {
        var tableId = unpacker.unpackUuid();

        return ((IgniteTablesInternal)ignite.tables()).table(tableId);
    }

    private void readAndSetColumnValue(ClientMessageUnpacker unpacker, TupleBuilderImpl builder, Column col)
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

    private void writeColumnValue(ClientMessagePacker packer, Tuple tuple, Column col) throws IOException {
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

    /** {@inheritDoc} */
    @Override public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    /** {@inheritDoc} */
    @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage(), cause);

        ctx.close();
    }
}
