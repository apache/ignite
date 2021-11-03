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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.requests.sql.ClientSqlCloseRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlColumnMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteBatchRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlExecuteRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlFetchRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlPrimaryKeyMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlQueryMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlSchemasMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.ClientSqlTableMetadataRequest;
import org.apache.ignite.client.handler.requests.sql.JdbcMetadataCatalog;
import org.apache.ignite.client.handler.requests.table.ClientSchemasGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableDropRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTablesGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleContainsKeyRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteAllExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleDeleteRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndDeleteRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndReplaceRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndReplaceSchemalessRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndUpsertRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetAndUpsertSchemalessRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleInsertAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleInsertAllSchemalessRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleInsertRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleInsertSchemalessRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleReplaceExactRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleReplaceExactSchemalessRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleReplaceRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleReplaceSchemalessRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertAllRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertAllSchemalessRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertSchemalessRequest;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.internal.client.proto.ClientErrorCode;
import org.apache.ignite.internal.client.proto.ClientMessageCommon;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.internal.client.proto.ServerMessageType;
import org.apache.ignite.internal.processors.query.calcite.QueryProcessor;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.table.manager.IgniteTables;

/**
 * Handles messages from thin clients.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ClientInboundMessageHandler extends ChannelInboundHandlerAdapter {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ClientInboundMessageHandler.class);

    /** Ignite tables API. */
    private final IgniteTables igniteTables;

    /** Context. */
    private ClientContext clientContext;

    /** Handler. */
    private final JdbcQueryEventHandler handler;

    /**
     * Constructor.
     *
     * @param igniteTables Ignite tables API entry point.
     * @param processor    Sql query processor.
     */
    public ClientInboundMessageHandler(IgniteTables igniteTables,
            QueryProcessor processor) {
        assert igniteTables != null;

        this.igniteTables = igniteTables;

        this.handler = new JdbcQueryEventHandlerImpl(processor, new JdbcMetadataCatalog(igniteTables));
    }

    /** {@inheritDoc} */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Each inbound handler in a pipeline has to release the received messages.
        try (var unpacker = getUnpacker((ByteBuf) msg)) {
            // Packer buffer is released by Netty on send, or by inner exception handlers below.
            var packer = getPacker(ctx.alloc());

            if (clientContext == null) {
                handshake(ctx, unpacker, packer);
            } else {
                processOperation(ctx, unpacker, packer);
            }
        }
    }

    private void handshake(ChannelHandlerContext ctx, ClientMessageUnpacker unpacker, ClientMessagePacker packer) {
        try {
            writeMagic(ctx);
            var clientVer = ProtocolVersion.unpack(unpacker);

            if (!clientVer.equals(ProtocolVersion.LATEST_VER)) {
                throw new IgniteException("Unsupported version: "
                        + clientVer.major() + "." + clientVer.minor() + "." + clientVer.patch());
            }

            var clientCode = unpacker.unpackInt();
            var featuresLen = unpacker.unpackBinaryHeader();
            var features = BitSet.valueOf(unpacker.readPayload(featuresLen));

            clientContext = new ClientContext(clientVer, clientCode, features);

            LOG.debug("Handshake: " + clientContext);

            var extensionsLen = unpacker.unpackMapHeader();
            unpacker.skipValue(extensionsLen);

            // Response.
            ProtocolVersion.LATEST_VER.pack(packer);

            packer.packInt(ClientErrorCode.SUCCESS)
                    .packBinaryHeader(0) // Features.
                    .packMapHeader(0); // Extensions.

            write(packer, ctx);
        } catch (Throwable t) {
            packer.close();

            var errPacker = getPacker(ctx.alloc());

            try {
                ProtocolVersion.LATEST_VER.pack(errPacker);

                String message = t.getMessage();

                if (message == null) {
                    message = t.getClass().getName();
                }

                errPacker.packInt(ClientErrorCode.FAILED).packString(message);

                write(errPacker, ctx);
            } catch (Throwable t2) {
                errPacker.close();
                exceptionCaught(ctx, t2);
            }
        }
    }

    private void writeMagic(ChannelHandlerContext ctx) {
        ctx.write(Unpooled.wrappedBuffer(ClientMessageCommon.MAGIC_BYTES));
    }

    private void write(ClientMessagePacker packer, ChannelHandlerContext ctx) {
        var buf = packer.getBuffer();

        // writeAndFlush releases pooled buffer.
        ctx.writeAndFlush(buf);
    }

    private void writeError(long requestId, Throwable err, ChannelHandlerContext ctx) {
        var packer = getPacker(ctx.alloc());

        try {
            assert err != null;

            packer.packInt(ServerMessageType.RESPONSE);
            packer.packLong(requestId);
            packer.packInt(ClientErrorCode.FAILED);

            String msg = err.getMessage();

            if (msg == null) {
                msg = err.getClass().getName();
            }

            packer.packString(msg);

            write(packer, ctx);
        } catch (Throwable t) {
            packer.close();
            exceptionCaught(ctx, t);
        }
    }

    private ClientMessagePacker getPacker(ByteBufAllocator alloc) {
        // Outgoing messages are released on write.
        return new ClientMessagePacker(alloc.buffer());
    }

    private ClientMessageUnpacker getUnpacker(ByteBuf buf) {
        return new ClientMessageUnpacker(buf);
    }

    private void processOperation(ChannelHandlerContext ctx, ClientMessageUnpacker in, ClientMessagePacker out) {
        long requestId = -1;

        try {
            var opCode = in.unpackInt();
            requestId = in.unpackLong();

            out.packInt(ServerMessageType.RESPONSE)
                    .packLong(requestId)
                    .packInt(ClientErrorCode.SUCCESS);

            var fut = processOperation(in, out, opCode);

            if (fut == null) {
                // Operation completed synchronously.
                write(out, ctx);
            } else {
                var reqId = requestId;

                fut.whenComplete((Object res, Object err) -> {
                    if (err != null) {
                        out.close();
                        writeError(reqId, (Throwable) err, ctx);
                    } else {
                        write(out, ctx);
                    }
                });
            }
        } catch (Throwable t) {
            out.close();

            writeError(requestId, t, ctx);
        }
    }

    private CompletableFuture processOperation(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            int opCode
    ) {
        switch (opCode) {
            case ClientOp.TABLE_DROP:
                return ClientTableDropRequest.process(in, igniteTables);

            case ClientOp.TABLES_GET:
                return ClientTablesGetRequest.process(out, igniteTables);

            case ClientOp.SCHEMAS_GET:
                return ClientSchemasGetRequest.process(in, out, igniteTables);

            case ClientOp.TABLE_GET:
                return ClientTableGetRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_UPSERT:
                return ClientTupleUpsertRequest.process(in, igniteTables);

            case ClientOp.TUPLE_UPSERT_SCHEMALESS:
                return ClientTupleUpsertSchemalessRequest.process(in, igniteTables);

            case ClientOp.TUPLE_GET:
                return ClientTupleGetRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_UPSERT_ALL:
                return ClientTupleUpsertAllRequest.process(in, igniteTables);

            case ClientOp.TUPLE_UPSERT_ALL_SCHEMALESS:
                return ClientTupleUpsertAllSchemalessRequest.process(in, igniteTables);

            case ClientOp.TUPLE_GET_ALL:
                return ClientTupleGetAllRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_GET_AND_UPSERT:
                return ClientTupleGetAndUpsertRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_GET_AND_UPSERT_SCHEMALESS:
                return ClientTupleGetAndUpsertSchemalessRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_INSERT:
                return ClientTupleInsertRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_INSERT_SCHEMALESS:
                return ClientTupleInsertSchemalessRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_INSERT_ALL:
                return ClientTupleInsertAllRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_INSERT_ALL_SCHEMALESS:
                return ClientTupleInsertAllSchemalessRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_REPLACE:
                return ClientTupleReplaceRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_REPLACE_SCHEMALESS:
                return ClientTupleReplaceSchemalessRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_REPLACE_EXACT:
                return ClientTupleReplaceExactRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_REPLACE_EXACT_SCHEMALESS:
                return ClientTupleReplaceExactSchemalessRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_GET_AND_REPLACE:
                return ClientTupleGetAndReplaceRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_GET_AND_REPLACE_SCHEMALESS:
                return ClientTupleGetAndReplaceSchemalessRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_DELETE:
                return ClientTupleDeleteRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_DELETE_ALL:
                return ClientTupleDeleteAllRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_DELETE_EXACT:
                return ClientTupleDeleteExactRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_DELETE_ALL_EXACT:
                return ClientTupleDeleteAllExactRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_GET_AND_DELETE:
                return ClientTupleGetAndDeleteRequest.process(in, out, igniteTables);

            case ClientOp.TUPLE_CONTAINS_KEY:
                return ClientTupleContainsKeyRequest.process(in, out, igniteTables);

            case ClientOp.SQL_EXEC:
                return ClientSqlExecuteRequest.execute(in, out, handler);

            case ClientOp.SQL_EXEC_BATCH:
                return ClientSqlExecuteBatchRequest.process(in, out, handler);

            case ClientOp.SQL_NEXT:
                return ClientSqlFetchRequest.process(in, out, handler);

            case ClientOp.SQL_CURSOR_CLOSE:
                return ClientSqlCloseRequest.process(in, out, handler);

            case ClientOp.SQL_TABLE_META:
                return ClientSqlTableMetadataRequest.process(in, out, handler);

            case ClientOp.SQL_COLUMN_META:
                return ClientSqlColumnMetadataRequest.process(in, out, handler);

            case ClientOp.SQL_SCHEMAS_META:
                return ClientSqlSchemasMetadataRequest.process(in, out, handler);

            case ClientOp.SQL_PK_META:
                return ClientSqlPrimaryKeyMetadataRequest.process(in, out, handler);

            case ClientOp.SQL_QUERY_META:
                return ClientSqlQueryMetadataRequest.process(in, out, handler);

            default:
                throw new IgniteException("Unexpected operation code: " + opCode);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    /** {@inheritDoc} */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOG.error(cause.getMessage(), cause);

        ctx.close();
    }
}
