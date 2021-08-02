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
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.client.handler.requests.table.ClientSchemasGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableDropRequest;
import org.apache.ignite.client.handler.requests.table.ClientTableGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTablesGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleGetRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertRequest;
import org.apache.ignite.client.handler.requests.table.ClientTupleUpsertSchemalessRequest;
import org.apache.ignite.client.proto.ClientErrorCode;
import org.apache.ignite.client.proto.ClientMessageCommon;
import org.apache.ignite.client.proto.ClientMessagePacker;
import org.apache.ignite.client.proto.ClientMessageUnpacker;
import org.apache.ignite.client.proto.ClientOp;
import org.apache.ignite.client.proto.ProtocolVersion;
import org.apache.ignite.client.proto.ServerMessageType;
import org.apache.ignite.lang.IgniteException;
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
    @Override public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Each inbound handler in a pipeline has to release the received messages.
        try (var unpacker = getUnpacker((ByteBuf) msg)) {
            // Packer buffer is released by Netty on send, or by inner exception handlers below.
            var packer = getPacker(ctx.alloc());

            if (clientContext == null)
                handshake(ctx, unpacker, packer);
            else
                processOperation(ctx, unpacker, packer);
        }
    }

    private void handshake(ChannelHandlerContext ctx, ClientMessageUnpacker unpacker, ClientMessagePacker packer) {
        try {
            writeMagic(ctx);
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
            packer.close();

            var errPacker = getPacker(ctx.alloc());

            try {
                ProtocolVersion.LATEST_VER.pack(errPacker);
                errPacker.packInt(ClientErrorCode.FAILED).packString(t.getMessage());

                write(errPacker, ctx);
            }
            catch (Throwable t2) {
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

            if (msg == null)
                msg = err.getClass().getName();

            packer.packString(msg);

            write(packer, ctx);
        }
        catch (Throwable t) {
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
            }
            else {
                var reqId = requestId;

                fut.whenComplete((Object res, Object err) -> {
                    if (err != null) {
                        out.close();
                        writeError(reqId, (Throwable) err, ctx);
                    } else
                        write(out, ctx);
                });
            }
        }
        catch (Throwable t) {
            out.close();

            writeError(requestId, t, ctx);
        }
    }

    private CompletableFuture processOperation(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            int opCode
    ) throws IOException {
        // TODO: Handle all operations asynchronously (add async table API).
        switch (opCode) {
            case ClientOp.TABLE_DROP:
                return ClientTableDropRequest.process(in, ignite.tables());

            case ClientOp.TABLES_GET:
                return ClientTablesGetRequest.process(out, ignite.tables());

            case ClientOp.SCHEMAS_GET:
                return ClientSchemasGetRequest.process(in, out, ignite.tables());

            case ClientOp.TABLE_GET:
                return ClientTableGetRequest.process(in, out, ignite.tables());

            case ClientOp.TUPLE_UPSERT:
                return ClientTupleUpsertRequest.process(in, ignite.tables());

            case ClientOp.TUPLE_UPSERT_SCHEMALESS:
                return ClientTupleUpsertSchemalessRequest.process(in, ignite.tables());

            case ClientOp.TUPLE_GET:
                return ClientTupleGetRequest.process(in, out, ignite.tables());

            default:
                throw new IgniteException("Unexpected operation code: " + opCode);
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
