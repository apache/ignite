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

package org.apache.ignite.internal.network.netty;

import java.nio.ByteBuffer;
import java.util.List;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.stream.ChunkedInput;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;

/**
 * An encoder for the outbound messages that uses {@link DirectMessageWriter}.
 */
public class OutboundEncoder extends MessageToMessageEncoder<NetworkMessage> {
    /** Serialization registry. */
    private final MessageSerializationRegistry serializationRegistry;

    /**
     * Constructor.
     *
     * @param registry Serialization registry.
     */
    public OutboundEncoder(MessageSerializationRegistry registry) {
        serializationRegistry = registry;
    }

    /** {@inheritDoc} */
    @Override protected void encode(ChannelHandlerContext ctx, NetworkMessage msg, List<Object> out) throws Exception {
        MessageSerializer<NetworkMessage> serializer =
            serializationRegistry.createSerializer(msg.groupType(), msg.messageType());

        out.add(new NetworkMessageChunkedInput(msg, serializer, serializationRegistry));
    }

    /**
     * Chunked input for network message.
     */
    private static class NetworkMessageChunkedInput implements ChunkedInput<ByteBuf> {
        /** Network message. */
        private final NetworkMessage msg;

        /** Message serializer. */
        private final MessageSerializer<NetworkMessage> serializer;

        /** Message writer. */
        private final DirectMessageWriter writer;

        /** Whether the message was fully written. */
        private boolean finished = false;

        /**
         * Constructor.
         *
         * @param msg Network message.
         * @param serializer Serializer.
         */
        private NetworkMessageChunkedInput(
            NetworkMessage msg,
            MessageSerializer<NetworkMessage> serializer,
            MessageSerializationRegistry registry
        ) {
            this.msg = msg;
            this.serializer = serializer;
            this.writer = new DirectMessageWriter(registry, ConnectionManager.DIRECT_PROTOCOL_VERSION);
        }

        /** {@inheritDoc} */
        @Override public boolean isEndOfInput() throws Exception {
            return finished;
        }

        /** {@inheritDoc} */
        @Override public void close() throws Exception {

        }

        /** {@inheritDoc} */
        @Deprecated
        @Override public ByteBuf readChunk(ChannelHandlerContext ctx) throws Exception {
            return readChunk(ctx.alloc());
        }

        /** {@inheritDoc} */
        @Override public ByteBuf readChunk(ByteBufAllocator allocator) throws Exception {
            ByteBuf buffer = allocator.ioBuffer();
            int capacity = buffer.capacity();

            ByteBuffer byteBuffer = buffer.internalNioBuffer(0, capacity);

            int initialPosition = byteBuffer.position();

            writer.setBuffer(byteBuffer);

            finished = serializer.writeMessage(msg, writer);

            buffer.writerIndex(byteBuffer.position() - initialPosition);

            return buffer;
        }

        /** {@inheritDoc} */
        @Override public long length() {
            // Return negative values, because object's size is unknown.
            return -1;
        }

        /** {@inheritDoc} */
        @Override public long progress() {
            // Not really needed, as there won't be listeners for the write operation's progress.
            return 0;
        }
    }
}
