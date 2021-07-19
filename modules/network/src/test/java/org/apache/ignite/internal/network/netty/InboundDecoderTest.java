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
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.ignite.internal.network.AllTypesMessage;
import org.apache.ignite.internal.network.AllTypesMessageGenerator;
import org.apache.ignite.internal.network.NestedMessageMessage;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TestMessage;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.TestMessagesFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link InboundDecoder}.
 */
public class InboundDecoderTest {
    /** {@link ByteBuf} allocator. */
    private final UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    /** Registry. */
    private final MessageSerializationRegistry registry = new TestMessageSerializationRegistryImpl();

    /**
     * Tests that an {@link InboundDecoder} can successfully read a message with all types supported
     * by direct marshalling.
     *
     * @param seed Random seed.
     */
    @ParameterizedTest
    @MethodSource("messageGenerationSeed")
    public void testAllTypes(long seed) {
        AllTypesMessage msg = AllTypesMessageGenerator.generate(seed, true);

        AllTypesMessage received = sendAndReceive(msg);

        assertEquals(msg, received);
    }

    /**
     * Tests that the {@link InboundDecoder} is able to transfer a message with a nested {@code null} message
     * (happy case is tested by {@link #testAllTypes}).
     */
    @Test
    public void testNullNestedMessage() {
        NestedMessageMessage msg = new TestMessagesFactory().nestedMessageMessage()
            .nestedMessage(null)
            .build();

        NestedMessageMessage received = sendAndReceive(msg);

        assertNull(received.nestedMessage());
    }

    /**
     * Serializes and then deserializes the given message.
     */
    private <T extends NetworkMessage> T sendAndReceive(T msg) {
        var channel = new EmbeddedChannel(new InboundDecoder(registry));

        var writer = new DirectMessageWriter(registry, ConnectionManager.DIRECT_PROTOCOL_VERSION);

        MessageSerializer<NetworkMessage> serializer = registry.createSerializer(msg.groupType(), msg.messageType());

        ByteBuffer buf = ByteBuffer.allocate(10_000);

        T received;

        do {
            buf.clear();

            writer.setBuffer(buf);

            serializer.writeMessage(msg, writer);

            buf.flip();

            ByteBuf buffer = allocator.buffer(buf.limit());

            buffer.writeBytes(buf);

            channel.writeInbound(buffer);
        } while ((received = channel.readInbound()) == null);

        return received;
    }

    /**
     * Tests that an {@link InboundDecoder} doesn't hang if it encounters a byte buffer with only partially written
     * header.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartialHeader() throws Exception {
        var channel = new EmbeddedChannel(new InboundDecoder(registry));

        ByteBuf buffer = allocator.buffer();

        buffer.writeByte(1);

        CompletableFuture
            .runAsync(() -> {
                channel.writeInbound(buffer);

                // In case if a header was written partially, readInbound() should not loop forever, as
                // there might not be new data anymore (example: remote host gone offline).
                channel.readInbound();
            })
            .get(3, TimeUnit.SECONDS);
    }

    /**
     * Tests that an {@link InboundDecoder} can handle a {@link ByteBuf} where reader index
     * is not {@code 0} at the start of the {@link InboundDecoder#decode}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartialReadWithReuseBuffer() throws Exception {
        ChannelHandlerContext ctx = Mockito.mock(ChannelHandlerContext.class);

        var channel = new EmbeddedChannel();

        Mockito.doReturn(channel).when(ctx).channel();

        var decoder = new InboundDecoder(registry);

        var list = new ArrayList<>();

        var writer = new DirectMessageWriter(registry, ConnectionManager.DIRECT_PROTOCOL_VERSION);

        var msg = new TestMessagesFactory().testMessage().msg("abcdefghijklmn").build();

        MessageSerializer<NetworkMessage> serializer = registry.createSerializer(msg.groupType(), msg.messageType());

        ByteBuffer nioBuffer = ByteBuffer.allocate(10_000);

        writer.setBuffer(nioBuffer);

        // Write message to the ByteBuffer.
        boolean fullyWritten = serializer.writeMessage(msg, writer);

        assertTrue(fullyWritten);

        nioBuffer.flip();

        ByteBuf buffer = allocator.buffer();

        // Write first 3 bytes of a message.
        for (int i = 0; i < 3; i++)
            buffer.writeByte(nioBuffer.get());

        decoder.decode(ctx, buffer, list);

        // At this point a header and a first byte of a message have been decoded.
        assertEquals(0, list.size());

        // Write next 3 bytes of a message.
        for (int i = 0; i < 3; i++)
            buffer.writeByte(nioBuffer.get());

        // Reader index of a buffer is not zero and it must be handled correctly by the InboundDecoder.
        decoder.decode(ctx, buffer, list);

        // Check if reader index has been tracked correctly.
        assertEquals(6, buffer.readerIndex());

        assertEquals(0, list.size());

        buffer.writeBytes(nioBuffer);

        decoder.decode(ctx, buffer, list);

        assertEquals(1, list.size());

        TestMessage actualMessage = (TestMessage) list.get(0);

        assertEquals(msg, actualMessage);
    }

    /**
     * Source of parameters for the {@link #testAllTypes(long)} method.
     * Creates seeds for a {@link AllTypesMessage} generation.
     * @return Random seeds.
     */
    private static LongStream messageGenerationSeed() {
        var random = new Random();
        return IntStream.range(0, 100).mapToLong(ignored -> random.nextLong());
    }
}
