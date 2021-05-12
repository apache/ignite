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

package org.apache.ignite.network.internal.netty;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.ignite.network.internal.AllTypesMessage;
import org.apache.ignite.network.internal.AllTypesMessageGenerator;
import org.apache.ignite.network.internal.AllTypesMessageSerializationFactory;
import org.apache.ignite.network.internal.direct.DirectMessageWriter;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.MessageSerializer;
import org.apache.ignite.network.message.NetworkMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link InboundDecoder}.
 */
public class InboundDecoderTest {
    /** {@link ByteBuf} allocator. */
    private final UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    /**
     * Tests that an {@link InboundDecoder} can successfully read a message with all types supported
     * by direct marshalling.
     *
     * @param seed Random seed.
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @MethodSource("messageGenerationSeed")
    public void testAllTypes(long seed) throws Exception {
        var registry = new MessageSerializationRegistry();

        AllTypesMessage msg = AllTypesMessageGenerator.generate(seed, true);

        registry.registerFactory(msg.directType(), new AllTypesMessageSerializationFactory());

        var channel = new EmbeddedChannel(new InboundDecoder(registry));

        var writer = new DirectMessageWriter(registry, ConnectionManager.DIRECT_PROTOCOL_VERSION);

        MessageSerializer<NetworkMessage> serializer = registry.createSerializer(msg.directType());

        ByteBuffer buf = ByteBuffer.allocate(10_000);

        AllTypesMessage output;

        do {
            buf.clear();

            writer.setBuffer(buf);

            serializer.writeMessage(msg, writer);

            buf.flip();

            ByteBuf buffer = allocator.buffer(buf.limit());

            buffer.writeBytes(buf);

            channel.writeInbound(buffer);
        } while ((output = channel.readInbound()) == null);

        assertEquals(msg, output);
    }

    /**
     * Tests that an {@link InboundDecoder} doesn't hang if it encounters a byte buffer with only partially written
     * header.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPartialHeader() throws Exception {
        var registry = new MessageSerializationRegistry();

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
     * Source of parameters for the {@link #test(long)} method.
     * Creates seeds for a {@link AllTypesMessage} generation.
     * @return Random seeds.
     */
    private static LongStream messageGenerationSeed() {
        var random = new Random();
        return IntStream.range(0, 100).mapToLong(ignored -> random.nextLong());
    }
}
