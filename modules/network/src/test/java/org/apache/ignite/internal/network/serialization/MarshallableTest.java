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

package org.apache.ignite.internal.network.serialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.embedded.EmbeddedChannel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import org.apache.ignite.internal.network.direct.DirectMessageWriter;
import org.apache.ignite.internal.network.netty.ConnectionManager;
import org.apache.ignite.internal.network.netty.InboundDecoder;
import org.apache.ignite.internal.network.serialization.marshal.MarshalException;
import org.apache.ignite.internal.network.serialization.marshal.MarshalledObject;
import org.apache.ignite.internal.network.serialization.marshal.UserObjectMarshaller;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TestMessageSerializationRegistryImpl;
import org.apache.ignite.network.TestMessagesFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests marshallable serialization.
 */
public class MarshallableTest {
    /** {@link ByteBuf} allocator. */
    private final UnpooledByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

    /** Registry. */
    private final MessageSerializationRegistry registry = new TestMessageSerializationRegistryImpl();

    private final TestMessagesFactory msgFactory = new TestMessagesFactory();

    /**
     * Tests that marshallable object can be serialized along with its descriptor.
     */
    @Test
    public void testMarshallable() {
        // Test map that will be sent as a Marshallable object within the MessageWithMarshallable message
        Map<String, SimpleSerializableObject> testMap = Map.of("test", new SimpleSerializableObject(10));

        ByteBuffer outBuffer = write(testMap);

        Map<String, SimpleSerializableObject> received = read(outBuffer);

        assertEquals(testMap, received);
    }

    /** Writes a map to a buffer through the {@link MessageWithMarshallable}. */
    private ByteBuffer write(Map<String, SimpleSerializableObject> testMap) {
        var serializers = new Serialization();

        var writer = new DirectMessageWriter(serializers.perSessionSerializationService, ConnectionManager.DIRECT_PROTOCOL_VERSION);

        MessageWithMarshallable msg = msgFactory.messageWithMarshallable().marshallableMap(testMap).build();

        MessageSerializer<NetworkMessage> serializer = registry.createSerializer(msg.groupType(), msg.messageType());

        ByteBuffer nioBuffer = ByteBuffer.allocate(1000);

        writer.setBuffer(nioBuffer);

        // Write a message to the ByteBuffer.
        boolean fullyWritten = serializer.writeMessage(msg, writer);

        assertTrue(fullyWritten);

        return nioBuffer;
    }

    /** Reads a {@link MessageWithMarshallable} from the buffer (byte by byte) and checks for the class descriptor merging. */
    private Map<String, SimpleSerializableObject> read(ByteBuffer outBuffer) {
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);

        var channel = new EmbeddedChannel();

        doReturn(channel).when(ctx).channel();

        var serializers = new Serialization();

        PerSessionSerializationService perSessionSerializationService = serializers.perSessionSerializationService;
        ClassDescriptor descriptor = serializers.descriptor;

        final var decoder = new InboundDecoder(perSessionSerializationService);

        int size = outBuffer.position();

        outBuffer.flip();

        ByteBuf inBuffer = allocator.buffer();

        // List that holds decoded object
        final var list = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            // Write bytes to a decoding buffer one by one
            inBuffer.writeByte(outBuffer.get());

            decoder.decode(ctx, inBuffer, list);

            if (i < size - 1) {
                // Any time before the buffer is fully read, message object should not be decoded
                assertThat(list, is(empty()));
            }
        }

        // Buffer is fully read, message object should be decoded
        assertThat(list, hasSize(1));

        // Check that the descriptor of the SimpleSerializableObject was received
        Map<Integer, ClassDescriptor> mergedDescriptors = perSessionSerializationService.getDescriptorMapView();
        assertEquals(1, mergedDescriptors.size());

        ClassDescriptor mergedDescriptor = mergedDescriptors.values().stream().findFirst().get();

        assertEquals(descriptor.className(), mergedDescriptor.className());

        MessageWithMarshallable received = (MessageWithMarshallable) list.get(0);

        return received.marshallableMap();
    }

    /** Helper class that holds classes needed for serialization. */
    private class Serialization {
        private final PerSessionSerializationService perSessionSerializationService;

        private final ClassDescriptor descriptor;

        Serialization() {
            var descriptorRegistry = new ClassDescriptorRegistry();
            var factory = new ClassDescriptorFactory(descriptorRegistry);

            // Create descriptor for SimpleSerializableObject
            this.descriptor = factory.create(SimpleSerializableObject.class);

            var userObjectSerializer = new StubMarshaller(descriptor);

            var ser = new UserObjectSerializationContext(descriptorRegistry, factory, userObjectSerializer);

            var serializationService = new SerializationService(registry, ser);
            this.perSessionSerializationService = new PerSessionSerializationService(serializationService);
        }
    }

    /**
     *  Stub implementation of the {@link UserObjectMarshaller}, which uses the JDK's serializable
     *  serialization to actually marshall an object.
     */
    private static class StubMarshaller implements UserObjectMarshaller {

        private final ClassDescriptor descriptor;

        StubMarshaller(ClassDescriptor descriptor) {
            this.descriptor = descriptor;
        }

        @Override
        public MarshalledObject marshal(@Nullable Object object) throws MarshalException {
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(object);
                oos.close();
                return new MarshalledObject(baos.toByteArray(), Collections.singleton(descriptor));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T> @Nullable T unmarshal(byte[] bytes, DescriptorRegistry mergedDescriptors) {
            try (var bais = new ByteArrayInputStream(bytes); var ois = new ObjectInputStream(bais)) {
                return (T) ois.readObject();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
