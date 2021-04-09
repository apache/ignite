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

package org.apache.ignite.network;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.network.internal.MessageSerializerFactory;
import org.apache.ignite.network.internal.direct.DirectMessageReader;
import org.apache.ignite.network.internal.direct.DirectMessageWriter;
import org.apache.ignite.network.message.MessageDeserializer;
import org.apache.ignite.network.message.MessageSerializer;
import org.apache.ignite.network.message.MessageSerializerProvider;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.network.scalecube.TestMessage;
import org.apache.ignite.network.scalecube.TestMessageSerializerProvider;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Direct Message Writing/Reading works. This test won't be needed after we implement Netty Transport
 * for Ignite (IGNITE-14088).
 */
public class DirectSerializationTest {
    /** */
    @Test
    public void test() {
        MessageSerializerProvider[] messageMapperProviders = new MessageSerializerProvider[Short.MAX_VALUE << 1];

        TestMessageSerializerProvider tProv = new TestMessageSerializerProvider();

        messageMapperProviders[TestMessage.TYPE] = tProv;

        MessageSerializerFactory factory = new MessageSerializerFactory(Arrays.asList(messageMapperProviders));

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10_000; i++) {
            sb.append("a");
        }
        String msgStr = sb.toString();

        Map<Integer, String> someMap = new HashMap<>();

        for (int i = 0; i < 26; i++) {
            someMap.put(i, "" + (char) ('a' + i));
        }

        TestMessage message = new TestMessage(msgStr, someMap);
        short directType = message.directType();

        DirectMessageWriter writer = new DirectMessageWriter((byte) 1);
        MessageSerializer<NetworkMessage> serializer = factory.createSerializer(directType);

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(4096);

        boolean writing = true;
        while (writing && byteBuffer.capacity() < 20_000) {
            writer.setBuffer(byteBuffer);
            writing = !serializer.writeMessage(message, writer);

            if (writing) {
                byteBuffer.flip();
                ByteBuffer tmp = ByteBuffer.allocateDirect(byteBuffer.capacity() + 4096);
                tmp.put(byteBuffer);
                byteBuffer = tmp;
            }
        }

        assertFalse(writing);

        byteBuffer.flip();

        DirectMessageReader reader = new DirectMessageReader(factory, (byte) 1);
        reader.setBuffer(byteBuffer);

        byte type1 = byteBuffer.get();
        byte type2 = byteBuffer.get();

        short messageType = makeMessageType(type1, type2);

        MessageDeserializer<NetworkMessage> deserializer = factory.createDeserializer(messageType);
        boolean read = deserializer.readMessage(reader);

        assertTrue(read);

        TestMessage readMessage = (TestMessage) deserializer.getMessage();

        assertEquals(message.msg(), readMessage.msg());
        assertTrue(message.getMap().equals(readMessage.getMap()));
    }

    /**
     * Concatenates the two parameter bytes to form a message type value.
     *
     * @param b0 The first byte.
     * @param b1 The second byte.
     */
    public static short makeMessageType(byte b0, byte b1) {
        return (short)((b1 & 0xFF) << 8 | b0 & 0xFF);
    }
}
