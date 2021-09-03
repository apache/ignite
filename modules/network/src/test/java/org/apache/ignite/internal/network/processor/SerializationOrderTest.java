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

package org.apache.ignite.internal.network.processor;

import org.apache.ignite.network.TestMessagesFactory;
import org.apache.ignite.network.serialization.MessageDeserializer;
import org.apache.ignite.network.serialization.MessageReader;
import org.apache.ignite.network.serialization.MessageSerializer;
import org.apache.ignite.network.serialization.MessageWriter;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test class for checking that writing and reading fields in the generated (de-)serializers is ordered
 * alphanumerically.
 */
public class SerializationOrderTest {
    /** */
    private final TestMessagesFactory messageFactory = new TestMessagesFactory();

    /** */
    private final SerializationOrderMessageSerializationFactory serializationFactory =
        new SerializationOrderMessageSerializationFactory(messageFactory);

    /**
     * Tests that a generated {@link MessageSerializer} writes message fields in alphanumerical order.
     */
    @Test
    void testSerializationOrder() {
        SerializationOrderMessage msg = messageFactory.serializationOrderMessage()
            .a(1).b("2").c(3).d("4")
            .build();

        MessageSerializer<SerializationOrderMessage> serializer = serializationFactory.createSerializer();

        var mockWriter = mock(MessageWriter.class);

        when(mockWriter.isHeaderWritten()).thenReturn(true);
        when(mockWriter.writeInt(anyString(), anyInt())).thenReturn(true);
        when(mockWriter.writeString(anyString(), anyString())).thenReturn(true);

        serializer.writeMessage(msg, mockWriter);

        InOrder inOrder = inOrder(mockWriter);

        inOrder.verify(mockWriter).writeInt(eq("a"), eq(1));
        inOrder.verify(mockWriter).writeString(eq("b"), eq("2"));
        inOrder.verify(mockWriter).writeInt(eq("c"), eq(3));
        inOrder.verify(mockWriter).writeString(eq("d"), eq("4"));
    }

    /**
     * Tests that a generated {@link MessageDeserializer} reads message fields in alphanumerical order.
     */
    @Test
    void testDeserializationOrder() {
        MessageDeserializer<SerializationOrderMessage> deserializer = serializationFactory.createDeserializer();

        var mockReader = mock(MessageReader.class);

        when(mockReader.beforeMessageRead()).thenReturn(true);
        when(mockReader.isLastRead()).thenReturn(true);
        when(mockReader.readString(anyString())).thenReturn("foobar");

        deserializer.readMessage(mockReader);

        InOrder inOrder = inOrder(mockReader);

        inOrder.verify(mockReader).readInt(eq("a"));
        inOrder.verify(mockReader).readString(eq("b"));
        inOrder.verify(mockReader).readInt(eq("c"));
        inOrder.verify(mockReader).readString(eq("d"));
    }
}
