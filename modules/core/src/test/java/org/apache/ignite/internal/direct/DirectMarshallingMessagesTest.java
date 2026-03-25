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

package org.apache.ignite.internal.direct;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.junit.Assert.assertArrayEquals;

/**
 * Messages marshalling test.
 */
public class DirectMarshallingMessagesTest extends GridCommonAbstractTest {
    /**
     * Size of chunk for marshalling.
     * <p>
     * Should be small to ensure message is written/read in parts.
     */
    private static final int CHUNK_SIZE = 16;

    /** Message factory. */
    private final MessageFactory msgFactory =
        new IgniteMessageFactoryImpl(new MessageFactoryProvider[] {
            new GridIoMessageFactory(jdk(), U.gridClassLoader()),
            factory -> factory.register(
                TestNestedContainersMessage.TYPE,
                TestNestedContainersMessage::new,
                new TestNestedContainersMessageSerializer()
            )
        });

    /** */
    @Test
    public void testNestedContainers() {
        TestNestedContainersMessage msg = new TestNestedContainersMessage();

        msg.nestedMap = Map.of(
            1, Map.of(1, 2L),
            2, Map.of(1, 2L)
        );

        msg.nestedCollection = Map.of(
            1, Arrays.asList(1),
            2, Arrays.asList(2)
        );

        msg.nestedArr = Map.of(
            1, new String[]{"AAA", "AAA"},
            2, new String[]{"BBB", "BBB"}
        );

        TestNestedContainersMessage resMsg = doMarshalUnmarshalChunked(msg);

        assertEquals(msg.nestedMap, resMsg.nestedMap);
        assertEquals(msg.nestedCollection, resMsg.nestedCollection);
        assertArrayEquals(msg.nestedArr.get(1), resMsg.nestedArr.get(1));
        assertArrayEquals(msg.nestedArr.get(2), resMsg.nestedArr.get(2));
    }

    /**
     * @param srcMsg Message to marshal.
     * @param <T> Message type.
     * @return Unmarshalled message.
     */
    private <T extends Message> T doMarshalUnmarshalChunked(T srcMsg) {
        ByteBuffer buf = ByteBuffer.allocate(256);

        DirectMessageWriter writer = new DirectMessageWriter(msgFactory);

        boolean fullyWritten = false;

        while (!fullyWritten) {
            ByteBuffer chunk = ByteBuffer.allocate(CHUNK_SIZE);

            writer.setBuffer(chunk);

            fullyWritten = writer.writeMessage(srcMsg, false);

            chunk.flip();

            buf.put(chunk);
        }

        byte[] bytes = new byte[buf.position()];

        buf.flip();
        buf.get(bytes);

        DirectMessageReader reader = new DirectMessageReader(msgFactory, null);

        Message resMsg = null;

        int pos = 0;

        while (resMsg == null) {
            int len = Math.min(CHUNK_SIZE, bytes.length - pos);

            ByteBuffer chunk = ByteBuffer.allocate(len);

            chunk.put(bytes, pos, len);

            chunk.flip();

            reader.setBuffer(chunk);

            resMsg = reader.readMessage(false);

            pos += chunk.position();
        }

        return (T)resMsg;
    }
}
