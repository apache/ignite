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
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.util.distributed.SingleNodeMessage;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;

/**
 * Messages marshalling test.
 */
public class DirectMarshallingMessagesTest extends GridCommonAbstractTest {
    /** Protocol version. */
    private static final byte PROTO_VER = 2;

    /** Message factory. */
    private final IgniteMessageFactory msgFactory =
        new IgniteMessageFactoryImpl(new MessageFactory[] {new GridIoMessageFactory()});

    /** */
    @Test
    public void testSingleNodeMessage() {
        SingleNodeMessage<?> srcMsg =
            new SingleNodeMessage<>(UUID.randomUUID(), TEST_PROCESS, "data", new Exception("error"));

        SingleNodeMessage<?> resMsg = doMarshalUnmarshal(srcMsg);

        assertEquals(srcMsg.type(), resMsg.type());
        assertEquals(srcMsg.processId(), resMsg.processId());
        assertEquals(srcMsg.response(), resMsg.response());
        assertEquals(srcMsg.error().getClass(), resMsg.error().getClass());
        assertEquals(srcMsg.error().getMessage(), resMsg.error().getMessage());
    }

    /**
     * @param srcMsg Message to marshal.
     * @param <T> Message type.
     * @return Unmarshalled message.
     */
    private <T extends Message> T doMarshalUnmarshal(T srcMsg) {
        ByteBuffer buf = ByteBuffer.allocate(8 * 1024);

        boolean fullyWritten = loopBuffer(buf, 0, buf0 -> srcMsg.writeTo(buf0, new DirectMessageWriter()));
        assertTrue("The message was not written completely.", fullyWritten);

        buf.flip();

        byte b0 = buf.get();
        byte b1 = buf.get();

        short type = (short)((b1 & 0xFF) << 8 | b0 & 0xFF);

        assertEquals(srcMsg.directType(), type);

        T resMsg = (T)msgFactory.create(type);

        boolean fullyRead = loopBuffer(buf, buf.position(),
            buf0 -> resMsg.readFrom(buf0, new DirectMessageReader(msgFactory)));
        assertTrue("The message was not read completely.", fullyRead);

        return resMsg;
    }

    /**
     * @param buf Byte buffer.
     * @param start Start position.
     * @param func Function that is sequentially executed on a different-sized part of the buffer.
     * @return {@code True} if the function returns {@code True} at least once, {@code False} otherwise.
     */
    private boolean loopBuffer(ByteBuffer buf, int start, Function<ByteBuffer, Boolean> func) {
        int pos = start;

        do {
            buf.position(start);
            buf.limit(++pos);

            if (func.apply(buf))
                return true;
        }
        while (pos < buf.capacity());

        return false;
    }
}
