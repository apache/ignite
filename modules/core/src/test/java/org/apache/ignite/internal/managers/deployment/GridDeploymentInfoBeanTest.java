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

package org.apache.ignite.internal.managers.deployment;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.lang.IgniteUuid.randomUuid;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageSerializer;
import org.junit.Test;

/**
 * Tests for {@link GridDeploymentInfoBean} serialization.
 */
public class GridDeploymentInfoBeanTest {
    /** Message factory. */
    private final MessageFactory msgFactory =
        new IgniteMessageFactoryImpl(new MessageFactoryProvider[] {new GridIoMessageFactory()});

    /**
     * Tests serialization round trip.
     */
    @Test
    public void testSerialization() {
        var participants = Map.of(
            randomUUID(), randomUuid(),
            randomUUID(), randomUuid()
        );

        var srcMsg = new GridDeploymentInfoBean(
            randomUuid(),
            "userVersion",
            DeploymentMode.SHARED,
            participants
        );

        // locDepOwner is deprecated but serialized
        srcMsg.localDeploymentOwner(true);

        var resMsg = doMarshalUnmarshal(srcMsg);

        assertEquals(srcMsg.classLoaderId(), resMsg.classLoaderId());
        assertEquals(srcMsg.userVersion(), resMsg.userVersion());
        assertEquals(srcMsg.deployMode(), resMsg.deployMode());
        assertEquals(srcMsg.participants(), resMsg.participants());
        assertEquals(srcMsg.localDeploymentOwner(), resMsg.localDeploymentOwner());
    }

    /**
     * @param srcMsg Message to marshal.
     * @param <T> Message type.
     * @return Unmarshalled message.
     */
    private <T extends Message> T doMarshalUnmarshal(T srcMsg) {
        var buf = ByteBuffer.allocate(8 * 1024);

        MessageSerializer serializer = msgFactory.serializer(srcMsg.directType());
        assertNotNull(serializer);

        // Write phase
        var fullyWritten = loopBuffer(buf, 0, wBuf -> {
            var writer = new DirectMessageWriter(msgFactory);
            writer.setBuffer(wBuf);
            return serializer.writeTo(srcMsg, writer);
        });
        assertTrue("The message was not written completely.", fullyWritten);

        // Read type code (little-endian) and create message
        buf.flip();

        byte b0 = buf.get();
        byte b1 = buf.get();

        var type = (short)((b1 & 0xFF) << 8 | b0 & 0xFF);
        assertEquals(srcMsg.directType(), type);

        var resMsg = (T)msgFactory.create(type);

        // Read phase
        var fullyRead = loopBuffer(buf, buf.position(), rBuf -> {
            var reader = new DirectMessageReader(msgFactory, null);
            reader.setBuffer(rBuf);
            return serializer.readFrom(resMsg, reader);
        });
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
