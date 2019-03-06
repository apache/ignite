/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.util;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.junit.Test;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.util.GridMessageCollection.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 *
 */
public class GridMessageCollectionTest {
    /** */
    private byte proto;

    /**
     * @param proto Protocol version.
     * @return Writer.
     */
    protected MessageWriter writer(byte proto) {
        return new DirectMessageWriter(proto);
    }

    /**
     * @param msgFactory Message factory.
     * @param proto Protocol version.
     * @return Writer.
     */
    protected MessageReader reader(MessageFactory msgFactory, byte proto) {
        return new DirectMessageReader(msgFactory, proto);
    }

    /**
     *
     */
    @Test
    public void testMarshal() {
        UUIDCollectionMessage um0 = UUIDCollectionMessage.of();
        UUIDCollectionMessage um1 = UUIDCollectionMessage.of(randomUUID());
        UUIDCollectionMessage um2 = UUIDCollectionMessage.of(randomUUID(), randomUUID());
        UUIDCollectionMessage um3 = UUIDCollectionMessage.of(randomUUID(), randomUUID(), randomUUID());

        assertNull(um0);
        assertEquals(3, um3.uuids().size());

        proto = 2;
        doTestMarshal(um0, um1, um2, um3);

        proto = 1;
        doTestMarshal(um0, um1, um2, um3);
    }

    /**
     * @param um0 Null.
     * @param um1 One uuid list.
     * @param um2 Two uuid list.
     * @param um3 Three uuid list.
     */
    private void doTestMarshal(
        UUIDCollectionMessage um0,
        UUIDCollectionMessage um1,
        UUIDCollectionMessage um2,
        UUIDCollectionMessage um3
    ) {
        doTestMarshal(um1);
        doTestMarshal(um2);
        doTestMarshal(um3);

        doTestMarshal(of(um0));
        doTestMarshal(of(um1));
        doTestMarshal(of(um2));
        doTestMarshal(of(um3));

        doTestMarshal(of(um2, um3));
        doTestMarshal(of(um1, um0, um3));

        doTestMarshal(of(of(um3), of(um2)));
        doTestMarshal(of(of(of(of(of(um0))), um1, of(um3))));
    }

    /**
     * @param m Message.
     */
    private void doTestMarshal(Message m) {
        ByteBuffer buf = ByteBuffer.allocate(8 * 1024);

        m.writeTo(buf, writer(proto));

        buf.flip();

        byte b0 = buf.get();
        byte b1 = buf.get();

        short type = (short)((b1 & 0xFF) << 8 | b0 & 0xFF);

        assertEquals(m.directType(), type);

        GridIoMessageFactory msgFactory = new GridIoMessageFactory(null);

        Message mx = msgFactory.create(type);

        mx.readFrom(buf, reader(msgFactory, proto));

        assertEquals(m, mx);
    }
}
