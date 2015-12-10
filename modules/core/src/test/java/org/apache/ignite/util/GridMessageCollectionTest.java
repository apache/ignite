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

package org.apache.ignite.util;

import java.nio.ByteBuffer;
import junit.framework.TestCase;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.util.UUIDCollectionMessage;
import org.apache.ignite.plugin.extensions.communication.Message;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.util.GridMessageCollection.of;

/**
 *
 */
public class GridMessageCollectionTest extends TestCase {
    /** */
    private byte proto;

    /**
     *
     */
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

        DirectMessageWriter w = new DirectMessageWriter(proto);

        m.writeTo(buf, w);

        buf.flip();

        DirectMessageReader r = new DirectMessageReader(new GridIoMessageFactory(null), proto);

        r.setBuffer(buf);

        Message mx = r.readMessage(null);

        assertEquals(m, mx);
    }
}
