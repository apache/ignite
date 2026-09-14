/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.tools.compatibility.messages;

import java.util.List;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.tools.compatibility.messages.dto.MessageRepresentation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests message collection independently of XML writing. */
public class MessageTableCollectorTest {
    /** Collects registrations and compiled schemas without XML serialization. */
    @Test public void testCollect() throws Exception {
        MessageFactoryProvider[] providers = {new CoreMessagesProvider()};
        MessageTableCollector collector = new MessageTableCollector(providers);
        List<MessageRepresentation> msgs = collector.collect();

        assertEquals(msgs, collector.collect());
        assertFalse(msgs.isEmpty());

        int prevId = Integer.MIN_VALUE;
        boolean compressedFound = false;

        for (MessageRepresentation msg : msgs) {
            assertTrue(msg.id() > prevId);
            prevId = msg.id();

            if (msg.id() == 5000) {
                compressedFound = true;
                assertEquals("org.apache.ignite.internal.managers.communication.CompressedMessage", msg.className());
                assertTrue(msg.schema().fields().isEmpty());
                assertTrue(msg.schema().annotations().isEmpty());
            }
        }

        assertTrue(compressedFound);
    }
}
