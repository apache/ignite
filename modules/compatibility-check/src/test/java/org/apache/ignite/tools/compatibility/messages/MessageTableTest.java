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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests metadata collection and XML writing independently. */
public class MessageTableTest {
    /** Temporary output directory. */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** Collects registrations and compiled schemas without XML serialization. */
    @Test public void testCollect() throws Exception {
        MessageTable.Data data = MessageTable.collect();

        assertEquals(data, MessageTable.collect());
        assertEquals(List.of(
            "org.apache.ignite.internal.CoreMessagesProvider",
            "org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory",
            "org.apache.ignite.internal.processors.query.calcite.message.CalciteMessageFactory",
            "org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory"
        ), data.providers());
        assertFalse(data.messages().isEmpty());

        int prevId = Integer.MIN_VALUE;
        boolean compressedFound = false;

        for (MessageRepresentation msg : data.messages()) {
            assertTrue(msg.id() > prevId);
            prevId = msg.id();

            if (msg.id() == 5000) {
                compressedFound = true;
                assertEquals("org.apache.ignite.internal.managers.communication.CompressedMessage", msg.className());
                assertTrue(msg.schema().fields().isEmpty());
                assertFalse(msg.schema().jdkMarshalled());
            }
        }

        assertTrue(compressedFound);
        assertEquals(new Schema.Field("long", "reqId", ""), data.messages().get(0).schema().fields().get(0));
    }

    /** The command writes the collected table and preserves the checked-in format. */
    @Test public void testMain() throws Exception {
        Path out = tmp.getRoot().toPath().resolve("result/table.xml");

        MessageTable.main(new String[] {out.toString()});

        String table = Files.readString(out);

        assertEquals(new XmlTableWriter().write(MessageTable.collect()), table);
        assertEquals(Files.readString(Path.of("src/main/resources/messages/table.xml")), table);

        try (var files = Files.list(out.getParent())) {
            assertEquals(1, files.count());
        }
    }
}
