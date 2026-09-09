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
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests message table generation. */
public class MessageTableTest {
    /** Temporary output directory. */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** Exercise real providers and compiled schemas without starting a node. */
    @Test public void testGeneratedTable() throws Exception {
        String table = MessageTable.generate();

        assertEquals(table, MessageTable.generate());
        assertTrue(table.contains("\"long reqId\""));

        var json = new ObjectMapper().readTree(table);

        for (var msg : json.get("messages")) {
            if (msg.get("id").asInt() == 5000)
                assertEquals(new ObjectMapper().valueToTree(List.of()), msg.get("schema"));
        }

        assertEquals(new ObjectMapper().valueToTree(List.of(
            "org.apache.ignite.internal.CoreMessagesProvider",
            "org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory",
            "org.apache.ignite.internal.processors.query.calcite.message.CalciteMessageFactory",
            "org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory"
        )), json.get("providers"));
        assertNull(json.get("modules"));

        Path out = tmp.getRoot().toPath().resolve("result/table.json");

        MessageTable.main(new String[] {out.toString()});
        assertEquals(table, Files.readString(out));

        try (var files = Files.list(out.getParent())) {
            assertEquals(1, files.count());
        }
    }
}
