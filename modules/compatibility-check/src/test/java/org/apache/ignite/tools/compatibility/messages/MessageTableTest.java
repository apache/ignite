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

import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.xml.parsers.DocumentBuilderFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests message table generation. */
public class MessageTableTest {
    /** Temporary output directory. */
    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    /** Exercise real providers and compiled schemas without starting a node. */
    @Test public void testGeneratedTable() throws Exception {
        String table = MessageTable.generate();

        assertEquals(table, MessageTable.generate());
        Document doc = DocumentBuilderFactory.newDefaultInstance().newDocumentBuilder()
            .parse(new InputSource(new StringReader(table)));

        assertEquals("messageTable", doc.getDocumentElement().getTagName());
        assertEquals("1", doc.getDocumentElement().getAttribute("formatVersion"));

        String[] providers = {
            "org.apache.ignite.internal.CoreMessagesProvider",
            "org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory",
            "org.apache.ignite.internal.processors.query.calcite.message.CalciteMessageFactory",
            "org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory"
        };

        NodeList providerNodes = doc.getElementsByTagName("provider");

        assertEquals(providers.length, providerNodes.getLength());

        for (int i = 0; i < providers.length; i++)
            assertEquals(providers[i], providerNodes.item(i).getTextContent());

        NodeList msgs = doc.getElementsByTagName("message");
        boolean compressedFound = false;
        int prevId = Integer.MIN_VALUE;

        for (int i = 0; i < msgs.getLength(); i++) {
            Element msg = (Element)msgs.item(i);
            int id = Integer.parseInt(msg.getAttribute("id"));

            assertTrue(id > prevId);
            prevId = id;

            if (id == 5000) {
                compressedFound = true;
                assertEquals("org.apache.ignite.internal.managers.communication.CompressedMessage",
                    msg.getAttribute("class"));
                assertEquals(0, msg.getElementsByTagName("field").getLength());
            }
        }

        assertTrue(compressedFound);
        assertTrue(table.contains("<field>long reqId</field>"));
        assertTrue(table.contains("&lt;"));

        Document expected = DocumentBuilderFactory.newDefaultInstance().newDocumentBuilder()
            .parse(Path.of("src/main/resources/messages/table.xml").toFile());

        assertTrue(expected.isEqualNode(doc));

        Path out = tmp.getRoot().toPath().resolve("result/table.xml");

        MessageTable.main(new String[] {out.toString()});
        assertEquals(table, Files.readString(out));

        try (var files = Files.list(out.getParent())) {
            assertEquals(1, files.count());
        }
    }
}
