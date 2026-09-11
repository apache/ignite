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

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessageFactory;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/** Exports the actual production registrations and their compiled field descriptions. */
public final class MessageTable {
    /** No instances. */
    private MessageTable() {
        // No-op.
    }

    /**
     * Exports the registered messages to an XML file.
     *
     * @param args Output file path.
     * @throws Exception If the build is incomplete or the inputs cannot be read.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 1)
            throw new IllegalArgumentException("Expected: output-file");

        Path out = Path.of(args[0]).toAbsolutePath();
        String table = generate();

        Files.createDirectories(out.getParent());
        Files.writeString(out, table);
    }

    /**
     * @return Canonical XML table.
     * @throws Exception If any registered message cannot be described.
     */
    static String generate() throws Exception {
        MessageFactoryProvider[] providers = {
            new CoreMessagesProvider(),
            new GridH2ValueMessageFactory(),
            new CalciteMessageFactory(),
            new ZkMessageFactory()
        };

        IgniteMessageFactoryImpl<?, ?> factory = new IgniteMessageFactoryImpl<>(providers);
        short[] ids = factory.registeredDirectTypes();

        Arrays.sort(ids);

        Document doc = DocumentBuilderFactory.newDefaultInstance().newDocumentBuilder().newDocument();
        Element root = doc.createElement("messageTable");

        doc.appendChild(root);
        root.setAttribute("formatVersion", "1");

        Element providerList = appendElement(root, "providers");

        for (MessageFactoryProvider provider : providers)
            appendElement(providerList, "provider").setTextContent(provider.getClass().getName());

        Element msgs = appendElement(root, "messages");

        try (MessageSchema schemas = new MessageSchema()) {
            for (short id : ids) {
                Message msg = factory.create(id);
                Element msgElement = appendElement(msgs, "message");

                msgElement.setAttribute("id", Short.toString(id));
                msgElement.setAttribute("class", msg.getClass().getName());

                for (MessageSchema.Field field : schemas.read(msg.getClass())) {
                    if (field.name().isEmpty()) {
                        appendElement(msgElement, field.serialization());

                        continue;
                    }

                    Element fieldElement = appendElement(msgElement, "field");

                    appendElement(fieldElement, "type").setTextContent(field.type());
                    appendElement(fieldElement, "name").setTextContent(field.name());

                    if (!field.serialization().isEmpty())
                        appendElement(fieldElement, "serialization").setTextContent(field.serialization());
                }
            }
        }

        Transformer transformer = TransformerFactory.newDefaultInstance().newTransformer();

        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");

        StringWriter out = new StringWriter();

        transformer.transform(new DOMSource(doc), new StreamResult(out));

        return out.toString();
    }

    /**
     * Appends an element to the given parent.
     *
     * @param parent Parent element.
     * @param name Element name.
     * @return Created element.
     */
    private static Element appendElement(Element parent, String name) {
        Element element = parent.getOwnerDocument().createElement(name);

        parent.appendChild(element);

        return element;
    }
}
