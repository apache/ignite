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
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamWriter;
import org.apache.ignite.internal.CoreMessagesProvider;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.query.calcite.message.CalciteMessageFactory;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2ValueMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.discovery.zk.internal.ZkMessageFactory;

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

        StringWriter out = new StringWriter();
        XMLStreamWriter xml = XMLOutputFactory.newDefaultFactory().createXMLStreamWriter(out);

        try (MessageSchema schemas = new MessageSchema()) {
            xml.writeStartDocument("UTF-8", "1.0");
            xml.writeCharacters("\n");
            xml.writeStartElement("messageTable");
            xml.writeAttribute("formatVersion", "1");
            xml.writeCharacters("\n  ");
            xml.writeStartElement("providers");

            for (MessageFactoryProvider provider : providers) {
                xml.writeCharacters("\n    ");
                xml.writeStartElement("provider");
                xml.writeCharacters(provider.getClass().getName());
                xml.writeEndElement();
            }

            xml.writeCharacters("\n  ");
            xml.writeEndElement();
            xml.writeCharacters("\n  ");
            xml.writeStartElement("messages");

            for (short id : ids) {
                Message msg = factory.create(id);

                xml.writeCharacters("\n    ");
                xml.writeStartElement("message");
                xml.writeAttribute("id", Short.toString(id));
                xml.writeAttribute("class", msg.getClass().getName());

                for (MessageSchema.Field field : schemas.read(msg.getClass())) {
                    xml.writeCharacters("\n      ");
                    if (field.name().isEmpty()) {
                        xml.writeEmptyElement(field.serialization());

                        continue;
                    }

                    xml.writeStartElement("field");
                    xml.writeCharacters("\n        ");
                    xml.writeStartElement("type");
                    xml.writeCharacters(field.type());
                    xml.writeEndElement();
                    xml.writeCharacters("\n        ");
                    xml.writeStartElement("name");
                    xml.writeCharacters(field.name());
                    xml.writeEndElement();

                    if (!field.serialization().isEmpty()) {
                        xml.writeCharacters("\n        ");
                        xml.writeStartElement("serialization");
                        xml.writeCharacters(field.serialization());
                        xml.writeEndElement();
                    }

                    xml.writeCharacters("\n      ");
                    xml.writeEndElement();
                }

                xml.writeCharacters("\n    ");
                xml.writeEndElement();
            }

            xml.writeCharacters("\n  ");
            xml.writeEndElement();
            xml.writeCharacters("\n");
            xml.writeEndElement();
            xml.writeCharacters("\n");
            xml.writeEndDocument();
        }
        finally {
            xml.close();
        }

        return out.toString();
    }
}
