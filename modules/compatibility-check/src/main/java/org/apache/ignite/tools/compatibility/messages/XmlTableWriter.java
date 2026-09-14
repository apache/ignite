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

package org.apache.ignite.tools.compatibility.messages;

import java.io.StringWriter;
import java.util.List;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

/** Writes collected message metadata as XML with fixed formatting. */
class XmlTableWriter {
    /**
     * Writes collected metadata without loading message classes.
     *
     * @param data Collected table.
     * @return XML table.
     * @throws XMLStreamException If XML writing fails.
     */
    String write(MessageTable.Data data) throws XMLStreamException {
        StringWriter out = new StringWriter();
        XMLStreamWriter xml = XMLOutputFactory.newDefaultFactory().createXMLStreamWriter(out);

        try {
            xml.writeStartDocument("UTF-8", "1.0");
            xml.writeCharacters("\n");
            xml.writeStartElement("messageTable");
            xml.writeAttribute("formatVersion", "1");
            xml.writeCharacters("\n  ");

            writeProviders(xml, data.providers());

            writeMessages(xml, data.messages());

            xml.writeEndElement();
            xml.writeCharacters("\n");
            xml.writeEndDocument();
        }
        finally {
            xml.close();
        }

        return out.toString();
    }

    /** Writes provider names. */
    private static void writeProviders(XMLStreamWriter xml, List<String> providers) throws XMLStreamException {
        xml.writeStartElement("providers");

        for (String provider : providers) {
            xml.writeCharacters("\n    ");
            xml.writeStartElement("provider");
            xml.writeCharacters(provider);
            xml.writeEndElement();
        }

        xml.writeCharacters("\n  ");
        xml.writeEndElement();
        xml.writeCharacters("\n  ");
    }

    /** Writes the collected messages in their existing order. */
    private static void writeMessages(XMLStreamWriter xml, List<MessageRepresentation> msgs) throws XMLStreamException {
        xml.writeStartElement("messages");

        for (MessageRepresentation msg : msgs)
            writeMessage(xml, msg);

        xml.writeCharacters("\n  ");
        xml.writeEndElement();
        xml.writeCharacters("\n");
    }

    /** Writes one message and its fields. */
    private static void writeMessage(XMLStreamWriter xml, MessageRepresentation msg) throws XMLStreamException {
        xml.writeCharacters("\n    ");
        xml.writeStartElement("message");
        xml.writeAttribute("id", Short.toString(msg.id()));
        xml.writeAttribute("class", msg.className());

        if (msg.schema().jdkMarshalled()) {
            xml.writeCharacters("\n      ");
            xml.writeEmptyElement("jdkMarshalled");
        }

        for (Schema.Field field : msg.schema().fields()) {
            xml.writeCharacters("\n      ");

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
}
