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
import java.util.Comparator;
import java.util.List;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.tools.compatibility.messages.dto.AnnotationRepresentation;
import org.apache.ignite.tools.compatibility.messages.dto.FieldRepresentation;
import org.apache.ignite.tools.compatibility.messages.dto.MessageRepresentation;

/** Writes collected message metadata as XML with fixed formatting. */
class XmlTableWriter {
    /** Apache license header for generated XML files. */
    private static final String LICENSE = "\n"
        + "  Licensed to the Apache Software Foundation (ASF) under one or more\n"
        + "  contributor license agreements.  See the NOTICE file distributed with\n"
        + "  this work for additional information regarding copyright ownership.\n"
        + "  The ASF licenses this file to You under the Apache License, Version 2.0\n"
        + "  (the \"License\"); you may not use this file except in compliance with\n"
        + "  the License.  You may obtain a copy of the License at\n"
        + "\n"
        + "       http://www.apache.org/licenses/LICENSE-2.0\n"
        + "\n"
        + "  Unless required by applicable law or agreed to in writing, software\n"
        + "  distributed under the License is distributed on an \"AS IS\" BASIS,\n"
        + "  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n"
        + "  See the License for the specific language governing permissions and\n"
        + "  limitations under the License.\n";

    /**
     * Writes collected metadata without loading message classes.
     *
     * @param msgs Collected messages.
     * @return XML table.
     * @throws XMLStreamException If XML writing fails.
     */
    String toXml(List<MessageRepresentation> msgs) throws XMLStreamException {
        StringWriter out = new StringWriter();
        XMLStreamWriter xml = XMLOutputFactory.newDefaultFactory().createXMLStreamWriter(out);

        try {
            xml.writeStartDocument("UTF-8", "1.0");
            xml.writeCharacters("\n");
            xml.writeComment(LICENSE);
            xml.writeCharacters("\n");
            xml.writeStartElement("messageTable");
            xml.writeAttribute("formatVersion", "1");
            xml.writeCharacters("\n  ");

            writeMessages(xml, msgs);

            xml.writeEndElement();
            xml.writeCharacters("\n");
            xml.writeEndDocument();
        }
        finally {
            xml.close();
        }

        return out.toString();
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

        writeAnnotations(xml, msg.schema().annotations(), "      ");

        writeOrderedFields(xml, msg.schema().fields());
        writeMarshalledFields(xml, msg.schema().fields());

        xml.writeCharacters("\n    ");
        xml.writeEndElement();
    }

    /** Writes ordered wire fields. */
    private static void writeOrderedFields(XMLStreamWriter xml, List<FieldRepresentation> fields)
        throws XMLStreamException {
        List<FieldRepresentation> orderedFields = fields.stream().filter(field -> !isMarshalled(field)).toList();

        if (orderedFields.isEmpty())
            return;

        xml.writeCharacters("\n      ");
        xml.writeStartElement("orderedFields");

        for (FieldRepresentation field : orderedFields)
            writeField(xml, field, "        ");

        xml.writeCharacters("\n      ");
        xml.writeEndElement();
    }

    /** Writes logical fields converted to ordered wire fields by the generated marshaller. */
    private static void writeMarshalledFields(XMLStreamWriter xml, List<FieldRepresentation> fields)
        throws XMLStreamException {
        List<FieldRepresentation> marshalledFields = fields.stream().filter(XmlTableWriter::isMarshalled).toList();

        if (marshalledFields.isEmpty())
            return;

        xml.writeCharacters("\n      ");
        xml.writeStartElement("marshalledFields");

        for (FieldRepresentation field : marshalledFields)
            writeField(xml, field, "        ");

        xml.writeCharacters("\n      ");
        xml.writeEndElement();
    }

    /** Writes one field using the given indentation. */
    private static void writeField(XMLStreamWriter xml, FieldRepresentation field, String indent)
        throws XMLStreamException {
        xml.writeCharacters("\n" + indent);
        xml.writeStartElement("field");

        if (field.order() != null)
            xml.writeAttribute("order", Integer.toString(field.order()));

        writeAnnotations(xml, field.annotations(), indent + "  ");

        xml.writeCharacters("\n" + indent + "  ");
        xml.writeStartElement("type");
        xml.writeCharacters(field.type());
        xml.writeEndElement();

        xml.writeCharacters("\n" + indent + "  ");

        xml.writeStartElement("name");
        xml.writeCharacters(field.name());
        xml.writeEndElement();

        xml.writeCharacters("\n" + indent);
        xml.writeEndElement();
    }

    /** Returns {@code true} if the field is a logical field converted by the generated marshaller. */
    private static boolean isMarshalled(FieldRepresentation field) {
        return field.annotations().stream().anyMatch(a -> Marshalled.class.getName().equals(a.name()));
    }

    /** Writes class or field annotations in name order using fixed indentation. */
    private static void writeAnnotations(XMLStreamWriter xml, List<AnnotationRepresentation> annotations, String indent)
        throws XMLStreamException {
        if (annotations.isEmpty())
            return;

        xml.writeCharacters("\n" + indent);
        xml.writeStartElement("annotations");

        List<AnnotationRepresentation> sorted = annotations.stream()
            .sorted(Comparator.comparing(AnnotationRepresentation::name)).toList();

        for (AnnotationRepresentation annotation : sorted) {
            xml.writeCharacters("\n" + indent + "  ");

            if (annotation.value() == null)
                xml.writeEmptyElement(annotation.name());
            else {
                xml.writeStartElement(annotation.name());
                xml.writeCharacters(annotation.value());
                xml.writeEndElement();
            }
        }

        xml.writeCharacters("\n" + indent);
        xml.writeEndElement();
    }
}
