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

import java.util.List;
import org.apache.ignite.internal.Compress;
import org.apache.ignite.internal.CustomMapper;
import org.apache.ignite.internal.JdkMarshalled;
import org.apache.ignite.tools.compatibility.messages.dto.AnnotationRepresentation;
import org.apache.ignite.tools.compatibility.messages.dto.FieldRepresentation;
import org.apache.ignite.tools.compatibility.messages.dto.MessageRepresentation;
import org.apache.ignite.tools.compatibility.messages.dto.Schema;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests XML output using data without loading message classes. */
public class XmlTableWriterTest {
    /** Writes synthetic data, including escaping, field order, empty fields and a message-level marker. */
    @Test public void testWrite() throws Exception {
        List<MessageRepresentation> msgs = List.of(
            new MessageRepresentation((short)7, "example.Message", new Schema(
                List.of(new AnnotationRepresentation(JdkMarshalled.class.getName(), null)), List.of(
                    new FieldRepresentation(2, "java.util.List<java.lang.String>", "names", List.of(
                        new AnnotationRepresentation("since", "2.18.0"),
                        new AnnotationRepresentation(CustomMapper.class.getName(), "A&B"),
                        new AnnotationRepresentation(Compress.class.getName(), null)
                    )),
                    new FieldRepresentation(null, "int", "count", List.of())
            ))),
            new MessageRepresentation((short)8, "example.Empty", new Schema(List.of(), List.of()))
        );

        String expected = """
            <?xml version="1.0" encoding="UTF-8"?>
            <messageTable formatVersion="1">
              <messages>
                <message id="7" class="example.Message">
                  <annotations>
                    <org.apache.ignite.internal.JdkMarshalled/>
                  </annotations>
                  <field order="2">
                    <annotations>
                      <org.apache.ignite.internal.Compress/>
                      <org.apache.ignite.internal.CustomMapper>A&amp;B</org.apache.ignite.internal.CustomMapper>
                      <since>2.18.0</since>
                    </annotations>
                    <type>java.util.List&lt;java.lang.String&gt;</type>
                    <name>names</name>
                  </field>
                  <field>
                    <type>int</type>
                    <name>count</name>
                  </field>
                </message>
                <message id="8" class="example.Empty">
                </message>
              </messages>
            </messageTable>
            """;

        assertEquals(expected, new XmlTableWriter().toXml(msgs));
    }
}
