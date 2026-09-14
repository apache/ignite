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
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests XML output using data without loading message classes. */
public class XmlTableWriterTest {
    /** Writes synthetic data, including escaping, field order, empty fields and a message-level marker. */
    @Test public void testWrite() throws Exception {
        MessageTable.Data data = new MessageTable.Data(List.of("example.Provider"), List.of(
            new MessageRepresentation((short)7, "example.Message", new Schema(true, List.of(
                new Schema.Field("java.util.List<java.lang.String>", "names", "customMapper=A&B"),
                new Schema.Field("int", "count", "")
            ))),
            new MessageRepresentation((short)8, "example.Empty", new Schema(false, List.of()))
        ));

        String expected = """
            <?xml version="1.0" encoding="UTF-8"?>
            <messageTable formatVersion="1">
              <providers>
                <provider>example.Provider</provider>
              </providers>
              <messages>
                <message id="7" class="example.Message">
                  <jdkMarshalled/>
                  <field>
                    <type>java.util.List&lt;java.lang.String&gt;</type>
                    <name>names</name>
                    <serialization>customMapper=A&amp;B</serialization>
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

        assertEquals(expected, new XmlTableWriter().write(data));
    }
}
