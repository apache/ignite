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
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.NioField;
import org.apache.ignite.internal.Order;
import org.apache.ignite.tools.compatibility.messages.dto.AnnotationRepresentation;
import org.apache.ignite.tools.compatibility.messages.dto.FieldRepresentation;
import org.apache.ignite.tools.compatibility.messages.messages.TestChildMessage;
import org.apache.ignite.tools.compatibility.messages.messages.TestEmptyMessage;
import org.apache.ignite.tools.compatibility.messages.messages.TestInvalidOrderMessage;
import org.apache.ignite.tools.compatibility.messages.messages.TestParentMessage;
import org.apache.ignite.tools.compatibility.messages.messages.TestUnannotatedMessage;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Verifies field schemas read from bytecode rather than compiler output resources. */
public class MessageSchemaReaderTest {
    /** CLASS annotations are invisible to reflection but available through the compiler model. */
    @Test public void testCompiledFields() throws Exception {
        assertNull(TestParentMessage.class.getDeclaredField("id").getAnnotation(Order.class));

        try (MessageSchemaReader reader = new MessageSchemaReader()) {
            assertEquals(List.of(new AnnotationRepresentation(JdkMarshalled.class.getName(), null)),
                reader.read(TestChildMessage.class).annotations());
            assertEquals(List.of(), reader.read(TestEmptyMessage.class).annotations());
            assertEquals(List.of(
                new FieldRepresentation(0, "int", "id", List.of()),
                new FieldRepresentation(1, "byte[]", "parentValueBytes", List.of()),
                new FieldRepresentation(2, "byte[]", "id", List.of(
                    new AnnotationRepresentation(Compress.class.getName(), null),
                    new AnnotationRepresentation(NioField.class.getName(), null))),
                new FieldRepresentation(3, TestChildMessage.Mode.class.getCanonicalName(), "mode",
                    List.of(new AnnotationRepresentation(CustomMapper.class.getName(), "example.Mapper"))),
                new FieldRepresentation(null, "java.lang.Object", "parentValue",
                    List.of(new AnnotationRepresentation(Marshalled.class.getName(), "value=parentValueBytes"))),
                new FieldRepresentation(null, "java.util.List<? extends java.lang.String>", "payload",
                    List.of(new AnnotationRepresentation(Marshalled.class.getName(), "value=id")))
            ), reader.read(TestChildMessage.class).fields());
            assertEquals(List.of(), reader.read(TestEmptyMessage.class).fields());
            assertEquals(List.of(), reader.read(TestUnannotatedMessage.class).fields());
            assertEquals(
                List.of(new FieldRepresentation(1, "int", "id", List.of())),
                reader.read(TestInvalidOrderMessage.class).fields()
            );
        }
    }

}
