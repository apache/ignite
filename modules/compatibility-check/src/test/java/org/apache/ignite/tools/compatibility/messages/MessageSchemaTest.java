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
import org.apache.ignite.internal.EmptyMessage;
import org.apache.ignite.internal.JdkMarshalled;
import org.apache.ignite.internal.Marshalled;
import org.apache.ignite.internal.NioField;
import org.apache.ignite.internal.Order;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Verifies field schemas read from bytecode rather than compiler output resources. */
public class MessageSchemaTest {
    /** CLASS annotations are invisible to reflection but available through the compiler model. */
    @Test public void testCompiledFields() throws Exception {
        assertNull(Parent.class.getDeclaredField("id").getAnnotation(Order.class));

        try (MessageSchema reader = new MessageSchema()) {
            assertEquals(List.of(
                "jdkMarshalled",
                "int id",
                "byte[] id compress nio",
                Mode.class.getCanonicalName() + " mode customMapper=example.Mapper",
                "marshalled java.util.List<? extends java.lang.String> payload value=id"
            ), reader.read(Child.class));
            assertEquals(List.of(), reader.read(Empty.class));
            assertEquals(List.of(), reader.read(Unannotated.class));
            assertEquals(List.of("int id"), reader.read(InvalidOrder.class));
        }
    }

    /** Parent fields precede child fields, and marshaller selection is inherited. */
    @JdkMarshalled
    private static class Parent {
        /** Parent field. */
        @Order(0)
        private int id;
    }

    /** Declaration order is deliberately different from serialization order. */
    private static class Child extends Parent {
        /** Enum mapped by a named mapper. */
        @Order(1)
        @CustomMapper("example.Mapper")
        private Mode mode;

        /** Child field hides the parent's field. */
        @Order(0)
        @Compress
        @NioField
        private byte[] id;

        /** Logical value mapped to a wire field. */
        @Marshalled("id")
        private List<? extends String> payload;

        /** Ordinary, non-wire field. */
        private int ignored;

        /** Ordinary methods do not become schema entries. */
        int ignored() {
            return ignored;
        }
    }

    /** Empty messages have an explicit marker. */
    @EmptyMessage
    private static class Empty {
        // No-op.
    }

    /** Classes without field annotations produce an empty schema. */
    private static class Unannotated {
        // No-op.
    }

    /** Order validation belongs to the code generator, not the table exporter. */
    private static class InvalidOrder {
        /** Missing preceding field. */
        @Order(1)
        private int id;
    }

    /** Fixture enum. */
    private enum Mode {
        /** Single fixture value. */
        ONE
    }
}
