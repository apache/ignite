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

package org.apache.ignite.internal.schema.marshaller;

import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * POJO validation tests.
 */
public class RecordMarshallerValidationsTest {
    /** Key columns for test. */
    private static Column[] KEY_COLS = new Column[]{new Column("id", INT32, false)};

    /**
     * Returns list of marshaller factories for the test.
     */
    private static List<MarshallerFactory> marshallerFactoryProvider() {
        return List.of(new ReflectionMarshallerFactory());
    }

    /**
     * Check default value is taken into account if there is no field in specified class to marshall from.
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void testColsWithDefaultValue(MarshallerFactory factory) throws MarshallerException {
        Column[] valCols = new Column[] {
                new Column("fbyte1", INT32, false),
                new Column("fbyte2", INT32, false, () -> 0x42)
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, KEY_COLS, valCols);

        final TruncatedRecClass rec = new TruncatedRecClass(1, 1);

        RecordMarshaller<TruncatedRecClass> marshaller = factory.create(schema, TruncatedRecClass.class);

        BinaryRow row = marshaller.marshal(rec);

        TruncatedRecClass restoredRec = marshaller.unmarshal(new Row(schema, row));

        assertTrue(rec.getClass().isInstance(restoredRec));

        assertEquals(rec, restoredRec);
    }

    /**
     * Check nullable flag is taken into account if there is no field in specified class to marshall from.
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void testColsWithNullable(MarshallerFactory factory) throws MarshallerException {
        Column[] valCols = new Column[] {
                new Column("fbyte1", INT32, false),
                new Column("fbyte2", INT32, true)
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, KEY_COLS, valCols);

        final TruncatedRecClass rec = new TruncatedRecClass(1, 1);

        RecordMarshaller<TruncatedRecClass> marshaller = factory.create(schema, TruncatedRecClass.class);

        BinaryRow row = marshaller.marshal(rec);

        TruncatedRecClass restoredRec = marshaller.unmarshal(new Row(schema, row));

        assertTrue(rec.getClass().isInstance(restoredRec));

        assertEquals(rec, restoredRec);
    }


    /**
     * Check ability to read into truncated class, but not write it.
     */
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void testReadOnly(MarshallerFactory factory) throws MarshallerException {
        Column[] valCols = new Column[] {
                new Column("fbyte1", INT32, false),
                new Column("fbyte2", INT32, false)
        };

        SchemaDescriptor schema = new SchemaDescriptor(1, KEY_COLS, valCols);

        final FullRecClass rec = new FullRecClass(1, 1, 2);

        RecordMarshaller<FullRecClass> marshallerFull = factory.create(schema, FullRecClass.class);

        BinaryRow row = marshallerFull.marshal(rec);

        RecordMarshaller<TruncatedRecClass> marshaller = factory.create(schema, TruncatedRecClass.class);

        TruncatedRecClass restoredRec = marshaller.unmarshal(new Row(schema, row));

        assertEquals(rec.id, restoredRec.id);
        assertEquals(rec.fbyte1, restoredRec.fbyte1);

        assertThrows(MarshallerException.class, () -> marshaller.marshal(restoredRec));
    }

    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void truncatedKey(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(1, new Column[]{
                new Column("k1", INT32, false),
                new Column("k2", INT32, false)},
                new Column[]{new Column("v1", STRING, false)}
        );
        RecordMarshaller<TestK1K2V1> marshallerFull = factory.create(schema, TestK1K2V1.class);

        TestK1K2V1 fullRec = new TestK1K2V1(1, 1, "v1");

        BinaryRow row = marshallerFull.marshal(fullRec);

        Object restoredRec = marshallerFull.unmarshal(new Row(schema, row));

        assertTrue(fullRec.getClass().isInstance(restoredRec));

        assertThrows(IllegalArgumentException. class, () -> factory.create(schema, TestK2V1.class), "No field found for column k1");
    }

    /**
     * Test class with only one field.
     */
    public static class TruncatedRecClass {
        int id;
        int fbyte1;

        public TruncatedRecClass() {
        }

        public TruncatedRecClass(int id, int fbyte1) {
            this.id = id;
            this.fbyte1 = fbyte1;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TruncatedRecClass truncatedRecClass = (TruncatedRecClass) o;
            return id == truncatedRecClass.id && fbyte1 == truncatedRecClass.fbyte1;
        }
    }

    /**
     * Test class with all fields.
     */
    public static class FullRecClass {
        int id;
        int fbyte1;
        int fbyte2;

        public FullRecClass() {
        }

        /** Constructor. */
        public FullRecClass(int id, int fbyte1, int fbyte2) {
            this.id = id;
            this.fbyte1 = fbyte1;
            this.fbyte2 = fbyte2;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FullRecClass that = (FullRecClass) o;
            return id == that.id && fbyte1 == that.fbyte1 && fbyte2 == that.fbyte2;
        }
    }

    private static class TestK1K2V1 {
        private int k1;
        private int k2;
        private String v1;

        public TestK1K2V1() {

        }

        public TestK1K2V1(int k1, int k2, String v1) {
            this.k1 = k1;
            this.k2 = k2;
            this.v1 = v1;
        }
    }

    private static class TestK2V1 {
        private int k2;
        private String v1;

        public TestK2V1() {

        }

        public TestK2V1(int k2, String v1) {
            this.k2 = k2;
            this.v1 = v1;
        }
    }
}

