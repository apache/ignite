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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.function.Function;
import org.apache.ignite.internal.schema.testobjects.TestOuterObject;
import org.apache.ignite.internal.schema.testobjects.TestOuterObject.NestedObject;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.MapperBuilder;
import org.apache.ignite.table.mapper.OneColumnMapper;
import org.apache.ignite.table.mapper.PojoMapper;
import org.apache.ignite.table.mapper.TypeConverter;
import org.junit.jupiter.api.Test;

/**
 * Columns mappers test.
 */
public class MapperTest {

    @Test
    public void supportedClassKinds() {
        class LocalClass {
            long id;
        }

        Function anonymous = (i) -> i;

        Mapper.of(Long.class);
        Mapper.of(TestOuterObject.class);
        Mapper.of(NestedObject.class);
        Mapper.of(ArrayList.class);
        Mapper.of(byte[].class);

        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestOuterObject.InnerObject.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(LocalClass.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(AbstractTestObject.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(anonymous.getClass()));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(int[].class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(Object[].class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestInterface.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestAnnotation.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(EnumTestObject.class));

        Mapper.of(Long.class, "column");
        Mapper.of(byte[].class, "column");
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestOuterObject.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(NestedObject.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(AbstractTestObject.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(int[].class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(Object.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(ArrayList.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestInterface.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestOuterObject.InnerObject.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(LocalClass.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(anonymous.getClass(), "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(TestAnnotation.class, "column"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(EnumTestObject.class, "column"));
    }

    @Test
    public void testNativeTypeMapping() {
        TestConverter conv = new TestConverter();

        // One-column mapping
        for (Class<?> c : new Class[]{
                Byte.class,
                Short.class,
                Integer.class,
                Long.class,
                Float.class,
                Double.class,
                BigInteger.class,
                BigDecimal.class,
                BitSet.class,
                byte[].class,
                String.class,
                LocalDate.class,
                LocalTime.class,
                LocalDateTime.class,
        }) {
            assertNull(((OneColumnMapper<?>) Mapper.of(c)).mappedColumn());

            assertEquals("col1", ((OneColumnMapper<?>) Mapper.of(c, "col1")).mappedColumn());
            assertNull(((OneColumnMapper<?>) Mapper.of(c, "col1")).converter());
        }

        // One-column mapping with converter.
        assertEquals("col1", ((OneColumnMapper<String>) Mapper.of(String.class, "col1", conv)).mappedColumn());

        assertNotNull(((OneColumnMapper<String>) Mapper.of(String.class, "col1", conv)).converter());

        // Multi-column mapping
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(String.class, "value", "col1"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(String.class, "value", "col1", "coder", "col2"));
        assertThrows(IllegalArgumentException.class, () -> Mapper.of(String.class, "value", "col1", "coder", "col2"));
    }

    @Test
    public void testPojoMapping() {
        TypeConverter<TestObject, byte[]> conv = new TypeConverter<>() {
            @Override
            public byte[] toColumnType(TestObject obj) {
                return new byte[0];
            }

            @Override
            public TestObject toObjectType(byte[] data) {
                return null;
            }
        };

        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.of(TestObject.class);

            assertEquals("id", mapper.fieldForColumn("id"));
            assertEquals("longCol", mapper.fieldForColumn("longCol"));
            assertEquals("stringCol", mapper.fieldForColumn("stringCol"));
            assertNull(mapper.fieldForColumn("val"));
        }

        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.of(TestObject.class, "id", "col1");

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertNull(mapper.fieldForColumn("id"));
            assertNull(mapper.fieldForColumn("longCol"));
            assertNull(mapper.fieldForColumn("stringCol"));
            assertNull(mapper.fieldForColumn("val"));
        }

        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.of(TestObject.class, "id", "col1", "stringCol", "stringCol");

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertEquals("stringCol", mapper.fieldForColumn("stringCol"));
            assertNull(mapper.fieldForColumn("longCol"));
            assertNull(mapper.fieldForColumn("val"));
        }

        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.of(TestObject.class, "id", "col1");

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertNull(mapper.fieldForColumn("longCol"));
            assertNull(mapper.fieldForColumn("stringCol"));
            assertNull(mapper.fieldForColumn("val"));
        }

        {
            assertEquals("col1", ((OneColumnMapper<TestObject>) Mapper.of(TestObject.class, "col1", conv)).mappedColumn());
        }
    }

    @Test
    public void builderSupportsClassKinds() {
        class LocalClass {
            long id;
        }

        Function anonymous = (i) -> i;

        Mapper.builder(TestOuterObject.class);
        Mapper.builder(NestedObject.class);

        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestOuterObject.InnerObject.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(AbstractTestObject.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(LocalClass.class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(anonymous.getClass()));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(int[].class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(Object[].class));
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestInterface.class)); // Interface
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestAnnotation.class)); // annotation
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(EnumTestObject.class)); // enum
    }

    @Test
    public void misleadingBuilderUsage() {
        // Empty mapping.
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class).build());

        // Many fields to one column.
        {
            assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                                                                       .map("id", "key")
                                                                       .map("longCol", "key")
            );
            assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                                                                       .map("id", "key")
                                                                       .map("longCol", "key", new TestConverter())
            );
            assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                                                                       .map("id", "key", "longCol", "key")
            );
        }

        // Missed column name
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                                                                   .map("id", "id", "longCol"));

        // One field to many columns.
        {
            assertThrows(IllegalStateException.class, () -> Mapper.builder(TestObject.class)
                                                                    .map("id", "key")
                                                                    .map("id", "val1")
                                                                    .map("stringCol", "val2")
                                                                    .build()
            );
            assertThrows(IllegalStateException.class, () -> Mapper.builder(TestObject.class)
                                                                    .map("id", "key")
                                                                    .map("id", "val1", new TestConverter())
                                                                    .map("stringCol", "val2")
                                                                    .build()
            );
            assertThrows(IllegalStateException.class, () -> Mapper.builder(TestObject.class)
                                                                    .map("id", "key", "id", "val1", "stringCol", "val2")
                                                                    .build()
            );
        }

        // Invalid field name
        {
            assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                                                                       .map("val", "val"));

            assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                                                                       .map("val", "val", new TestConverter()));

            assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                                                                       .map("id", "id", "val", "val"));
        }

        // Duplicate converters.
        assertThrows(IllegalArgumentException.class, () -> Mapper.builder(TestObject.class)
                                                                   .map("id", "key")
                                                                   .convert("val1", new TestConverter())
                                                                   .convert("val1", new TestConverter())
        );

        MapperBuilder<TestObject> usedBuilder = Mapper.builder(TestObject.class).map("id", "key");
        usedBuilder.build();

        // Mapper builder reuse fails.
        assertThrows(IllegalStateException.class, () -> usedBuilder.map("id", "key"));
        assertThrows(IllegalStateException.class, () -> usedBuilder.map("id", "key", "longCol", "val"));
        assertThrows(IllegalStateException.class, () -> usedBuilder.map("id", "key", new TestConverter()));
        assertThrows(IllegalStateException.class, usedBuilder::automap);
    }


    @Test
    public void mapperBuilder() {
        // Automapping.
        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.builder(TestObject.class).automap().build();

            assertEquals("id", mapper.fieldForColumn("id"));
            assertEquals("longCol", mapper.fieldForColumn("longCol"));
            assertEquals("stringCol", mapper.fieldForColumn("stringCol"));
            assertNull(mapper.fieldForColumn("val"));
        }

        // Automap call order
        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.builder(TestObject.class)
                                                                             .map("id", "col1").automap().build();

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertNull(mapper.fieldForColumn("id"));
            assertEquals("longCol", mapper.fieldForColumn("longCol"));
            assertEquals("stringCol", mapper.fieldForColumn("stringCol"));
            assertNull(mapper.fieldForColumn("val"));
        }

        // Automap call reverse order
        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.builder(TestObject.class)
                                                                             .automap().map("id", "col1").build();

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertNull(mapper.fieldForColumn("id"));
            assertEquals("longCol", mapper.fieldForColumn("longCol"));
            assertEquals("stringCol", mapper.fieldForColumn("stringCol"));
            assertNull(mapper.fieldForColumn("val"));
        }

        // Converter call order
        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.builder(TestObject.class)
                                                                             .map("id", "col1").convert("col1", new TestConverter())
                                                                             .build();

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertNotNull(mapper.converterForColumn("col1"));

            assertNull(mapper.fieldForColumn("id"));
            assertNull(mapper.converterForColumn("id"));
        }

        // Converter call order
        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.builder(TestObject.class)
                                                                             .convert("col1", new TestConverter())
                                                                             .map("id", "col1").build();

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertNotNull(mapper.converterForColumn("col1"));

            assertNull(mapper.fieldForColumn("id"));
            assertNull(mapper.converterForColumn("id"));
        }

        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.builder(TestObject.class)
                                                                             .convert("col2", new TestConverter())
                                                                             .map("id", "col1", new TestConverter()).build();

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertNull(mapper.fieldForColumn("col2")); // OK. Orphan converter will never used.
            assertNotNull(mapper.converterForColumn("col1"));
            assertNotNull(mapper.converterForColumn("col2"));

            assertNull(mapper.fieldForColumn("id"));
            assertNull(mapper.converterForColumn("id"));
        }

        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.builder(TestObject.class)
                                                                             .map("id", "col1", new TestConverter())
                                                                             .map("stringCol", "stringCol", new TestConverter()).build();

            assertEquals("id", mapper.fieldForColumn("col1"));
            assertEquals("stringCol", mapper.fieldForColumn("stringCol"));
            assertNotNull(mapper.converterForColumn("col1"));
            assertNotNull(mapper.converterForColumn("stringCol"));

            assertNull(mapper.fieldForColumn("id"));
            assertNull(mapper.converterForColumn("id"));
        }

        // Converter with automap
        {
            PojoMapper<TestObject> mapper = (PojoMapper<TestObject>) Mapper.builder(TestObject.class)
                                                                             .convert("col1", new TestConverter()).automap().build();

            assertEquals("id", mapper.fieldForColumn("id"));
            assertNotNull(mapper.converterForColumn("col1"));
            assertNull(mapper.converterForColumn("id"));
        }
    }

    /**
     * Test converter.
     */
    static class TestConverter implements TypeConverter<String, Integer> {
        @Override
        public String toObjectType(Integer obj) {
            return obj == null ? null : obj.toString();
        }

        @Override
        public Integer toColumnType(String data) {
            return data == null ? null : Integer.parseInt(data);
        }
    }

    /**
     * Test object.
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "unused"})
    static class TestObject {
        private long id;

        private long longCol;

        private String stringCol;
    }

    /**
     * Test object.
     */
    @SuppressWarnings({"InstanceVariableMayNotBeInitialized", "unused"})
    abstract static class AbstractTestObject {
        private long id;
    }

    /**
     * Test object.
     */
    enum EnumTestObject {
        ONE,
        TWO
    }

    /**
     * Test object.
     */
    @interface TestAnnotation {
        long id = 0L;
    }

    /**
     * Test object.
     */
    interface TestInterface {
        int id = 0;
    }
}
