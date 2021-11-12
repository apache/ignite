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

import static org.apache.ignite.internal.schema.NativeTypes.BYTES;
import static org.apache.ignite.internal.schema.NativeTypes.DATE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INT16;
import static org.apache.ignite.internal.schema.NativeTypes.INT32;
import static org.apache.ignite.internal.schema.NativeTypes.INT64;
import static org.apache.ignite.internal.schema.NativeTypes.INT8;
import static org.apache.ignite.internal.schema.NativeTypes.STRING;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;
import static org.apache.ignite.internal.schema.NativeTypes.datetime;
import static org.apache.ignite.internal.schema.NativeTypes.time;
import static org.apache.ignite.internal.schema.NativeTypes.timestamp;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.facebook.presto.bytecode.Access;
import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.ClassDefinition;
import com.facebook.presto.bytecode.ClassGenerator;
import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.bytecode.MethodDefinition;
import com.facebook.presto.bytecode.ParameterizedType;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.expression.BytecodeExpressions;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.processing.Generated;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.reflection.ReflectionMarshallerFactory;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithAllTypes;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithNoDefaultConstructor;
import org.apache.ignite.internal.schema.testobjects.TestObjectWithPrivateConstructor;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.ObjectFactory;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * RecordMarshaller test.
 */
public class RecordMarshallerTest {
    /**
     * Returns list of marshaller factories for the test.
     */
    private static List<MarshallerFactory> marshallerFactoryProvider() {
        return List.of(new ReflectionMarshallerFactory());
    }
    
    /** Random. */
    private Random rnd;
    
    /**
     * Init random.
     */
    @BeforeEach
    public void initRandom() {
        long seed = System.currentTimeMillis();
        
        System.out.println("Using seed: " + seed + "L;");
        
        rnd = new Random(seed);
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void complexType(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(1, keyColumns(), valueColumnsAllTypes());
        
        final TestObjectWithAllTypes rec = TestObjectWithAllTypes.randomObject(rnd);
        
        RecordMarshaller<TestObjectWithAllTypes> marshaller = factory.create(schema, TestObjectWithAllTypes.class);
        
        BinaryRow row = marshaller.marshal(rec);
        
        TestObjectWithAllTypes restoredRec = marshaller.unmarshal(new Row(schema, row));
        
        assertTrue(rec.getClass().isInstance(restoredRec));
        
        assertEquals(rec, restoredRec);
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void truncatedType(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(1, keyColumns(), valueColumnsAllTypes());
        
        RecordMarshaller<TestTruncatedObject> marshaller = factory.create(schema, TestTruncatedObject.class);
        
        final TestTruncatedObject rec = TestTruncatedObject.randomObject(rnd);
        
        BinaryRow row = marshaller.marshal(rec);
        
        Object restoredRec = marshaller.unmarshal(new Row(schema, row));
        
        assertTrue(rec.getClass().isInstance(restoredRec));
        
        assertEquals(rec, restoredRec);
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void widerType(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                keyColumns(),
                new Column[]{
                        new Column("primitiveDoubleCol", DOUBLE, false),
                        new Column("stringCol", STRING, true),
                }
        );
        
        RecordMarshaller<TestObjectWithAllTypes> marshaller = factory.create(schema, TestObjectWithAllTypes.class);
        
        final TestObjectWithAllTypes rec = TestObjectWithAllTypes.randomObject(rnd);
        
        BinaryRow row = marshaller.marshal(rec);
        
        TestObjectWithAllTypes restoredRec = marshaller.unmarshal(new Row(schema, row));
        
        assertTrue(rec.getClass().isInstance(restoredRec));
        
        TestObjectWithAllTypes expectedRec = new TestObjectWithAllTypes();
        
        expectedRec.setPrimitiveLongCol(rec.getPrimitiveLongCol());
        expectedRec.setIntCol(rec.getIntCol());
        expectedRec.setPrimitiveDoubleCol(rec.getPrimitiveDoubleCol());
        expectedRec.setStringCol(rec.getStringCol());
        
        assertEquals(expectedRec, restoredRec);
        
        // Check non-mapped fields has default values.
        assertNull(restoredRec.getUuidCol());
        assertEquals(0, restoredRec.getPrimitiveIntCol());
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void mapping(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(1,
                new Column[]{new Column("key", INT64, false)},
                new Column[]{
                        new Column("col1", INT32, false),
                        new Column("col2", INT64, true),
                        new Column("col3", STRING, false)
                });
        
        Mapper<TestObject> mapper = Mapper.builderFor(TestObject.class)
                .map("id", "key")
                .map("intCol", "col1")
                .map("stringCol", "col3")
                .build();
        
        RecordMarshaller<TestObject> marshaller = factory.create(schema, mapper);
        
        final TestObject rec = TestObject.randomObject(rnd);
        
        BinaryRow row = marshaller.marshal(rec);
        
        Object restoredRec = marshaller.unmarshal(new Row(schema, row));
        
        assertTrue(rec.getClass().isInstance(restoredRec));
        
        rec.longCol2 = null; // Nullify non-mapped field.
        
        assertEquals(rec, restoredRec);
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithWrongFieldType(MarshallerFactory factory) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                keyColumns(),
                new Column[]{
                        new Column("bitmaskCol", NativeTypes.bitmaskOf(42), true),
                        new Column("shortCol", UUID, true)
                }
        );
        
        RecordMarshaller<TestObjectWithAllTypes> marshaller = factory.create(schema, TestObjectWithAllTypes.class);
        
        final TestObjectWithAllTypes rec = TestObjectWithAllTypes.randomObject(rnd);
        
        assertThrows(
                MarshallerException.class,
                () -> marshaller.marshal(rec),
                "Failed to write field [name=shortCol]"
        );
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithIncorrectBitmaskSize(MarshallerFactory factory) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                keyColumns(),
                new Column[]{
                        new Column("primitiveLongCol", INT64, false),
                        new Column("bitmaskCol", NativeTypes.bitmaskOf(9), true),
                }
        );
        
        RecordMarshaller<TestObjectWithAllTypes> marshaller = factory.create(schema, TestObjectWithAllTypes.class);
        
        final TestObjectWithAllTypes rec = TestObjectWithAllTypes.randomObject(rnd);
        
        assertThrows(
                MarshallerException.class,
                () -> marshaller.marshal(rec),
                "Failed to write field [name=bitmaskCol]"
        );
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithPrivateConstructor(MarshallerFactory factory) throws MarshallerException, IllegalAccessException {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("primLongCol", INT64, false)},
                new Column[]{new Column("primIntCol", INT32, false)}
        );
        
        RecordMarshaller<TestObjectWithPrivateConstructor> marshaller = factory.create(schema, TestObjectWithPrivateConstructor.class);
        
        final TestObjectWithPrivateConstructor rec = TestObjectWithPrivateConstructor.randomObject(rnd);
        
        BinaryRow row = marshaller.marshal(rec);
        
        TestObjectWithPrivateConstructor restoredRec = marshaller.unmarshal(new Row(schema, row));
        
        assertDeepEquals(TestObjectWithPrivateConstructor.class, rec, restoredRec);
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classWithNoDefaultConstructor(MarshallerFactory factory) {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("primLongCol", INT64, false)},
                new Column[]{new Column("primIntCol", INT32, false)}
        );
        
        final Object rec = TestObjectWithNoDefaultConstructor.randomObject(rnd);
        
        assertThrows(IgniteInternalException.class, () -> factory.create(schema, rec.getClass()));
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void privateClass(MarshallerFactory factory) throws MarshallerException {
        SchemaDescriptor schema = new SchemaDescriptor(
                1,
                new Column[]{new Column("primLongCol", INT64, false)},
                new Column[]{new Column("primIntCol", INT32, false)}
        );
        
        final ObjectFactory<PrivateTestObject> objFactory = new ObjectFactory<>(PrivateTestObject.class);
        final RecordMarshaller<PrivateTestObject> marshaller = factory
                .create(schema, PrivateTestObject.class);
        
        final PrivateTestObject rec = PrivateTestObject.randomObject(rnd);
        
        BinaryRow row = marshaller.marshal(objFactory.create());
        
        Object restoredRec = marshaller.unmarshal(new Row(schema, row));
        
        assertTrue(rec.getClass().isInstance(restoredRec));
    }
    
    @ParameterizedTest
    @MethodSource("marshallerFactoryProvider")
    public void classLoader(MarshallerFactory factory) throws MarshallerException, IllegalAccessException {
        final ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(new DynamicClassLoader(getClass().getClassLoader()));
            
            Column[] keyCols = new Column[]{
                    new Column("key", INT64, false)
            };
            
            Column[] valCols = new Column[]{
                    new Column("col0", INT64, false),
                    new Column("col1", INT64, false),
                    new Column("col2", INT64, false),
            };
            
            SchemaDescriptor schema = new SchemaDescriptor(1, keyCols, valCols);
            
            final Class<Object> recClass = (Class<Object>) createGeneratedObjectClass();
            final ObjectFactory<Object> objFactory = new ObjectFactory<>(recClass);
            
            RecordMarshaller<Object> marshaller = factory.create(schema, recClass);
            
            Object rec = objFactory.create();
            
            BinaryRow row = marshaller.marshal(rec);
            
            Object restoredRec = marshaller.unmarshal(new Row(schema, row));
            
            assertDeepEquals(recClass, rec, restoredRec);
        } finally {
            Thread.currentThread().setContextClassLoader(loader);
        }
    }
    
    /**
     * Validate all types are tested.
     */
    @Test
    public void ensureAllTypesChecked() {
        Set<NativeTypeSpec> testedTypes = Stream.concat(Arrays.stream(keyColumns()), Arrays.stream(valueColumnsAllTypes()))
                .map(c -> c.type().spec())
                .collect(Collectors.toSet());
        
        Set<NativeTypeSpec> missedTypes = Arrays.stream(NativeTypeSpec.values())
                .filter(t -> !testedTypes.contains(t)).collect(Collectors.toSet());
        
        assertEquals(Collections.emptySet(), missedTypes);
    }
    
    /**
     * Generate class for test objects.
     *
     * @return Generated test object class.
     */
    private Class<?> createGeneratedObjectClass() {
        final String packageName = getClass().getPackageName();
        final String className = "GeneratedTestObject";
        
        final ClassDefinition classDef = new ClassDefinition(
                EnumSet.of(Access.PUBLIC),
                packageName.replace('.', '/') + '/' + className,
                ParameterizedType.type(Object.class)
        );
        classDef.declareAnnotation(Generated.class).setValue("value", getClass().getCanonicalName());
        
        classDef.declareField(EnumSet.of(Access.PRIVATE), "key", ParameterizedType.type(long.class));
        
        for (int i = 0; i < 3; i++) {
            classDef.declareField(EnumSet.of(Access.PRIVATE), "col" + i, ParameterizedType.type(long.class));
        }
        
        // Build constructor.
        final MethodDefinition methodDef = classDef.declareConstructor(EnumSet.of(Access.PUBLIC));
        final Variable rnd = methodDef.getScope().declareVariable(Random.class, "rnd");
        
        BytecodeBlock body = methodDef.getBody()
                .append(methodDef.getThis())
                .invokeConstructor(classDef.getSuperClass())
                .append(rnd.set(BytecodeExpressions.newInstance(Random.class)));
        
        body.append(methodDef.getThis().setField("key", rnd.invoke("nextLong", long.class).cast(long.class)));
        
        for (int i = 0; i < 3; i++) {
            body.append(methodDef.getThis().setField("col" + i, rnd.invoke("nextLong", long.class).cast(long.class)));
        }
        
        body.ret();
        
        return ClassGenerator.classGenerator(Thread.currentThread().getContextClassLoader())
                .fakeLineNumbers(true)
                .runAsmVerifier(true)
                .dumpRawBytecode(true)
                .defineClass(classDef, Object.class);
    }
    
    private <T> void assertDeepEquals(Class<T> recClass, T rec, T restoredRec) throws IllegalAccessException {
        assertTrue(recClass.isInstance(restoredRec));
        
        for (Field fld : recClass.getDeclaredFields()) {
            fld.setAccessible(true);
            assertEquals(fld.get(rec), fld.get(restoredRec), fld.getName());
        }
    }
    
    private Column[] keyColumns() {
        return new Column[]{
                new Column("primitiveLongCol", INT64, false),
                new Column("intCol", INT32, true)
        };
    }
    
    private Column[] valueColumnsAllTypes() {
        return new Column[]{
                new Column("primitiveByteCol", INT8, false, () -> (byte) 0x42),
                new Column("primitiveShortCol", INT16, false, () -> (short) 0x4242),
                new Column("primitiveIntCol", INT32, false, () -> 0x42424242),
                new Column("primitiveFloatCol", FLOAT, false),
                new Column("primitiveDoubleCol", DOUBLE, false),
                
                new Column("byteCol", INT8, true),
                new Column("shortCol", INT16, true),
                new Column("longCol", INT64, true),
                new Column("nullLongCol", INT64, true),
                new Column("floatCol", FLOAT, true),
                new Column("doubleCol", DOUBLE, true),
                
                new Column("dateCol", DATE, true),
                new Column("timeCol", time(), true),
                new Column("dateTimeCol", datetime(), true),
                new Column("timestampCol", timestamp(), true),
                
                new Column("uuidCol", UUID, true),
                new Column("bitmaskCol", NativeTypes.bitmaskOf(42), true),
                new Column("stringCol", STRING, true),
                new Column("nullBytesCol", BYTES, true),
                new Column("bytesCol", BYTES, true),
                new Column("numberCol", NativeTypes.numberOf(12), true),
                new Column("decimalCol", NativeTypes.decimalOf(19, 3), true),
        };
    }
    
    /**
     * Test object.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestObject {
        static TestObject randomObject(Random rnd) {
            final TestObject obj = new TestObject();
            
            obj.id = rnd.nextLong();
            obj.intCol = rnd.nextInt();
            obj.longCol2 = rnd.nextLong();
            obj.stringCol = IgniteTestUtils.randomString(rnd, 100);
            
            return obj;
        }
        
        private long id;
        
        private int intCol;
        
        private Long longCol2;
        
        private String stringCol;
        
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            
            TestObject that = (TestObject) o;
            
            return id == that.id
                    && intCol == that.intCol
                    && Objects.equals(longCol2, that.longCol2)
                    && Objects.equals(stringCol, that.stringCol);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
    
    /**
     * Test object with less amount of fields.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    public static class TestTruncatedObject {
        static TestTruncatedObject randomObject(Random rnd) {
            final TestTruncatedObject obj = new TestTruncatedObject();
            
            obj.primitiveIntCol = rnd.nextInt();
            obj.primitiveLongCol = rnd.nextLong();
            obj.primitiveDoubleCol = rnd.nextDouble();
            
            obj.uuidCol = java.util.UUID.randomUUID();
            obj.stringCol = IgniteTestUtils.randomString(rnd, 100);
            
            return obj;
        }
        
        // Primitive typed
        private int primitiveIntCol;
        
        private long primitiveLongCol;
        
        private float primitiveFloatCol;
        
        private double primitiveDoubleCol;
        
        private String stringCol;
        
        private java.util.UUID uuidCol;
        
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            
            TestTruncatedObject object = (TestTruncatedObject) o;
            
            return primitiveIntCol == object.primitiveIntCol
                    && primitiveLongCol == object.primitiveLongCol
                    && Float.compare(object.primitiveFloatCol, primitiveFloatCol) == 0
                    && Double.compare(object.primitiveDoubleCol, primitiveDoubleCol) == 0
                    && Objects.equals(stringCol, ((TestTruncatedObject) o).stringCol)
                    && Objects.equals(uuidCol, ((TestTruncatedObject) o).uuidCol);
        }
        
        @Override
        public int hashCode() {
            return 42;
        }
    }
    
    /**
     * Test object without default constructor.
     */
    @SuppressWarnings("InstanceVariableMayNotBeInitialized")
    private static class PrivateTestObject {
        static PrivateTestObject randomObject(Random rnd) {
            return new PrivateTestObject(rnd.nextLong(), rnd.nextInt());
        }
        
        private long primLongCol;
        
        private int primIntCol;
        
        PrivateTestObject() {
        }
        
        PrivateTestObject(long longVal, int intVal) {
            primLongCol = longVal;
            primIntCol = intVal;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            
            PrivateTestObject object = (PrivateTestObject) o;
            
            return primLongCol == object.primLongCol && primIntCol == object.primIntCol;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(primLongCol);
        }
    }
}
