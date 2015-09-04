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

package org.apache.ignite.internal.portable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgnitePortables;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.portable.builder.PortableBuilderImpl;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectAllTypes;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectContainer;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectInner;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectOuter;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectPlainPortable;
import org.apache.ignite.internal.processors.cache.portable.CacheObjectPortableProcessorImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableBuilder;
import org.apache.ignite.portable.PortableIdMapper;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableObject;
import org.apache.ignite.portable.PortableTypeConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import sun.misc.Unsafe;

/**
 * Portable builder test.
 */
public class GridPortableBuilderSelfTest extends GridCommonAbstractTest {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(Key.class.getName(), Value.class.getName(),
            "org.gridgain.grid.internal.util.portable.mutabletest.*"));

        PortableTypeConfiguration customIdMapper = new PortableTypeConfiguration();

        customIdMapper.setClassName(CustomIdMapper.class.getName());
        customIdMapper.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return ~PortableContext.DFLT_ID_MAPPER.typeId(clsName);
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return typeId + ~PortableContext.DFLT_ID_MAPPER.fieldId(typeId, fieldName);
            }
        });

        marsh.setTypeConfigurations(Collections.singleton(customIdMapper));

        marsh.setConvertStringToBytes(useUtf8());

        cfg.setMarshaller(marsh);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @return Whether to use UTF8 strings.
     */
    protected boolean useUtf8() {
        return true;
    }

    /**
     *
     */
    public void testAllFieldsSerialization() {
        TestObjectAllTypes obj = new TestObjectAllTypes();
        obj.setDefaultData();
        obj.enumArr = null;

        TestObjectAllTypes deserialized = builder(toPortable(obj)).build().deserialize();

        GridTestUtils.deepEquals(obj, deserialized);
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("byteField", (byte)1);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals((byte)1, po.<Byte>field("byteField").byteValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testShortField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("shortField", (short)1);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals((short)1, po.<Short>field("shortField").shortValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("intField", 1);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1, po.<Integer>field("intField").intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("longField", 1L);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1L, po.<Long>field("longField").longValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloatField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("floatField", 1.0f);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1.0f, po.<Float>field("floatField").floatValue(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("doubleField", 1.0d);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1.0d, po.<Double>field("doubleField").doubleValue(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("charField", (char)1);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals((char)1, po.<Character>field("charField").charValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testBooleanField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("booleanField", true);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(po.<Boolean>field("booleanField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecimalField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("decimalField", BigDecimal.TEN);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(BigDecimal.TEN, po.<String>field("decimalField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("stringField", "str");

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals("str", po.<String>field("stringField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUuidField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        UUID uuid = UUID.randomUUID();

        builder.setField("uuidField", uuid);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(uuid, po.<UUID>field("uuidField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("byteArrayField", new byte[] {1, 2, 3});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new byte[] {1, 2, 3}, po.<byte[]>field("byteArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testShortArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("shortArrayField", new short[] {1, 2, 3});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new short[] {1, 2, 3}, po.<short[]>field("shortArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("intArrayField", new int[] {1, 2, 3});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new int[] {1, 2, 3}, po.<int[]>field("intArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("longArrayField", new long[] {1, 2, 3});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new long[] {1, 2, 3}, po.<long[]>field("longArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloatArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("floatArrayField", new float[] {1, 2, 3});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new float[] {1, 2, 3}, po.<float[]>field("floatArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("doubleArrayField", new double[] {1, 2, 3});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new double[] {1, 2, 3}, po.<double[]>field("doubleArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("charArrayField", new char[] {1, 2, 3});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new char[] {1, 2, 3}, po.<char[]>field("charArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBooleanArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("booleanArrayField", new boolean[] {true, false});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        boolean[] arr = po.field("booleanArrayField");

        assertEquals(2, arr.length);

        assertTrue(arr[0]);
        assertFalse(arr[1]);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecimalArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("decimalArrayField", new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN}, po.<String[]>field("decimalArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("stringArrayField", new String[] {"str1", "str2", "str3"});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new String[] {"str1", "str2", "str3"}, po.<String[]>field("stringArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUuidArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        UUID[] arr = new UUID[] {UUID.randomUUID(), UUID.randomUUID()};

        builder.setField("uuidArrayField", arr);

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(arr, po.<UUID[]>field("uuidArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("objectField", new Value(1));

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1, po.<PortableObject>field("objectField").<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectArrayField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("objectArrayField", new Value[] {new Value(1), new Value(2)});

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        Object[] arr = po.field("objectArrayField");

        assertEquals(2, arr.length);

        assertEquals(1, ((PortableObject)arr[0]).<Value>deserialize().i);
        assertEquals(2, ((PortableObject)arr[1]).<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollectionField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("collectionField", Arrays.asList(new Value(1), new Value(2)));

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        List<PortableObject> list = po.field("collectionField");

        assertEquals(2, list.size());

        assertEquals(1, list.get(0).<Value>deserialize().i);
        assertEquals(2, list.get(1).<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapField() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("mapField", F.asMap(new Key(1), new Value(1), new Key(2), new Value(2)));

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        Map<PortableObject, PortableObject> map = po.field("mapField");

        assertEquals(2, map.size());

        for (Map.Entry<PortableObject, PortableObject> e : map.entrySet())
            assertEquals(e.getKey().<Key>deserialize().i, e.getValue().<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSeveralFields() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("i", 111);
        builder.setField("f", 111.111f);
        builder.setField("iArr", new int[] {1, 2, 3});
        builder.setField("obj", new Key(1));
        builder.setField("col", Arrays.asList(new Value(1), new Value(2)));

        PortableObject po = builder.build();

        assertEquals("class".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(111, po.<Integer>field("i").intValue());
        assertEquals(111.111f, po.<Float>field("f").floatValue(), 0);
        assertTrue(Arrays.equals(new int[] {1, 2, 3}, po.<int[]>field("iArr")));
        assertEquals(1, po.<PortableObject>field("obj").<Key>deserialize().i);

        List<PortableObject> list = po.field("col");

        assertEquals(2, list.size());

        assertEquals(1, list.get(0).<Value>deserialize().i);
        assertEquals(2, list.get(1).<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffheapPortable() throws Exception {
        PortableBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("i", 111);
        builder.setField("f", 111.111f);
        builder.setField("iArr", new int[] {1, 2, 3});
        builder.setField("obj", new Key(1));
        builder.setField("col", Arrays.asList(new Value(1), new Value(2)));

        PortableObject po = builder.build();

        byte[] arr = ((CacheObjectPortableProcessorImpl)(grid(0)).context().cacheObjects()).marshal(po);

        long ptr = UNSAFE.allocateMemory(arr.length + 5);

        try {
            long ptr0 = ptr;

            UNSAFE.putBoolean(null, ptr0++, false);

            UNSAFE.putInt(ptr0, arr.length);

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF, null, ptr0 + 4, arr.length);

            PortableObject offheapObj = (PortableObject)
                ((CacheObjectPortableProcessorImpl)(grid(0)).context().cacheObjects()).unmarshal(ptr, false);

            assertEquals(PortableObjectOffheapImpl.class, offheapObj.getClass());

            assertEquals("class".hashCode(), offheapObj.typeId());
            assertEquals(100, offheapObj.hashCode());

            assertEquals(111, offheapObj.<Integer>field("i").intValue());
            assertEquals(111.111f, offheapObj.<Float>field("f").floatValue(), 0);
            assertTrue(Arrays.equals(new int[] {1, 2, 3}, offheapObj.<int[]>field("iArr")));
            assertEquals(1, offheapObj.<PortableObject>field("obj").<Key>deserialize().i);

            List<PortableObject> list = offheapObj.field("col");

            assertEquals(2, list.size());

            assertEquals(1, list.get(0).<Value>deserialize().i);
            assertEquals(2, list.get(1).<Value>deserialize().i);

            assertEquals(po, offheapObj);
            assertEquals(offheapObj, po);
        }
        finally {
            UNSAFE.freeMemory(ptr);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testBuildAndDeserialize() throws Exception {
        PortableBuilder builder = builder(Value.class.getName());

        builder.hashCode(100);

        builder.setField("i", 1);

        PortableObject po = builder.build();

        assertEquals("value".hashCode(), po.typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1, po.<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetaData2() throws Exception {
        PortableBuilder builder = builder("org.test.MetaTest2");

        builder.setField("objectField", "a", Object.class);

        PortableObject po = builder.build();

        PortableMetadata meta = po.metaData();

        assertEquals("MetaTest2", meta.typeName());
        assertEquals("Object", meta.fieldTypeName("objectField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetaData() throws Exception {
        PortableBuilder builder = builder("org.test.MetaTest");

        builder.hashCode(100);

        builder.setField("intField", 1);
        builder.setField("byteArrayField", new byte[] {1, 2, 3});

        PortableObject po = builder.build();

        PortableMetadata meta = po.metaData();

        assertEquals("MetaTest", meta.typeName());

        Collection<String> fields = meta.fields();

        assertEquals(2, fields.size());

        assertTrue(fields.contains("intField"));
        assertTrue(fields.contains("byteArrayField"));

        assertEquals("int", meta.fieldTypeName("intField"));
        assertEquals("byte[]", meta.fieldTypeName("byteArrayField"));

        builder = builder("org.test.MetaTest");

        builder.hashCode(100);

        builder.setField("intField", 2);
        builder.setField("uuidField", UUID.randomUUID());

        po = builder.build();

        meta = po.metaData();

        assertEquals("MetaTest", meta.typeName());

        fields = meta.fields();

        assertEquals(3, fields.size());

        assertTrue(fields.contains("intField"));
        assertTrue(fields.contains("byteArrayField"));
        assertTrue(fields.contains("uuidField"));

        assertEquals("int", meta.fieldTypeName("intField"));
        assertEquals("byte[]", meta.fieldTypeName("byteArrayField"));
        assertEquals("UUID", meta.fieldTypeName("uuidField"));
    }

    /**
     *
     */
    public void testGetFromCopiedObj() {
        PortableObject objStr = builder(TestObjectAllTypes.class.getName()).setField("str", "aaa").build();

        PortableBuilderImpl builder = builder(objStr);
        assertEquals("aaa", builder.getField("str"));

        builder.setField("str", "bbb");
        assertEquals("bbb", builder.getField("str"));

        assertNull(builder.getField("i_"));
        assertEquals("bbb", builder.build().<TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    public void testCopyFromInnerObjects() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new TestObjectAllTypes());
        list.add(list.get(0));

        TestObjectContainer c = new TestObjectContainer(list);

        PortableBuilderImpl builder = builder(toPortable(c));
        builder.<List>getField("foo").add("!!!");

        PortableObject res = builder.build();

        TestObjectContainer deserialized = res.deserialize();

        List deserializedList = (List)deserialized.foo;

        assertSame(deserializedList.get(0), deserializedList.get(1));
        assertEquals("!!!", deserializedList.get(2));
        assertTrue(deserializedList.get(0) instanceof TestObjectAllTypes);
    }

    /**
     *
     */
    public void testSetPortableObject() {
        PortableObject portableObj = builder(TestObjectContainer.class.getName())
            .setField("foo", toPortable(new TestObjectAllTypes()))
            .build();

        assertTrue(portableObj.<TestObjectContainer>deserialize().foo instanceof TestObjectAllTypes);
    }

    /**
     *
     */
    public void testPlainPortableObjectCopyFrom() {
        TestObjectPlainPortable obj = new TestObjectPlainPortable(toPortable(new TestObjectAllTypes()));

        PortableBuilderImpl builder = builder(toPortable(obj));
        assertTrue(builder.getField("plainPortable") instanceof PortableObject);

        TestObjectPlainPortable deserialized = builder.build().deserialize();
        assertTrue(deserialized.plainPortable instanceof PortableObject);
    }

    /**
     *
     */
    public void testRemoveFromNewObject() {
        PortableBuilder builder = builder(TestObjectAllTypes.class.getName());

        builder.setField("str", "a");

        builder.removeField("str");

        assertNull(builder.build().<TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    public void testRemoveFromExistingObject() {
        TestObjectAllTypes obj = new TestObjectAllTypes();
        obj.setDefaultData();
        obj.enumArr = null;

        PortableBuilder builder = builder(toPortable(obj));

        builder.removeField("str");

        assertNull(builder.build().<TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    public void testRemoveFromExistingObjectAfterGet() {
        TestObjectAllTypes obj = new TestObjectAllTypes();
        obj.setDefaultData();
        obj.enumArr = null;

        PortableBuilderImpl builder = builder(toPortable(obj));

        builder.getField("i_");

        builder.removeField("str");

        assertNull(builder.build().<TestObjectAllTypes>deserialize().str);
    }

    /**
     * @throws IgniteCheckedException If any error occurs.
     */
    public void testDontBrokeCyclicDependency() throws IgniteCheckedException {
        TestObjectOuter outer = new TestObjectOuter();
        outer.inner = new TestObjectInner();
        outer.inner.outer = outer;
        outer.foo = "a";

        PortableBuilder builder = builder(toPortable(outer));

        builder.setField("foo", "b");

        TestObjectOuter res = builder.build().deserialize();

        assertEquals("b", res.foo);
        assertSame(res, res.inner.outer);
    }

    /**
     * @return Portables.
     */
    private IgnitePortables portables() {
        return grid(0).portables();
    }

    /**
     * @param obj Object.
     * @return Portable object.
     */
    private PortableObject toPortable(Object obj) {
        return portables().toPortable(obj);
    }

    /**
     * @return Builder.
     */
    private <T> PortableBuilder builder(int typeId) {
        return portables().builder(typeId);
    }

    /**
     * @return Builder.
     */
    private <T> PortableBuilder builder(String clsName) {
        return portables().builder(clsName);
    }

    /**
     * @return Builder.
     */
    private <T> PortableBuilderImpl builder(PortableObject obj) {
        return (PortableBuilderImpl)portables().builder(obj);
    }

    /**
     *
     */
    private static class CustomIdMapper {
        /** */
        private String str = "a";

        /** */
        private int i = 10;
    }

    /**
     */
    private static class Key {
        /** */
        private int i;

        /**
         */
        private Key() {
            // No-op.
        }

        /**
         * @param i Index.
         */
        private Key(int i) {
            this.i = i;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key = (Key)o;

            return i == key.i;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return i;
        }
    }

    /**
     */
    private static class Value {
        /** */
        private int i;

        /**
         */
        private Value() {
            // No-op.
        }

        /**
         * @param i Index.
         */
        private Value(int i) {
            this.i = i;
        }
    }
}