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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.portable.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectAllTypes;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectContainer;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectInner;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectOuter;
import org.apache.ignite.internal.portable.mutabletest.GridPortableTestClasses.TestObjectPlainPortable;
import org.apache.ignite.internal.processors.cache.portable.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import sun.misc.Unsafe;

/**
 * Portable builder test.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class BinaryObjectBuilderSelfTest extends GridCommonAbstractTest {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        BinaryTypeConfiguration customTypeCfg = new BinaryTypeConfiguration();

        customTypeCfg.setTypeName(CustomIdMapper.class.getName());
        customTypeCfg.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return ~BinaryInternalIdMapper.defaultInstance().typeId(clsName);
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return typeId + ~BinaryInternalIdMapper.defaultInstance().fieldId(typeId, fieldName);
            }
        });

        BinaryConfiguration bCfg = new BinaryConfiguration();
        
        bCfg.setCompactFooter(compactFooter());

        bCfg.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(Key.class.getName()),
            new BinaryTypeConfiguration(Value.class.getName()),
            new BinaryTypeConfiguration("org.gridgain.grid.internal.util.portable.mutabletest.*"),
            customTypeCfg));

        cfg.setBinaryConfiguration(bCfg);

        cfg.setMarshaller(new BinaryMarshaller());

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
     * @return Whether to use compact footer.
     */
    protected boolean compactFooter() {
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
    public void testNullField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(42);

        builder.setField("objField", (Object)null);

        builder.setField("otherField", "value");

        BinaryObject obj = builder.build();

        assertNull(obj.field("objField"));
        assertEquals("value", obj.field("otherField"));
        assertEquals(42, obj.hashCode());

        builder = builder(obj);

        builder.setField("objField", "value");
        builder.setField("otherField", (Object)null);

        obj = builder.build();

        assertNull(obj.field("otherField"));
        assertEquals("value", obj.field("objField"));
        assertEquals(42, obj.hashCode());
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("byteField", (byte)1);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals((byte) 1, po.<Byte>field("byteField").byteValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testShortField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("shortField", (short)1);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals((short)1, po.<Short>field("shortField").shortValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("intField", 1);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1, po.<Integer>field("intField").intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("longField", 1L);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1L, po.<Long>field("longField").longValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloatField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("floatField", 1.0f);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1.0f, po.<Float>field("floatField").floatValue(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("doubleField", 1.0d);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1.0d, po.<Double>field("doubleField").doubleValue(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("charField", (char)1);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals((char)1, po.<Character>field("charField").charValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testBooleanField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("booleanField", true);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(po.<Boolean>field("booleanField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecimalField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("decimalField", BigDecimal.TEN);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(BigDecimal.TEN, po.<BigDecimal>field("decimalField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("stringField", "str");

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals("str", po.<String>field("stringField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDateField() throws Exception {
        Date date = new Date();

        assertEquals(date, builder("C").setField("d", date).build().<Date>field("d"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimestampField() throws Exception {
        Timestamp ts = new Timestamp(new Date().getTime());
        ts.setNanos(1000);

        assertEquals(ts, builder("C").setField("t", ts).build().<Timestamp>field("t"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUuidField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        UUID uuid = UUID.randomUUID();

        builder.setField("uuidField", uuid);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(uuid, po.<UUID>field("uuidField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("byteArrayField", new byte[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new byte[] {1, 2, 3}, po.<byte[]>field("byteArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testShortArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("shortArrayField", new short[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new short[] {1, 2, 3}, po.<short[]>field("shortArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("intArrayField", new int[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new int[] {1, 2, 3}, po.<int[]>field("intArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("longArrayField", new long[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new long[] {1, 2, 3}, po.<long[]>field("longArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloatArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("floatArrayField", new float[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new float[] {1, 2, 3}, po.<float[]>field("floatArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("doubleArrayField", new double[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new double[] {1, 2, 3}, po.<double[]>field("doubleArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("charArrayField", new char[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new char[] {1, 2, 3}, po.<char[]>field("charArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBooleanArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("booleanArrayField", new boolean[] {true, false});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
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
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("decimalArrayField", new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN}, po.<String[]>field("decimalArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("stringArrayField", new String[] {"str1", "str2", "str3"});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(new String[] {"str1", "str2", "str3"}, po.<String[]>field("stringArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDateArrayField() throws Exception {
        Date date1 = new Date();
        Date date2 = new Date(date1.getTime() + 1000);

        Date[] dateArr = new Date[] { date1, date2 };

        assertTrue(Arrays.equals(dateArr, builder("C").setField("da", dateArr).build().<Date[]>field("da")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimestampArrayField() throws Exception {
        Timestamp ts1 = new Timestamp(new Date().getTime());
        Timestamp ts2 = new Timestamp(new Date().getTime() + 1000);

        ts1.setNanos(1000);
        ts2.setNanos(2000);

        Timestamp[] tsArr = new Timestamp[] { ts1, ts2 };

        assertTrue(Arrays.equals(tsArr, builder("C").setField("ta", tsArr).build().<Timestamp[]>field("ta")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUuidArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        UUID[] arr = new UUID[] {UUID.randomUUID(), UUID.randomUUID()};

        builder.setField("uuidArrayField", arr);

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertTrue(Arrays.equals(arr, po.<UUID[]>field("uuidArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("objectField", new Value(1));

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1, po.<BinaryObject>field("objectField").<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("objectArrayField", new Value[] {new Value(1), new Value(2)});

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        Object[] arr = po.field("objectArrayField");

        assertEquals(2, arr.length);

        assertEquals(1, ((BinaryObject)arr[0]).<Value>deserialize().i);
        assertEquals(2, ((BinaryObject)arr[1]).<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollectionField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("collectionField", Arrays.asList(new Value(1), new Value(2)));

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        List<BinaryObject> list = po.field("collectionField");

        assertEquals(2, list.size());

        assertEquals(1, list.get(0).<Value>deserialize().i);
        assertEquals(2, list.get(1).<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("mapField", F.asMap(new Key(1), new Value(1), new Key(2), new Value(2)));

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        Map<BinaryObject, BinaryObject> map = po.field("mapField");

        assertEquals(2, map.size());

        for (Map.Entry<BinaryObject, BinaryObject> e : map.entrySet())
            assertEquals(e.getKey().<Key>deserialize().i, e.getValue().<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSeveralFields() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("i", 111);
        builder.setField("f", 111.111f);
        builder.setField("iArr", new int[] {1, 2, 3});
        builder.setField("obj", new Key(1));
        builder.setField("col", Arrays.asList(new Value(1), new Value(2)));

        BinaryObject po = builder.build();

        assertEquals("class".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(111, po.<Integer>field("i").intValue());
        assertEquals(111.111f, po.<Float>field("f").floatValue(), 0);
        assertTrue(Arrays.equals(new int[] {1, 2, 3}, po.<int[]>field("iArr")));
        assertEquals(1, po.<BinaryObject>field("obj").<Key>deserialize().i);

        List<BinaryObject> list = po.field("col");

        assertEquals(2, list.size());

        assertEquals(1, list.get(0).<Value>deserialize().i);
        assertEquals(2, list.get(1).<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffheapPortable() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.hashCode(100);

        builder.setField("i", 111);
        builder.setField("f", 111.111f);
        builder.setField("iArr", new int[] {1, 2, 3});
        builder.setField("obj", new Key(1));
        builder.setField("col", Arrays.asList(new Value(1), new Value(2)));

        BinaryObject po = builder.build();

        byte[] arr = ((CacheObjectBinaryProcessorImpl)(grid(0)).context().cacheObjects()).marshal(po);

        long ptr = UNSAFE.allocateMemory(arr.length + 5);

        try {
            long ptr0 = ptr;

            UNSAFE.putBoolean(null, ptr0++, false);

            UNSAFE.putInt(ptr0, arr.length);

            UNSAFE.copyMemory(arr, BYTE_ARR_OFF, null, ptr0 + 4, arr.length);

            BinaryObject offheapObj = (BinaryObject)
                ((CacheObjectBinaryProcessorImpl)(grid(0)).context().cacheObjects()).unmarshal(ptr, false);

            assertEquals(BinaryObjectOffheapImpl.class, offheapObj.getClass());

            assertEquals("class".hashCode(), offheapObj.type().typeId());
            assertEquals(100, offheapObj.hashCode());

            assertEquals(111, offheapObj.<Integer>field("i").intValue());
            assertEquals(111.111f, offheapObj.<Float>field("f").floatValue(), 0);
            assertTrue(Arrays.equals(new int[] {1, 2, 3}, offheapObj.<int[]>field("iArr")));
            assertEquals(1, offheapObj.<BinaryObject>field("obj").<Key>deserialize().i);

            List<BinaryObject> list = offheapObj.field("col");

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
        BinaryObjectBuilder builder = builder(Value.class.getName());

        builder.hashCode(100);

        builder.setField("i", 1);

        BinaryObject po = builder.build();

        assertEquals("value".hashCode(), po.type().typeId());
        assertEquals(100, po.hashCode());

        assertEquals(1, po.<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetaData2() throws Exception {
        BinaryObjectBuilder builder = builder("org.test.MetaTest2");

        builder.setField("objectField", "a", Object.class);

        BinaryObject po = builder.build();

        BinaryType meta = po.type();

        assertEquals("MetaTest2", meta.typeName());
        assertEquals("Object", meta.fieldTypeName("objectField"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMetaData() throws Exception {
        BinaryObjectBuilder builder = builder("org.test.MetaTest");

        builder.hashCode(100);

        builder.setField("intField", 1);
        builder.setField("byteArrayField", new byte[] {1, 2, 3});

        BinaryObject po = builder.build();

        BinaryType meta = po.type();

        assertEquals("MetaTest", meta.typeName());

        Collection<String> fields = meta.fieldNames();

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

        meta = po.type();

        assertEquals("MetaTest", meta.typeName());

        fields = meta.fieldNames();

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
        BinaryObject objStr = builder(TestObjectAllTypes.class.getName()).setField("str", "aaa").build();

        BinaryObjectBuilderImpl builder = builder(objStr);
        assertEquals("aaa", builder.getField("str"));

        builder.setField("str", "bbb");
        assertEquals("bbb", builder.getField("str"));

        assertNull(builder.getField("i_"));
        assertEquals("bbb", builder.build().<TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    public void testCopyFromInnerObjects() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new TestObjectAllTypes());
        list.add(list.get(0));

        TestObjectContainer c = new TestObjectContainer(list);

        BinaryObjectBuilderImpl builder = builder(toPortable(c));
        builder.<List>getField("foo").add("!!!");

        BinaryObject res = builder.build();

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
        BinaryObject portableObj = builder(TestObjectContainer.class.getName())
            .setField("foo", toPortable(new TestObjectAllTypes()))
            .build();

        assertTrue(portableObj.<TestObjectContainer>deserialize().foo instanceof TestObjectAllTypes);
    }

    /**
     *
     */
    public void testPlainPortableObjectCopyFrom() {
        TestObjectPlainPortable obj = new TestObjectPlainPortable(toPortable(new TestObjectAllTypes()));

        BinaryObjectBuilderImpl builder = builder(toPortable(obj));
        assertTrue(builder.getField("plainPortable") instanceof BinaryObject);

        TestObjectPlainPortable deserialized = builder.build().deserialize();
        assertTrue(deserialized.plainPortable != null);
    }

    /**
     *
     */
    public void testRemoveFromNewObject() {
        BinaryObjectBuilder builder = builder(TestObjectAllTypes.class.getName());

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

        BinaryObjectBuilder builder = builder(toPortable(obj));

        builder.removeField("str");

        BinaryObject binary = builder.build();

        TestObjectAllTypes deserialzied = binary.deserialize();

        assertNull(deserialzied.str);
    }

    /**
     *
     */
    public void testRemoveFromExistingObjectAfterGet() {
        TestObjectAllTypes obj = new TestObjectAllTypes();
        obj.setDefaultData();
        obj.enumArr = null;

        BinaryObjectBuilderImpl builder = builder(toPortable(obj));

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

        BinaryObjectBuilder builder = builder(toPortable(outer));

        builder.setField("foo", "b");

        TestObjectOuter res = builder.build().deserialize();

        assertEquals("b", res.foo);
        assertSame(res, res.inner.outer);
    }

    /**
     * @return Portables.
     */
    private IgniteBinary portables() {
        return grid(0).binary();
    }

    /**
     * @param obj Object.
     * @return Portable object.
     */
    private BinaryObject toPortable(Object obj) {
        return portables().toBinary(obj);
    }

    /**
     * @return Builder.
     */
    private BinaryObjectBuilder builder(String clsName) {
        return portables().builder(clsName);
    }

    /**
     * @return Builder.
     */
    private BinaryObjectBuilderImpl builder(BinaryObject obj) {
        return (BinaryObjectBuilderImpl)portables().builder(obj);
    }

    /**
     *
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class CustomIdMapper {
        /** */
        private String str = "a";

        /** */
        private int i = 10;
    }

    /**
     */
    @SuppressWarnings("UnusedDeclaration")
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
    @SuppressWarnings("UnusedDeclaration")
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