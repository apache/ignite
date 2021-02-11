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

package org.apache.ignite.internal.binary;

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
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.util.GridUnsafe.BIG_ENDIAN;

/**
 * Binary builder test.
 */
public class BinaryObjectBuilderDefaultMappersSelfTest extends GridCommonAbstractTest {
    /** */
    private static IgniteConfiguration cfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        BinaryTypeConfiguration customTypeCfg = new BinaryTypeConfiguration();

        customTypeCfg.setTypeName(CustomIdMapper.class.getName());
        customTypeCfg.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return ~BinaryContext.defaultIdMapper().typeId(clsName);
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return typeId + ~BinaryContext.defaultIdMapper().fieldId(typeId, fieldName);
            }
        });

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setCompactFooter(compactFooter());

        bCfg.setTypeConfigurations(Arrays.asList(
            new BinaryTypeConfiguration(Key.class.getName()),
            new BinaryTypeConfiguration(Value.class.getName()),
            new BinaryTypeConfiguration("org.gridgain.grid.internal.util.binary.mutabletest.*"),
            customTypeCfg));

        bCfg.setIdMapper(new BinaryBasicIdMapper(false));
        bCfg.setNameMapper(new BinaryBasicNameMapper(false));

        cfg.setBinaryConfiguration(bCfg);

        cfg.setMarshaller(new BinaryMarshaller());

        this.cfg = cfg;

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(1);
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
    @Test
    public void testAllFieldsSerialization() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();
        obj.setDefaultData();
        obj.enumArr = null;

        GridBinaryTestClasses.TestObjectAllTypes deserialized = builder(toBinary(obj)).build().deserialize();

        GridTestUtils.deepEquals(obj, deserialized);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNullField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("objField", (Object)null);

        builder.setField("otherField", "value");

        BinaryObject obj = builder.build();

        assertNull(obj.field("objField"));
        assertEquals("value", obj.field("otherField"));
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(obj), obj.hashCode());

        builder = builder(obj);

        builder.setField("objField", "value", Object.class);
        builder.setField("otherField", (Object)null);

        obj = builder.build();

        assertNull(obj.field("otherField"));
        assertEquals("value", obj.field("objField"));
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(obj), obj.hashCode());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testByteField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("byteField", (byte)1);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals((byte) 1, po.<Byte>field("byteField").byteValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShortField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("shortField", (short)1);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals((short)1, po.<Short>field("shortField").shortValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIntField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("intField", 1);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals(1, po.<Integer>field("intField").intValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("longField", 1L);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals(1L, po.<Long>field("longField").longValue());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloatField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("floatField", 1.0f);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals(1.0f, po.<Float>field("floatField").floatValue(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDoubleField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("doubleField", 1.0d);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals(1.0d, po.<Double>field("doubleField").doubleValue(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCharField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("charField", (char)1);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals((char)1, po.<Character>field("charField").charValue());
    }

    /**
     * @return Expected hash code.
     * @param fullName Full name of type.
     */
    private int expectedHashCode(String fullName) {
        BinaryIdMapper idMapper = cfg.getBinaryConfiguration().getIdMapper();
        BinaryNameMapper nameMapper = cfg.getBinaryConfiguration().getNameMapper();

        if (idMapper == null)
            idMapper = BinaryContext.defaultIdMapper();

        if (nameMapper == null)
            nameMapper = BinaryContext.defaultNameMapper();

        return idMapper.typeId(nameMapper.typeName(fullName));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBooleanField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("booleanField", true);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(po.<Boolean>field("booleanField"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("decimalField", BigDecimal.TEN);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals(BigDecimal.TEN, po.<BigDecimal>field("decimalField"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStringField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("stringField", "str");

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals("str", po.<String>field("stringField"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDateField() throws Exception {
        Date date = new Date();

        assertEquals(date, builder("C").setField("d", date).build().<Date>field("d"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTimestampField() throws Exception {
        Timestamp ts = new Timestamp(new Date().getTime());
        ts.setNanos(1000);

        assertEquals(ts, builder("C").setField("t", ts).build().<Timestamp>field("t"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testUuidField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        UUID uuid = UUID.randomUUID();

        builder.setField("uuidField", uuid);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals(uuid, po.<UUID>field("uuidField"));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testByteArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("byteArrayField", new byte[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new byte[] {1, 2, 3}, po.<byte[]>field("byteArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testShortArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("shortArrayField", new short[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new short[] {1, 2, 3}, po.<short[]>field("shortArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIntArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("intArrayField", new int[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new int[] {1, 2, 3}, po.<int[]>field("intArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLongArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("longArrayField", new long[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new long[] {1, 2, 3}, po.<long[]>field("longArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFloatArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("floatArrayField", new float[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new float[] {1, 2, 3}, po.<float[]>field("floatArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDoubleArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("doubleArrayField", new double[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new double[] {1, 2, 3}, po.<double[]>field("doubleArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCharArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("charArrayField", new char[] {1, 2, 3});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new char[] {1, 2, 3}, po.<char[]>field("charArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBooleanArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("booleanArrayField", new boolean[] {true, false});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        boolean[] arr = po.field("booleanArrayField");

        assertEquals(2, arr.length);

        assertTrue(arr[0]);
        assertFalse(arr[1]);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDecimalArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("decimalArrayField", new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN}, po.<String[]>field("decimalArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStringArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("stringArrayField", new String[] {"str1", "str2", "str3"});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(new String[] {"str1", "str2", "str3"}, po.<String[]>field("stringArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDateArrayField() throws Exception {
        Date date1 = new Date();
        Date date2 = new Date(date1.getTime() + 1000);

        Date[] dateArr = new Date[] { date1, date2 };

        assertTrue(Arrays.equals(dateArr, builder("C").setField("da", dateArr).build().<Date[]>field("da")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
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
    @Test
    public void testUuidArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        UUID[] arr = new UUID[] {UUID.randomUUID(), UUID.randomUUID()};

        builder.setField("uuidArrayField", arr);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertTrue(Arrays.equals(arr, po.<UUID[]>field("uuidArrayField")));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("objectField", new Value(1));

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        assertEquals(1, po.<BinaryObject>field("objectField").<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testObjectArrayField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("objectArrayField", new Value[] {new Value(1), new Value(2)});

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        Object[] arr = po.field("objectArrayField");

        assertEquals(2, arr.length);

        assertEquals(1, ((BinaryObject)arr[0]).<Value>deserialize().i);
        assertEquals(2, ((BinaryObject)arr[1]).<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCollectionField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("collectionField", Arrays.asList(new Value(1), new Value(2)));
        builder.setField("collectionField2", Arrays.asList(new Value(1), new Value(2)), Collection.class);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        List<Value> list = po.field("collectionField");

        assertEquals(2, list.size());
        assertEquals(1, list.get(0).i);
        assertEquals(2, list.get(1).i);

        List<BinaryObject> list2 = po.field("collectionField2");

        assertEquals(2, list2.size());
        assertEquals(1, list2.get(0).<Value>deserialize().i);
        assertEquals(2, list2.get(1).<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMapField() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("mapField", F.asMap(new Key(1), new Value(1), new Key(2), new Value(2)));
        builder.setField("mapField2", F.asMap(new Key(1), new Value(1), new Key(2), new Value(2)), Map.class);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

        // Test non-standard map.
        Map<Key, Value> map = po.field("mapField");

        assertEquals(2, map.size());

        for (Map.Entry<Key, Value> e : map.entrySet())
            assertEquals(e.getKey().i, e.getValue().i);

        // Test binary map
        Map<BinaryObject, BinaryObject> map2 = po.field("mapField2");

        assertEquals(2, map2.size());

        for (Map.Entry<BinaryObject, BinaryObject> e : map2.entrySet())
            assertEquals(e.getKey().<Key>deserialize().i, e.getValue().<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSeveralFields() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("i", 111);
        builder.setField("f", 111.111f);
        builder.setField("iArr", new int[] {1, 2, 3});
        builder.setField("obj", new Key(1));
        builder.setField("col", Arrays.asList(new Value(1), new Value(2)), Collection.class);

        BinaryObject po = builder.build();

        assertEquals(expectedHashCode("Class"), po.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), po.hashCode());

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
    @Test
    public void testOffheapBinary() throws Exception {
        BinaryObjectBuilder builder = builder("Class");

        builder.setField("i", 111);
        builder.setField("f", 111.111f);
        builder.setField("iArr", new int[] {1, 2, 3});
        builder.setField("obj", new Key(1));
        builder.setField("col", Arrays.asList(new Value(1), new Value(2)), Collection.class);

        BinaryObject po = builder.build();

        byte[] arr = ((CacheObjectBinaryProcessorImpl)(grid(0)).context().cacheObjects()).marshal(po);

        long ptr = GridUnsafe.allocateMemory(arr.length + 5);

        try {
            long ptr0 = ptr;

            GridUnsafe.putBoolean(null, ptr0++, false);

            int len = arr.length;

            if (BIG_ENDIAN)
                GridUnsafe.putIntLE(ptr0, len);
            else
                GridUnsafe.putInt(ptr0, len);

            GridUnsafe.copyHeapOffheap(arr, GridUnsafe.BYTE_ARR_OFF, ptr0 + 4, arr.length);

            BinaryObject offheapObj = (BinaryObject)
                ((CacheObjectBinaryProcessorImpl)(grid(0)).context().cacheObjects()).unmarshal(ptr, false);

            assertEquals(BinaryObjectOffheapImpl.class, offheapObj.getClass());

            assertEquals(expectedHashCode("Class"), offheapObj.type().typeId());
            assertEquals(BinaryArrayIdentityResolver.instance().hashCode(po), offheapObj.hashCode());

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
            GridUnsafe.freeMemory(ptr);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testBuildAndDeserialize() throws Exception {
        BinaryObjectBuilder builder = builder(Value.class.getName());

        builder.setField("i", 1);

        BinaryObject bo = builder.build();

        assertEquals(expectedHashCode(Value.class.getName()), bo.type().typeId());
        assertEquals(BinaryArrayIdentityResolver.instance().hashCode(bo), bo.hashCode());

        assertEquals(1, bo.<Value>deserialize().i);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMetaData2() throws Exception {
        BinaryObjectBuilder builder = builder("org.test.MetaTest2");

        builder.setField("objectField", "a", Object.class);

        BinaryObject bo = builder.build();

        BinaryType meta = bo.type();

        assertEquals(expectedTypeName("org.test.MetaTest2"), meta.typeName());
        assertEquals("Object", meta.fieldTypeName("objectField"));
    }

    /**
     * @param fullClsName Class name.
     * @return Expected type name according to configuration.
     */
    private String expectedTypeName(String fullClsName) {
        BinaryNameMapper mapper = cfg.getBinaryConfiguration().getNameMapper();

        if (mapper == null)
            mapper = BinaryContext.defaultNameMapper();

        return mapper.typeName(fullClsName);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testMetaData() throws Exception {
        BinaryObjectBuilder builder = builder("org.test.MetaTest");

        builder.setField("intField", 1);
        builder.setField("byteArrayField", new byte[] {1, 2, 3});

        BinaryObject po = builder.build();

        BinaryType meta = po.type();

        assertEquals(expectedTypeName("org.test.MetaTest"), meta.typeName());

        Collection<String> fields = meta.fieldNames();

        assertEquals(2, fields.size());

        assertTrue(fields.contains("intField"));
        assertTrue(fields.contains("byteArrayField"));

        assertEquals("int", meta.fieldTypeName("intField"));
        assertEquals("byte[]", meta.fieldTypeName("byteArrayField"));

        builder = builder("org.test.MetaTest");

        builder.setField("intField", 2);
        builder.setField("uuidField", UUID.randomUUID());

        po = builder.build();

        meta = po.type();

        assertEquals(expectedTypeName("org.test.MetaTest"), meta.typeName());

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
    @Test
    public void testGetFromCopiedObj() {
        BinaryObject objStr = builder(GridBinaryTestClasses.TestObjectAllTypes.class.getName()).setField("str", "aaa").build();

        BinaryObjectBuilderImpl builder = builder(objStr);
        assertEquals("aaa", builder.getField("str"));

        builder.setField("str", "bbb");
        assertEquals("bbb", builder.getField("str"));

        assertNull(builder.getField("i_"));
        Assert.assertEquals("bbb", builder.build().<GridBinaryTestClasses.TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testCopyFromInnerObjects() {
        ArrayList<Object> list = new ArrayList<>();
        list.add(new GridBinaryTestClasses.TestObjectAllTypes());
        list.add(list.get(0));

        GridBinaryTestClasses.TestObjectContainer c = new GridBinaryTestClasses.TestObjectContainer(list);

        BinaryObjectBuilderImpl builder = builder(toBinary(c));
        builder.<List>getField("foo").add("!!!");

        BinaryObject res = builder.build();

        GridBinaryTestClasses.TestObjectContainer deserialized = res.deserialize();

        List deserializedList = (List)deserialized.foo;

        assertSame(deserializedList.get(0), deserializedList.get(1));
        assertEquals("!!!", deserializedList.get(2));
        assertTrue(deserializedList.get(0) instanceof GridBinaryTestClasses.TestObjectAllTypes);
    }

    /**
     *
     */
    @Test
    public void testSetBinaryObject() {
        // Prepare marshaller context.
        CacheObjectBinaryProcessorImpl proc = ((CacheObjectBinaryProcessorImpl)(grid(0)).context().cacheObjects());

        proc.marshal(new GridBinaryTestClasses.TestObjectContainer());
        proc.marshal(new GridBinaryTestClasses.TestObjectAllTypes());

        // Actual test.
        BinaryObject binaryObj = builder(GridBinaryTestClasses.TestObjectContainer.class.getName())
            .setField("foo", toBinary(new GridBinaryTestClasses.TestObjectAllTypes()))
            .build();

        assertTrue(binaryObj.<GridBinaryTestClasses.TestObjectContainer>deserialize().foo instanceof
            GridBinaryTestClasses.TestObjectAllTypes);
    }

    /**
     *
     */
    @Test
    public void testPlainBinaryObjectCopyFrom() {
        GridBinaryTestClasses.TestObjectPlainBinary obj = new GridBinaryTestClasses.TestObjectPlainBinary(toBinary(new GridBinaryTestClasses.TestObjectAllTypes()));

        BinaryObjectBuilderImpl builder = builder(toBinary(obj));
        assertTrue(builder.getField("plainBinary") instanceof BinaryObject);

        GridBinaryTestClasses.TestObjectPlainBinary deserialized = builder.build().deserialize();
        assertTrue(deserialized.plainBinary != null);
    }

    /**
     *
     */
    @Test
    public void testRemoveFromNewObject() {
        BinaryObjectBuilder builder = builder(GridBinaryTestClasses.TestObjectAllTypes.class.getName());

        builder.setField("str", "a");

        builder.removeField("str");

        Assert.assertNull(builder.build().<GridBinaryTestClasses.TestObjectAllTypes>deserialize().str);
    }

    /**
     *
     */
    @Test
    public void testRemoveFromExistingObject() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();
        obj.setDefaultData();
        obj.enumArr = null;

        BinaryObjectBuilder builder = builder(toBinary(obj));

        builder.removeField("str");

        BinaryObject binary = builder.build();

        GridBinaryTestClasses.TestObjectAllTypes deserialzied = binary.deserialize();

        assertNull(deserialzied.str);
    }

    /**
     *
     */
    @Test
    public void testRemoveFromExistingObjectAfterGet() {
        GridBinaryTestClasses.TestObjectAllTypes obj = new GridBinaryTestClasses.TestObjectAllTypes();
        obj.setDefaultData();
        obj.enumArr = null;

        BinaryObjectBuilderImpl builder = builder(toBinary(obj));

        builder.getField("i_");

        builder.removeField("str");

        Assert.assertNull(builder.build().<GridBinaryTestClasses.TestObjectAllTypes>deserialize().str);
    }

    /**
     * @throws IgniteCheckedException If any error occurs.
     */
    @Test
    public void testDontBrokeCyclicDependency() throws IgniteCheckedException {
        GridBinaryTestClasses.TestObjectOuter outer = new GridBinaryTestClasses.TestObjectOuter();
        outer.inner = new GridBinaryTestClasses.TestObjectInner();
        outer.inner.outer = outer;
        outer.foo = "a";

        BinaryObjectBuilder builder = builder(toBinary(outer));

        builder.setField("foo", "b");

        GridBinaryTestClasses.TestObjectOuter res = builder.build().deserialize();

        assertEquals("b", res.foo);
        assertSame(res, res.inner.outer);
    }

    /**
     * @return Binaries.
     */
    private IgniteBinary binaries() {
        return grid(0).binary();
    }

    /**
     * @param obj Object.
     * @return Binary object.
     */
    private BinaryObject toBinary(Object obj) {
        return binaries().toBinary(obj);
    }

    /**
     * @return Builder.
     */
    private BinaryObjectBuilder builder(String clsName) {
        return binaries().builder(clsName);
    }

    /**
     * @return Builder.
     */
    private BinaryObjectBuilderImpl builder(BinaryObject obj) {
        return (BinaryObjectBuilderImpl)binaries().builder(obj);
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
