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

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.portable.builder.PortableBuilderImpl;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableBuilder;
import org.apache.ignite.portable.PortableException;
import org.apache.ignite.portable.PortableIdMapper;
import org.apache.ignite.portable.PortableInvalidClassException;
import org.apache.ignite.portable.PortableMarshalAware;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableObject;
import org.apache.ignite.portable.PortableRawReader;
import org.apache.ignite.portable.PortableRawWriter;
import org.apache.ignite.portable.PortableReader;
import org.apache.ignite.portable.PortableSerializer;
import org.apache.ignite.portable.PortableTypeConfiguration;
import org.apache.ignite.portable.PortableWriter;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;
import sun.misc.Unsafe;

import static org.apache.ignite.internal.portable.PortableThreadLocalMemoryAllocator.THREAD_LOCAL_ALLOC;
import static org.junit.Assert.assertArrayEquals;

/**
 * Portable marshaller tests.
 */
@SuppressWarnings({"OverlyStrongTypeCast", "ArrayHashCode", "ConstantConditions"})
public class GridPortableMarshallerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** */
    protected static final PortableMetaDataHandler META_HND = new PortableMetaDataHandler() {
        @Override public void addMeta(int typeId, PortableMetadata meta) {
            // No-op.
        }

        @Override public PortableMetadata metadata(int typeId) {
            return null;
        }
    };

    /**
     * @throws Exception If failed.
     */
    public void testNull() throws Exception {
        assertNull(marshalUnmarshal(null));
    }

    /**
     * @throws Exception If failed.
     */
    public void testByte() throws Exception {
        assertEquals((byte)100, marshalUnmarshal((byte)100).byteValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testShort() throws Exception {
        assertEquals((short)100, marshalUnmarshal((short)100).shortValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInt() throws Exception {
        assertEquals(100, marshalUnmarshal(100).intValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testLong() throws Exception {
        assertEquals(100L, marshalUnmarshal(100L).longValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloat() throws Exception {
        assertEquals(100.001f, marshalUnmarshal(100.001f).floatValue(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDouble() throws Exception {
        assertEquals(100.001d, marshalUnmarshal(100.001d).doubleValue(), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testChar() throws Exception {
        assertEquals((char)100, marshalUnmarshal((char)100).charValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoolean() throws Exception {
        assertEquals(true, marshalUnmarshal(true).booleanValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecimal() throws Exception {
        BigDecimal val;

        assertEquals((val = BigDecimal.ZERO), marshalUnmarshal(val));
        assertEquals((val = BigDecimal.valueOf(Long.MAX_VALUE, 0)), marshalUnmarshal(val));
        assertEquals((val = BigDecimal.valueOf(Long.MIN_VALUE, 0)), marshalUnmarshal(val));
        assertEquals((val = BigDecimal.valueOf(Long.MAX_VALUE, 8)), marshalUnmarshal(val));
        assertEquals((val = BigDecimal.valueOf(Long.MIN_VALUE, 8)), marshalUnmarshal(val));

        assertEquals((val = new BigDecimal(new BigInteger("-79228162514264337593543950336"))), marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testString() throws Exception {
        assertEquals("str", marshalUnmarshal("str"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUuid() throws Exception {
        UUID uuid = UUID.randomUUID();

        assertEquals(uuid, marshalUnmarshal(uuid));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDate() throws Exception {
        Date date = new Date();

        Date val = marshalUnmarshal(date);

        assertEquals(date, val);
        assertEquals(Timestamp.class, val.getClass()); // With default configuration should unmarshal as Timestamp.

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setUseTimestamp(false);

        val = marshalUnmarshal(date, marsh);

        assertEquals(date, val);
        assertEquals(Date.class, val.getClass());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimestamp() throws Exception {
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        ts.setNanos(999999999);

        assertEquals(ts, marshalUnmarshal(ts));
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteArray() throws Exception {
        byte[] arr = new byte[] {10, 20, 30};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testShortArray() throws Exception {
        short[] arr = new short[] {10, 20, 30};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntArray() throws Exception {
        int[] arr = new int[] {10, 20, 30};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongArray() throws Exception {
        long[] arr = new long[] {10, 20, 30};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloatArray() throws Exception {
        float[] arr = new float[] {10.1f, 20.1f, 30.1f};

        assertArrayEquals(arr, marshalUnmarshal(arr), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleArray() throws Exception {
        double[] arr = new double[] {10.1d, 20.1d, 30.1d};

        assertArrayEquals(arr, marshalUnmarshal(arr), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharArray() throws Exception {
        char[] arr = new char[] {10, 20, 30};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBooleanArray() throws Exception {
        boolean[] arr = new boolean[] {true, false, true};

        assertBooleanArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDecimalArray() throws Exception {
        BigDecimal[] arr = new BigDecimal[] { BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.TEN } ;

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringArray() throws Exception {
        String[] arr = new String[] {"str1", "str2", "str3"};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUuidArray() throws Exception {
        UUID[] arr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDateArray() throws Exception {
        Date[] arr = new Date[] {new Date(11111), new Date(22222), new Date(33333)};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectArray() throws Exception {
        Object[] arr = new Object[] {1, 2, 3};

        assertArrayEquals(arr, marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollection() throws Exception {
        testCollection(new ArrayList<Integer>(3));
        testCollection(new LinkedHashSet<Integer>());
        testCollection(new HashSet<Integer>());
        testCollection(new TreeSet<Integer>());
        testCollection(new ConcurrentSkipListSet<Integer>());
    }

    /**
     * @throws Exception If failed.
     */
    private void testCollection(Collection<Integer> col) throws Exception {
        col.add(1);
        col.add(2);
        col.add(3);

        assertEquals(col, marshalUnmarshal(col));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMap() throws Exception {
        testMap(new HashMap<Integer, String>());
        testMap(new LinkedHashMap());
        testMap(new TreeMap<Integer, String>());
        testMap(new ConcurrentHashMap8<Integer, String>());
        testMap(new ConcurrentHashMap<Integer, String>());
    }

    /**
     * @throws Exception If failed.
     */
    private void testMap(Map<Integer, String> map) throws Exception {
        map.put(1, "str1");
        map.put(2, "str2");
        map.put(3, "str3");

        assertEquals(map, marshalUnmarshal(map));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMapEntry() throws Exception {
        Map.Entry<Integer, String> e = new GridMapEntry<>(1, "str1");

        assertEquals(e, marshalUnmarshal(e));

        Map<Integer, String> map = new HashMap<>(1);

        map.put(2, "str2");

        e = F.firstEntry(map);

        Map.Entry<Integer, String> e0 = marshalUnmarshal(e);

        assertEquals(2, e0.getKey().intValue());
        assertEquals("str2", e0.getValue());
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableObject() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(new PortableTypeConfiguration(SimpleObject.class.getName())));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject po0 = marshalUnmarshal(po, marsh);

        assertTrue(po.hasField("b"));
        assertTrue(po.hasField("s"));
        assertTrue(po.hasField("i"));
        assertTrue(po.hasField("l"));
        assertTrue(po.hasField("f"));
        assertTrue(po.hasField("d"));
        assertTrue(po.hasField("c"));
        assertTrue(po.hasField("bool"));

        assertFalse(po.hasField("no_such_field"));

        assertEquals(obj, po.deserialize());
        assertEquals(obj, po0.deserialize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testEnum() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(TestEnum.class.getName()));

        assertEquals(TestEnum.B, marshalUnmarshal(TestEnum.B, marsh));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUseTimestampFlag() throws Exception {
        PortableTypeConfiguration cfg1 = new PortableTypeConfiguration(DateClass1.class.getName());

        PortableTypeConfiguration cfg2 = new PortableTypeConfiguration(DateClass2.class.getName());

        cfg2.setUseTimestamp(false);

        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(cfg1, cfg2));

        Date date = new Date();
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        DateClass1 obj1 = new DateClass1();
        obj1.date = date;
        obj1.ts = ts;

        DateClass2 obj2 = new DateClass2();
        obj2.date = date;
        obj2.ts = ts;

        PortableObject po1 = marshal(obj1, marsh);

        assertEquals(date, po1.field("date"));
        assertEquals(Timestamp.class, po1.field("date").getClass());
        assertEquals(ts, po1.field("ts"));

        PortableObject po2 = marshal(obj2, marsh);

        assertEquals(date, po2.field("date"));
        assertEquals(Date.class, po2.field("date").getClass());
        assertEquals(new Date(ts.getTime()), po2.field("ts"));
        assertEquals(Date.class, po2.field("ts").getClass());

        obj1 = po1.deserialize();
        assertEquals(date, obj1.date);
        assertEquals(Date.class, obj1.date.getClass());
        assertEquals(ts, obj1.ts);

        obj2 = po2.deserialize();
        assertEquals(date, obj2.date);
        assertEquals(Date.class, obj2.date.getClass());
        assertEquals(ts, obj2.ts);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleObject() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        assertEquals(obj.hashCode(), po.hashCode());

        assertEquals(obj, po.deserialize());

        assertEquals(obj.b, (byte)po.field("b"));
        assertEquals(obj.s, (short)po.field("s"));
        assertEquals(obj.i, (int)po.field("i"));
        assertEquals(obj.l, (long)po.field("l"));
        assertEquals(obj.f, (float)po.field("f"), 0);
        assertEquals(obj.d, (double)po.field("d"), 0);
        assertEquals(obj.c, (char)po.field("c"));
        assertEquals(obj.bool, (boolean)po.field("bool"));
        assertEquals(obj.str, po.field("str"));
        assertEquals(obj.uuid, po.field("uuid"));
        assertEquals(obj.date, po.field("date"));
        assertEquals(Date.class, obj.date.getClass());
        assertEquals(obj.ts, po.field("ts"));
        assertArrayEquals(obj.bArr, (byte[])po.field("bArr"));
        assertArrayEquals(obj.sArr, (short[])po.field("sArr"));
        assertArrayEquals(obj.iArr, (int[])po.field("iArr"));
        assertArrayEquals(obj.lArr, (long[])po.field("lArr"));
        assertArrayEquals(obj.fArr, (float[])po.field("fArr"), 0);
        assertArrayEquals(obj.dArr, (double[])po.field("dArr"), 0);
        assertArrayEquals(obj.cArr, (char[])po.field("cArr"));
        assertBooleanArrayEquals(obj.boolArr, (boolean[])po.field("boolArr"));
        assertArrayEquals(obj.strArr, (String[])po.field("strArr"));
        assertArrayEquals(obj.uuidArr, (UUID[])po.field("uuidArr"));
        assertArrayEquals(obj.dateArr, (Date[])po.field("dateArr"));
        assertArrayEquals(obj.objArr, (Object[])po.field("objArr"));
        assertEquals(obj.col, po.field("col"));
        assertEquals(obj.map, po.field("map"));
        assertEquals(new Integer(obj.enumVal.ordinal()), new Integer(((Enum<?>)po.field("enumVal")).ordinal()));
        assertArrayEquals(ordinals(obj.enumArr), ordinals((Enum<?>[])po.field("enumArr")));
        assertNull(po.field("unknown"));

        PortableObject innerPo = po.field("inner");

        assertEquals(obj.inner, innerPo.deserialize());

        assertEquals(obj.inner.b, (byte)innerPo.field("b"));
        assertEquals(obj.inner.s, (short)innerPo.field("s"));
        assertEquals(obj.inner.i, (int)innerPo.field("i"));
        assertEquals(obj.inner.l, (long)innerPo.field("l"));
        assertEquals(obj.inner.f, (float)innerPo.field("f"), 0);
        assertEquals(obj.inner.d, (double)innerPo.field("d"), 0);
        assertEquals(obj.inner.c, (char)innerPo.field("c"));
        assertEquals(obj.inner.bool, (boolean)innerPo.field("bool"));
        assertEquals(obj.inner.str, innerPo.field("str"));
        assertEquals(obj.inner.uuid, innerPo.field("uuid"));
        assertEquals(obj.inner.date, innerPo.field("date"));
        assertEquals(Date.class, obj.inner.date.getClass());
        assertEquals(obj.inner.ts, innerPo.field("ts"));
        assertArrayEquals(obj.inner.bArr, (byte[])innerPo.field("bArr"));
        assertArrayEquals(obj.inner.sArr, (short[])innerPo.field("sArr"));
        assertArrayEquals(obj.inner.iArr, (int[])innerPo.field("iArr"));
        assertArrayEquals(obj.inner.lArr, (long[])innerPo.field("lArr"));
        assertArrayEquals(obj.inner.fArr, (float[])innerPo.field("fArr"), 0);
        assertArrayEquals(obj.inner.dArr, (double[])innerPo.field("dArr"), 0);
        assertArrayEquals(obj.inner.cArr, (char[])innerPo.field("cArr"));
        assertBooleanArrayEquals(obj.inner.boolArr, (boolean[])innerPo.field("boolArr"));
        assertArrayEquals(obj.inner.strArr, (String[])innerPo.field("strArr"));
        assertArrayEquals(obj.inner.uuidArr, (UUID[])innerPo.field("uuidArr"));
        assertArrayEquals(obj.inner.dateArr, (Date[])innerPo.field("dateArr"));
        assertArrayEquals(obj.inner.objArr, (Object[])innerPo.field("objArr"));
        assertEquals(obj.inner.col, innerPo.field("col"));
        assertEquals(obj.inner.map, innerPo.field("map"));
        assertEquals(new Integer(obj.inner.enumVal.ordinal()),
            new Integer(((Enum<?>)innerPo.field("enumVal")).ordinal()));
        assertArrayEquals(ordinals(obj.inner.enumArr), ordinals((Enum<?>[])innerPo.field("enumArr")));
        assertNull(innerPo.field("inner"));
        assertNull(innerPo.field("unknown"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortable() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName()),
            new PortableTypeConfiguration(TestPortableObject.class.getName())
        ));

        TestPortableObject obj = portableObject();

        PortableObject po = marshal(obj, marsh);

        assertEquals(obj.hashCode(), po.hashCode());

        assertEquals(obj, po.deserialize());

        assertEquals(obj.b, (byte)po.field("_b"));
        assertEquals(obj.s, (short)po.field("_s"));
        assertEquals(obj.i, (int)po.field("_i"));
        assertEquals(obj.l, (long)po.field("_l"));
        assertEquals(obj.f, (float)po.field("_f"), 0);
        assertEquals(obj.d, (double)po.field("_d"), 0);
        assertEquals(obj.c, (char)po.field("_c"));
        assertEquals(obj.bool, (boolean)po.field("_bool"));
        assertEquals(obj.str, po.field("_str"));
        assertEquals(obj.uuid, po.field("_uuid"));
        assertEquals(obj.date, po.field("_date"));
        assertEquals(obj.ts, po.field("_ts"));
        assertArrayEquals(obj.bArr, (byte[])po.field("_bArr"));
        assertArrayEquals(obj.sArr, (short[])po.field("_sArr"));
        assertArrayEquals(obj.iArr, (int[])po.field("_iArr"));
        assertArrayEquals(obj.lArr, (long[])po.field("_lArr"));
        assertArrayEquals(obj.fArr, (float[])po.field("_fArr"), 0);
        assertArrayEquals(obj.dArr, (double[])po.field("_dArr"), 0);
        assertArrayEquals(obj.cArr, (char[])po.field("_cArr"));
        assertBooleanArrayEquals(obj.boolArr, (boolean[])po.field("_boolArr"));
        assertArrayEquals(obj.strArr, (String[])po.field("_strArr"));
        assertArrayEquals(obj.uuidArr, (UUID[])po.field("_uuidArr"));
        assertArrayEquals(obj.dateArr, (Date[])po.field("_dateArr"));
        assertArrayEquals(obj.objArr, (Object[])po.field("_objArr"));
        assertEquals(obj.col, po.field("_col"));
        assertEquals(obj.map, po.field("_map"));
        assertEquals(new Integer(obj.enumVal.ordinal()), new Integer(((Enum<?>)po.field("_enumVal")).ordinal()));
        assertArrayEquals(ordinals(obj.enumArr), ordinals((Enum<?>[])po.field("_enumArr")));
        assertNull(po.field("unknown"));

        PortableObject simplePo = po.field("_simple");

        assertEquals(obj.simple, simplePo.deserialize());

        assertEquals(obj.simple.b, (byte)simplePo.field("b"));
        assertEquals(obj.simple.s, (short)simplePo.field("s"));
        assertEquals(obj.simple.i, (int)simplePo.field("i"));
        assertEquals(obj.simple.l, (long)simplePo.field("l"));
        assertEquals(obj.simple.f, (float)simplePo.field("f"), 0);
        assertEquals(obj.simple.d, (double)simplePo.field("d"), 0);
        assertEquals(obj.simple.c, (char)simplePo.field("c"));
        assertEquals(obj.simple.bool, (boolean)simplePo.field("bool"));
        assertEquals(obj.simple.str, simplePo.field("str"));
        assertEquals(obj.simple.uuid, simplePo.field("uuid"));
        assertEquals(obj.simple.date, simplePo.field("date"));
        assertEquals(Date.class, obj.simple.date.getClass());
        assertEquals(obj.simple.ts, simplePo.field("ts"));
        assertArrayEquals(obj.simple.bArr, (byte[])simplePo.field("bArr"));
        assertArrayEquals(obj.simple.sArr, (short[])simplePo.field("sArr"));
        assertArrayEquals(obj.simple.iArr, (int[])simplePo.field("iArr"));
        assertArrayEquals(obj.simple.lArr, (long[])simplePo.field("lArr"));
        assertArrayEquals(obj.simple.fArr, (float[])simplePo.field("fArr"), 0);
        assertArrayEquals(obj.simple.dArr, (double[])simplePo.field("dArr"), 0);
        assertArrayEquals(obj.simple.cArr, (char[])simplePo.field("cArr"));
        assertBooleanArrayEquals(obj.simple.boolArr, (boolean[])simplePo.field("boolArr"));
        assertArrayEquals(obj.simple.strArr, (String[])simplePo.field("strArr"));
        assertArrayEquals(obj.simple.uuidArr, (UUID[])simplePo.field("uuidArr"));
        assertArrayEquals(obj.simple.dateArr, (Date[])simplePo.field("dateArr"));
        assertArrayEquals(obj.simple.objArr, (Object[])simplePo.field("objArr"));
        assertEquals(obj.simple.col, simplePo.field("col"));
        assertEquals(obj.simple.map, simplePo.field("map"));
        assertEquals(new Integer(obj.simple.enumVal.ordinal()),
            new Integer(((Enum<?>)simplePo.field("enumVal")).ordinal()));
        assertArrayEquals(ordinals(obj.simple.enumArr), ordinals((Enum<?>[])simplePo.field("enumArr")));
        assertNull(simplePo.field("simple"));
        assertNull(simplePo.field("portable"));
        assertNull(simplePo.field("unknown"));

        PortableObject portablePo = po.field("_portable");

        assertEquals(obj.portable, portablePo.deserialize());

        assertEquals(obj.portable.b, (byte)portablePo.field("_b"));
        assertEquals(obj.portable.s, (short)portablePo.field("_s"));
        assertEquals(obj.portable.i, (int)portablePo.field("_i"));
        assertEquals(obj.portable.l, (long)portablePo.field("_l"));
        assertEquals(obj.portable.f, (float)portablePo.field("_f"), 0);
        assertEquals(obj.portable.d, (double)portablePo.field("_d"), 0);
        assertEquals(obj.portable.c, (char)portablePo.field("_c"));
        assertEquals(obj.portable.bool, (boolean)portablePo.field("_bool"));
        assertEquals(obj.portable.str, portablePo.field("_str"));
        assertEquals(obj.portable.uuid, portablePo.field("_uuid"));
        assertEquals(obj.portable.date, portablePo.field("_date"));
        assertEquals(obj.portable.ts, portablePo.field("_ts"));
        assertArrayEquals(obj.portable.bArr, (byte[])portablePo.field("_bArr"));
        assertArrayEquals(obj.portable.sArr, (short[])portablePo.field("_sArr"));
        assertArrayEquals(obj.portable.iArr, (int[])portablePo.field("_iArr"));
        assertArrayEquals(obj.portable.lArr, (long[])portablePo.field("_lArr"));
        assertArrayEquals(obj.portable.fArr, (float[])portablePo.field("_fArr"), 0);
        assertArrayEquals(obj.portable.dArr, (double[])portablePo.field("_dArr"), 0);
        assertArrayEquals(obj.portable.cArr, (char[])portablePo.field("_cArr"));
        assertBooleanArrayEquals(obj.portable.boolArr, (boolean[])portablePo.field("_boolArr"));
        assertArrayEquals(obj.portable.strArr, (String[])portablePo.field("_strArr"));
        assertArrayEquals(obj.portable.uuidArr, (UUID[])portablePo.field("_uuidArr"));
        assertArrayEquals(obj.portable.dateArr, (Date[])portablePo.field("_dateArr"));
        assertArrayEquals(obj.portable.objArr, (Object[])portablePo.field("_objArr"));
        assertEquals(obj.portable.col, portablePo.field("_col"));
        assertEquals(obj.portable.map, portablePo.field("_map"));
        assertEquals(new Integer(obj.portable.enumVal.ordinal()),
            new Integer(((Enum<?>)portablePo.field("_enumVal")).ordinal()));
        assertArrayEquals(ordinals(obj.portable.enumArr), ordinals((Enum<?>[])portablePo.field("_enumArr")));
        assertNull(portablePo.field("_simple"));
        assertNull(portablePo.field("_portable"));
        assertNull(portablePo.field("unknown"));
    }

    /**
     * @param obj Simple object.
     * @param po Portable object.
     */
    private void checkSimpleObjectData(SimpleObject obj, PortableObject po) {
        assertEquals(obj.b, (byte)po.field("b"));
        assertEquals(obj.s, (short)po.field("s"));
        assertEquals(obj.i, (int)po.field("i"));
        assertEquals(obj.l, (long)po.field("l"));
        assertEquals(obj.f, (float)po.field("f"), 0);
        assertEquals(obj.d, (double)po.field("d"), 0);
        assertEquals(obj.c, (char)po.field("c"));
        assertEquals(obj.bool, (boolean)po.field("bool"));
        assertEquals(obj.str, po.field("str"));
        assertEquals(obj.uuid, po.field("uuid"));
        assertEquals(obj.date, po.field("date"));
        assertEquals(Date.class, obj.date.getClass());
        assertEquals(obj.ts, po.field("ts"));
        assertArrayEquals(obj.bArr, (byte[])po.field("bArr"));
        assertArrayEquals(obj.sArr, (short[])po.field("sArr"));
        assertArrayEquals(obj.iArr, (int[])po.field("iArr"));
        assertArrayEquals(obj.lArr, (long[])po.field("lArr"));
        assertArrayEquals(obj.fArr, (float[])po.field("fArr"), 0);
        assertArrayEquals(obj.dArr, (double[])po.field("dArr"), 0);
        assertArrayEquals(obj.cArr, (char[])po.field("cArr"));
        assertBooleanArrayEquals(obj.boolArr, (boolean[])po.field("boolArr"));
        assertArrayEquals(obj.strArr, (String[])po.field("strArr"));
        assertArrayEquals(obj.uuidArr, (UUID[])po.field("uuidArr"));
        assertArrayEquals(obj.dateArr, (Date[])po.field("dateArr"));
        assertArrayEquals(obj.objArr, (Object[])po.field("objArr"));
        assertEquals(obj.col, po.field("col"));
        assertEquals(obj.map, po.field("map"));
        assertEquals(new Integer(obj.enumVal.ordinal()), new Integer(((Enum<?>)po.field("enumVal")).ordinal()));
        assertArrayEquals(ordinals(obj.enumArr), ordinals((Enum<?>[])po.field("enumArr")));
        assertNull(po.field("unknown"));

        assertEquals(obj, po.deserialize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInvalidClass() throws Exception {
        byte[] arr = new byte[20];

        arr[0] = 103;

        U.intToBytes(Integer.reverseBytes(11111), arr, 2);

        final PortableObject po = new PortableObjectImpl(initPortableContext(new PortableMarshaller()), arr, 0);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
                                       @Override public Object call() throws Exception {
                                           po.deserialize();

                                           return null;
                                       }
                                   }, PortableInvalidClassException.class, "Unknown type ID: 11111"
        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassWithoutPublicConstructor() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
                                        new PortableTypeConfiguration(NoPublicConstructor.class.getName()),
                                        new PortableTypeConfiguration(NoPublicDefaultConstructor.class.getName()),
                                        new PortableTypeConfiguration(ProtectedConstructor.class.getName()))
        );

        initPortableContext(marsh);

        NoPublicConstructor npc = new NoPublicConstructor();
        PortableObject npc2 = marshal(npc, marsh);

        assertEquals("test", npc2.<NoPublicConstructor>deserialize().val);

        NoPublicDefaultConstructor npdc = new NoPublicDefaultConstructor(239);
        PortableObject npdc2 = marshal(npdc, marsh);

        assertEquals(239, npdc2.<NoPublicDefaultConstructor>deserialize().val);

        ProtectedConstructor pc = new ProtectedConstructor();
        PortableObject pc2 = marshal(pc, marsh);

        assertEquals(ProtectedConstructor.class, pc2.<ProtectedConstructor>deserialize().getClass());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomSerializer() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        PortableTypeConfiguration type =
            new PortableTypeConfiguration(CustomSerializedObject1.class.getName());

        type.setSerializer(new CustomSerializer1());

        marsh.setTypeConfigurations(Arrays.asList(type));

        CustomSerializedObject1 obj1 = new CustomSerializedObject1(10);

        PortableObject po1 = marshal(obj1, marsh);

        assertEquals(20, po1.<CustomSerializedObject1>deserialize().val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomSerializerWithGlobal() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setSerializer(new CustomSerializer1());

        PortableTypeConfiguration type1 =
            new PortableTypeConfiguration(CustomSerializedObject1.class.getName());
        PortableTypeConfiguration type2 =
            new PortableTypeConfiguration(CustomSerializedObject2.class.getName());

        type2.setSerializer(new CustomSerializer2());

        marsh.setTypeConfigurations(Arrays.asList(type1, type2));

        CustomSerializedObject1 obj1 = new CustomSerializedObject1(10);

        PortableObject po1 = marshal(obj1, marsh);

        assertEquals(20, po1.<CustomSerializedObject1>deserialize().val);

        CustomSerializedObject2 obj2 = new CustomSerializedObject2(10);

        PortableObject po2 = marshal(obj2, marsh);

        assertEquals(30, po2.<CustomSerializedObject2>deserialize().val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomIdMapper() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        PortableTypeConfiguration type =
            new PortableTypeConfiguration(CustomMappedObject1.class.getName());

        type.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 11111;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                assert typeId == 11111;

                if ("val1".equals(fieldName))
                    return 22222;
                else if ("val2".equals(fieldName))
                    return 33333;

                assert false : "Unknown field: " + fieldName;

                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(type));

        CustomMappedObject1 obj1 = new CustomMappedObject1(10, "str");

        PortableObject po1 = marshal(obj1, marsh);

        assertEquals(11111, po1.typeId());
        assertEquals(22222, intFromPortable(po1, 18));
        assertEquals(33333, intFromPortable(po1, 31));

        assertEquals(10, po1.<CustomMappedObject1>deserialize().val1);
        assertEquals("str", po1.<CustomMappedObject1>deserialize().val2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomIdMapperWithGlobal() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 11111;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                assert typeId == 11111;

                if ("val1".equals(fieldName)) return 22222;
                else if ("val2".equals(fieldName)) return 33333;

                assert false : "Unknown field: " + fieldName;

                return 0;
            }
        });

        PortableTypeConfiguration type1 =
            new PortableTypeConfiguration(CustomMappedObject1.class.getName());
        PortableTypeConfiguration type2 =
            new PortableTypeConfiguration(CustomMappedObject2.class.getName());

        type2.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 44444;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                assert typeId == 44444;

                if ("val1".equals(fieldName)) return 55555;
                else if ("val2".equals(fieldName)) return 66666;

                assert false : "Unknown field: " + fieldName;

                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(type1, type2));

        CustomMappedObject1 obj1 = new CustomMappedObject1(10, "str1");

        PortableObject po1 = marshal(obj1, marsh);

        assertEquals(11111, po1.typeId());
        assertEquals(22222, intFromPortable(po1, 18));
        assertEquals(33333, intFromPortable(po1, 31));

        assertEquals(10, po1.<CustomMappedObject1>deserialize().val1);
        assertEquals("str1", po1.<CustomMappedObject1>deserialize().val2);

        CustomMappedObject2 obj2 = new CustomMappedObject2(20, "str2");

        PortableObject po2 = marshal(obj2, marsh);

        assertEquals(44444, po2.typeId());
        assertEquals(55555, intFromPortable(po2, 18));
        assertEquals(66666, intFromPortable(po2, 31));

        assertEquals(20, po2.<CustomMappedObject2>deserialize().val1);
        assertEquals("str2", po2.<CustomMappedObject2>deserialize().val2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicObject() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(DynamicObject.class.getName())
        ));

        PortableObject po1 = marshal(new DynamicObject(0, 10, 20, 30), marsh);

        assertEquals(new Integer(10), po1.field("val1"));
        assertEquals(null, po1.field("val2"));
        assertEquals(null, po1.field("val3"));

        DynamicObject do1 = po1.deserialize();

        assertEquals(10, do1.val1);
        assertEquals(0, do1.val2);
        assertEquals(0, do1.val3);

        PortableObject po2 = marshal(new DynamicObject(1, 10, 20, 30), marsh);

        assertEquals(new Integer(10), po2.field("val1"));
        assertEquals(new Integer(20), po2.field("val2"));
        assertEquals(null, po2.field("val3"));

        DynamicObject do2 = po2.deserialize();

        assertEquals(10, do2.val1);
        assertEquals(20, do2.val2);
        assertEquals(0, do2.val3);

        PortableObject po3 = marshal(new DynamicObject(2, 10, 20, 30), marsh);

        assertEquals(new Integer(10), po3.field("val1"));
        assertEquals(new Integer(20), po3.field("val2"));
        assertEquals(new Integer(30), po3.field("val3"));

        DynamicObject do3 = po3.deserialize();

        assertEquals(10, do3.val1);
        assertEquals(20, do3.val2);
        assertEquals(30, do3.val3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCycleLink() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(CycleLinkObject.class.getName())
        ));

        CycleLinkObject obj = new CycleLinkObject();

        obj.self = obj;

        PortableObject po = marshal(obj, marsh);

        CycleLinkObject obj0 = po.deserialize();

        assert obj0.self == obj0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDetached() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(DetachedTestObject.class.getName()),
            new PortableTypeConfiguration(DetachedInnerTestObject.class.getName())
        ));

        UUID id = UUID.randomUUID();

        DetachedTestObject obj = marshal(new DetachedTestObject(
            new DetachedInnerTestObject(null, id)), marsh).deserialize();

        assertEquals(id, obj.inner1.id);
        assertEquals(id, obj.inner4.id);

        assert obj.inner1 == obj.inner4;

        PortableObjectImpl innerPo = (PortableObjectImpl)obj.inner2;

        assert innerPo.detached();

        DetachedInnerTestObject inner = innerPo.deserialize();

        assertEquals(id, inner.id);

        PortableObjectImpl detachedPo = (PortableObjectImpl)innerPo.detach();

        assert detachedPo.detached();

        inner = detachedPo.deserialize();

        assertEquals(id, inner.id);

        innerPo = (PortableObjectImpl)obj.inner3;

        assert innerPo.detached();

        inner = innerPo.deserialize();

        assertEquals(id, inner.id);
        assertNotNull(inner.inner);

        detachedPo = (PortableObjectImpl)innerPo.detach();

        assert detachedPo.detached();

        inner = innerPo.deserialize();

        assertEquals(id, inner.id);
        assertNotNull(inner.inner);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollectionFields() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(CollectionFieldsObject.class.getName()),
            new PortableTypeConfiguration(Key.class.getName()),
            new PortableTypeConfiguration(Value.class.getName())
        ));

        Object[] arr = new Object[] {new Value(1), new Value(2), new Value(3)};
        Collection<Value> col = Arrays.asList(new Value(4), new Value(5), new Value(6));
        Map<Key, Value> map = F.asMap(new Key(10), new Value(10), new Key(20), new Value(20), new Key(30), new Value(30));

        CollectionFieldsObject obj = new CollectionFieldsObject(arr, col, map);

        PortableObject po = marshal(obj, marsh);

        Object[] arr0 = po.field("arr");

        assertEquals(3, arr0.length);

        int i = 1;

        for (Object valPo : arr0)
            assertEquals(i++, ((PortableObject)valPo).<Value>deserialize().val);

        Collection<PortableObject> col0 = po.field("col");

        i = 4;

        for (PortableObject valPo : col0)
            assertEquals(i++, valPo.<Value>deserialize().val);

        Map<PortableObject, PortableObject> map0 = po.field("map");

        for (Map.Entry<PortableObject, PortableObject> e : map0.entrySet())
            assertEquals(e.getKey().<Key>deserialize().key, e.getValue().<Value>deserialize().val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultMapping() throws Exception {
        PortableMarshaller marsh1 = new PortableMarshaller();

        PortableTypeConfiguration customMappingType =
            new PortableTypeConfiguration(TestPortableObject.class.getName());

        customMappingType.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                String typeName;

                try {
                    Method mtd = PortableContext.class.getDeclaredMethod("typeName", String.class);

                    mtd.setAccessible(true);

                    typeName = (String)mtd.invoke(null, clsName);
                }
                catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                return typeName.toLowerCase().hashCode();
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return fieldName.toLowerCase().hashCode();
            }
        });

        marsh1.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName()),
            customMappingType
        ));

        TestPortableObject obj = portableObject();

        PortableObjectImpl po = marshal(obj, marsh1);

        PortableMarshaller marsh2 = new PortableMarshaller();

        marsh2.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName()),
            new PortableTypeConfiguration(TestPortableObject.class.getName())
        ));

        PortableContext ctx = initPortableContext(marsh2);

        po.context(ctx);

        assertEquals(obj, po.deserialize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeNames() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        PortableTypeConfiguration customType1 = new PortableTypeConfiguration(Value.class.getName());

        customType1.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 300;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        PortableTypeConfiguration customType2 = new PortableTypeConfiguration("org.gridgain.NonExistentClass1");

        customType2.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 400;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        PortableTypeConfiguration customType3 = new PortableTypeConfiguration("NonExistentClass2");

        customType3.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        PortableTypeConfiguration customType4 = new PortableTypeConfiguration("NonExistentClass5");

        customType4.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 0;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(Key.class.getName()),
            new PortableTypeConfiguration("org.gridgain.NonExistentClass3"),
            new PortableTypeConfiguration("NonExistentClass4"),
            customType1,
            customType2,
            customType3,
            customType4
        ));

        PortableContext ctx = initPortableContext(marsh);

        assertEquals("notconfiguredclass".hashCode(), ctx.typeId("NotConfiguredClass"));
        assertEquals("key".hashCode(), ctx.typeId("Key"));
        assertEquals("nonexistentclass3".hashCode(), ctx.typeId("NonExistentClass3"));
        assertEquals("nonexistentclass4".hashCode(), ctx.typeId("NonExistentClass4"));
        assertEquals(300, ctx.typeId(getClass().getSimpleName() + "$Value"));
        assertEquals(400, ctx.typeId("NonExistentClass1"));
        assertEquals(500, ctx.typeId("NonExistentClass2"));
        assertEquals("nonexistentclass5".hashCode(), ctx.typeId("NonExistentClass5"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldIdMapping() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        PortableTypeConfiguration customType1 = new PortableTypeConfiguration(Value.class.getName());

        customType1.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 300;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                switch (fieldName) {
                    case "val1":
                        return 301;

                    case "val2":
                        return 302;

                    default:
                        return 0;
                }
            }
        });

        PortableTypeConfiguration customType2 = new PortableTypeConfiguration("NonExistentClass1");

        customType2.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 400;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                switch (fieldName) {
                    case "val1":
                        return 401;

                    case "val2":
                        return 402;

                    default:
                        return 0;
                }
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(new PortableTypeConfiguration(Key.class.getName()),
                                                  new PortableTypeConfiguration("NonExistentClass2"),
                                                  customType1,
                                                  customType2));

        PortableContext ctx = initPortableContext(marsh);

        assertEquals("val".hashCode(), ctx.fieldId("key".hashCode(), "val"));
        assertEquals("val".hashCode(), ctx.fieldId("nonexistentclass2".hashCode(), "val"));
        assertEquals("val".hashCode(), ctx.fieldId("notconfiguredclass".hashCode(), "val"));
        assertEquals(301, ctx.fieldId(300, "val1"));
        assertEquals(302, ctx.fieldId(300, "val2"));
        assertEquals("val3".hashCode(), ctx.fieldId(300, "val3"));
        assertEquals(401, ctx.fieldId(400, "val1"));
        assertEquals(402, ctx.fieldId(400, "val2"));
        assertEquals("val3".hashCode(), ctx.fieldId(400, "val3"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDuplicateTypeId() throws Exception {
        final PortableMarshaller marsh = new PortableMarshaller();

        PortableTypeConfiguration customType1 = new PortableTypeConfiguration("org.gridgain.Class1");

        customType1.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        PortableTypeConfiguration customType2 = new PortableTypeConfiguration("org.gridgain.Class2");

        customType2.setIdMapper(new PortableIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        marsh.setTypeConfigurations(Arrays.asList(customType1, customType2));

        try {
            initPortableContext(marsh);
        }
        catch (IgniteCheckedException e) {
            assertEquals("Duplicate type ID [clsName=org.gridgain.Class1, id=100]",
                e.getCause().getCause().getMessage());

            return;
        }

        assert false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopy() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        final PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, null);

        assertEquals(obj, copy.deserialize());

        copy = copy(po, new HashMap<String, Object>());

        assertEquals(obj, copy.deserialize());

        Map<String, Object> map = new HashMap<>(1, 1.0f);

        map.put("i", 3);

        copy = copy(po, map);

        assertEquals((byte)2, copy.<Byte>field("b").byteValue());
        assertEquals((short)2, copy.<Short>field("s").shortValue());
        assertEquals(3, copy.<Integer>field("i").intValue());
        assertEquals(2L, copy.<Long>field("l").longValue());
        assertEquals(2.2f, copy.<Float>field("f").floatValue(), 0);
        assertEquals(2.2d, copy.<Double>field("d").doubleValue(), 0);
        assertEquals((char)2, copy.<Character>field("c").charValue());
        assertEquals(false, copy.<Boolean>field("bool").booleanValue());

        SimpleObject obj0 = copy.deserialize();

        assertEquals((byte)2, obj0.b);
        assertEquals((short)2, obj0.s);
        assertEquals(3, obj0.i);
        assertEquals(2L, obj0.l);
        assertEquals(2.2f, obj0.f, 0);
        assertEquals(2.2d, obj0.d, 0);
        assertEquals((char)2, obj0.c);
        assertEquals(false, obj0.bool);

        map = new HashMap<>(3, 1.0f);

        map.put("b", (byte)3);
        map.put("l", 3L);
        map.put("bool", true);

        copy = copy(po, map);

        assertEquals((byte)3, copy.<Byte>field("b").byteValue());
        assertEquals((short)2, copy.<Short>field("s").shortValue());
        assertEquals(2, copy.<Integer>field("i").intValue());
        assertEquals(3L, copy.<Long>field("l").longValue());
        assertEquals(2.2f, copy.<Float>field("f").floatValue(), 0);
        assertEquals(2.2d, copy.<Double>field("d").doubleValue(), 0);
        assertEquals((char)2, copy.<Character>field("c").charValue());
        assertEquals(true, copy.<Boolean>field("bool").booleanValue());

        obj0 = copy.deserialize();

        assertEquals((byte)3, obj0.b);
        assertEquals((short)2, obj0.s);
        assertEquals(2, obj0.i);
        assertEquals(3L, obj0.l);
        assertEquals(2.2f, obj0.f, 0);
        assertEquals(2.2d, obj0.d, 0);
        assertEquals((char)2, obj0.c);
        assertEquals(true, obj0.bool);

        map = new HashMap<>(8, 1.0f);

        map.put("b", (byte)3);
        map.put("s", (short)3);
        map.put("i", 3);
        map.put("l", 3L);
        map.put("f", 3.3f);
        map.put("d", 3.3d);
        map.put("c", (char)3);
        map.put("bool", true);

        copy = copy(po, map);

        assertEquals((byte)3, copy.<Byte>field("b").byteValue());
        assertEquals((short)3, copy.<Short>field("s").shortValue());
        assertEquals(3, copy.<Integer>field("i").intValue());
        assertEquals(3L, copy.<Long>field("l").longValue());
        assertEquals(3.3f, copy.<Float>field("f").floatValue(), 0);
        assertEquals(3.3d, copy.<Double>field("d").doubleValue(), 0);
        assertEquals((char)3, copy.<Character>field("c").charValue());
        assertEquals(true, copy.<Boolean>field("bool").booleanValue());

        obj0 = copy.deserialize();

        assertEquals((byte)3, obj0.b);
        assertEquals((short)3, obj0.s);
        assertEquals(3, obj0.i);
        assertEquals(3L, obj0.l);
        assertEquals(3.3f, obj0.f, 0);
        assertEquals(3.3d, obj0.d, 0);
        assertEquals((char)3, obj0.c);
        assertEquals(true, obj0.bool);

//        GridTestUtils.assertThrows(
//            log,
//            new Callable<Object>() {
//                @Override public Object call() throws Exception {
//                    po.copy(F.<String, Object>asMap("i", false));
//
//                    return null;
//                }
//            },
//            PortableException.class,
//            "Invalid value type for field: i"
//        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyString() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("str", "str3"));

        assertEquals("str3", copy.<String>field("str"));

        SimpleObject obj0 = copy.deserialize();

        assertEquals("str3", obj0.str);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyUuid() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        UUID uuid = UUID.randomUUID();

        PortableObject copy = copy(po, F.<String, Object>asMap("uuid", uuid));

        assertEquals(uuid, copy.<UUID>field("uuid"));

        SimpleObject obj0 = copy.deserialize();

        assertEquals(uuid, obj0.uuid);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyByteArray() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("bArr", new byte[]{1, 2, 3}));

        assertArrayEquals(new byte[] {1, 2, 3}, copy.<byte[]>field("bArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new byte[] {1, 2, 3}, obj0.bArr);
    }

    /**
     * @param po Portable object.
     * @param fields Fields.
     * @return Copy.
     */
    private PortableObject copy(PortableObject po, Map<String, Object> fields) {
        PortableBuilder builder = PortableBuilderImpl.wrap(po);

        if (fields != null) {
            for (Map.Entry<String, Object> e : fields.entrySet())
                builder.setField(e.getKey(), e.getValue());
        }

        return builder.build();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyShortArray() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("sArr", new short[]{1, 2, 3}));

        assertArrayEquals(new short[] {1, 2, 3}, copy.<short[]>field("sArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new short[] {1, 2, 3}, obj0.sArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyIntArray() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("iArr", new int[]{1, 2, 3}));

        assertArrayEquals(new int[] {1, 2, 3}, copy.<int[]>field("iArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new int[] {1, 2, 3}, obj0.iArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyLongArray() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("lArr", new long[]{1, 2, 3}));

        assertArrayEquals(new long[] {1, 2, 3}, copy.<long[]>field("lArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new long[] {1, 2, 3}, obj0.lArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyFloatArray() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("fArr", new float[]{1, 2, 3}));

        assertArrayEquals(new float[] {1, 2, 3}, copy.<float[]>field("fArr"), 0);

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new float[] {1, 2, 3}, obj0.fArr, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyDoubleArray() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("dArr", new double[]{1, 2, 3}));

        assertArrayEquals(new double[] {1, 2, 3}, copy.<double[]>field("dArr"), 0);

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new double[] {1, 2, 3}, obj0.dArr, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyCharArray() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("cArr", new char[]{1, 2, 3}));

        assertArrayEquals(new char[]{1, 2, 3}, copy.<char[]>field("cArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new char[]{1, 2, 3}, obj0.cArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyStringArray() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        PortableObject copy = copy(po, F.<String, Object>asMap("strArr", new String[]{"str1", "str2"}));

        assertArrayEquals(new String[]{"str1", "str2"}, copy.<String[]>field("strArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new String[]{"str1", "str2"}, obj0.strArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyObject() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        SimpleObject newObj = new SimpleObject();

        newObj.i = 12345;
        newObj.fArr = new float[] {5, 8, 0};
        newObj.str = "newStr";

        PortableObject copy = copy(po, F.<String, Object>asMap("inner", newObj));

        assertEquals(newObj, copy.<PortableObject>field("inner").deserialize());

        SimpleObject obj0 = copy.deserialize();

        assertEquals(newObj, obj0.inner);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyNonPrimitives() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        Map<String, Object> map = new HashMap<>(3, 1.0f);

        SimpleObject newObj = new SimpleObject();

        newObj.i = 12345;
        newObj.fArr = new float[] {5, 8, 0};
        newObj.str = "newStr";

        map.put("str", "str555");
        map.put("inner", newObj);
        map.put("bArr", new byte[]{6, 7, 9});

        PortableObject copy = copy(po, map);

        assertEquals("str555", copy.<String>field("str"));
        assertEquals(newObj, copy.<PortableObject>field("inner").deserialize());
        assertArrayEquals(new byte[]{6, 7, 9}, copy.<byte[]>field("bArr"));

        SimpleObject obj0 = copy.deserialize();

        assertEquals("str555", obj0.str);
        assertEquals(newObj, obj0.inner);
        assertArrayEquals(new byte[] {6, 7, 9}, obj0.bArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPortableCopyMixed() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(new PortableTypeConfiguration(SimpleObject.class.getName())));

        SimpleObject obj = simpleObject();

        PortableObject po = marshal(obj, marsh);

        Map<String, Object> map = new HashMap<>(3, 1.0f);

        SimpleObject newObj = new SimpleObject();

        newObj.i = 12345;
        newObj.fArr = new float[] {5, 8, 0};
        newObj.str = "newStr";

        map.put("i", 1234);
        map.put("str", "str555");
        map.put("inner", newObj);
        map.put("s", (short)2323);
        map.put("bArr", new byte[]{6, 7, 9});
        map.put("b", (byte)111);

        PortableObject copy = copy(po, map);

        assertEquals(1234, copy.<Integer>field("i").intValue());
        assertEquals("str555", copy.<String>field("str"));
        assertEquals(newObj, copy.<PortableObject>field("inner").deserialize());
        assertEquals((short)2323, copy.<Short>field("s").shortValue());
        assertArrayEquals(new byte[] {6, 7, 9}, copy.<byte[]>field("bArr"));
        assertEquals((byte)111, copy.<Byte>field("b").byteValue());

        SimpleObject obj0 = copy.deserialize();

        assertEquals(1234, obj0.i);
        assertEquals("str555", obj0.str);
        assertEquals(newObj, obj0.inner);
        assertEquals((short)2323, obj0.s);
        assertArrayEquals(new byte[] {6, 7, 9}, obj0.bArr);
        assertEquals((byte)111, obj0.b);
    }

    /**
     * @throws Exception If failed.
     */
    public void testKeepDeserialized() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(SimpleObject.class.getName()));
        marsh.setKeepDeserialized(true);

        PortableObject po = marshal(simpleObject(), marsh);

        assert po.deserialize() == po.deserialize();

        marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(SimpleObject.class.getName()));
        marsh.setKeepDeserialized(false);

        po = marshal(simpleObject(), marsh);

        assert po.deserialize() != po.deserialize();

        marsh = new PortableMarshaller();

        marsh.setKeepDeserialized(true);
        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())));

        po = marshal(simpleObject(), marsh);

        assert po.deserialize() == po.deserialize();

        marsh = new PortableMarshaller();

        marsh.setKeepDeserialized(false);
        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(SimpleObject.class.getName())));

        po = marshal(simpleObject(), marsh);

        assert po.deserialize() != po.deserialize();

        marsh = new PortableMarshaller();

        marsh.setKeepDeserialized(true);

        PortableTypeConfiguration typeCfg = new PortableTypeConfiguration(SimpleObject.class.getName());

        typeCfg.setKeepDeserialized(false);

        marsh.setTypeConfigurations(Arrays.asList(typeCfg));

        po = marshal(simpleObject(), marsh);

        assert po.deserialize() != po.deserialize();

        marsh = new PortableMarshaller();

        marsh.setKeepDeserialized(false);

        typeCfg = new PortableTypeConfiguration(SimpleObject.class.getName());

        typeCfg.setKeepDeserialized(true);

        marsh.setTypeConfigurations(Arrays.asList(typeCfg));

        po = marshal(simpleObject(), marsh);

        assert po.deserialize() == po.deserialize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffheapPortable() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(new PortableTypeConfiguration(SimpleObject.class.getName())));

        PortableContext ctx = initPortableContext(marsh);

        SimpleObject simpleObj = simpleObject();

        PortableObjectImpl obj = marshal(simpleObj, marsh);

        long ptr = 0;

        long ptr1 = 0;

        long ptr2 = 0;

        try {
            ptr = copyOffheap(obj);

            PortableObjectOffheapImpl offheapObj = new PortableObjectOffheapImpl(ctx,
                ptr,
                0,
                obj.array().length);

            assertTrue(offheapObj.equals(offheapObj));
            assertFalse(offheapObj.equals(null));
            assertFalse(offheapObj.equals("str"));
            assertTrue(offheapObj.equals(obj));
            assertTrue(obj.equals(offheapObj));

            ptr1 = copyOffheap(obj);

            PortableObjectOffheapImpl offheapObj1 = new PortableObjectOffheapImpl(ctx,
                ptr1,
                0,
                obj.array().length);

            assertTrue(offheapObj.equals(offheapObj1));
            assertTrue(offheapObj1.equals(offheapObj));

            assertEquals(obj.typeId(), offheapObj.typeId());
            assertEquals(obj.hashCode(), offheapObj.hashCode());

            checkSimpleObjectData(simpleObj, offheapObj);

            PortableObjectOffheapImpl innerOffheapObj = offheapObj.field("inner");

            assertNotNull(innerOffheapObj);

            checkSimpleObjectData(simpleObj.inner, innerOffheapObj);

            obj = (PortableObjectImpl)offheapObj.heapCopy();

            assertEquals(obj.typeId(), offheapObj.typeId());
            assertEquals(obj.hashCode(), offheapObj.hashCode());

            checkSimpleObjectData(simpleObj, obj);

            PortableObjectImpl innerObj = obj.field("inner");

            assertNotNull(innerObj);

            checkSimpleObjectData(simpleObj.inner, innerObj);

            simpleObj.d = 0;

            obj = marshal(simpleObj, marsh);

            assertFalse(offheapObj.equals(obj));
            assertFalse(obj.equals(offheapObj));

            ptr2 = copyOffheap(obj);

            PortableObjectOffheapImpl offheapObj2 = new PortableObjectOffheapImpl(ctx,
                ptr2,
                0,
                obj.array().length);

            assertFalse(offheapObj.equals(offheapObj2));
            assertFalse(offheapObj2.equals(offheapObj));
        }
        finally {
            UNSAFE.freeMemory(ptr);

            if (ptr1 > 0)
                UNSAFE.freeMemory(ptr1);

            if (ptr2 > 0)
                UNSAFE.freeMemory(ptr2);
        }
    }

    /**
     *
     */
    public void testReadResolve() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(
            Arrays.asList(MySingleton.class.getName(), SingletonMarker.class.getName()));

        PortableObjectImpl portableObj = marshal(MySingleton.INSTANCE, marsh);

        assertTrue(portableObj.array().length <= 1024); // Check that big string was not serialized.

        MySingleton singleton = portableObj.deserialize();

        assertSame(MySingleton.INSTANCE, singleton);
    }

    /**
     *
     */
    public void testReadResolveOnPortableAware() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Collections.singletonList(MyTestClass.class.getName()));

        PortableObjectImpl portableObj = marshal(new MyTestClass(), marsh);

        MyTestClass obj = portableObj.deserialize();

        assertEquals("readResolve", obj.s);
    }

    /**
     * @throws Exception If ecxeption thrown.
     */
    public void testDeclareReadResolveInParent() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        marsh.setClassNames(Arrays.asList(ChildPortable.class.getName()));

        PortableObjectImpl portableObj = marshal(new ChildPortable(), marsh);

        ChildPortable singleton = portableObj.deserialize();

        assertNotNull(singleton.s);
    }

    /**
     *
     */
    public void testDecimalFields() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        Collection<String> clsNames = new ArrayList<>();

        clsNames.add(DecimalReflective.class.getName());
        clsNames.add(DecimalMarshalAware.class.getName());

        marsh.setClassNames(clsNames);

        // 1. Test reflective stuff.
        DecimalReflective obj1 = new DecimalReflective();

        obj1.val = BigDecimal.ZERO;
        obj1.valArr = new BigDecimal[] { BigDecimal.ONE, BigDecimal.TEN };

        PortableObjectImpl portObj = marshal(obj1, marsh);

        assertEquals(obj1.val, portObj.field("val"));
        assertArrayEquals(obj1.valArr, portObj.<BigDecimal[]>field("valArr"));

        assertEquals(obj1.val, portObj.<DecimalReflective>deserialize().val);
        assertArrayEquals(obj1.valArr, portObj.<DecimalReflective>deserialize().valArr);

        // 2. Test marshal aware stuff.
        DecimalMarshalAware obj2 = new DecimalMarshalAware();

        obj2.val = BigDecimal.ZERO;
        obj2.valArr = new BigDecimal[] { BigDecimal.ONE, BigDecimal.TEN.negate() };
        obj2.rawVal = BigDecimal.TEN;
        obj2.rawValArr = new BigDecimal[] { BigDecimal.ZERO, BigDecimal.ONE };

        portObj = marshal(obj2, marsh);

        assertEquals(obj2.val, portObj.field("val"));
        assertArrayEquals(obj2.valArr, portObj.<BigDecimal[]>field("valArr"));

        assertEquals(obj2.val, portObj.<DecimalMarshalAware>deserialize().val);
        assertArrayEquals(obj2.valArr, portObj.<DecimalMarshalAware>deserialize().valArr);
        assertEquals(obj2.rawVal, portObj.<DecimalMarshalAware>deserialize().rawVal);
        assertArrayEquals(obj2.rawValArr, portObj.<DecimalMarshalAware>deserialize().rawValArr);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testFinalField() throws IgniteCheckedException {
        PortableMarshaller marsh = new PortableMarshaller();

        SimpleObjectWithFinal obj = new SimpleObjectWithFinal();

        SimpleObjectWithFinal po0 = marshalUnmarshal(obj, marsh);

        assertEquals(obj.time, po0.time);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testThreadLocalArrayReleased() throws IgniteCheckedException {
        // Checking the writer directly.
        assertEquals(false, THREAD_LOCAL_ALLOC.isThreadLocalArrayAcquired());

        try (PortableWriterExImpl writer = new PortableWriterExImpl(initPortableContext(new PortableMarshaller()), 0)) {
            assertEquals(true, THREAD_LOCAL_ALLOC.isThreadLocalArrayAcquired());

            writer.writeString("Thread local test");

            writer.array();

            assertEquals(true, THREAD_LOCAL_ALLOC.isThreadLocalArrayAcquired());
        }

        // Checking the portable marshaller.
        assertEquals(false, THREAD_LOCAL_ALLOC.isThreadLocalArrayAcquired());

        PortableMarshaller marsh = new PortableMarshaller();

        initPortableContext(marsh);

        marsh.marshal(new SimpleObject());

        assertEquals(false, THREAD_LOCAL_ALLOC.isThreadLocalArrayAcquired());

        // Checking the builder.
        PortableBuilder builder = new PortableBuilderImpl(initPortableContext(new PortableMarshaller()),
            "org.gridgain.foo.bar.TestClass");

        builder.setField("a", "1");

        PortableObject portableObj = builder.build();

        assertEquals(false, THREAD_LOCAL_ALLOC.isThreadLocalArrayAcquired());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDuplicateName() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        initPortableContext(marsh);

        Test1.Job job1 = new Test1().new Job();
        Test2.Job job2 = new Test2().new Job();

        marsh.marshal(job1);

        try {
            marsh.marshal(job2);
        } catch (PortableException e) {
            assertEquals(true, e.getMessage().contains("Failed to register class"));
            return;
        }

        assert false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassFieldsMarshalling() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        initPortableContext(marsh);

        ObjectWithClassFields obj = new ObjectWithClassFields();
        obj.cls1 = GridPortableMarshallerSelfTest.class;

        byte[] marshal = marsh.marshal(obj);

        ObjectWithClassFields obj2 = marsh.unmarshal(marshal, null);

        assertEquals(obj.cls1, obj2.cls1);
        assertNull(obj2.cls2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMarshallingThroughJdk() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        initPortableContext(marsh);

        InetSocketAddress addr = new InetSocketAddress("192.168.0.2", 4545);

        byte[] arr = marsh.marshal(addr);

        InetSocketAddress addr2 = marsh.unmarshal(arr, null);

        assertEquals(addr.getHostString(), addr2.getHostString());
        assertEquals(addr.getPort(), addr2.getPort());

        TestAddress testAddr = new TestAddress();
        testAddr.addr = addr;
        testAddr.str1 = "Hello World";

        SimpleObject simpleObj = new SimpleObject();
        simpleObj.c = 'g';
        simpleObj.date = new Date();

        testAddr.obj = simpleObj;

        arr = marsh.marshal(testAddr);

        TestAddress testAddr2 = marsh.unmarshal(arr, null);

        assertEquals(testAddr.addr.getHostString(), testAddr2.addr.getHostString());
        assertEquals(testAddr.addr.getPort(), testAddr2.addr.getPort());
        assertEquals(testAddr.str1, testAddr2.str1);
        assertEquals(testAddr.obj.c, testAddr2.obj.c);
        assertEquals(testAddr.obj.date, testAddr2.obj.date);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPredefinedTypeIds() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        PortableContext pCtx = initPortableContext(marsh);

        Field field = pCtx.getClass().getDeclaredField("predefinedTypeNames");

        field.setAccessible(true);

        Map<String, Integer> map = (Map<String, Integer>)field.get(pCtx);

        assertTrue(map.size() > 0);

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            int id = entry.getValue();

            if (id == GridPortableMarshaller.UNREGISTERED_TYPE_ID)
                continue;

            PortableClassDescriptor desc = pCtx.descriptorForTypeId(false, entry.getValue(), null);

            assertEquals(desc.typeId(), pCtx.typeId(desc.describedClass().getName()));
            assertEquals(desc.typeId(), pCtx.typeId(pCtx.typeName(desc.describedClass().getName())));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCyclicReferencesMarshalling() throws Exception {
        PortableMarshaller marsh = new PortableMarshaller();

        SimpleObject obj = simpleObject();

        obj.bArr = obj.inner.bArr;
        obj.cArr = obj.inner.cArr;
        obj.boolArr = obj.inner.boolArr;
        obj.sArr = obj.inner.sArr;
        obj.strArr = obj.inner.strArr;
        obj.iArr = obj.inner.iArr;
        obj.lArr = obj.inner.lArr;
        obj.fArr = obj.inner.fArr;
        obj.dArr = obj.inner.dArr;
        obj.dateArr = obj.inner.dateArr;
        obj.uuidArr = obj.inner.uuidArr;
        obj.objArr = obj.inner.objArr;
        obj.bdArr = obj.inner.bdArr;
        obj.map = obj.inner.map;
        obj.col = obj.inner.col;
        obj.mEntry = obj.inner.mEntry;

        SimpleObject res = (SimpleObject)marshalUnmarshal(obj, marsh);

        assertEquals(obj, res);

        assertTrue(res.bArr == res.inner.bArr);
        assertTrue(res.cArr == res.inner.cArr);
        assertTrue(res.boolArr == res.inner.boolArr);
        assertTrue(res.sArr == res.inner.sArr);
        assertTrue(res.strArr == res.inner.strArr);
        assertTrue(res.iArr == res.inner.iArr);
        assertTrue(res.lArr == res.inner.lArr);
        assertTrue(res.fArr == res.inner.fArr);
        assertTrue(res.dArr == res.inner.dArr);
        assertTrue(res.dateArr == res.inner.dateArr);
        assertTrue(res.uuidArr == res.inner.uuidArr);
        assertTrue(res.objArr == res.inner.objArr);
        assertTrue(res.bdArr == res.inner.bdArr);
        assertTrue(res.map == res.inner.map);
        assertTrue(res.col == res.inner.col);
        assertTrue(res.mEntry == res.inner.mEntry);
    }

    /**
     *
     */
    private static class ObjectWithClassFields {
        private Class<?> cls1;

        private Class<?> cls2;
    }

    /**
     *
     */
    private static class TestAddress {
        /** */
        private SimpleObject obj;

        /** */
        private InetSocketAddress addr;

        /** */
        private String str1;
    }

    /**
     *
     */
    private static class Test1 {
        /**
         *
         */
        private class Job {

        }
    }

    /**
     *
     */
    private static class Test2 {
        /**
         *
         */
        private class Job {

        }
    }

    /**
     * @param obj Object.
     * @return Offheap address.
     */
    private long copyOffheap(PortableObjectImpl obj) {
        byte[] arr = obj.array();

        long ptr = UNSAFE.allocateMemory(arr.length);

        UNSAFE.copyMemory(arr, BYTE_ARR_OFF, null, ptr, arr.length);

        return ptr;
    }

    /**
     * @param enumArr Enum array.
     * @return Ordinals.
     */
    private <T extends Enum<?>> Integer[] ordinals(T[] enumArr) {
        Integer[] ords = new Integer[enumArr.length];

        for (int i = 0; i < enumArr.length; i++)
            ords[i] = enumArr[i].ordinal();

        return ords;
    }

    /**
     * @param po Portable object.
     * @param off Offset.
     * @return Value.
     */
    private int intFromPortable(PortableObject po, int off) {
        byte[] arr = U.field(po, "arr");

        return Integer.reverseBytes(U.bytesToInt(arr, off));
    }

    /**
     * @param obj Original object.
     * @return Result object.
     */
    private <T> T marshalUnmarshal(T obj) throws IgniteCheckedException {
        return marshalUnmarshal(obj, new PortableMarshaller());
    }

    /**
     * @param obj Original object.
     * @param marsh Marshaller.
     * @return Result object.
     */
    private <T> T marshalUnmarshal(Object obj, PortableMarshaller marsh) throws IgniteCheckedException {
        initPortableContext(marsh);

        byte[] bytes = marsh.marshal(obj);

        return marsh.unmarshal(bytes, null);
    }

    /**
     * @param obj Object.
     * @param marsh Marshaller.
     * @return Portable object.
     */
    private <T> PortableObjectImpl marshal(T obj, PortableMarshaller marsh) throws IgniteCheckedException {
        initPortableContext(marsh);

        byte[] bytes = marsh.marshal(obj);

        return new PortableObjectImpl(U.<GridPortableMarshaller>field(marsh, "impl").context(),
            bytes, 0);
    }

    /**
     * @return Portable context.
     */
    protected PortableContext initPortableContext(PortableMarshaller marsh) throws IgniteCheckedException {
        PortableContext ctx = new PortableContext(META_HND, null);

        marsh.setContext(new MarshallerContextTestImpl(null));

        IgniteUtils.invoke(PortableMarshaller.class, marsh, "setPortableContext", ctx);

        return ctx;
    }

    /**
     * @param exp Expected.
     * @param act Actual.
     */
    private void assertBooleanArrayEquals(boolean[] exp, boolean[] act) {
        assertEquals(exp.length, act.length);

        for (int i = 0; i < act.length; i++)
            assertEquals(exp[i], act[i]);
    }

    /**
     *
     */
    private static class SimpleObjectWithFinal {
        /** */
        private final long time = System.currentTimeMillis();
    }

    /**
     * @return Simple object.
     */
    private SimpleObject simpleObject() {
        SimpleObject inner = new SimpleObject();

        inner.b = 1;
        inner.s = 1;
        inner.i = 1;
        inner.l = 1;
        inner.f = 1.1f;
        inner.d = 1.1d;
        inner.c = 1;
        inner.bool = true;
        inner.str = "str1";
        inner.uuid = UUID.randomUUID();
        inner.date = new Date();
        inner.ts = new Timestamp(System.currentTimeMillis());
        inner.bArr = new byte[] {1, 2, 3};
        inner.sArr = new short[] {1, 2, 3};
        inner.iArr = new int[] {1, 2, 3};
        inner.lArr = new long[] {1, 2, 3};
        inner.fArr = new float[] {1.1f, 2.2f, 3.3f};
        inner.dArr = new double[] {1.1d, 2.2d, 3.3d};
        inner.cArr = new char[] {1, 2, 3};
        inner.boolArr = new boolean[] {true, false, true};
        inner.strArr = new String[] {"str1", "str2", "str3"};
        inner.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        inner.dateArr = new Date[] {new Date(11111), new Date(22222), new Date(33333)};
        inner.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        inner.col = new ArrayList<>();
        inner.map = new HashMap<>();
        inner.enumVal = TestEnum.A;
        inner.enumArr = new TestEnum[] {TestEnum.A, TestEnum.B};
        inner.bdArr = new BigDecimal[] {new BigDecimal(1000), BigDecimal.ONE};

        inner.col.add("str1");
        inner.col.add("str2");
        inner.col.add("str3");

        inner.map.put(1, "str1");
        inner.map.put(2, "str2");
        inner.map.put(3, "str3");

        inner.mEntry = inner.map.entrySet().iterator().next();

        SimpleObject outer = new SimpleObject();

        outer.b = 2;
        outer.s = 2;
        outer.i = 2;
        outer.l = 2;
        outer.f = 2.2f;
        outer.d = 2.2d;
        outer.c = 2;
        outer.bool = false;
        outer.str = "str2";
        outer.uuid = UUID.randomUUID();
        outer.date = new Date();
        outer.ts = new Timestamp(System.currentTimeMillis());
        outer.bArr = new byte[] {10, 20, 30};
        outer.sArr = new short[] {10, 20, 30};
        outer.iArr = new int[] {10, 20, 30};
        outer.lArr = new long[] {10, 20, 30};
        outer.fArr = new float[] {10.01f, 20.02f, 30.03f};
        outer.dArr = new double[] {10.01d, 20.02d, 30.03d};
        outer.cArr = new char[] {10, 20, 30};
        outer.boolArr = new boolean[] {false, true, false};
        outer.strArr = new String[] {"str10", "str20", "str30"};
        outer.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        outer.dateArr = new Date[] {new Date(44444), new Date(55555), new Date(66666)};
        outer.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        outer.col = new ArrayList<>();
        outer.map = new HashMap<>();
        outer.enumVal = TestEnum.B;
        outer.enumArr = new TestEnum[] {TestEnum.B, TestEnum.C};
        outer.inner = inner;
        outer.bdArr = new BigDecimal[] {new BigDecimal(5000), BigDecimal.TEN};


        outer.col.add("str4");
        outer.col.add("str5");
        outer.col.add("str6");

        outer.map.put(4, "str4");
        outer.map.put(5, "str5");
        outer.map.put(6, "str6");

        outer.mEntry = outer.map.entrySet().iterator().next();

        return outer;
    }

    /**
     * @return Portable object.
     */
    private TestPortableObject portableObject() {
        SimpleObject innerSimple = new SimpleObject();

        innerSimple.b = 1;
        innerSimple.s = 1;
        innerSimple.i = 1;
        innerSimple.l = 1;
        innerSimple.f = 1.1f;
        innerSimple.d = 1.1d;
        innerSimple.c = 1;
        innerSimple.bool = true;
        innerSimple.str = "str1";
        innerSimple.uuid = UUID.randomUUID();
        innerSimple.date = new Date();
        innerSimple.ts = new Timestamp(System.currentTimeMillis());
        innerSimple.bArr = new byte[] {1, 2, 3};
        innerSimple.sArr = new short[] {1, 2, 3};
        innerSimple.iArr = new int[] {1, 2, 3};
        innerSimple.lArr = new long[] {1, 2, 3};
        innerSimple.fArr = new float[] {1.1f, 2.2f, 3.3f};
        innerSimple.dArr = new double[] {1.1d, 2.2d, 3.3d};
        innerSimple.cArr = new char[] {1, 2, 3};
        innerSimple.boolArr = new boolean[] {true, false, true};
        innerSimple.strArr = new String[] {"str1", "str2", "str3"};
        innerSimple.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerSimple.dateArr = new Date[] {new Date(11111), new Date(22222), new Date(33333)};
        innerSimple.objArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerSimple.col = new ArrayList<>();
        innerSimple.map = new HashMap<>();
        innerSimple.enumVal = TestEnum.A;
        innerSimple.enumArr = new TestEnum[] {TestEnum.A, TestEnum.B};

        innerSimple.col.add("str1");
        innerSimple.col.add("str2");
        innerSimple.col.add("str3");

        innerSimple.map.put(1, "str1");
        innerSimple.map.put(2, "str2");
        innerSimple.map.put(3, "str3");

        TestPortableObject innerPortable = new TestPortableObject();

        innerPortable.b = 2;
        innerPortable.s = 2;
        innerPortable.i = 2;
        innerPortable.l = 2;
        innerPortable.f = 2.2f;
        innerPortable.d = 2.2d;
        innerPortable.c = 2;
        innerPortable.bool = true;
        innerPortable.str = "str2";
        innerPortable.uuid = UUID.randomUUID();
        innerPortable.date = new Date();
        innerPortable.ts = new Timestamp(System.currentTimeMillis());
        innerPortable.bArr = new byte[] {10, 20, 30};
        innerPortable.sArr = new short[] {10, 20, 30};
        innerPortable.iArr = new int[] {10, 20, 30};
        innerPortable.lArr = new long[] {10, 20, 30};
        innerPortable.fArr = new float[] {10.01f, 20.02f, 30.03f};
        innerPortable.dArr = new double[] {10.01d, 20.02d, 30.03d};
        innerPortable.cArr = new char[] {10, 20, 30};
        innerPortable.boolArr = new boolean[] {true, false, true};
        innerPortable.strArr = new String[] {"str10", "str20", "str30"};
        innerPortable.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerPortable.dateArr = new Date[] {new Date(44444), new Date(55555), new Date(66666)};
        innerPortable.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerPortable.bRaw = 3;
        innerPortable.sRaw = 3;
        innerPortable.iRaw = 3;
        innerPortable.lRaw = 3;
        innerPortable.fRaw = 3.3f;
        innerPortable.dRaw = 3.3d;
        innerPortable.cRaw = 3;
        innerPortable.boolRaw = true;
        innerPortable.strRaw = "str3";
        innerPortable.uuidRaw = UUID.randomUUID();
        innerPortable.dateRaw = new Date();
        innerPortable.tsRaw = new Timestamp(System.currentTimeMillis());
        innerPortable.bArrRaw = new byte[] {11, 21, 31};
        innerPortable.sArrRaw = new short[] {11, 21, 31};
        innerPortable.iArrRaw = new int[] {11, 21, 31};
        innerPortable.lArrRaw = new long[] {11, 21, 31};
        innerPortable.fArrRaw = new float[] {11.11f, 21.12f, 31.13f};
        innerPortable.dArrRaw = new double[] {11.11d, 21.12d, 31.13d};
        innerPortable.cArrRaw = new char[] {11, 21, 31};
        innerPortable.boolArrRaw = new boolean[] {true, false, true};
        innerPortable.strArrRaw = new String[] {"str11", "str21", "str31"};
        innerPortable.uuidArrRaw = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerPortable.dateArrRaw = new Date[] {new Date(77777), new Date(88888), new Date(99999)};
        innerPortable.objArrRaw = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerPortable.col = new ArrayList<>();
        innerPortable.colRaw = new ArrayList<>();
        innerPortable.map = new HashMap<>();
        innerPortable.mapRaw = new HashMap<>();
        innerPortable.enumVal = TestEnum.B;
        innerPortable.enumValRaw = TestEnum.C;
        innerPortable.enumArr = new TestEnum[] {TestEnum.B, TestEnum.C};
        innerPortable.enumArrRaw = new TestEnum[] {TestEnum.C, TestEnum.D};

        innerPortable.col.add("str4");
        innerPortable.col.add("str5");
        innerPortable.col.add("str6");

        innerPortable.map.put(4, "str4");
        innerPortable.map.put(5, "str5");
        innerPortable.map.put(6, "str6");

        innerPortable.colRaw.add("str7");
        innerPortable.colRaw.add("str8");
        innerPortable.colRaw.add("str9");

        innerPortable.mapRaw.put(7, "str7");
        innerPortable.mapRaw.put(8, "str8");
        innerPortable.mapRaw.put(9, "str9");

        TestPortableObject outer = new TestPortableObject();

        outer.b = 4;
        outer.s = 4;
        outer.i = 4;
        outer.l = 4;
        outer.f = 4.4f;
        outer.d = 4.4d;
        outer.c = 4;
        outer.bool = true;
        outer.str = "str4";
        outer.uuid = UUID.randomUUID();
        outer.date = new Date();
        outer.ts = new Timestamp(System.currentTimeMillis());
        outer.bArr = new byte[] {12, 22, 32};
        outer.sArr = new short[] {12, 22, 32};
        outer.iArr = new int[] {12, 22, 32};
        outer.lArr = new long[] {12, 22, 32};
        outer.fArr = new float[] {12.21f, 22.22f, 32.23f};
        outer.dArr = new double[] {12.21d, 22.22d, 32.23d};
        outer.cArr = new char[] {12, 22, 32};
        outer.boolArr = new boolean[] {true, false, true};
        outer.strArr = new String[] {"str12", "str22", "str32"};
        outer.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        outer.dateArr = new Date[] {new Date(10101), new Date(20202), new Date(30303)};
        outer.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        outer.simple = innerSimple;
        outer.portable = innerPortable;
        outer.bRaw = 5;
        outer.sRaw = 5;
        outer.iRaw = 5;
        outer.lRaw = 5;
        outer.fRaw = 5.5f;
        outer.dRaw = 5.5d;
        outer.cRaw = 5;
        outer.boolRaw = true;
        outer.strRaw = "str5";
        outer.uuidRaw = UUID.randomUUID();
        outer.dateRaw = new Date();
        outer.tsRaw = new Timestamp(System.currentTimeMillis());
        outer.bArrRaw = new byte[] {13, 23, 33};
        outer.sArrRaw = new short[] {13, 23, 33};
        outer.iArrRaw = new int[] {13, 23, 33};
        outer.lArrRaw = new long[] {13, 23, 33};
        outer.fArrRaw = new float[] {13.31f, 23.32f, 33.33f};
        outer.dArrRaw = new double[] {13.31d, 23.32d, 33.33d};
        outer.cArrRaw = new char[] {13, 23, 33};
        outer.boolArrRaw = new boolean[] {true, false, true};
        outer.strArrRaw = new String[] {"str13", "str23", "str33"};
        outer.uuidArrRaw = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        outer.dateArr = new Date[] {new Date(40404), new Date(50505), new Date(60606)};
        outer.objArrRaw = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        outer.col = new ArrayList<>();
        outer.colRaw = new ArrayList<>();
        outer.map = new HashMap<>();
        outer.mapRaw = new HashMap<>();
        outer.enumVal = TestEnum.D;
        outer.enumValRaw = TestEnum.E;
        outer.enumArr = new TestEnum[] {TestEnum.D, TestEnum.E};
        outer.enumArrRaw = new TestEnum[] {TestEnum.E, TestEnum.A};
        outer.simpleRaw = innerSimple;
        outer.portableRaw = innerPortable;

        outer.col.add("str10");
        outer.col.add("str11");
        outer.col.add("str12");

        outer.map.put(10, "str10");
        outer.map.put(11, "str11");
        outer.map.put(12, "str12");

        outer.colRaw.add("str13");
        outer.colRaw.add("str14");
        outer.colRaw.add("str15");

        outer.mapRaw.put(16, "str16");
        outer.mapRaw.put(17, "str16");
        outer.mapRaw.put(18, "str17");

        return outer;
    }

    /**
     */
    private enum TestEnum {
        A, B, C, D, E
    }

    /** */
    private static class SimpleObject {
        /** */
        private byte b;

        /** */
        private short s;

        /** */
        private int i;

        /** */
        private long l;

        /** */
        private float f;

        /** */
        private double d;

        /** */
        private char c;

        /** */
        private boolean bool;

        /** */
        private String str;

        /** */
        private UUID uuid;

        /** */
        private Date date;

        /** */
        private Timestamp ts;

        /** */
        private byte[] bArr;

        /** */
        private short[] sArr;

        /** */
        private int[] iArr;

        /** */
        private long[] lArr;

        /** */
        private float[] fArr;

        /** */
        private double[] dArr;

        /** */
        private char[] cArr;

        /** */
        private boolean[] boolArr;

        /** */
        private String[] strArr;

        /** */
        private UUID[] uuidArr;

        /** */
        private Date[] dateArr;

        /** */
        private Object[] objArr;

        /** */
        private BigDecimal[] bdArr;

        /** */
        private Collection<String> col;

        /** */
        private Map<Integer, String> map;

        /** */
        private TestEnum enumVal;

        /** */
        private TestEnum[] enumArr;

        /** */
        private Map.Entry<Integer, String> mEntry;

        /** */
        private SimpleObject inner;

        /** {@inheritDoc} */
        @SuppressWarnings("FloatingPointEquality")
        @Override public boolean equals(Object other) {
            if (this == other)
                return true;

            if (other == null || getClass() != other.getClass())
                return false;

            SimpleObject obj = (SimpleObject)other;

            return GridTestUtils.deepEquals(this, obj);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SimpleObject.class, this);
        }
    }

    /** */
    private static class TestPortableObject implements PortableMarshalAware {
        /** */
        private byte b;

        /** */
        private byte bRaw;

        /** */
        private short s;

        /** */
        private short sRaw;

        /** */
        private int i;

        /** */
        private int iRaw;

        /** */
        private long l;

        /** */
        private long lRaw;

        /** */
        private float f;

        /** */
        private float fRaw;

        /** */
        private double d;

        /** */
        private double dRaw;

        /** */
        private char c;

        /** */
        private char cRaw;

        /** */
        private boolean bool;

        /** */
        private boolean boolRaw;

        /** */
        private String str;

        /** */
        private String strRaw;

        /** */
        private UUID uuid;

        /** */
        private UUID uuidRaw;

        /** */
        private Date date;

        /** */
        private Date dateRaw;

        /** */
        private Timestamp ts;

        /** */
        private Timestamp tsRaw;

        /** */
        private byte[] bArr;

        /** */
        private byte[] bArrRaw;

        /** */
        private short[] sArr;

        /** */
        private short[] sArrRaw;

        /** */
        private int[] iArr;

        /** */
        private int[] iArrRaw;

        /** */
        private long[] lArr;

        /** */
        private long[] lArrRaw;

        /** */
        private float[] fArr;

        /** */
        private float[] fArrRaw;

        /** */
        private double[] dArr;

        /** */
        private double[] dArrRaw;

        /** */
        private char[] cArr;

        /** */
        private char[] cArrRaw;

        /** */
        private boolean[] boolArr;

        /** */
        private boolean[] boolArrRaw;

        /** */
        private String[] strArr;

        /** */
        private String[] strArrRaw;

        /** */
        private UUID[] uuidArr;

        /** */
        private UUID[] uuidArrRaw;

        /** */
        private Date[] dateArr;

        /** */
        private Date[] dateArrRaw;

        /** */
        private Object[] objArr;

        /** */
        private Object[] objArrRaw;

        /** */
        private Collection<String> col;

        /** */
        private Collection<String> colRaw;

        /** */
        private Map<Integer, String> map;

        /** */
        private Map<Integer, String> mapRaw;

        /** */
        private TestEnum enumVal;

        /** */
        private TestEnum enumValRaw;

        /** */
        private TestEnum[] enumArr;

        /** */
        private TestEnum[] enumArrRaw;

        /** */
        private SimpleObject simple;

        /** */
        private SimpleObject simpleRaw;

        /** */
        private TestPortableObject portable;

        /** */
        private TestPortableObject portableRaw;

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeByte("_b", b);
            writer.writeShort("_s", s);
            writer.writeInt("_i", i);
            writer.writeLong("_l", l);
            writer.writeFloat("_f", f);
            writer.writeDouble("_d", d);
            writer.writeChar("_c", c);
            writer.writeBoolean("_bool", bool);
            writer.writeString("_str", str);
            writer.writeUuid("_uuid", uuid);
            writer.writeDate("_date", date);
            writer.writeTimestamp("_ts", ts);
            writer.writeByteArray("_bArr", bArr);
            writer.writeShortArray("_sArr", sArr);
            writer.writeIntArray("_iArr", iArr);
            writer.writeLongArray("_lArr", lArr);
            writer.writeFloatArray("_fArr", fArr);
            writer.writeDoubleArray("_dArr", dArr);
            writer.writeCharArray("_cArr", cArr);
            writer.writeBooleanArray("_boolArr", boolArr);
            writer.writeStringArray("_strArr", strArr);
            writer.writeUuidArray("_uuidArr", uuidArr);
            writer.writeDateArray("_dateArr", dateArr);
            writer.writeObjectArray("_objArr", objArr);
            writer.writeCollection("_col", col);
            writer.writeMap("_map", map);
            writer.writeEnum("_enumVal", enumVal);
            writer.writeEnumArray("_enumArr", enumArr);
            writer.writeObject("_simple", simple);
            writer.writeObject("_portable", portable);

            PortableRawWriter raw = writer.rawWriter();

            raw.writeByte(bRaw);
            raw.writeShort(sRaw);
            raw.writeInt(iRaw);
            raw.writeLong(lRaw);
            raw.writeFloat(fRaw);
            raw.writeDouble(dRaw);
            raw.writeChar(cRaw);
            raw.writeBoolean(boolRaw);
            raw.writeString(strRaw);
            raw.writeUuid(uuidRaw);
            raw.writeDate(dateRaw);
            raw.writeTimestamp(tsRaw);
            raw.writeByteArray(bArrRaw);
            raw.writeShortArray(sArrRaw);
            raw.writeIntArray(iArrRaw);
            raw.writeLongArray(lArrRaw);
            raw.writeFloatArray(fArrRaw);
            raw.writeDoubleArray(dArrRaw);
            raw.writeCharArray(cArrRaw);
            raw.writeBooleanArray(boolArrRaw);
            raw.writeStringArray(strArrRaw);
            raw.writeUuidArray(uuidArrRaw);
            raw.writeDateArray(dateArrRaw);
            raw.writeObjectArray(objArrRaw);
            raw.writeCollection(colRaw);
            raw.writeMap(mapRaw);
            raw.writeEnum(enumValRaw);
            raw.writeEnumArray(enumArrRaw);
            raw.writeObject(simpleRaw);
            raw.writeObject(portableRaw);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            b = reader.readByte("_b");
            s = reader.readShort("_s");
            i = reader.readInt("_i");
            l = reader.readLong("_l");
            f = reader.readFloat("_f");
            d = reader.readDouble("_d");
            c = reader.readChar("_c");
            bool = reader.readBoolean("_bool");
            str = reader.readString("_str");
            uuid = reader.readUuid("_uuid");
            date = reader.readDate("_date");
            ts = reader.readTimestamp("_ts");
            bArr = reader.readByteArray("_bArr");
            sArr = reader.readShortArray("_sArr");
            iArr = reader.readIntArray("_iArr");
            lArr = reader.readLongArray("_lArr");
            fArr = reader.readFloatArray("_fArr");
            dArr = reader.readDoubleArray("_dArr");
            cArr = reader.readCharArray("_cArr");
            boolArr = reader.readBooleanArray("_boolArr");
            strArr = reader.readStringArray("_strArr");
            uuidArr = reader.readUuidArray("_uuidArr");
            dateArr = reader.readDateArray("_dateArr");
            objArr = reader.readObjectArray("_objArr");
            col = reader.readCollection("_col");
            map = reader.readMap("_map");
            enumVal = reader.readEnum("_enumVal");
            enumArr = reader.readEnumArray("_enumArr");
            simple = reader.readObject("_simple");
            portable = reader.readObject("_portable");

            PortableRawReader raw = reader.rawReader();

            bRaw = raw.readByte();
            sRaw = raw.readShort();
            iRaw = raw.readInt();
            lRaw = raw.readLong();
            fRaw = raw.readFloat();
            dRaw = raw.readDouble();
            cRaw = raw.readChar();
            boolRaw = raw.readBoolean();
            strRaw = raw.readString();
            uuidRaw = raw.readUuid();
            dateRaw = raw.readDate();
            tsRaw = raw.readTimestamp();
            bArrRaw = raw.readByteArray();
            sArrRaw = raw.readShortArray();
            iArrRaw = raw.readIntArray();
            lArrRaw = raw.readLongArray();
            fArrRaw = raw.readFloatArray();
            dArrRaw = raw.readDoubleArray();
            cArrRaw = raw.readCharArray();
            boolArrRaw = raw.readBooleanArray();
            strArrRaw = raw.readStringArray();
            uuidArrRaw = raw.readUuidArray();
            dateArrRaw = raw.readDateArray();
            objArrRaw = raw.readObjectArray();
            colRaw = raw.readCollection();
            mapRaw = raw.readMap();
            enumValRaw = raw.readEnum();
            enumArrRaw = raw.readEnumArray();
            simpleRaw = raw.readObject();
            portableRaw = raw.readObject();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("FloatingPointEquality")
        @Override public boolean equals(Object other) {
            if (this == other)
                return true;

            if (other == null || getClass() != other.getClass())
                return false;

            TestPortableObject obj = (TestPortableObject)other;

            return GridTestUtils.deepEquals(this, obj);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestPortableObject.class, this);
        }
    }

    /**
     */
    private static class CustomSerializedObject1 implements PortableMarshalAware {
        /** */
        private int val;

        /**
         */
        private CustomSerializedObject1() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private CustomSerializedObject1(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            // No-op.
        }
    }

    /**
     */
    private static class CustomSerializedObject2 implements PortableMarshalAware {
        /** */
        private int val;

        /**
         */
        private CustomSerializedObject2() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private CustomSerializedObject2(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            // No-op.
        }
    }

    /**
     */
    private static class CustomSerializer1 implements PortableSerializer {
        /** {@inheritDoc} */
        @Override public void writePortable(Object obj, PortableWriter writer) throws PortableException {
            CustomSerializedObject1 o = (CustomSerializedObject1)obj;

            writer.writeInt("val", o.val * 2);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(Object obj, PortableReader reader) throws PortableException {
            CustomSerializedObject1 o = (CustomSerializedObject1)obj;

            o.val = reader.readInt("val");
        }
    }

    /**
     */
    private static class CustomSerializer2 implements PortableSerializer {
        /** {@inheritDoc} */
        @Override public void writePortable(Object obj, PortableWriter writer) throws PortableException {
            CustomSerializedObject2 o = (CustomSerializedObject2)obj;

            writer.writeInt("val", o.val * 3);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(Object obj, PortableReader reader) throws PortableException {
            CustomSerializedObject2 o = (CustomSerializedObject2)obj;

            o.val = reader.readInt("val");
        }
    }

    /**
     */
    private static class CustomMappedObject1 {
        /** */
        private int val1;

        /** */
        private String val2;

        /**
         */
        private CustomMappedObject1() {
            // No-op.
        }

        /**
         * @param val1 Value 1.
         * @param val2 Value 2.
         */
        private CustomMappedObject1(int val1, String val2) {
            this.val1 = val1;
            this.val2 = val2;
        }
    }

    /**
     */
    private static class CustomMappedObject2 {
        /** */
        private int val1;

        /** */
        private String val2;

        /**
         */
        private CustomMappedObject2() {
            // No-op.
        }

        /**
         * @param val1 Value 1.
         * @param val2 Value 2.
         */
        private CustomMappedObject2(int val1, String val2) {
            this.val1 = val1;
            this.val2 = val2;
        }
    }

    /**
     */
    private static class DynamicObject implements PortableMarshalAware {
        /** */
        private int idx;

        /** */
        private int val1;

        /** */
        private int val2;

        /** */
        private int val3;

        /**
         */
        private DynamicObject() {
            // No-op.
        }

        /**
         * @param val1 Value 1.
         * @param val2 Value 2.
         * @param val3 Value 3.
         */
        private DynamicObject(int idx, int val1, int val2, int val3) {
            this.idx = idx;
            this.val1 = val1;
            this.val2 = val2;
            this.val3 = val3;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeInt("val1", val1);

            if (idx > 0)
                writer.writeInt("val2", val2);

            if (idx > 1)
                writer.writeInt("val3", val3);

            idx++;
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            val1 = reader.readInt("val1");
            val2 = reader.readInt("val2");
            val3 = reader.readInt("val3");
        }
    }

    /**
     */
    private static class CycleLinkObject {
        /** */
        private CycleLinkObject self;
    }

    /**
     */
    private static class DetachedTestObject implements PortableMarshalAware {
        /** */
        private DetachedInnerTestObject inner1;

        /** */
        private Object inner2;

        /** */
        private Object inner3;

        /** */
        private DetachedInnerTestObject inner4;

        /**
         */
        private DetachedTestObject() {
            // No-op.
        }

        /**
         * @param inner Inner object.
         */
        private DetachedTestObject(DetachedInnerTestObject inner) {
            inner1 = inner;
            inner2 = inner;
            inner3 = new DetachedInnerTestObject(inner, inner.id);
            inner4 = inner;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            PortableRawWriterEx raw = (PortableRawWriterEx)writer.rawWriter();

            raw.writeObject(inner1);
            raw.writeObjectDetached(inner2);
            raw.writeObjectDetached(inner3);
            raw.writeObject(inner4);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            PortableRawReaderEx raw = (PortableRawReaderEx)reader.rawReader();

            inner1 = (DetachedInnerTestObject)raw.readObject();
            inner2 = raw.readObjectDetached();
            inner3 = raw.readObjectDetached();
            inner4 = (DetachedInnerTestObject)raw.readObject();
        }
    }

    /**
     */
    private static class DetachedInnerTestObject {
        /** */
        private DetachedInnerTestObject inner;

        /** */
        private UUID id;

        /**
         */
        private DetachedInnerTestObject() {
            // No-op.
        }

        /**
         * @param inner Inner object.
         * @param id ID.
         */
        private DetachedInnerTestObject(DetachedInnerTestObject inner, UUID id) {
            this.inner = inner;
            this.id = id;
        }
    }

    /**
     */
    @SuppressWarnings("UnusedDeclaration")
    private static class CollectionFieldsObject {
        /** */
        private Object[] arr;

        /** */
        private Collection<Value> col;

        /** */
        private Map<Key, Value> map;

        /**
         */
        private CollectionFieldsObject() {
            // No-op.
        }

        /**
         * @param arr Array.
         * @param col Collection.
         * @param map Map.
         */
        private CollectionFieldsObject(Object[] arr, Collection<Value> col, Map<Key, Value> map) {
            this.arr = arr;
            this.col = col;
            this.map = map;
        }
    }

    /**
     */
    private static class Key {
        /** */
        private int key;

        /**
         */
        private Key() {
            // No-op.
        }

        /**n
         * @param key Key.
         */
        private Key(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key key0 = (Key)o;

            return key == key0.key;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }

    /**
     */
    private static class Value {
        /** */
        private int val;

        /**
         */
        private Value() {
            // No-op.
        }

        /**
         * @param val Value.
         */
        private Value(int val) {
            this.val = val;
        }
    }

    /**
     */
    private static class DateClass1 {
        /** */
        private Date date;

        /** */
        private Timestamp ts;
    }

    /**
     */
    private static class DateClass2 {
        /** */
        private Date date;

        /** */
        private Timestamp ts;
    }

    /**
     *
     */
    private static class NoPublicConstructor {
        /** */
        private String val = "test";

        /**
         * @return Value.
         */
        public String getVal() {
            return val;
        }
    }

    /**
     *
     */
    @SuppressWarnings("PublicConstructorInNonPublicClass")
    private static class NoPublicDefaultConstructor {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public NoPublicDefaultConstructor(int val) {
            this.val = val;
        }
    }

    /**
     *
     */
    private static class ProtectedConstructor {
        /**
         *  Protected constructor.
         */
        protected ProtectedConstructor() {
            // No-op.
        }
    }

    /**
     *
     */
    private static class MyTestClass implements PortableMarshalAware {
        /** */
        private boolean readyToSerialize;

        /** */
        private String s;

        /**
         * @return Object.
         */
        Object writeReplace() {
            readyToSerialize = true;

            return this;
        }

        /**
         * @return Object.
         */
        Object readResolve() {
            s = "readResolve";

            return this;
        }

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            if (!readyToSerialize)
                fail();
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            s = "readPortable";
        }
    }

    /**
     *
     */
    private static class MySingleton {
        public static final MySingleton INSTANCE = new MySingleton();

        /** */
        private String s;

        /** Initializer. */
        {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < 10000; i++) {
                builder.append("+");
            }

            s = builder.toString();
        }

        /**
         * @return Object.
         */
        Object writeReplace() {
            return new SingletonMarker();
        }
    }

    /**
     *
     */
    private static class SingletonMarker {
        /**
         * @return Object.
         */
        Object readResolve() {
            return MySingleton.INSTANCE;
        }
    }

    /**
     *
     */
    public static class ChildPortable extends ParentPortable {

    }

    /**
     *
     */
    private static class ParentPortable {
        /** */
        public String s;

        /**
         * Package only visibility!!!!
         *
         * @return Object.
         */
        Object readResolve() {
            s = "readResolve";

            return this;
        }
    }

    /**
     *
     */
    private static class DecimalReflective {
        /** */
        public BigDecimal val;

        /** */
        public BigDecimal[] valArr;
    }

    /**
     *
     */
    private static class DecimalMarshalAware extends DecimalReflective implements PortableMarshalAware {
        /** */
        public BigDecimal rawVal;

        /** */
        public BigDecimal[] rawValArr;

        /** {@inheritDoc} */
        @Override public void writePortable(PortableWriter writer) throws PortableException {
            writer.writeDecimal("val", val);
            writer.writeDecimalArray("valArr", valArr);

            PortableRawWriter rawWriter = writer.rawWriter();

            rawWriter.writeDecimal(rawVal);
            rawWriter.writeDecimalArray(rawValArr);
        }

        /** {@inheritDoc} */
        @Override public void readPortable(PortableReader reader) throws PortableException {
            val = reader.readDecimal("val");
            valArr = reader.readDecimalArray("valArr");

            PortableRawReader rawReader = reader.rawReader();

            rawVal = rawReader.readDecimal();
            rawValArr = rawReader.readDecimalArray();
        }
    }
}