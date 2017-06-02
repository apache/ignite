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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import junit.framework.Assert;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryBasicIdMapper;
import org.apache.ignite.binary.BinaryBasicNameMapper;
import org.apache.ignite.binary.BinaryCollectionFactory;
import org.apache.ignite.binary.BinaryField;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryMapFactory;
import org.apache.ignite.binary.BinaryNameMapper;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.binary.BinaryReader;
import org.apache.ignite.binary.BinarySerializer;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.binary.BinaryTypeConfiguration;
import org.apache.ignite.binary.BinaryWriter;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridMapEntry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.binary.streams.BinaryMemoryAllocator.INSTANCE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Binary marshaller tests.
 */
@SuppressWarnings({"OverlyStrongTypeCast", "ArrayHashCode", "ConstantConditions"})
public class BinaryMarshallerSelfTest extends GridCommonAbstractTest {
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
        assertEquals(100.001f, marshalUnmarshal(100.001f), 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDouble() throws Exception {
        assertEquals(100.001d, marshalUnmarshal(100.001d), 0);
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
    public void testStringVer1() throws Exception {
        doTestString(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStringVer2() throws Exception {
        doTestString(true);
    }

    /**
     * @throws Exception If failed
     */
    private void doTestString(boolean ver2) throws Exception {
        // Ascii check.
        String str = "ascii0123456789";
        assertEquals(str, marshalUnmarshal(str));

        byte[] bytes = str.getBytes(UTF_8);
        assertEquals(str, BinaryUtils.utf8BytesToStr(bytes, 0, bytes.length));

        bytes = BinaryUtils.strToUtf8Bytes(str);
        assertEquals(str, new String(bytes, UTF_8));

        // Extended symbols set check set.
        str = "的的abcdкириллица";
        assertEquals(str, marshalUnmarshal(str));

        bytes = str.getBytes(UTF_8);
        assertEquals(str, BinaryUtils.utf8BytesToStr(bytes, 0, bytes.length));

        bytes = BinaryUtils.strToUtf8Bytes(str);
        assertEquals(str, new String(bytes, UTF_8));

        // Special symbols check.
        str = new String(new char[] {0xD800, '的', 0xD800, 0xD800, 0xDC00, 0xDFFF});
        if (ver2) {
            bytes = BinaryUtils.strToUtf8Bytes(str);
            assertEquals(str, BinaryUtils.utf8BytesToStr(bytes, 0, bytes.length));
        }
        else
            assertNotEquals(str, marshalUnmarshal(str));

        str = new String(new char[] {55296});
        if (ver2) {
            bytes = BinaryUtils.strToUtf8Bytes(str);
            assertEquals(str, BinaryUtils.utf8BytesToStr(bytes, 0, bytes.length));
        }
        else
            assertNotEquals(str, marshalUnmarshal(str));

        bytes = str.getBytes(UTF_8);
        assertNotEquals(str, new String(bytes, UTF_8));

        bytes = str.getBytes(UTF_8);
        assertNotEquals(str, BinaryUtils.utf8BytesToStr(bytes, 0, bytes.length));

        str = new String(new char[] {0xD801, 0xDC37});
        assertEquals(str, marshalUnmarshal(str));

        bytes = str.getBytes(UTF_8);
        assertEquals(str, new String(bytes, UTF_8));
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
        BigDecimal[] arr = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.TEN};

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
    public void testException() throws Exception {
        Exception ex = new RuntimeException();

        // Checks that Optimize marshaller will be used, because Throwable has writeObject method.
        // Exception's stacktrace equals to zero-length array by default and generates at Throwable's writeObject method.
        assertNotEquals(0, marshalUnmarshal(ex).getStackTrace().length);
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
        testMap(new LinkedHashMap<Integer, String>());
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
     * Test serialization of custom collections.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testCustomCollections() throws Exception {
        CustomCollections cc = new CustomCollections();

        cc.list.add(1);
        cc.customList.add(new Value(1));

        CustomCollections copiedCc = marshalUnmarshal(cc);

        assert copiedCc.customList.getClass().equals(CustomArrayList.class);

        assertEquals(cc.list.size(), copiedCc.list.size());
        assertEquals(cc.customList.size(), copiedCc.customList.size());

        assertEquals(cc.list.get(0), copiedCc.list.get(0));
        assertEquals(cc.customList.get(0), copiedCc.customList.get(0));
    }

    /**
     * Test serialization of custom collections.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testCustomCollections2() throws Exception {
        CustomArrayList arrList = new CustomArrayList();

        arrList.add(1);

        Object cp = marshalUnmarshal(arrList);

        assert cp.getClass().equals(CustomArrayList.class);

        CustomArrayList customCp = (CustomArrayList)cp;

        assertEquals(customCp.size(), arrList.size());

        assertEquals(customCp.get(0), arrList.get(0));
    }

    /**
     * Test custom collections with factories.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testCustomCollectionsWithFactory() throws Exception {
        CustomCollectionsWithFactory cc = new CustomCollectionsWithFactory();

        cc.list.add(new DummyHolder(1));
        cc.map.put(new DummyHolder(2), new DummyHolder(3));

        CustomCollectionsWithFactory copiedCc = marshalUnmarshal(cc);

        assertEquals(cc.list.size(), copiedCc.list.size());
        assertEquals(cc.map.size(), copiedCc.map.size());

        assertEquals(cc.list.get(0), copiedCc.list.get(0));
        assertEquals(cc.map.get(new DummyHolder(2)), copiedCc.map.get(new DummyHolder(2)));
    }

    /**
     * @throws Exception If failed.
     */
    public void testExternalizableInEnclosing() throws Exception {
        SimpleEnclosingObject obj = new SimpleEnclosingObject();
        obj.simpl = new SimpleExternalizable("field");

        SimpleEnclosingObject other = marshalUnmarshal(obj);

        assertEquals(((SimpleExternalizable)obj.simpl).field, ((SimpleExternalizable)other.simpl).field);
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
    public void testBinaryObject() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(new BinaryTypeConfiguration(SimpleObject.class.getName())));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject po0 = marshalUnmarshal(po, marsh);

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
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(new BinaryTypeConfiguration(TestEnum.class.getName())));

        assertEquals(TestEnum.B, marshalUnmarshal(TestEnum.B, marsh));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDateAndTimestampInSingleObject() throws Exception {
        BinaryTypeConfiguration cfg1 = new BinaryTypeConfiguration(DateClass1.class.getName());

        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(cfg1));

        Date date = new Date();
        Timestamp ts = new Timestamp(System.currentTimeMillis());

        DateClass1 obj1 = new DateClass1();
        obj1.date = date;
        obj1.ts = ts;

        BinaryObject po1 = marshal(obj1, marsh);

        assertEquals(date, po1.field("date"));
        assertEquals(Date.class, po1.field("date").getClass());
        assertEquals(ts, po1.field("ts"));
        assertEquals(Timestamp.class, po1.field("ts").getClass());

        obj1 = po1.deserialize();
        assertEquals(date, obj1.date);
        assertEquals(ts, obj1.ts);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleObject() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

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
        assertEquals(new Integer(obj.enumVal.ordinal()), new Integer(((BinaryObject)po.field("enumVal")).enumOrdinal()));
        assertArrayEquals(ordinals(obj.enumArr), ordinals((BinaryObject[])po.field("enumArr")));
        assertNull(po.field("unknown"));

        BinaryObject innerPo = po.field("inner");

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
            new Integer(((BinaryObject)innerPo.field("enumVal")).enumOrdinal()));
        assertArrayEquals(ordinals(obj.inner.enumArr), ordinals((BinaryObject[])innerPo.field("enumArr")));
        assertNull(innerPo.field("inner"));
        assertNull(innerPo.field("unknown"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinary() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName()),
            new BinaryTypeConfiguration(TestBinary.class.getName())
        ));

        TestBinary obj = binaryObject();

        BinaryObject po = marshal(obj, marsh);

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
        assertEquals(new Integer(obj.enumVal.ordinal()), new Integer(((BinaryObject)po.field("_enumVal")).enumOrdinal()));
        assertArrayEquals(ordinals(obj.enumArr), ordinals((BinaryObject[])po.field("_enumArr")));
        assertNull(po.field("unknown"));

        BinaryObject simplePo = po.field("_simple");

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
            new Integer(((BinaryObject)simplePo.field("enumVal")).enumOrdinal()));
        assertArrayEquals(ordinals(obj.simple.enumArr), ordinals((BinaryObject[])simplePo.field("enumArr")));
        assertNull(simplePo.field("simple"));
        assertNull(simplePo.field("binary"));
        assertNull(simplePo.field("unknown"));

        BinaryObject binaryPo = po.field("_binary");

        assertEquals(obj.binary, binaryPo.deserialize());

        assertEquals(obj.binary.b, (byte)binaryPo.field("_b"));
        assertEquals(obj.binary.s, (short)binaryPo.field("_s"));
        assertEquals(obj.binary.i, (int)binaryPo.field("_i"));
        assertEquals(obj.binary.l, (long)binaryPo.field("_l"));
        assertEquals(obj.binary.f, (float)binaryPo.field("_f"), 0);
        assertEquals(obj.binary.d, (double)binaryPo.field("_d"), 0);
        assertEquals(obj.binary.c, (char)binaryPo.field("_c"));
        assertEquals(obj.binary.bool, (boolean)binaryPo.field("_bool"));
        assertEquals(obj.binary.str, binaryPo.field("_str"));
        assertEquals(obj.binary.uuid, binaryPo.field("_uuid"));
        assertEquals(obj.binary.date, binaryPo.field("_date"));
        assertEquals(obj.binary.ts, binaryPo.field("_ts"));
        assertArrayEquals(obj.binary.bArr, (byte[])binaryPo.field("_bArr"));
        assertArrayEquals(obj.binary.sArr, (short[])binaryPo.field("_sArr"));
        assertArrayEquals(obj.binary.iArr, (int[])binaryPo.field("_iArr"));
        assertArrayEquals(obj.binary.lArr, (long[])binaryPo.field("_lArr"));
        assertArrayEquals(obj.binary.fArr, (float[])binaryPo.field("_fArr"), 0);
        assertArrayEquals(obj.binary.dArr, (double[])binaryPo.field("_dArr"), 0);
        assertArrayEquals(obj.binary.cArr, (char[])binaryPo.field("_cArr"));
        assertBooleanArrayEquals(obj.binary.boolArr, (boolean[])binaryPo.field("_boolArr"));
        assertArrayEquals(obj.binary.strArr, (String[])binaryPo.field("_strArr"));
        assertArrayEquals(obj.binary.uuidArr, (UUID[])binaryPo.field("_uuidArr"));
        assertArrayEquals(obj.binary.dateArr, (Date[])binaryPo.field("_dateArr"));
        assertArrayEquals(obj.binary.objArr, (Object[])binaryPo.field("_objArr"));
        assertEquals(obj.binary.col, binaryPo.field("_col"));
        assertEquals(obj.binary.map, binaryPo.field("_map"));
        assertEquals(new Integer(obj.binary.enumVal.ordinal()),
            new Integer(((BinaryObject)binaryPo.field("_enumVal")).enumOrdinal()));
        assertArrayEquals(ordinals(obj.binary.enumArr), ordinals((BinaryObject[])binaryPo.field("_enumArr")));
        assertNull(binaryPo.field("_simple"));
        assertNull(binaryPo.field("_binary"));
        assertNull(binaryPo.field("unknown"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectFieldOfExternalizableCollection() throws Exception {
        EnclosingObj obj = new EnclosingObj();

        obj.queue = new TestQueue("test");

        assertEquals(obj, marshalUnmarshal(obj));
    }

    /**
     * @throws Exception If failed.
     */
    public void testVoid() throws Exception {
        Class clazz = Void.class;

        assertEquals(clazz, marshalUnmarshal(clazz));

        clazz = Void.TYPE;

        assertEquals(clazz, marshalUnmarshal(clazz));
    }

    /**
     *
     */
    private static class EnclosingObj implements Serializable {
        /** Queue. */
        Queue<Integer> queue = new TestQueue("test");

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            EnclosingObj obj = (EnclosingObj)o;

            return Objects.equals(queue, obj.queue);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(queue);
        }
    }

    /**
     *
     */
    private static class TestQueue extends AbstractQueue<Integer> implements Externalizable {
        /** Name. */
        private String name;

        /**
         * {@link Externalizable} support.
         */
        public TestQueue() {
            // No-op.
        }

        /**
         * @param name Name.
         */
        public TestQueue(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<Integer> iterator() {
            return Collections.emptyIterator();
        }

        /** {@inheritDoc} */
        @Override public int size() {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(name);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            name = (String)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public boolean offer(Integer integer) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Integer poll() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public Integer peek() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestQueue integers = (TestQueue)o;

            return Objects.equals(name, integers.name);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(name);
        }
    }

    /**
     * @param obj Simple object.
     * @param po Binary object.
     */
    private void checkSimpleObjectData(SimpleObject obj, BinaryObject po) {
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
        assertEquals(new Integer(obj.enumVal.ordinal()), new Integer(((BinaryObject)po.field("enumVal")).enumOrdinal()));
        assertArrayEquals(ordinals(obj.enumArr), ordinals((BinaryObject[])po.field("enumArr")));
        assertNull(po.field("unknown"));

        assertEquals(obj, po.deserialize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassWithoutPublicConstructor() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(NoPublicConstructor.class.getName()),
            new BinaryTypeConfiguration(NoPublicDefaultConstructor.class.getName()),
            new BinaryTypeConfiguration(ProtectedConstructor.class.getName()))
        );

        NoPublicConstructor npc = new NoPublicConstructor();
        BinaryObject npc2 = marshal(npc, marsh);

        assertEquals("test", npc2.<NoPublicConstructor>deserialize().val);

        NoPublicDefaultConstructor npdc = new NoPublicDefaultConstructor(239);
        BinaryObject npdc2 = marshal(npdc, marsh);

        assertEquals(239, npdc2.<NoPublicDefaultConstructor>deserialize().val);

        ProtectedConstructor pc = new ProtectedConstructor();
        BinaryObject pc2 = marshal(pc, marsh);

        assertEquals(ProtectedConstructor.class, pc2.<ProtectedConstructor>deserialize().getClass());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomSerializer() throws Exception {
        BinaryTypeConfiguration type =
            new BinaryTypeConfiguration(CustomSerializedObject1.class.getName());

        type.setSerializer(new CustomSerializer1());

        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(type));

        CustomSerializedObject1 obj1 = new CustomSerializedObject1(10);

        BinaryObject po1 = marshal(obj1, marsh);

        assertEquals(20, po1.<CustomSerializedObject1>deserialize().val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomSerializerWithGlobal() throws Exception {
        BinaryTypeConfiguration type1 =
            new BinaryTypeConfiguration(CustomSerializedObject1.class.getName());
        BinaryTypeConfiguration type2 =
            new BinaryTypeConfiguration(CustomSerializedObject2.class.getName());

        type2.setSerializer(new CustomSerializer2());

        BinaryMarshaller marsh = binaryMarshaller(new CustomSerializer1(), Arrays.asList(type1, type2));

        CustomSerializedObject1 obj1 = new CustomSerializedObject1(10);

        BinaryObject po1 = marshal(obj1, marsh);

        assertEquals(20, po1.<CustomSerializedObject1>deserialize().val);

        CustomSerializedObject2 obj2 = new CustomSerializedObject2(10);

        BinaryObject po2 = marshal(obj2, marsh);

        assertEquals(30, po2.<CustomSerializedObject2>deserialize().val);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomIdMapper() throws Exception {
        BinaryTypeConfiguration type =
            new BinaryTypeConfiguration(CustomMappedObject1.class.getName());

        type.setIdMapper(new BinaryIdMapper() {
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

        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(type));

        CustomMappedObject1 obj1 = new CustomMappedObject1(10, "str");

        BinaryObjectExImpl po1 = marshal(obj1, marsh);

        assertEquals(11111, po1.type().typeId());
        assertEquals((Integer)10, po1.field(22222));
        assertEquals("str", po1.field(33333));

        assertEquals(10, po1.<CustomMappedObject1>deserialize().val1);
        assertEquals("str", po1.<CustomMappedObject1>deserialize().val2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomIdMapperWithGlobal() throws Exception {
        BinaryTypeConfiguration type1 =
            new BinaryTypeConfiguration(CustomMappedObject1.class.getName());
        BinaryTypeConfiguration type2 =
            new BinaryTypeConfiguration(CustomMappedObject2.class.getName());

        type2.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 44444;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                assert typeId == 44444;

                if ("val1".equals(fieldName))
                    return 55555;
                else if ("val2".equals(fieldName))
                    return 66666;

                assert false : "Unknown field: " + fieldName;

                return 0;
            }
        });

        BinaryMarshaller marsh = binaryMarshaller(null, new BinaryIdMapper() {
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
        }, Arrays.asList(type1, type2));

        CustomMappedObject1 obj1 = new CustomMappedObject1(10, "str1");

        BinaryObjectExImpl po1 = marshal(obj1, marsh);

        assertEquals(11111, po1.type().typeId());
        assertEquals((Integer)10, po1.field(22222));
        assertEquals("str1", po1.field(33333));

        assertEquals(10, po1.<CustomMappedObject1>deserialize().val1);
        assertEquals("str1", po1.<CustomMappedObject1>deserialize().val2);

        CustomMappedObject2 obj2 = new CustomMappedObject2(20, "str2");

        BinaryObjectExImpl po2 = marshal(obj2, marsh);

        assertEquals(44444, po2.type().typeId());
        assertEquals((Integer)20, po2.field(55555));
        assertEquals("str2", po2.field(66666));

        assertEquals(20, po2.<CustomMappedObject2>deserialize().val1);
        assertEquals("str2", po2.<CustomMappedObject2>deserialize().val2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleNameLowerCaseMappers() throws Exception {
        BinaryTypeConfiguration innerClassType = new BinaryTypeConfiguration(InnerMappedObject.class.getName());
        BinaryTypeConfiguration publicClassType = new BinaryTypeConfiguration(TestMappedObject.class.getName());
        BinaryTypeConfiguration typeWithCustomMapper = new BinaryTypeConfiguration(CustomMappedObject2.class.getName());

        typeWithCustomMapper.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 44444;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                assert typeId == 44444;

                if ("val1".equals(fieldName))
                    return 55555;
                else if ("val2".equals(fieldName))
                    return 66666;

                assert false : "Unknown field: " + fieldName;

                return 0;
            }
        });

        BinaryMarshaller marsh = binaryMarshaller(new BinaryBasicNameMapper(true), new BinaryBasicIdMapper(true),
            Arrays.asList(innerClassType, publicClassType, typeWithCustomMapper));

        InnerMappedObject innerObj = new InnerMappedObject(10, "str1");

        BinaryObjectExImpl innerBo = marshal(innerObj, marsh);

        assertEquals("InnerMappedObject".toLowerCase().hashCode(), innerBo.type().typeId());

        assertEquals(10, innerBo.<CustomMappedObject1>deserialize().val1);
        assertEquals("str1", innerBo.<CustomMappedObject1>deserialize().val2);

        TestMappedObject publicObj = new TestMappedObject();

        BinaryObjectExImpl publicBo = marshal(publicObj, marsh);

        assertEquals("TestMappedObject".toLowerCase().hashCode(), publicBo.type().typeId());

        CustomMappedObject2 obj2 = new CustomMappedObject2(20, "str2");

        BinaryObjectExImpl po2 = marshal(obj2, marsh);

        assertEquals(44444, po2.type().typeId());
        assertEquals((Integer)20, po2.field(55555));
        assertEquals("str2", po2.field(66666));

        assertEquals(20, po2.<CustomMappedObject2>deserialize().val1);
        assertEquals("str2", po2.<CustomMappedObject2>deserialize().val2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicObject() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(DynamicObject.class.getName())
        ));

        BinaryObject po1 = marshal(new DynamicObject(0, 10, 20, 30), marsh);

        assertEquals(new Integer(10), po1.field("val1"));
        assertEquals(null, po1.field("val2"));
        assertEquals(null, po1.field("val3"));

        DynamicObject do1 = po1.deserialize();

        assertEquals(10, do1.val1);
        assertEquals(0, do1.val2);
        assertEquals(0, do1.val3);

        BinaryObject po2 = marshal(new DynamicObject(1, 10, 20, 30), marsh);

        assertEquals(new Integer(10), po2.field("val1"));
        assertEquals(new Integer(20), po2.field("val2"));
        assertEquals(null, po2.field("val3"));

        DynamicObject do2 = po2.deserialize();

        assertEquals(10, do2.val1);
        assertEquals(20, do2.val2);
        assertEquals(0, do2.val3);

        BinaryObject po3 = marshal(new DynamicObject(2, 10, 20, 30), marsh);

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
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(CycleLinkObject.class.getName())
        ));

        CycleLinkObject obj = new CycleLinkObject();

        obj.self = obj;

        BinaryObject po = marshal(obj, marsh);

        CycleLinkObject obj0 = po.deserialize();

        assert obj0.self == obj0;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDetached() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(DetachedTestObject.class.getName()),
            new BinaryTypeConfiguration(DetachedInnerTestObject.class.getName())
        ));

        UUID id = UUID.randomUUID();

        DetachedTestObject obj = marshal(new DetachedTestObject(
            new DetachedInnerTestObject(null, id)), marsh).deserialize();

        assertEquals(id, obj.inner1.id);
        assertEquals(id, obj.inner4.id);

        assert obj.inner1 == obj.inner4;

        BinaryObjectImpl innerPo = (BinaryObjectImpl)obj.inner2;

        assert innerPo.detached();

        DetachedInnerTestObject inner = innerPo.deserialize();

        assertEquals(id, inner.id);

        BinaryObjectImpl detachedPo = (BinaryObjectImpl)innerPo.detach();

        assert detachedPo.detached();

        inner = detachedPo.deserialize();

        assertEquals(id, inner.id);

        innerPo = (BinaryObjectImpl)obj.inner3;

        assert innerPo.detached();

        inner = innerPo.deserialize();

        assertEquals(id, inner.id);
        assertNotNull(inner.inner);

        detachedPo = (BinaryObjectImpl)innerPo.detach();

        assert detachedPo.detached();

        inner = innerPo.deserialize();

        assertEquals(id, inner.id);
        assertNotNull(inner.inner);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollectionFields() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(CollectionFieldsObject.class.getName()),
            new BinaryTypeConfiguration(Key.class.getName()),
            new BinaryTypeConfiguration(Value.class.getName())
        ));

        Object[] arr = new Object[] {new Value(1), new Value(2), new Value(3)};
        Collection<Value> col = new ArrayList<>(Arrays.asList(new Value(4), new Value(5), new Value(6)));
        Map<Key, Value> map = new HashMap<>(F.asMap(new Key(10), new Value(10), new Key(20), new Value(20), new Key(30), new Value(30)));

        CollectionFieldsObject obj = new CollectionFieldsObject(arr, col, map);

        BinaryObject po = marshal(obj, marsh);

        Object[] arr0 = po.field("arr");

        assertEquals(3, arr0.length);

        int i = 1;

        for (Object valPo : arr0)
            assertEquals(i++, ((BinaryObject)valPo).<Value>deserialize().val);

        Collection<BinaryObject> col0 = po.field("col");

        i = 4;

        for (BinaryObject valPo : col0)
            assertEquals(i++, valPo.<Value>deserialize().val);

        Map<BinaryObject, BinaryObject> map0 = po.field("map");

        for (Map.Entry<BinaryObject, BinaryObject> e : map0.entrySet())
            assertEquals(e.getKey().<Key>deserialize().key, e.getValue().<Value>deserialize().val);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testDefaultMapping() throws Exception {
        BinaryTypeConfiguration customMappingType =
            new BinaryTypeConfiguration(TestBinary.class.getName());

        customMappingType.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                String typeName;

                try {
                    Method mtd = BinaryContext.class.getDeclaredMethod("typeName", String.class);

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

        BinaryMarshaller marsh1 = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName()),
            customMappingType
        ));

        TestBinary obj = binaryObject();

        BinaryObjectImpl po = marshal(obj, marsh1);

        BinaryMarshaller marsh2 = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName()),
            new BinaryTypeConfiguration(TestBinary.class.getName())
        ));

        po = marshal(obj, marsh2);

        assertEquals(obj, po.deserialize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeNamesSimpleNameMapper() throws Exception {
        BinaryTypeConfiguration customType1 = new BinaryTypeConfiguration(Value.class.getName());

        customType1.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 300;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType2 = new BinaryTypeConfiguration("org.gridgain.NonExistentClass1");

        customType2.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 400;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType3 = new BinaryTypeConfiguration("NonExistentClass2");

        customType3.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType4 = new BinaryTypeConfiguration("NonExistentClass0");

        customType4.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 0;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryMarshaller marsh = binaryMarshaller(new BinaryBasicNameMapper(true), new BinaryBasicIdMapper(true),
            Arrays.asList(
                new BinaryTypeConfiguration(Key.class.getName()),
                new BinaryTypeConfiguration("org.gridgain.NonExistentClass3"),
                new BinaryTypeConfiguration("NonExistentClass4"),
                customType1,
                customType2,
                customType3,
                customType4
            ));

        BinaryContext ctx = binaryContext(marsh);

        // Full name hashCode.
        assertEquals("notconfiguredclass".hashCode(), ctx.typeId("NotConfiguredClass"));
        assertEquals("key".hashCode(), ctx.typeId(Key.class.getName()));
        assertEquals("nonexistentclass3".hashCode(), ctx.typeId("org.gridgain.NonExistentClass3"));
        assertEquals("nonexistentclass4".hashCode(), ctx.typeId("NonExistentClass4"));
        assertEquals(300, ctx.typeId(Value.class.getName()));
        assertEquals(400, ctx.typeId("org.gridgain.NonExistentClass1"));
        assertEquals(500, ctx.typeId("NonExistentClass2"));

        // BinaryIdMapper.typeId() contract.
        assertEquals("nonexistentclass0".hashCode(), ctx.typeId("NonExistentClass0"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeNamesFullNameMappers() throws Exception {
        BinaryTypeConfiguration customType1 = new BinaryTypeConfiguration(Value.class.getName());

        customType1.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 300;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType2 = new BinaryTypeConfiguration("org.gridgain.NonExistentClass1");

        customType2.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 400;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType3 = new BinaryTypeConfiguration("NonExistentClass2");

        customType3.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType4 = new BinaryTypeConfiguration("NonExistentClass0");

        customType4.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 0;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryMarshaller marsh = binaryMarshaller(new BinaryBasicNameMapper(false), new BinaryBasicIdMapper(false),
            Arrays.asList(
                new BinaryTypeConfiguration(Key.class.getName()),
                new BinaryTypeConfiguration("org.gridgain.NonExistentClass3"),
                new BinaryTypeConfiguration("NonExistentClass4"),
                customType1,
                customType2,
                customType3,
                customType4
            ));

        BinaryContext ctx = binaryContext(marsh);

        // Full name hashCode.
        assertEquals("NotConfiguredClass".hashCode(), ctx.typeId("NotConfiguredClass"));
        assertEquals(Key.class.getName().hashCode(), ctx.typeId(Key.class.getName()));
        assertEquals("org.gridgain.NonExistentClass3".hashCode(), ctx.typeId("org.gridgain.NonExistentClass3"));
        assertEquals("NonExistentClass4".hashCode(), ctx.typeId("NonExistentClass4"));
        assertEquals(300, ctx.typeId(Value.class.getName()));
        assertEquals(400, ctx.typeId("org.gridgain.NonExistentClass1"));
        assertEquals(500, ctx.typeId("NonExistentClass2"));

        // BinaryIdMapper.typeId() contract.
        assertEquals("nonexistentclass0".hashCode(), ctx.typeId("NonExistentClass0"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeNamesSimpleNameMappers() throws Exception {
        BinaryTypeConfiguration customType1 = new BinaryTypeConfiguration(Value.class.getName());

        customType1.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 300;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType2 = new BinaryTypeConfiguration("org.gridgain.NonExistentClass1");

        customType2.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 400;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType3 = new BinaryTypeConfiguration("NonExistentClass2");

        customType3.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType4 = new BinaryTypeConfiguration("NonExistentClass0");

        customType4.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 0;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType5 = new BinaryTypeConfiguration(DateClass1.class.getName());

        customType5.setNameMapper(new BinaryBasicNameMapper(false));
        customType5.setIdMapper(new BinaryBasicIdMapper(false));

        BinaryMarshaller marsh = binaryMarshaller(new BinaryBasicNameMapper(true), new BinaryBasicIdMapper(true),
            Arrays.asList(
                new BinaryTypeConfiguration(Key.class.getName()),
                new BinaryTypeConfiguration("org.gridgain.NonExistentClass3"),
                new BinaryTypeConfiguration("NonExistentClass4"),
                customType1,
                customType2,
                customType3,
                customType4,
                customType5
            ));

        BinaryContext ctx = binaryContext(marsh);

        assertEquals("notconfiguredclass".hashCode(), ctx.typeId("NotConfiguredClass"));
        assertEquals("notconfiguredclass".hashCode(), ctx.typeId("org.blabla.NotConfiguredClass"));
        assertEquals("key".hashCode(), ctx.typeId(Key.class.getName()));
        assertEquals("nonexistentclass3".hashCode(), ctx.typeId("org.gridgain.NonExistentClass3"));
        assertEquals("nonexistentclass4".hashCode(), ctx.typeId("NonExistentClass4"));

        assertEquals(300, ctx.typeId(Value.class.getName()));
        assertEquals(400, ctx.typeId("org.gridgain.NonExistentClass1"));
        assertEquals(500, ctx.typeId("NonExistentClass2"));

        assertEquals(DateClass1.class.getName().hashCode(), ctx.typeId(DateClass1.class.getName()));

        // BinaryIdMapper.typeId() contract.
        assertEquals("nonexistentclass0".hashCode(), ctx.typeId("NonExistentClass0"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTypeNamesCustomIdMapper() throws Exception {
        BinaryTypeConfiguration customType1 = new BinaryTypeConfiguration(Value.class.getName());

        customType1.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 300;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType2 = new BinaryTypeConfiguration("org.gridgain.NonExistentClass1");

        customType2.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 400;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType3 = new BinaryTypeConfiguration("NonExistentClass2");

        customType3.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 500;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType4 = new BinaryTypeConfiguration("NonExistentClass0");

        customType4.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 0;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType5 = new BinaryTypeConfiguration(DateClass1.class.getName());

        customType5.setIdMapper(new BinaryBasicIdMapper(false));

        BinaryTypeConfiguration customType6 = new BinaryTypeConfiguration(MyTestClass.class.getName());

        customType6.setIdMapper(new BinaryBasicIdMapper(true));
        customType6.setNameMapper(new BinaryBasicNameMapper(true));

        BinaryMarshaller marsh = binaryMarshaller(new BinaryBasicNameMapper(false), new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                if ("org.blabla.NotConfiguredSpecialClass".equals(clsName))
                    return 0;
                else if (Key.class.getName().equals(clsName))
                    return 991;
                else if ("org.gridgain.NonExistentClass3".equals(clsName))
                    return 992;
                else if ("NonExistentClass4".equals(clsName))
                    return 993;

                return 999;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        }, Arrays.asList(
            new BinaryTypeConfiguration(Key.class.getName()),
            new BinaryTypeConfiguration("org.gridgain.NonExistentClass3"),
            new BinaryTypeConfiguration("NonExistentClass4"),
            customType1,
            customType2,
            customType3,
            customType4,
            customType5,
            customType6
        ));

        BinaryContext ctx = binaryContext(marsh);

        assertEquals(999, ctx.typeId("NotConfiguredClass"));
        assertEquals(999, ctx.typeId("org.blabla.NotConfiguredClass"));

        // BinaryIdMapper.typeId() contract.
        assertEquals("notconfiguredspecialclass".hashCode(), ctx.typeId("org.blabla.NotConfiguredSpecialClass"));

        assertEquals(991, ctx.typeId(Key.class.getName()));
        assertEquals(992, ctx.typeId("org.gridgain.NonExistentClass3"));
        assertEquals(993, ctx.typeId("NonExistentClass4"));

        // Custom types.
        assertEquals(300, ctx.typeId(Value.class.getName()));
        assertEquals(400, ctx.typeId("org.gridgain.NonExistentClass1"));
        assertEquals(500, ctx.typeId("NonExistentClass2"));

        // BinaryIdMapper.typeId() contract.
        assertEquals("nonexistentclass0".hashCode(), ctx.typeId("NonExistentClass0"));

        assertEquals(DateClass1.class.getName().hashCode(), ctx.typeId(DateClass1.class.getName()));
        assertEquals("mytestclass".hashCode(), ctx.typeId(MyTestClass.class.getName()));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCustomTypeRegistration() throws Exception {
        BinaryTypeConfiguration customType = new BinaryTypeConfiguration(Value.class.getName());

        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(customType));

        BinaryContext ctx = binaryContext(marsh);

        int typeId = ctx.typeId(Value.class.getName());

        BinaryClassDescriptor descriptor = ctx.descriptorForTypeId(true, typeId, null, false);

        assertEquals(Value.class, descriptor.describedClass());
        assertEquals(true, descriptor.registered());
        assertEquals(true, descriptor.userType());

        // Custom explicit types must be registered in 'predefinedTypes' in order not to break the interoperability.
        Field field = ctx.getClass().getDeclaredField("predefinedTypes");

        field.setAccessible(true);

        Map<Integer, BinaryClassDescriptor> map = (Map<Integer, BinaryClassDescriptor>)field.get(ctx);

        assertTrue(map.size() > 0);

        assertNotNull(map.get(typeId));

        // Custom explicit types must NOT be registered in 'predefinedTypeNames'.
        field = ctx.getClass().getDeclaredField("predefinedTypeNames");

        field.setAccessible(true);

        Map<String, Integer> map2 = (Map<String, Integer>)field.get(ctx);

        assertTrue(map2.size() > 0);

        assertNull(map2.get(ctx.userTypeName(Value.class.getName())));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFieldIdMapping() throws Exception {
        BinaryTypeConfiguration customType1 = new BinaryTypeConfiguration(Value.class.getName());

        customType1.setIdMapper(new BinaryIdMapper() {
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

        BinaryTypeConfiguration customType2 = new BinaryTypeConfiguration("NonExistentClass1");

        customType2.setIdMapper(new BinaryIdMapper() {
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

        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(new BinaryTypeConfiguration(Key.class.getName()),
            new BinaryTypeConfiguration("NonExistentClass2"),
            customType1,
            customType2));

        BinaryContext ctx = binaryContext(marsh);

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
        BinaryTypeConfiguration customType1 = new BinaryTypeConfiguration("org.gridgain.Class1");

        customType1.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        BinaryTypeConfiguration customType2 = new BinaryTypeConfiguration("org.gridgain.Class2");

        customType2.setIdMapper(new BinaryIdMapper() {
            @Override public int typeId(String clsName) {
                return 100;
            }

            @Override public int fieldId(int typeId, String fieldName) {
                return 0;
            }
        });

        try {
            binaryMarshaller(Arrays.asList(customType1, customType2));
        }
        catch (IgniteCheckedException e) {
            assertEquals("Duplicate type ID [clsName=org.gridgain.Class2, id=100]",
                e.getCause().getCause().getMessage());

            return;
        }

        assert false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopy() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        final BinaryObject po = marshal(obj, marsh);

        assertEquals(obj, po.deserialize());

        BinaryObject copy = copy(po, null);

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
//            BinaryException.class,
//            "Invalid value type for field: i"
//        );
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyString() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("str", "str3"));

        assertEquals("str3", copy.<String>field("str"));

        SimpleObject obj0 = copy.deserialize();

        assertEquals("str3", obj0.str);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyUuid() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        UUID uuid = UUID.randomUUID();

        BinaryObject copy = copy(po, F.<String, Object>asMap("uuid", uuid));

        assertEquals(uuid, copy.<UUID>field("uuid"));

        SimpleObject obj0 = copy.deserialize();

        assertEquals(uuid, obj0.uuid);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyByteArray() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("bArr", new byte[] {1, 2, 3}));

        assertArrayEquals(new byte[] {1, 2, 3}, copy.<byte[]>field("bArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new byte[] {1, 2, 3}, obj0.bArr);
    }

    /**
     * @param po Binary object.
     * @param fields Fields.
     * @return Copy.
     */
    private BinaryObject copy(BinaryObject po, Map<String, Object> fields) {
        BinaryObjectBuilder builder = BinaryObjectBuilderImpl.wrap(po);

        if (fields != null) {
            for (Map.Entry<String, Object> e : fields.entrySet())
                builder.setField(e.getKey(), e.getValue());
        }

        return builder.build();
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyShortArray() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("sArr", new short[] {1, 2, 3}));

        assertArrayEquals(new short[] {1, 2, 3}, copy.<short[]>field("sArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new short[] {1, 2, 3}, obj0.sArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyIntArray() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("iArr", new int[] {1, 2, 3}));

        assertArrayEquals(new int[] {1, 2, 3}, copy.<int[]>field("iArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new int[] {1, 2, 3}, obj0.iArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyLongArray() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("lArr", new long[] {1, 2, 3}));

        assertArrayEquals(new long[] {1, 2, 3}, copy.<long[]>field("lArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new long[] {1, 2, 3}, obj0.lArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyFloatArray() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("fArr", new float[] {1, 2, 3}));

        assertArrayEquals(new float[] {1, 2, 3}, copy.<float[]>field("fArr"), 0);

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new float[] {1, 2, 3}, obj0.fArr, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyDoubleArray() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("dArr", new double[] {1, 2, 3}));

        assertArrayEquals(new double[] {1, 2, 3}, copy.<double[]>field("dArr"), 0);

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new double[] {1, 2, 3}, obj0.dArr, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyCharArray() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("cArr", new char[] {1, 2, 3}));

        assertArrayEquals(new char[] {1, 2, 3}, copy.<char[]>field("cArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new char[] {1, 2, 3}, obj0.cArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyStringArray() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        BinaryObject copy = copy(po, F.<String, Object>asMap("strArr", new String[] {"str1", "str2"}));

        assertArrayEquals(new String[] {"str1", "str2"}, copy.<String[]>field("strArr"));

        SimpleObject obj0 = copy.deserialize();

        assertArrayEquals(new String[] {"str1", "str2"}, obj0.strArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyObject() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        SimpleObject newObj = new SimpleObject();

        newObj.i = 12345;
        newObj.fArr = new float[] {5, 8, 0};
        newObj.str = "newStr";

        BinaryObject copy = copy(po, F.<String, Object>asMap("inner", newObj));

        assertEquals(newObj, copy.<BinaryObject>field("inner").deserialize());

        SimpleObject obj0 = copy.deserialize();

        assertEquals(newObj, obj0.inner);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyNonPrimitives() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(SimpleObject.class.getName())
        ));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        Map<String, Object> map = new HashMap<>(3, 1.0f);

        SimpleObject newObj = new SimpleObject();

        newObj.i = 12345;
        newObj.fArr = new float[] {5, 8, 0};
        newObj.str = "newStr";

        map.put("str", "str555");
        map.put("inner", newObj);
        map.put("bArr", new byte[] {6, 7, 9});

        BinaryObject copy = copy(po, map);

        assertEquals("str555", copy.<String>field("str"));
        assertEquals(newObj, copy.<BinaryObject>field("inner").deserialize());
        assertArrayEquals(new byte[] {6, 7, 9}, copy.<byte[]>field("bArr"));

        SimpleObject obj0 = copy.deserialize();

        assertEquals("str555", obj0.str);
        assertEquals(newObj, obj0.inner);
        assertArrayEquals(new byte[] {6, 7, 9}, obj0.bArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryCopyMixed() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(new BinaryTypeConfiguration(SimpleObject.class.getName())));

        SimpleObject obj = simpleObject();

        BinaryObject po = marshal(obj, marsh);

        Map<String, Object> map = new HashMap<>(3, 1.0f);

        SimpleObject newObj = new SimpleObject();

        newObj.i = 12345;
        newObj.fArr = new float[] {5, 8, 0};
        newObj.str = "newStr";

        map.put("i", 1234);
        map.put("str", "str555");
        map.put("inner", newObj);
        map.put("s", (short)2323);
        map.put("bArr", new byte[] {6, 7, 9});
        map.put("b", (byte)111);

        BinaryObject copy = copy(po, map);

        assertEquals(1234, copy.<Integer>field("i").intValue());
        assertEquals("str555", copy.<String>field("str"));
        assertEquals(newObj, copy.<BinaryObject>field("inner").deserialize());
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
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(new BinaryTypeConfiguration(SimpleObject.class.getName())));

        BinaryObjectImpl po = marshal(simpleObject(), marsh);

        CacheObjectContext coCtx = new CacheObjectContext(newContext(), null, null, false, true, false);

        assert po.value(coCtx, false) == po.value(coCtx, false);

        po = marshal(simpleObject(), marsh);

        assert po.deserialize() != po.deserialize();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOffheapBinary() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(new BinaryTypeConfiguration(SimpleObject.class.getName())));

        BinaryContext ctx = binaryContext(marsh);

        SimpleObject simpleObj = simpleObject();

        BinaryObjectImpl obj = marshal(simpleObj, marsh);

        long ptr = 0;

        long ptr1 = 0;

        long ptr2 = 0;

        try {
            ptr = copyOffheap(obj);

            BinaryObjectOffheapImpl offheapObj = new BinaryObjectOffheapImpl(ctx,
                ptr,
                0,
                obj.array().length);

            assertTrue(offheapObj.equals(offheapObj));
            assertFalse(offheapObj.equals(null));
            assertFalse(offheapObj.equals("str"));
            assertTrue(offheapObj.equals(obj));
            assertTrue(obj.equals(offheapObj));

            ptr1 = copyOffheap(obj);

            BinaryObjectOffheapImpl offheapObj1 = new BinaryObjectOffheapImpl(ctx,
                ptr1,
                0,
                obj.array().length);

            assertTrue(offheapObj.equals(offheapObj1));
            assertTrue(offheapObj1.equals(offheapObj));

            assertEquals(obj.type().typeId(), offheapObj.type().typeId());
            assertEquals(obj.hashCode(), offheapObj.hashCode());

            checkSimpleObjectData(simpleObj, offheapObj);

            BinaryObjectOffheapImpl innerOffheapObj = offheapObj.field("inner");

            assertNotNull(innerOffheapObj);

            checkSimpleObjectData(simpleObj.inner, innerOffheapObj);

            obj = (BinaryObjectImpl)offheapObj.heapCopy();

            assertEquals(obj.type().typeId(), offheapObj.type().typeId());
            assertEquals(obj.hashCode(), offheapObj.hashCode());

            checkSimpleObjectData(simpleObj, obj);

            BinaryObjectImpl innerObj = obj.field("inner");

            assertNotNull(innerObj);

            checkSimpleObjectData(simpleObj.inner, innerObj);

            simpleObj.d = 0;

            obj = marshal(simpleObj, marsh);

            assertFalse(offheapObj.equals(obj));
            assertFalse(obj.equals(offheapObj));

            ptr2 = copyOffheap(obj);

            BinaryObjectOffheapImpl offheapObj2 = new BinaryObjectOffheapImpl(ctx,
                ptr2,
                0,
                obj.array().length);

            assertFalse(offheapObj.equals(offheapObj2));
            assertFalse(offheapObj2.equals(offheapObj));
        }
        finally {
            GridUnsafe.freeMemory(ptr);

            if (ptr1 > 0)
                GridUnsafe.freeMemory(ptr1);

            if (ptr2 > 0)
                GridUnsafe.freeMemory(ptr2);
        }
    }

    /**
     *
     */
    public void testReadResolve() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(
            new BinaryTypeConfiguration(MySingleton.class.getName()),
            new BinaryTypeConfiguration(SingletonMarker.class.getName())));

        BinaryObjectImpl binaryObj = marshal(MySingleton.INSTANCE, marsh);

        assertTrue(binaryObj.array().length <= 1024); // Check that big string was not serialized.

        MySingleton singleton = binaryObj.deserialize();

        assertSame(MySingleton.INSTANCE, singleton);
    }

    /**
     *
     */
    public void testReadResolveOnBinaryAware() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Collections.singletonList(
            new BinaryTypeConfiguration(MyTestClass.class.getName())));

        BinaryObjectImpl binaryObj = marshal(new MyTestClass(), marsh);

        MyTestClass obj = binaryObj.deserialize();

        assertEquals("readResolve", obj.s);
    }

    /**
     * @throws Exception If ecxeption thrown.
     */
    public void testDeclareReadResolveInParent() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(Arrays.asList(new BinaryTypeConfiguration(ChildBinary.class.getName())));

        BinaryObjectImpl binaryObj = marshal(new ChildBinary(), marsh);

        ChildBinary singleton = binaryObj.deserialize();

        assertNotNull(singleton.s);
    }

    /**
     *
     */
    public void testDecimalFields() throws Exception {
        Collection<BinaryTypeConfiguration> clsNames = new ArrayList<>();

        clsNames.add(new BinaryTypeConfiguration(DecimalReflective.class.getName()));
        clsNames.add(new BinaryTypeConfiguration(DecimalMarshalAware.class.getName()));

        BinaryMarshaller marsh = binaryMarshaller(clsNames);

        // 1. Test reflective stuff.
        DecimalReflective obj1 = new DecimalReflective();

        obj1.val = BigDecimal.ZERO;
        obj1.valArr = new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN};

        BinaryObjectImpl portObj = marshal(obj1, marsh);

        assertEquals(obj1.val, portObj.field("val"));
        assertArrayEquals(obj1.valArr, portObj.<BigDecimal[]>field("valArr"));

        assertEquals(obj1.val, portObj.<DecimalReflective>deserialize().val);
        assertArrayEquals(obj1.valArr, portObj.<DecimalReflective>deserialize().valArr);

        // 2. Test marshal aware stuff.
        DecimalMarshalAware obj2 = new DecimalMarshalAware();

        obj2.val = BigDecimal.ZERO;
        obj2.valArr = new BigDecimal[] {BigDecimal.ONE, BigDecimal.TEN.negate()};
        obj2.rawVal = BigDecimal.TEN;
        obj2.rawValArr = new BigDecimal[] {BigDecimal.ZERO, BigDecimal.ONE};

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
        BinaryMarshaller marsh = binaryMarshaller();

        SimpleObjectWithFinal obj = new SimpleObjectWithFinal();

        SimpleObjectWithFinal po0 = marshalUnmarshal(obj, marsh);

        assertEquals(obj.time, po0.time);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testThreadLocalArrayReleased() throws Exception {
        // Checking the writer directly.
        assertEquals(false, INSTANCE.isAcquired());

        BinaryMarshaller marsh = binaryMarshaller();

        try (BinaryWriterExImpl writer = new BinaryWriterExImpl(binaryContext(marsh))) {
            assertEquals(true, INSTANCE.isAcquired());

            writer.writeString("Thread local test");

            writer.array();

            assertEquals(true, INSTANCE.isAcquired());
        }

        // Checking the binary marshaller.
        assertEquals(false, INSTANCE.isAcquired());

        marsh = binaryMarshaller();

        marsh.marshal(new SimpleObject());

        assertEquals(false, INSTANCE.isAcquired());

        marsh = binaryMarshaller();

        // Checking the builder.
        BinaryObjectBuilder builder = new BinaryObjectBuilderImpl(binaryContext(marsh),
            "org.gridgain.foo.bar.TestClass");

        builder.setField("a", "1");

        BinaryObject binaryObj = builder.build();

        assertEquals(false, INSTANCE.isAcquired());
    }

    /**
     * @throws Exception If failed.
     */
    public void testDuplicateNameSimpleNameMapper() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryBasicNameMapper(true),
            new BinaryBasicIdMapper(true), null, null, null);

        Test1.Job job1 = new Test1().new Job();
        Test2.Job job2 = new Test2().new Job();

        marsh.marshal(job1);

        try {
            marsh.marshal(job2);
        }
        catch (BinaryObjectException e) {
            assertEquals(true, e.getMessage().contains("Failed to register class"));

            return;
        }

        assert false;
    }

    /**
     * @throws Exception If failed.
     */
    public void testDuplicateNameFullNameMapper() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller(new BinaryBasicNameMapper(false),
            new BinaryBasicIdMapper(false), null, null, null);

        Test1.Job job1 = new Test1().new Job();
        Test2.Job job2 = new Test2().new Job();

        marsh.marshal(job1);

        marsh.marshal(job2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClass() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller();

        Class cls = BinaryMarshallerSelfTest.class;

        Class unmarshalledCls = marshalUnmarshal(cls, marsh);

        Assert.assertEquals(cls, unmarshalledCls);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassFieldsMarshalling() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller();

        ObjectWithClassFields obj = new ObjectWithClassFields();
        obj.cls1 = BinaryMarshallerSelfTest.class;

        byte[] marshal = marsh.marshal(obj);

        ObjectWithClassFields obj2 = marsh.unmarshal(marshal, null);

        assertEquals(obj.cls1, obj2.cls1);
        assertNull(obj2.cls2);

        BinaryObject portObj = marshal(obj, marsh);

        Class cls1 = portObj.field("cls1");

        assertEquals(obj.cls1, cls1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMarshallingThroughJdk() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller();

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
        BinaryMarshaller marsh = binaryMarshaller();

        BinaryContext bCtx = binaryContext(marsh);

        Field field = bCtx.getClass().getDeclaredField("predefinedTypeNames");

        field.setAccessible(true);

        Map<String, Integer> map = (Map<String, Integer>)field.get(bCtx);

        assertTrue(map.size() > 0);

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            int id = entry.getValue();

            if (id == GridBinaryMarshaller.UNREGISTERED_TYPE_ID)
                continue;

            BinaryClassDescriptor desc = bCtx.descriptorForTypeId(false, entry.getValue(), null, false);

            assertEquals(desc.typeId(), bCtx.typeId(desc.describedClass().getName()));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testProxy() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller();

        SomeItf inItf = (SomeItf)Proxy.newProxyInstance(
            BinaryMarshallerSelfTest.class.getClassLoader(), new Class[] {SomeItf.class},
            new InvocationHandler() {
                private NonSerializable obj = new NonSerializable(null);

                @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                    if ("hashCode".equals(mtd.getName()))
                        return obj.hashCode();

                    obj.checkAfterUnmarshalled();

                    return 17;
                }
            }
        );

        SomeItf outItf = marsh.unmarshal(marsh.marshal(inItf), null);

        assertEquals(outItf.checkAfterUnmarshalled(), 17);
    }

    /**
     * Test object with {@link Proxy} field.
     *
     * @throws Exception If fails.
     */
    public void testObjectContainingProxy() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller();

        SomeItf inItf = (SomeItf)Proxy.newProxyInstance(
            BinaryMarshallerSelfTest.class.getClassLoader(), new Class[] {SomeItf.class},
            new InvocationHandler() {
                private NonSerializable obj = new NonSerializable(null);

                @Override public Object invoke(Object proxy, Method mtd, Object[] args) throws Throwable {
                    if ("hashCode".equals(mtd.getName()))
                        return obj.hashCode();

                    obj.checkAfterUnmarshalled();

                    return 17;
                }
            }
        );

        SomeItf outItf = marsh.unmarshal(marsh.marshal(inItf), null);

        assertEquals(outItf.checkAfterUnmarshalled(), 17);
    }

    /**
     * Test duplicate fields.
     *
     * @throws Exception If failed.
     */
    public void testDuplicateFields() throws Exception {
        BinaryMarshaller marsh = binaryMarshaller();

        DuplicateFieldsB obj = new DuplicateFieldsB(1, 2);

        BinaryObjectImpl objBin = marshal(obj, marsh);

        String fieldName = "x";
        String fieldNameA = DuplicateFieldsA.class.getName() + "." + fieldName;
        String fieldNameB = DuplicateFieldsB.class.getName() + "." + fieldName;

        // Check "hasField".
        assert !objBin.hasField(fieldName);
        assert objBin.hasField(fieldNameA);
        assert objBin.hasField(fieldNameB);

        // Check direct field access.
        assertNull(objBin.field(fieldName));
        assertEquals(Integer.valueOf(1), objBin.field(fieldNameA));
        assertEquals(Integer.valueOf(2), objBin.field(fieldNameB));

        // Check metadata.
        BinaryType type = objBin.type();

        Collection<String> fieldNames = type.fieldNames();

        assertEquals(2, fieldNames.size());

        assert !fieldNames.contains(fieldName);
        assert fieldNames.contains(fieldNameA);
        assert fieldNames.contains(fieldNameB);

        // Check field access through type.
        BinaryField field = type.field(fieldName);
        BinaryField fieldA = type.field(fieldNameA);
        BinaryField fieldB = type.field(fieldNameB);

        assert !field.exists(objBin);
        assert fieldA.exists(objBin);
        assert fieldB.exists(objBin);

        assertNull(field.value(objBin));
        assertEquals(Integer.valueOf(1), fieldA.value(objBin));
        assertEquals(Integer.valueOf(2), fieldB.value(objBin));

        // Check object deserialization.
        DuplicateFieldsB deserialized = objBin.deserialize();

        assertEquals(obj.xA(), deserialized.xA());
        assertEquals(obj.xB(), deserialized.xB());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleHandle() throws Exception {
        SingleHandleA a = new SingleHandleA(new SingleHandleB());

        BinaryObjectImpl bo = marshal(a, binaryMarshaller());

        Map<String, BinaryObject> map = bo.field("map");

        BinaryObject innerBo = map.get("key");

        assertEquals(SingleHandleB.class, innerBo.deserialize().getClass());
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnregisteredClass() throws Exception {
        BinaryMarshaller m = binaryMarshaller(null, Collections.singletonList(Value.class.getName()));

        ClassFieldObject res = m.unmarshal(m.marshal(new ClassFieldObject(Value.class)), null);

        assertEquals(Value.class, res.cls);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryEquals() throws Exception {
        Collection<String> excludedClasses = Arrays.asList(
            ObjectRaw.class.getName(),
            ObjectWithRaw.class.getName(),
            Value.class.getName());

        BinaryMarshaller m0 = binaryMarshaller(null, excludedClasses);
        BinaryMarshaller m1 = binaryMarshaller();

        Value obj = new Value(27);
        ObjectWithRaw objectWithRaw = new ObjectWithRaw(27, 13);
        ObjectRaw objectRaw = new ObjectRaw(27, 13);

        Value objOther = new Value(26);
        ObjectWithRaw objectWithRawOther = new ObjectWithRaw(26, 13);
        ObjectRaw objectRawOther = new ObjectRaw(26, 13);

        BinaryObjectImpl binObj0 = marshal(obj, m0);
        BinaryObjectImpl binObj1 = marshal(obj, m1);
        BinaryObjectImpl binObjWithRaw0 = marshal(objectWithRaw, m0);
        BinaryObjectImpl binObjWithRaw1 = marshal(objectWithRaw, m1);
        BinaryObjectImpl binObjRaw0 = marshal(objectRaw, m0);
        BinaryObjectImpl binObjRaw1 = marshal(objectRaw, m1);

        assertNotEquals(binObj0.array().length, binObj1.array().length);
        assertNotEquals(binObjWithRaw0.array().length, binObjWithRaw1.array().length);
        assertNotEquals(binObjRaw0.array().length, binObjRaw1.array().length);

        checkEquals(binObj0, binObj1);

        checkEquals(binObjWithRaw0, binObjWithRaw1);

        checkEquals(binObjRaw0, binObjRaw1);

        BinaryObjectOffheapImpl binObjOffheap0 = null;
        BinaryObjectOffheapImpl binObjOffheap1 = null;
        BinaryObjectOffheapImpl binObjWithRawOffheap0 = null;
        BinaryObjectOffheapImpl binObjWithRawOffheap1 = null;
        BinaryObjectOffheapImpl binObjRawOffheap0 = null;
        BinaryObjectOffheapImpl binObjRawOffheap1 = null;

        BinaryObjectImpl binObjOther0 = marshal(objOther, m0);
        BinaryObjectImpl binObjOther1 = marshal(objOther, m1);
        BinaryObjectImpl binObjWithRawOther0 = marshal(objectWithRawOther, m0);
        BinaryObjectImpl binObjWithRawOther1 = marshal(objectWithRawOther, m1);
        BinaryObjectImpl binObjRawOther0 = marshal(objectRawOther, m0);
        BinaryObjectImpl binObjRawOther1 = marshal(objectRawOther, m1);

        assertEquals(binObjOther0.length(), binObj0.length());
        assertEquals(binObjOther1.length(), binObj1.length());
        assertEquals(binObjWithRawOther0.length(), binObjWithRaw0.length());
        assertEquals(binObjWithRawOther1.length(), binObjWithRaw1.length());
        assertEquals(binObjRawOther0.length(), binObjRaw0.length());
        assertEquals(binObjRawOther1.length(), binObjRaw1.length());

        assertNotEquals(binObjOther0, binObj0);
        assertNotEquals(binObjOther1, binObj1);
        assertNotEquals(binObjWithRawOther0, binObjWithRaw0);
        assertNotEquals(binObjWithRawOther1, binObjWithRaw1);
        assertNotEquals(binObjRawOther0, binObjRaw0);
        assertNotEquals(binObjRawOther1, binObjRaw1);

        try {
            binObjOffheap0 = marshalOffHeap(binObj0, m0);
            binObjOffheap1 = marshalOffHeap(binObj1, m1);
            binObjWithRawOffheap0 = marshalOffHeap(binObjWithRaw0, m0);
            binObjWithRawOffheap1 = marshalOffHeap(binObjWithRaw1, m1);
            binObjRawOffheap0 = marshalOffHeap(binObjRaw0, m0);
            binObjRawOffheap1 = marshalOffHeap(binObjRaw1, m1);

            checkEquals(binObj0, binObjOffheap0);
            checkEquals(binObj1, binObjOffheap0);
            checkEquals(binObj0, binObjOffheap1);
            checkEquals(binObj1, binObjOffheap1);
            checkEquals(binObjOffheap0, binObjOffheap1);

            checkEquals(binObjWithRaw0, binObjWithRawOffheap0);
            checkEquals(binObjWithRaw0, binObjWithRawOffheap1);
            checkEquals(binObjWithRaw1, binObjWithRawOffheap0);
            checkEquals(binObjWithRaw1, binObjWithRawOffheap1);
            checkEquals(binObjWithRawOffheap0, binObjWithRawOffheap1);

            checkEquals(binObjRaw0, binObjRawOffheap0);
            checkEquals(binObjRaw1, binObjRawOffheap0);
            checkEquals(binObjRaw0, binObjRawOffheap1);
            checkEquals(binObjRaw1, binObjRawOffheap1);
            checkEquals(binObjRawOffheap0, binObjRawOffheap1);
        }
        finally {
            if (binObjOffheap0 != null) {
                GridUnsafe.freeMemory(binObjOffheap0.offheapAddress());
                binObjOffheap0 = null;
            }

            if (binObjOffheap1 != null) {
                GridUnsafe.freeMemory(binObjOffheap1.offheapAddress());
                binObjOffheap1 = null;
            }

            if (binObjWithRawOffheap0 != null) {
                GridUnsafe.freeMemory(binObjWithRawOffheap0.offheapAddress());
                binObjOffheap1 = null;
            }

            if (binObjWithRawOffheap1 != null) {
                GridUnsafe.freeMemory(binObjWithRawOffheap1.offheapAddress());
                binObjOffheap1 = null;
            }

            if (binObjRawOffheap0 != null) {
                GridUnsafe.freeMemory(binObjRawOffheap0.offheapAddress());
                binObjOffheap1 = null;
            }

            if (binObjRawOffheap1 != null) {
                GridUnsafe.freeMemory(binObjRawOffheap1.offheapAddress());
                binObjOffheap1 = null;
            }
        }
    }

    /**
     * @param binObj0 Object #0.
     * @param binObj1 Object #1.
     */
    private void checkEquals(Object binObj0, Object binObj1) {
        assertEquals(binObj0, binObj1);
        assertEquals(binObj1, binObj0);
        assertEquals(binObj0, binObj0);
        assertEquals(binObj1, binObj1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBinaryEqualsComplexObject() throws Exception {
        List<String> excludedClasses = Arrays.asList(
            TestClass0.class.getName(),
            TestClass1.class.getName(),
            TestClass2.class.getName());

        BinaryMarshaller m0 = binaryMarshaller(null, excludedClasses);
        BinaryMarshaller m1 = binaryMarshaller(null);

        TestClass0 obj0 = new TestClass0();
        TestClass1 obj1 = new TestClass1();
        TestClass2 obj2 = new TestClass2();

        BinaryObjectImpl binObj00 = marshal(obj0, m0);
        BinaryObjectImpl binObj01 = marshal(obj1, m0);
        BinaryObjectImpl binObj02 = marshal(obj2, m0);

        // The length of array must be equal. Object are different only by the class.
        assertEquals(binObj00.array().length, binObj01.array().length);
        assertEquals(binObj00.array().length, binObj02.array().length);

        BinaryObjectImpl binObj10 = marshal(obj0, m1);
        BinaryObjectImpl binObj11 = marshal(obj1, m1);
        BinaryObjectImpl binObj12 = marshal(obj2, m1);

        // The length of array must be equal. Object are different only by the class.
        assertEquals(binObj10.array().length, binObj11.array().length);
        assertEquals(binObj10.array().length, binObj12.array().length);

        assertNotEquals(binObj10.array().length, binObj00.array().length);

        assertEquals(binObj00, binObj10);
        assertEquals(binObj01, binObj11);
        assertEquals(binObj02, binObj12);

        assertNotEquals(binObj00, binObj01);
        assertNotEquals(binObj00, binObj02);
        assertNotEquals(binObj00, binObj11);
        assertNotEquals(binObj00, binObj12);

        assertNotEquals(binObj01, binObj00);
        assertNotEquals(binObj01, binObj02);
        assertNotEquals(binObj01, binObj10);
        assertNotEquals(binObj01, binObj12);

        assertNotEquals(binObj02, binObj00);
        assertNotEquals(binObj02, binObj01);
        assertNotEquals(binObj02, binObj00);
        assertNotEquals(binObj02, binObj11);
    }


    /**
     * The test must be refactored after {@link IgniteSystemProperties#IGNITE_BINARY_SORT_OBJECT_FIELDS}
     * is removed.
     *
     * @throws Exception If failed.
     */
    public void testFieldOrder() throws Exception {
        if (BinaryUtils.FIELDS_SORTED_ORDER)
            return;

        BinaryMarshaller m = binaryMarshaller();

        BinaryObjectImpl binObj = marshal(simpleObject(), m);

        Collection<String> fieldsBin =  binObj.type().fieldNames();

        Field[] fields = SimpleObject.class.getDeclaredFields();

        assertEquals(fields.length, fieldsBin.size());

        int i = 0;

        for (String fieldName : fieldsBin) {
            assertEquals(fields[i].getName(), fieldName);

            ++i;
        }
    }

    /**
     * The test must be refactored after {@link IgniteSystemProperties#IGNITE_BINARY_SORT_OBJECT_FIELDS}
     * is removed.
     *
     * @throws Exception If failed.
     */
    public void testFieldOrderByBuilder() throws Exception {
        if (BinaryUtils.FIELDS_SORTED_ORDER)
            return;

        BinaryMarshaller m = binaryMarshaller();

        BinaryObjectBuilder builder = new BinaryObjectBuilderImpl(binaryContext(m), "MyFakeClass");

        String[] fieldNames = {"field9", "field8", "field0", "field1", "field2"};

        for (String fieldName : fieldNames)
            builder.setField(fieldName, 0);

        BinaryObject binObj = builder.build();


        Collection<String> fieldsBin =  binObj.type().fieldNames();

        assertEquals(fieldNames.length, fieldsBin.size());

        int i = 0;

        for (String fieldName : fieldsBin) {
            assertEquals(fieldNames[i], fieldName);

            ++i;
        }
    }

    /**
     * @param obj Instance of the BinaryObjectImpl to offheap marshalling.
     * @param marsh Binary marshaller.
     * @return Instance of BinaryObjectOffheapImpl.
     */
    private BinaryObjectOffheapImpl marshalOffHeap(BinaryObjectImpl obj, BinaryMarshaller marsh) {
        long ptr = copyOffheap(obj);

        return new BinaryObjectOffheapImpl(binaryContext(marsh),
            ptr,
            0,
            obj.array().length);
    }

    /**
     *
     */
    private static interface SomeItf {
        /**
         * @return Check result.
         */
        int checkAfterUnmarshalled();
    }

    /**
     * Some non-serializable class.
     */
    @SuppressWarnings({"PublicField", "TransientFieldInNonSerializableClass", "FieldMayBeStatic"})
    private static class NonSerializableA {
        /** */
        private final long longVal = 0x33445566778899AAL;

        /** */
        protected Short shortVal = (short)0xAABB;

        /** */
        public String[] strArr = {"AA", "BB"};

        /** */
        public boolean flag1 = true;

        /** */
        public boolean flag2;

        /** */
        public Boolean flag3;

        /** */
        public Boolean flag4 = true;

        /** */
        public Boolean flag5 = false;

        /** */
        private transient int intVal = 0xAABBCCDD;

        /**
         * @param strArr Array.
         * @param shortVal Short value.
         */
        @SuppressWarnings({"UnusedDeclaration"})
        private NonSerializableA(@Nullable String[] strArr, @Nullable Short shortVal) {
            // No-op.
        }

        /**
         * Checks correctness of the state after unmarshalling.
         */
        void checkAfterUnmarshalled() {
            assertEquals(longVal, 0x33445566778899AAL);

            assertEquals(shortVal.shortValue(), (short)0xAABB);

            assertTrue(Arrays.equals(strArr, new String[] {"AA", "BB"}));

            assertEquals(0, intVal);

            assertTrue(flag1);
            assertFalse(flag2);
            assertNull(flag3);
            assertTrue(flag4);
            assertFalse(flag5);
        }
    }

    /**
     * Some non-serializable class.
     */
    @SuppressWarnings({"PublicField", "TransientFieldInNonSerializableClass", "PackageVisibleInnerClass"})
    static class NonSerializableB extends NonSerializableA {
        /** */
        public Short shortValue = 0x1122;

        /** */
        public long longValue = 0x8877665544332211L;

        /** */
        private transient NonSerializableA[] aArr = {
            new NonSerializableA(null, null),
            new NonSerializableA(null, null),
            new NonSerializableA(null, null)
        };

        /** */
        protected Double doubleVal = 123.456;

        /**
         * Just to eliminate the default constructor.
         */
        private NonSerializableB() {
            super(null, null);
        }

        /**
         * Checks correctness of the state after unmarshalling.
         */
        @Override void checkAfterUnmarshalled() {
            super.checkAfterUnmarshalled();

            assertEquals(shortValue.shortValue(), 0x1122);

            assertEquals(longValue, 0x8877665544332211L);

            assertNull(aArr);

            assertEquals(doubleVal, 123.456);
        }
    }

    /**
     * Some non-serializable class.
     */
    @SuppressWarnings({"TransientFieldInNonSerializableClass", "PublicField"})
    private static class NonSerializable extends NonSerializableB {
        /** */
        private int idVal = -17;

        /** */
        private final NonSerializableA aVal = new NonSerializableB();

        /** */
        private transient NonSerializableB bVal = new NonSerializableB();

        /** */
        private NonSerializableA[] bArr = new NonSerializableA[] {
            new NonSerializableB(),
            new NonSerializableA(null, null)
        };

        /** */
        public float floatVal = 567.89F;

        /**
         * Just to eliminate the default constructor.
         *
         * @param aVal Unused.
         */
        @SuppressWarnings({"UnusedDeclaration"})
        private NonSerializable(NonSerializableA aVal) {
        }

        /**
         * Checks correctness of the state after unmarshalling.
         */
        @Override void checkAfterUnmarshalled() {
            super.checkAfterUnmarshalled();

            assertEquals(idVal, -17);

            aVal.checkAfterUnmarshalled();

            assertNull(bVal);

            for (NonSerializableA a : bArr)
                a.checkAfterUnmarshalled();

            assertEquals(floatVal, 567.89F, 0);
        }
    }

    /**
     * Object with class fields.
     */
    private static class ObjectWithClassFields {
        /** */
        private Class<?> cls1;

        /** */
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
    private long copyOffheap(BinaryObjectImpl obj) {
        byte[] arr = obj.array();

        long ptr = GridUnsafe.allocateMemory(arr.length);

        GridUnsafe.copyHeapOffheap(arr, GridUnsafe.BYTE_ARR_OFF, ptr, arr.length);

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
     * @param enumArr Enum array.
     * @return Ordinals.
     */
    private <T extends Enum<?>> Integer[] ordinals(BinaryObject[] enumArr) {
        Integer[] ords = new Integer[enumArr.length];

        for (int i = 0; i < enumArr.length; i++)
            ords[i] = enumArr[i].enumOrdinal();

        return ords;
    }

    /**
     * @param po Binary object.
     * @param off Offset.
     * @return Value.
     */
    private int intFromBinary(BinaryObject po, int off) {
        byte[] arr = U.field(po, "arr");

        return Integer.reverseBytes(U.bytesToInt(arr, off));
    }

    /**
     * @param obj Original object.
     * @return Result object.
     */
    private <T> T marshalUnmarshal(T obj) throws IgniteCheckedException {
        return marshalUnmarshal(obj, binaryMarshaller());
    }

    /**
     * @param obj Original object.
     * @param marsh Marshaller.
     * @return Result object.
     */
    private <T> T marshalUnmarshal(Object obj, BinaryMarshaller marsh) throws IgniteCheckedException {
        byte[] bytes = marsh.marshal(obj);

        return marsh.unmarshal(bytes, null);
    }

    /**
     * @param obj Object.
     * @param marsh Marshaller.
     * @return Binary object.
     */
    private <T> BinaryObjectImpl marshal(T obj, BinaryMarshaller marsh) throws IgniteCheckedException {
        byte[] bytes = marsh.marshal(obj);

        return new BinaryObjectImpl(U.<GridBinaryMarshaller>field(marsh, "impl").context(),
            bytes, 0);
    }

    /**
     * @return Whether to use compact footers or not.
     */
    protected boolean compactFooter() {
        return true;
    }

    /**
     * @param marsh Marshaller.
     * @return Binary context.
     */
    protected BinaryContext binaryContext(BinaryMarshaller marsh) {
        GridBinaryMarshaller impl = U.field(marsh, "impl");

        return impl.context();
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller() throws IgniteCheckedException {
        return binaryMarshaller(null, null, null, null, null);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return binaryMarshaller(null, null, null, cfgs, null);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(Collection<BinaryTypeConfiguration> cfgs,
        Collection<String> excludedClasses) throws IgniteCheckedException {
        return binaryMarshaller(null, null, null, cfgs, excludedClasses);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(BinaryNameMapper nameMapper, BinaryIdMapper mapper,
        Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return binaryMarshaller(nameMapper, mapper, null, cfgs, null);
    }

    /**
     *
     */
    protected BinaryMarshaller binaryMarshaller(BinarySerializer serializer, Collection<BinaryTypeConfiguration> cfgs)
        throws IgniteCheckedException {
        return binaryMarshaller(null, null, serializer, cfgs, null);
    }

    /**
     * @return Binary marshaller.
     */
    protected BinaryMarshaller binaryMarshaller(
        BinaryNameMapper nameMapper,
        BinaryIdMapper mapper,
        BinarySerializer serializer,
        Collection<BinaryTypeConfiguration> cfgs,
        Collection<String> excludedClasses
    ) throws IgniteCheckedException {
        IgniteConfiguration iCfg = new IgniteConfiguration();

        BinaryConfiguration bCfg = new BinaryConfiguration();

        bCfg.setNameMapper(nameMapper);
        bCfg.setIdMapper(mapper);
        bCfg.setSerializer(serializer);
        bCfg.setCompactFooter(compactFooter());

        bCfg.setTypeConfigurations(cfgs);

        iCfg.setBinaryConfiguration(bCfg);

        BinaryContext ctx = new BinaryContext(BinaryCachingMetadataHandler.create(), iCfg, new NullLogger());

        BinaryMarshaller marsh = new BinaryMarshaller();

        marsh.setContext(new MarshallerContextTestImpl(null, excludedClasses));

        IgniteUtils.invoke(BinaryMarshaller.class, marsh, "setBinaryContext", ctx, iCfg);

        return marsh;
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
    private static SimpleObject simpleObject() {
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

        return outer;
    }

    /**
     * @return Binary object.
     */
    private TestBinary binaryObject() {
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

        TestBinary innerBinary = new TestBinary();

        innerBinary.b = 2;
        innerBinary.s = 2;
        innerBinary.i = 2;
        innerBinary.l = 2;
        innerBinary.f = 2.2f;
        innerBinary.d = 2.2d;
        innerBinary.c = 2;
        innerBinary.bool = true;
        innerBinary.str = "str2";
        innerBinary.uuid = UUID.randomUUID();
        innerBinary.date = new Date();
        innerBinary.ts = new Timestamp(System.currentTimeMillis());
        innerBinary.bArr = new byte[] {10, 20, 30};
        innerBinary.sArr = new short[] {10, 20, 30};
        innerBinary.iArr = new int[] {10, 20, 30};
        innerBinary.lArr = new long[] {10, 20, 30};
        innerBinary.fArr = new float[] {10.01f, 20.02f, 30.03f};
        innerBinary.dArr = new double[] {10.01d, 20.02d, 30.03d};
        innerBinary.cArr = new char[] {10, 20, 30};
        innerBinary.boolArr = new boolean[] {true, false, true};
        innerBinary.strArr = new String[] {"str10", "str20", "str30"};
        innerBinary.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerBinary.dateArr = new Date[] {new Date(44444), new Date(55555), new Date(66666)};
        innerBinary.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerBinary.bRaw = 3;
        innerBinary.sRaw = 3;
        innerBinary.iRaw = 3;
        innerBinary.lRaw = 3;
        innerBinary.fRaw = 3.3f;
        innerBinary.dRaw = 3.3d;
        innerBinary.cRaw = 3;
        innerBinary.boolRaw = true;
        innerBinary.strRaw = "str3";
        innerBinary.uuidRaw = UUID.randomUUID();
        innerBinary.dateRaw = new Date();
        innerBinary.tsRaw = new Timestamp(System.currentTimeMillis());
        innerBinary.bArrRaw = new byte[] {11, 21, 31};
        innerBinary.sArrRaw = new short[] {11, 21, 31};
        innerBinary.iArrRaw = new int[] {11, 21, 31};
        innerBinary.lArrRaw = new long[] {11, 21, 31};
        innerBinary.fArrRaw = new float[] {11.11f, 21.12f, 31.13f};
        innerBinary.dArrRaw = new double[] {11.11d, 21.12d, 31.13d};
        innerBinary.cArrRaw = new char[] {11, 21, 31};
        innerBinary.boolArrRaw = new boolean[] {true, false, true};
        innerBinary.strArrRaw = new String[] {"str11", "str21", "str31"};
        innerBinary.uuidArrRaw = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerBinary.dateArrRaw = new Date[] {new Date(77777), new Date(88888), new Date(99999)};
        innerBinary.objArrRaw = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
        innerBinary.col = new ArrayList<>();
        innerBinary.colRaw = new ArrayList<>();
        innerBinary.map = new HashMap<>();
        innerBinary.mapRaw = new HashMap<>();
        innerBinary.enumVal = TestEnum.B;
        innerBinary.enumValRaw = TestEnum.C;
        innerBinary.enumArr = new TestEnum[] {TestEnum.B, TestEnum.C};
        innerBinary.enumArrRaw = new TestEnum[] {TestEnum.C, TestEnum.D};

        innerBinary.col.add("str4");
        innerBinary.col.add("str5");
        innerBinary.col.add("str6");

        innerBinary.map.put(4, "str4");
        innerBinary.map.put(5, "str5");
        innerBinary.map.put(6, "str6");

        innerBinary.colRaw.add("str7");
        innerBinary.colRaw.add("str8");
        innerBinary.colRaw.add("str9");

        innerBinary.mapRaw.put(7, "str7");
        innerBinary.mapRaw.put(8, "str8");
        innerBinary.mapRaw.put(9, "str9");

        TestBinary outer = new TestBinary();

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
        outer.binary = innerBinary;
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
        outer.binaryRaw = innerBinary;

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
    private static class TestBinary implements Binarylizable {
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
        private TestBinary binary;

        /** */
        private TestBinary binaryRaw;

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
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
            writer.writeObject("_binary", binary);

            BinaryRawWriter raw = writer.rawWriter();

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
            raw.writeObject(binaryRaw);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
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
            binary = reader.readObject("_binary");

            BinaryRawReader raw = reader.rawReader();

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
            binaryRaw = raw.readObject();
        }

        /** {@inheritDoc} */
        @SuppressWarnings("FloatingPointEquality")
        @Override public boolean equals(Object other) {
            if (this == other)
                return true;

            if (other == null || getClass() != other.getClass())
                return false;

            TestBinary obj = (TestBinary)other;

            return GridTestUtils.deepEquals(this, obj);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(TestBinary.class, this);
        }
    }

    /**
     */
    private static class CustomSerializedObject1 implements Binarylizable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            // No-op.
        }
    }

    /**
     */
    private static class CustomSerializedObject2 implements Binarylizable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            // No-op.
        }
    }

    /**
     */
    private static class CustomSerializer1 implements BinarySerializer {
        /** {@inheritDoc} */
        @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
            CustomSerializedObject1 o = (CustomSerializedObject1)obj;

            writer.writeInt("val", o.val * 2);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
            CustomSerializedObject1 o = (CustomSerializedObject1)obj;

            o.val = reader.readInt("val");
        }
    }

    /**
     */
    private static class CustomSerializer2 implements BinarySerializer {
        /** {@inheritDoc} */
        @Override public void writeBinary(Object obj, BinaryWriter writer) throws BinaryObjectException {
            CustomSerializedObject2 o = (CustomSerializedObject2)obj;

            writer.writeInt("val", o.val * 3);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(Object obj, BinaryReader reader) throws BinaryObjectException {
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
    private static class InnerMappedObject extends CustomMappedObject1 {
        /**
         * @param val1 Val1
         * @param val2 Val2
         */
        InnerMappedObject(int val1, String val2) {
            super(val1, val2);
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
    private static class DynamicObject implements Binarylizable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val1", val1);

            if (idx > 0)
                writer.writeInt("val2", val2);

            if (idx > 1)
                writer.writeInt("val3", val3);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val1 = reader.readInt("val1");
            val2 = reader.readInt("val2");
            val3 = reader.readInt("val3");
        }
    }

    /**
     * Custom array list.
     */
    private static class CustomArrayList extends ArrayList {
        // No-op.
    }

    /**
     * Custom hash map.
     */
    private static class CustomHashMap extends HashMap {
        // No-op.
    }

    /**
     * Holder for non-stadard collections.
     */
    private static class CustomCollections {
        public List list = new ArrayList();
        public List customList = new CustomArrayList();
    }

    @SuppressWarnings("unchecked")
    private static class CustomCollectionsWithFactory implements Binarylizable {
        public List list = new CustomArrayList();
        public Map map = new CustomHashMap();

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeCollection("list", list);
            writer.writeMap("map", map);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            list = (List)reader.readCollection("list", new BinaryCollectionFactory<Object>() {
                @Override public Collection<Object> create(int size) {
                    return new CustomArrayList();
                }
            });

            map = reader.readMap("map", new BinaryMapFactory<Object, Object>() {
                @Override public Map<Object, Object> create(int size) {
                    return new CustomHashMap();
                }
            });
        }
    }

    /**
     * Dummy value holder.
     */
    private static class DummyHolder {
        /** Value. */
        public int val;

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public DummyHolder(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            return o != null && o instanceof DummyHolder && ((DummyHolder)o).val == val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
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
    private static class DetachedTestObject implements Binarylizable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            BinaryRawWriterEx raw = (BinaryRawWriterEx)writer.rawWriter();

            raw.writeObject(inner1);
            raw.writeObjectDetached(inner2);
            raw.writeObjectDetached(inner3);
            raw.writeObject(inner4);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            BinaryRawReaderEx raw = (BinaryRawReaderEx)reader.rawReader();

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

        /**
         * n
         *
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

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof Value))
                return false;

            Value value = (Value)o;

            return val == value.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
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
         * Protected constructor.
         */
        protected ProtectedConstructor() {
            // No-op.
        }
    }

    /**
     *
     */
    private static class MyTestClass implements Binarylizable {
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
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            if (!readyToSerialize)
                fail();
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            s = "readBinary";
        }
    }

    /**
     *
     */
    private static class MySingleton {
        public static final MySingleton INSTANCE = new MySingleton();

        /** */
        private String s;

        /** Initializer. */ {
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
    public static class ChildBinary extends ParentBinary {

    }

    /**
     *
     */
    public static class SimpleEnclosingObject {
        /** */
        private Object simpl;
    }

    /**
     *
     */
    public static class SimpleExternalizable implements Externalizable {
        /** */
        private String field;

        /**
         * {@link Externalizable} support.
         */
        public SimpleExternalizable() {
            // No-op.
        }

        /**
         * @param field Field.
         */
        public SimpleExternalizable(String field) {
            this.field = field;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, field);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            field = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            SimpleExternalizable that = (SimpleExternalizable)o;

            return field.equals(that.field);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return field.hashCode();
        }
    }

    /**
     *
     */
    private static class ParentBinary {
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
     * Class B for duplicate fields test.
     */
    private static class DuplicateFieldsA {
        /** Field. */
        int x;

        /**
         * Constructor.
         *
         * @param x Field.
         */
        protected DuplicateFieldsA(int x) {
            this.x = x;
        }

        /**
         * @return A's field.
         */
        public int xA() {
            return x;
        }
    }

    /**
     * Class B for duplicate fields test.
     */
    private static class DuplicateFieldsB extends DuplicateFieldsA {
        /** Field. */
        int x;

        /**
         * Constructor.
         *
         * @param xA Field for parent class.
         * @param xB Field for current class.
         */
        public DuplicateFieldsB(int xA, int xB) {
            super(xA);

            this.x = xB;
        }

        /**
         * @return B's field.
         */
        public int xB() {
            return x;
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
    private static class DecimalMarshalAware extends DecimalReflective implements Binarylizable {
        /** */
        public BigDecimal rawVal;

        /** */
        public BigDecimal[] rawValArr;

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeDecimal("val", val);
            writer.writeDecimalArray("valArr", valArr);

            BinaryRawWriter rawWriter = writer.rawWriter();

            rawWriter.writeDecimal(rawVal);
            rawWriter.writeDecimalArray(rawValArr);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readDecimal("val");
            valArr = reader.readDecimalArray("valArr");

            BinaryRawReader rawReader = reader.rawReader();

            rawVal = rawReader.readDecimal();
            rawValArr = rawReader.readDecimalArray();
        }
    }

    /**
     * Wrapper object.
     */
    private static class Wrapper {

        /** Value. */
        private final Object value;

        /** Constructor. */
        public Wrapper(Object value) {
            this.value = value;
        }

        /**
         * @return Value.
         */
        public Object getValue() {
            return value;
        }
    }

    /**
     */
    private static class SingleHandleA {
        /** */
        private SingleHandleB b;

        /** */
        private Map<Object, SingleHandleB> map = new HashMap<>();

        /**
         * @param b B.
         */
        SingleHandleA(SingleHandleB b) {
            this.b = b;

            map.put("key", b);
        }
    }

    /**
     */
    private static class SingleHandleB {
    }

    /**
     */
    private static class ClassFieldObject {
        /** */
        private Class<?> cls;

        /**
         * @param cls Class field.
         */
        public ClassFieldObject(Class<?> cls) {
            this.cls = cls;
        }
    }

    /**
     *
     */
    private static class TestClass0 {
        /** */
        private int intVal = 33;

        /** */
        private String strVal = "Test string value";

        /** */
        private SimpleObject obj = constSimpleObject();

        /**
         * @return Constant value of the SimpleObject.
         */
        public static SimpleObject constSimpleObject() {
            SimpleObject obj = simpleObject();

            obj.uuid = null;
            obj.date = new Date(33);
            obj.ts = new Timestamp(22);
            obj.uuidArr = new UUID[] {null, null, null};
            obj.dateArr = new Date[] {new Date(11111), new Date(22222), new Date(33333)};
            obj.objArr = new Object[] {null, null, null};

            obj.inner.uuid = null;
            obj.inner.date = new Date(33);
            obj.inner.ts = new Timestamp(22);
            obj.inner.uuidArr = new UUID[] {null, null, null};
            obj.inner.dateArr = new Date[] {new Date(11111), new Date(22222), new Date(33333)};
            obj.inner.objArr = new Object[] {null, null, null};

            return obj;
        }
    }

    /**
     *
     */
    private static class TestClass1 {
        /** */
        private int intVal = 33;

        /** */
        private String strVal = "Test string value";

        /** */
        private SimpleObject obj = TestClass0.constSimpleObject();
    }

    /**
     *
     */
    private static class TestClass2 extends TestClass0 {
    }

    /** */
    private static class ObjectWithRaw implements Binarylizable {
        /** */
        private int val;

        /** */
        private int rawVal;

        /**
         *
         */
        public ObjectWithRaw() {
        }

        /**
         * @param val Value.
         * @param rawVal Raw value.
         */
        public ObjectWithRaw(int val, int rawVal) {
            this.val = val;
            this.rawVal = rawVal;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.writeInt("val", val);

            writer.rawWriter().writeInt(rawVal);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val = reader.readInt("val");

            rawVal = reader.rawReader().readInt();
        }
    }

    /** */
    private static class ObjectRaw implements Binarylizable {
        /** */
        private int val0;

        /** */
        private int val1;

        /**
         *
         */
        public ObjectRaw() {
        }

        /**
         * @param val0 Value.
         * @param val1 Raw value.
         */
        public ObjectRaw(int val0, int val1) {
            this.val0 = val0;
            this.val1 = val1;
        }

        /** {@inheritDoc} */
        @Override public void writeBinary(BinaryWriter writer) throws BinaryObjectException {
            writer.rawWriter().writeInt(val0);
            writer.rawWriter().writeInt(val1);
        }

        /** {@inheritDoc} */
        @Override public void readBinary(BinaryReader reader) throws BinaryObjectException {
            val0 = reader.rawReader().readInt();
            val1 = reader.rawReader().readInt();
        }
    }
}