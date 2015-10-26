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