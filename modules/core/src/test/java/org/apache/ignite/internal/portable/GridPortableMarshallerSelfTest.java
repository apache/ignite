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

//        assertEquals(obj, copy.deserialize());
//
//        copy = copy(po, new HashMap<String, Object>());
//
//        assertEquals(obj, copy.deserialize());
//
//        Map<String, Object> map = new HashMap<>(1, 1.0f);
//
//        map.put("i", 3);
//
//        copy = copy(po, map);
//
//        assertEquals((byte)2, copy.<Byte>field("b").byteValue());
//        assertEquals((short)2, copy.<Short>field("s").shortValue());
//        assertEquals(3, copy.<Integer>field("i").intValue());
//        assertEquals(2L, copy.<Long>field("l").longValue());
//        assertEquals(2.2f, copy.<Float>field("f").floatValue(), 0);
//        assertEquals(2.2d, copy.<Double>field("d").doubleValue(), 0);
//        assertEquals((char)2, copy.<Character>field("c").charValue());
//        assertEquals(false, copy.<Boolean>field("bool").booleanValue());
//
//        SimpleObject obj0 = copy.deserialize();
//
//        assertEquals((byte)2, obj0.b);
////        assertEquals((short)2, obj0.s);
////        assertEquals(3, obj0.i);
////        assertEquals(2L, obj0.l);
////        assertEquals(2.2f, obj0.f, 0);
////        assertEquals(2.2d, obj0.d, 0);
////        assertEquals((char)2, obj0.c);
////        assertEquals(false, obj0.bool);
//
//        map = new HashMap<>(3, 1.0f);
//
//        map.put("b", (byte)3);
//        map.put("l", 3L);
//        map.put("bool", true);
//
//        copy = copy(po, map);
//
//        assertEquals((byte)3, copy.<Byte>field("b").byteValue());
//        assertEquals((short)2, copy.<Short>field("s").shortValue());
//        assertEquals(2, copy.<Integer>field("i").intValue());
//        assertEquals(3L, copy.<Long>field("l").longValue());
//        assertEquals(2.2f, copy.<Float>field("f").floatValue(), 0);
//        assertEquals(2.2d, copy.<Double>field("d").doubleValue(), 0);
//        assertEquals((char)2, copy.<Character>field("c").charValue());
//        assertEquals(true, copy.<Boolean>field("bool").booleanValue());
//
//        obj0 = copy.deserialize();
//
//        assertEquals((byte)3, obj0.b);
////        assertEquals((short)2, obj0.s);
////        assertEquals(2, obj0.i);
////        assertEquals(3L, obj0.l);
////        assertEquals(2.2f, obj0.f, 0);
////        assertEquals(2.2d, obj0.d, 0);
////        assertEquals((char)2, obj0.c);
////        assertEquals(true, obj0.bool);
//
//        map = new HashMap<>(8, 1.0f);
//
//        map.put("b", (byte)3);
//        map.put("s", (short)3);
//        map.put("i", 3);
//        map.put("l", 3L);
//        map.put("f", 3.3f);
//        map.put("d", 3.3d);
//        map.put("c", (char)3);
//        map.put("bool", true);
//
//        copy = copy(po, map);
//
//        assertEquals((byte)3, copy.<Byte>field("b").byteValue());
//        assertEquals((short)3, copy.<Short>field("s").shortValue());
//        assertEquals(3, copy.<Integer>field("i").intValue());
//        assertEquals(3L, copy.<Long>field("l").longValue());
//        assertEquals(3.3f, copy.<Float>field("f").floatValue(), 0);
//        assertEquals(3.3d, copy.<Double>field("d").doubleValue(), 0);
//        assertEquals((char)3, copy.<Character>field("c").charValue());
//        assertEquals(true, copy.<Boolean>field("bool").booleanValue());
//
//        obj0 = copy.deserialize();
//
//        assertEquals((byte)3, obj0.b);
////        assertEquals((short)3, obj0.s);
////        assertEquals(3, obj0.i);
////        assertEquals(3L, obj0.l);
////        assertEquals(3.3f, obj0.f, 0);
////        assertEquals(3.3d, obj0.d, 0);
////        assertEquals((char)3, obj0.c);
////        assertEquals(true, obj0.bool);
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
//        inner.i = 1;
//        inner.l = 1;
//        inner.f = 1.1f;
//        inner.d = 1.1d;
//        inner.c = 1;
//        inner.bool = true;
//        inner.str = "str1";
//        inner.uuid = UUID.randomUUID();
//        inner.date = new Date();
//        inner.ts = new Timestamp(System.currentTimeMillis());
//        inner.bArr = new byte[] {1, 2, 3};
//        inner.sArr = new short[] {1, 2, 3};
//        inner.iArr = new int[] {1, 2, 3};
//        inner.lArr = new long[] {1, 2, 3};
//        inner.fArr = new float[] {1.1f, 2.2f, 3.3f};
//        inner.dArr = new double[] {1.1d, 2.2d, 3.3d};
//        inner.cArr = new char[] {1, 2, 3};
//        inner.boolArr = new boolean[] {true, false, true};
//        inner.strArr = new String[] {"str1", "str2", "str3"};
//        inner.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
//        inner.dateArr = new Date[] {new Date(11111), new Date(22222), new Date(33333)};
//        inner.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
//        inner.col = new ArrayList<>();
//        inner.map = new HashMap<>();
//        inner.enumVal = TestEnum.A;
//        inner.enumArr = new TestEnum[] {TestEnum.A, TestEnum.B};
//        inner.bdArr = new BigDecimal[] {new BigDecimal(1000), BigDecimal.ONE};
//
//        inner.col.add("str1");
//        inner.col.add("str2");
//        inner.col.add("str3");
//
//        inner.map.put(1, "str1");
//        inner.map.put(2, "str2");
//        inner.map.put(3, "str3");
//
//        inner.mEntry = inner.map.entrySet().iterator().next();

        SimpleObject outer = new SimpleObject();

        outer.b = 2;
//        outer.s = 2;
//        outer.i = 2;
//        outer.l = 2;
//        outer.f = 2.2f;
//        outer.d = 2.2d;
//        outer.c = 2;
//        outer.bool = false;
//        outer.str = "str2";
//        outer.uuid = UUID.randomUUID();
//        outer.date = new Date();
//        outer.ts = new Timestamp(System.currentTimeMillis());
//        outer.bArr = new byte[] {10, 20, 30};
//        outer.sArr = new short[] {10, 20, 30};
//        outer.iArr = new int[] {10, 20, 30};
//        outer.lArr = new long[] {10, 20, 30};
//        outer.fArr = new float[] {10.01f, 20.02f, 30.03f};
//        outer.dArr = new double[] {10.01d, 20.02d, 30.03d};
//        outer.cArr = new char[] {10, 20, 30};
//        outer.boolArr = new boolean[] {false, true, false};
//        outer.strArr = new String[] {"str10", "str20", "str30"};
//        outer.uuidArr = new UUID[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
//        outer.dateArr = new Date[] {new Date(44444), new Date(55555), new Date(66666)};
//        outer.objArr = new Object[] {UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID()};
//        outer.col = new ArrayList<>();
//        outer.map = new HashMap<>();
//        outer.enumVal = TestEnum.B;
//        outer.enumArr = new TestEnum[] {TestEnum.B, TestEnum.C};
        outer.inner = inner;
//        outer.bdArr = new BigDecimal[] {new BigDecimal(5000), BigDecimal.TEN};


//        outer.col.add("str4");
//        outer.col.add("str5");
//        outer.col.add("str6");
//
//        outer.map.put(4, "str4");
//        outer.map.put(5, "str5");
//        outer.map.put(6, "str6");
//
//        outer.mEntry = outer.map.entrySet().iterator().next();

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
//
//        /** */
//        private int i;
//
//        /** */
//        private long l;
//
//        /** */
//        private float f;
//
//        /** */
//        private double d;
//
//        /** */
//        private char c;
//
//        /** */
//        private boolean bool;
//
//        /** */
//        private String str;
//
//        /** */
//        private UUID uuid;
//
//        /** */
//        private Date date;
//
//        /** */
//        private Timestamp ts;
//
//        /** */
//        private byte[] bArr;
//
//        /** */
//        private short[] sArr;
//
//        /** */
//        private int[] iArr;
//
//        /** */
//        private long[] lArr;
//
//        /** */
//        private float[] fArr;
//
//        /** */
//        private double[] dArr;
//
//        /** */
//        private char[] cArr;
//
//        /** */
//        private boolean[] boolArr;
//
//        /** */
//        private String[] strArr;
//
//        /** */
//        private UUID[] uuidArr;
//
//        /** */
//        private Date[] dateArr;
//
//        /** */
//        private Object[] objArr;
//
//        /** */
//        private BigDecimal[] bdArr;
//
//        /** */
//        private Collection<String> col;
//
//        /** */
//        private Map<Integer, String> map;
//
//        /** */
//        private TestEnum enumVal;
//
//        /** */
//        private TestEnum[] enumArr;
//
//        /** */
//        private Map.Entry<Integer, String> mEntry;

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