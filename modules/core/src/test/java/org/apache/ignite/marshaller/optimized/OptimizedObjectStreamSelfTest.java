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

package org.apache.ignite.marshaller.optimized;

import java.io.Externalizable;
import java.io.IOException;
import java.io.NotActiveException;
import java.io.NotSerializableException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Queue;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test for optimized object streams.
 */
public class OptimizedObjectStreamSelfTest extends GridCommonAbstractTest {
    /** */
    private static final MarshallerContext CTX = new MarshallerContextTestImpl();

    /** */
    private ConcurrentMap<Class, OptimizedClassDescriptor> clsMap = new ConcurrentHashMap8<>();

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
        byte val = 10;

        assertEquals(new Byte(val), marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testShort() throws Exception {
        short val = 100;

        assertEquals(new Short(val), marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInteger() throws Exception {
        int val = 100;

        assertEquals(new Integer(val), marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLong() throws Exception {
        long val = 1000L;

        assertEquals(new Long(val), marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloat() throws Exception {
        float val = 10.0f;

        assertEquals(val, marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testDouble() throws Exception {
        double val = 100.0d;

        assertEquals(val, marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoolean() throws Exception {
        boolean val = true;

        assertEquals(new Boolean(val), marshalUnmarshal(val));

        val = false;

        assertEquals(new Boolean(val), marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testChar() throws Exception {
        char val = 10;

        assertEquals(new Character(val), marshalUnmarshal(val));
    }

    /**
     * @throws Exception If failed.
     */
    public void testByteArray() throws Exception {
        byte[] arr = marshalUnmarshal(new byte[] {1, 2});

        assertArrayEquals(new byte[] {1, 2}, arr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testShortArray() throws Exception {
        short[] arr = marshalUnmarshal(new short[] {1, 2});

        assertArrayEquals(new short[] {1, 2}, arr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntArray() throws Exception {
        int[] arr = marshalUnmarshal(new int[] {1, 2});

        assertArrayEquals(new int[] {1, 2}, arr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLongArray() throws Exception {
        long[] arr = marshalUnmarshal(new long[] {1L, 2L});

        assertArrayEquals(new long[] {1, 2}, arr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testFloatArray() throws Exception {
        float[] arr = marshalUnmarshal(new float[] {1.0f, 2.0f});

        assertArrayEquals(new float[] {1.0f, 2.0f}, arr, 0.1f);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDoubleArray() throws Exception {
        double[] arr = marshalUnmarshal(new double[] {1.0d, 2.0d});

        assertArrayEquals(new double[] {1.0d, 2.0d}, arr, 0.1d);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBooleanArray() throws Exception {
        boolean[] arr = marshalUnmarshal(new boolean[] {true, false, false});

        assertEquals(3, arr.length);
        assertEquals(true, arr[0]);
        assertEquals(false, arr[1]);
        assertEquals(false, arr[2]);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCharArray() throws Exception {
        char[] arr = marshalUnmarshal(new char[] {1, 2});

        assertArrayEquals(new char[] {1, 2}, arr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testObject() throws Exception {
        TestObject obj = new TestObject();

        obj.longVal = 100L;
        obj.doubleVal = 100.0d;
        obj.longArr = new Long[] {200L, 300L};
        obj.doubleArr = new Double[] {200.0d, 300.0d};

        assertEquals(obj, marshalUnmarshal(obj));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRequireSerializable() throws Exception {
        try {
            OptimizedMarshaller marsh = new OptimizedMarshaller(true);

            marsh.setContext(CTX);

            marsh.marshal(new Object());

            assert false : "Exception not thrown.";
        }
        catch (IgniteCheckedException e) {
            NotSerializableException serEx = e.getCause(NotSerializableException.class);

            if (serEx == null)
                throw e;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPool() throws Exception {
        final TestObject obj = new TestObject();

        obj.longVal = 100L;
        obj.doubleVal = 100.0d;
        obj.longArr = new Long[100 * 1024];
        obj.doubleArr = new Double[100 * 1024];

        Arrays.fill(obj.longArr, 100L);
        Arrays.fill(obj.doubleArr, 100.0d);

        final OptimizedMarshaller marsh = new OptimizedMarshaller();

        marsh.setContext(CTX);

        marsh.setPoolSize(5);

        try {
            multithreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < 50; i++)
                        assertEquals(obj, marsh.unmarshal(marsh.marshal(obj), null));

                    return null;
                }
            }, 20);
        }
        finally {
            marsh.setPoolSize(0);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectWithNulls() throws Exception {
        TestObject obj = new TestObject();

        obj.longVal = 100L;
        obj.longArr = new Long[] {200L, 300L};

        assertEquals(obj, marshalUnmarshal(obj));
    }

    /**
     * @throws Exception If failed.
     */
    public void testObjectArray() throws Exception {
        TestObject obj1 = new TestObject();

        obj1.longVal = 100L;
        obj1.doubleVal = 100.0d;
        obj1.longArr = new Long[] {200L, 300L};
        obj1.doubleArr = new Double[] {200.0d, 300.0d};

        TestObject obj2 = new TestObject();

        obj2.longVal = 400L;
        obj2.doubleVal = 400.0d;
        obj2.longArr = new Long[] {500L, 600L};
        obj2.doubleArr = new Double[] {500.0d, 600.0d};

        TestObject[] arr = {obj1, obj2};

        assertArrayEquals(arr, (Object[])marshalUnmarshal(arr));

        String[] strArr = {"str1", "str2"};

        assertArrayEquals(strArr, (String[])marshalUnmarshal(strArr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testExternalizable() throws Exception {
        ExternalizableTestObject1 obj = new ExternalizableTestObject1();

        obj.longVal = 100L;
        obj.doubleVal = 100.0d;
        obj.longArr = new Long[] {200L, 300L};
        obj.doubleArr = new Double[] {200.0d, 300.0d};

        assertEquals(obj, marshalUnmarshal(obj));
    }

    /**
     * @throws Exception If failed.
     */
    public void testExternalizableWithNulls() throws Exception {
        ExternalizableTestObject2 obj = new ExternalizableTestObject2();

        obj.longVal = 100L;
        obj.doubleVal = 100.0d;
        obj.longArr = new Long[] {200L, 300L};
        obj.doubleArr = new Double[] {200.0d, 300.0d};

        obj = marshalUnmarshal(obj);

        assertEquals(100L, obj.longVal.longValue());
        assertNull(obj.doubleVal);
        assertArrayEquals(new Long[] {200L, 300L}, obj.longArr);
        assertNull(obj.doubleArr);
    }

    /**
     * @throws Exception If failed.
     */
    public void testLink() throws Exception {
        for (int i = 0; i < 20; i++) {
            LinkTestObject1 obj1 = new LinkTestObject1();
            LinkTestObject2 obj2 = new LinkTestObject2();
            LinkTestObject2 obj3 = new LinkTestObject2();

            obj1.val = 100;
            obj2.ref = obj1;
            obj3.ref = obj1;

            LinkTestObject2[] arr = new LinkTestObject2[] {obj2, obj3};

            assertArrayEquals(arr, (Object[])marshalUnmarshal(arr));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCycleLink() throws Exception {
        for (int i = 0; i < 20; i++) {
            CycleLinkTestObject obj = new CycleLinkTestObject();

            obj.val = 100;
            obj.ref = obj;

            assertEquals(obj, marshalUnmarshal(obj));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoDefaultConstructor() throws Exception {
        NoDefaultConstructorTestObject obj = new NoDefaultConstructorTestObject(100);

        assertEquals(obj, marshalUnmarshal(obj));
    }

    /**
     * @throws Exception If failed.
     */
    public void testEnum() throws Exception {
        assertEquals(TestEnum.B, marshalUnmarshal(TestEnum.B));

        TestEnum[] arr = new TestEnum[] {TestEnum.C, TestEnum.A, TestEnum.B, TestEnum.A};

        assertArrayEquals(arr, (Object[])marshalUnmarshal(arr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollection() throws Exception {
        TestObject obj1 = new TestObject();

        obj1.longVal = 100L;
        obj1.doubleVal = 100.0d;
        obj1.longArr = new Long[] {200L, 300L};
        obj1.doubleArr = new Double[] {200.0d, 300.0d};

        TestObject obj2 = new TestObject();

        obj2.longVal = 400L;
        obj2.doubleVal = 400.0d;
        obj2.longArr = new Long[] {500L, 600L};
        obj2.doubleArr = new Double[] {500.0d, 600.0d};

        Collection<TestObject> col = F.asList(obj1, obj2);

        assertEquals(col, marshalUnmarshal(col));
    }

    /**
     * @throws Exception If failed.
     */
    public void testMap() throws Exception {
        TestObject obj1 = new TestObject();

        obj1.longVal = 100L;
        obj1.doubleVal = 100.0d;
        obj1.longArr = new Long[] {200L, 300L};
        obj1.doubleArr = new Double[] {200.0d, 300.0d};

        TestObject obj2 = new TestObject();

        obj2.longVal = 400L;
        obj2.doubleVal = 400.0d;
        obj2.longArr = new Long[] {500L, 600L};
        obj2.doubleArr = new Double[] {500.0d, 600.0d};

        Map<Integer, TestObject> map = F.asMap(1, obj1, 2, obj2);

        assertEquals(map, marshalUnmarshal(map));
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

        assertEquals(date, marshalUnmarshal(date));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransient() throws Exception {
        TransientTestObject obj = marshalUnmarshal(new TransientTestObject(100, 200, "str1", "str2"));

        assertEquals(100, obj.val1);
        assertEquals(0, obj.val2);
        assertEquals("str1", obj.str1);
        assertNull(obj.str2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteReadObject() throws Exception {
        WriteReadTestObject obj = marshalUnmarshal(new WriteReadTestObject(100, "str"));

        assertEquals(100, obj.val);
        assertEquals("Optional data", obj.str);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteReplace() throws Exception {
        ReplaceTestObject obj = marshalUnmarshal(new ReplaceTestObject(100));

        assertEquals(200, obj.value());
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteReplaceNull() throws Exception {
        ReplaceNullTestObject obj = marshalUnmarshal(new ReplaceNullTestObject());

        assertNull(obj);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadResolve() throws Exception {
        ResolveTestObject obj = marshalUnmarshal(new ResolveTestObject(100));

        assertEquals(200, obj.value());
    }

    /**
     * @throws Exception If failed.
     */
    public void testArrayDeque() throws Exception {
        Queue<Integer> queue = new ArrayDeque<>();

        for (int i = 0; i < 100; i++)
            queue.add(i);

        Queue<Integer> newQueue = marshalUnmarshal(queue);

        assertEquals(queue.size(), newQueue.size());

        Integer i;

        while ((i = newQueue.poll()) != null)
            assertEquals(queue.poll(), i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testArrayList() throws Exception {
        Collection<Integer> list = new ArrayList<>();

        for (int i = 0; i < 100; i++)
            list.add(i);

        assertEquals(list, marshalUnmarshal(list));
    }

    /**
     * @throws Exception If failed.
     */
    public void testHashMap() throws Exception {
        Map<Integer, Integer> map = new HashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        assertEquals(map, marshalUnmarshal(map));
    }

    /**
     * @throws Exception If failed.
     */
    public void testHashSet() throws Exception {
        Collection<Integer> set = new HashSet<>();

        for (int i = 0; i < 100; i++)
            set.add(i);

        assertEquals(set, marshalUnmarshal(set));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("UseOfObsoleteCollectionType")
    public void testHashtable() throws Exception {
        Map<Integer, Integer> map = new Hashtable<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        assertEquals(map, marshalUnmarshal(map));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIdentityHashMap() throws Exception {
        Map<Integer, Integer> map = new IdentityHashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        assertEquals(map, marshalUnmarshal(map));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLinkedHashMap() throws Exception {
        Map<Integer, Integer> map = new LinkedHashMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        assertEquals(map, marshalUnmarshal(map));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLinkedHashSet() throws Exception {
        Collection<Integer> set = new LinkedHashSet<>();

        for (int i = 0; i < 100; i++)
            set.add(i);

        assertEquals(set, marshalUnmarshal(set));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLinkedList() throws Exception {
        Collection<Integer> list = new LinkedList<>();

        for (int i = 0; i < 100; i++)
            list.add(i);

        assertEquals(list, marshalUnmarshal(list));
    }

    /**
     * @throws Exception If failed.
     */
    public void testPriorityQueue() throws Exception {
        Queue<Integer> queue = new PriorityQueue<>();

        for (int i = 0; i < 100; i++)
            queue.add(i);

        Queue<Integer> newQueue = marshalUnmarshal(queue);

        assertEquals(queue.size(), newQueue.size());

        Integer i;

        while ((i = newQueue.poll()) != null)
            assertEquals(queue.poll(), i);
    }

    /**
     * @throws Exception If failed.
     */
    public void testProperties() throws Exception {
        Properties dflts = new Properties();

        dflts.setProperty("key1", "val1");
        dflts.setProperty("key2", "wrong");

        Properties props = new Properties(dflts);

        props.setProperty("key2", "val2");

        Properties newProps = marshalUnmarshal(props);

        assertEquals("val1", newProps.getProperty("key1"));
        assertEquals("val2", newProps.getProperty("key2"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTreeMap() throws Exception {
        Map<Integer, Integer> map = new TreeMap<>();

        for (int i = 0; i < 100; i++)
            map.put(i, i);

        assertEquals(map, marshalUnmarshal(map));
    }

    /**
     * @throws Exception If failed.
     */
    public void testTreeSet() throws Exception {
        Collection<Integer> set = new TreeSet<>();

        for (int i = 0; i < 100; i++)
            set.add(i);

        assertEquals(set, marshalUnmarshal(set));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("UseOfObsoleteCollectionType")
    public void testVector() throws Exception {
        Collection<Integer> vector = new Vector<>();

        for (int i = 0; i < 100; i++)
            vector.add(i);

        assertEquals(vector, marshalUnmarshal(vector));
    }

    /**
     * @throws Exception If failed.
     */
    public void testString() throws Exception {
        assertEquals("Latin", marshalUnmarshal("Latin"));
        assertEquals("Кириллица", marshalUnmarshal("Кириллица"));
        assertEquals("中国的", marshalUnmarshal("中国的"));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadLine() throws Exception {
        OptimizedObjectInputStream in = new OptimizedObjectInputStream(new GridUnsafeDataInput());

        byte[] bytes = "line1\nline2\r\nli\rne3\nline4".getBytes();

        in.in().bytes(bytes, bytes.length);

        assertEquals("line1", in.readLine());
        assertEquals("line2", in.readLine());
        assertEquals("line3", in.readLine());
        assertEquals("line4", in.readLine());
    }

    /**
     * @throws Exception If failed.
     */
    public void testHierarchy() throws Exception {
        C c = new C(100, "str", 200, "str", 300, "str");

        C newC = marshalUnmarshal(c);

        assertEquals(100, newC.valueA());
        assertEquals("Optional data", newC.stringA());
        assertEquals(200, newC.valueB());
        assertNull(newC.stringB());
        assertEquals(0, newC.valueC());
        assertEquals("Optional data", newC.stringC());
    }

    /**
     * @throws Exception If failed.
     */
    public void testInet4Address() throws Exception {
        Inet4Address addr = (Inet4Address)InetAddress.getByName("localhost");

        assertEquals(addr, marshalUnmarshal(addr));
    }

    /**
     * @throws Exception If failed.
     */
    public void testClass() throws Exception {
        assertEquals(int.class, marshalUnmarshal(int.class));
        assertEquals(Long.class, marshalUnmarshal(Long.class));
        assertEquals(TestObject.class, marshalUnmarshal(TestObject.class));
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteReadFields() throws Exception {
        WriteReadFieldsTestObject obj = marshalUnmarshal(new WriteReadFieldsTestObject(100, "str"));

        assertEquals(100, obj.val);
        assertEquals("Optional data", obj.str);
    }

    /**
     * @throws Exception If failed.
     */
    public void testWriteFields() throws Exception {
        WriteFieldsTestObject obj = marshalUnmarshal(new WriteFieldsTestObject(100, "str"));

        assertEquals(100, obj.val);
        assertEquals("Optional data", obj.str);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBigInteger() throws Exception {
        BigInteger b = new BigInteger("54654865468745468465321414646834562346475457488");

        assertEquals(b, marshalUnmarshal(b));
    }

    /**
     * @throws Exception If failed.
     */
    public void testBigDecimal() throws Exception {
        BigDecimal b = new BigDecimal("849572389457208934572093574.123512938654126458542145");

        assertEquals(b, marshalUnmarshal(b));
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleDateFormat() throws Exception {
        SimpleDateFormat f = new SimpleDateFormat("MM/dd/yyyy");

        assertEquals(f, marshalUnmarshal(f));
    }

    /**
     * @throws Exception If failed.
     */
    public void testComplexObject() throws Exception {
        ComplexTestObject obj = new ComplexTestObject();

        assertEquals(obj, marshalUnmarshal(obj));

        ExternalizableTestObject1 extObj1 = new ExternalizableTestObject1();

        extObj1.longVal = 1000L;
        extObj1.doubleVal = 1000.0d;
        extObj1.longArr = new Long[] {1000L, 2000L, 3000L};
        extObj1.doubleArr = new Double[] {1000.0d, 2000.0d, 3000.0d};

        ExternalizableTestObject1 extObj2 = new ExternalizableTestObject1();

        extObj2.longVal = 2000L;
        extObj2.doubleVal = 2000.0d;
        extObj2.longArr = new Long[] {4000L, 5000L, 6000L};
        extObj2.doubleArr = new Double[] {4000.0d, 5000.0d, 6000.0d};

        Properties props = new Properties();

        props.setProperty("name", "value");

        Collection<Integer> col = F.asList(10, 20, 30);

        Map<Integer, String> map = F.asMap(10, "str1", 20, "str2", 30, "str3");

        obj = new ComplexTestObject(
            (byte)1,
            (short)10,
            100,
            1000L,
            100.0f,
            1000.0d,
            'a',
            false,
            (byte)2,
            (short)20,
            200,
            2000L,
            200.0f,
            2000.0d,
            'b',
            true,
            new byte[] {1, 2, 3},
            new short[] {10, 20, 30},
            new int[] {100, 200, 300},
            new long[] {1000, 2000, 3000},
            new float[] {100.0f, 200.0f, 300.0f},
            new double[] {1000.0d, 2000.0d, 3000.0d},
            new char[] {'a', 'b', 'c'},
            new boolean[] {false, true},
            new ExternalizableTestObject1[] {extObj1, extObj2},
            "String",
            TestEnum.A,
            UUID.randomUUID(),
            props,
            new ArrayList<>(col),
            new HashMap<>(map),
            new HashSet<>(col),
            new LinkedList<>(col),
            new LinkedHashMap<>(map),
            new LinkedHashSet<>(col),
            new Date(),
            ExternalizableTestObject2.class
        );

        assertEquals(obj, marshalUnmarshal(obj));
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadToArray() throws Exception {
        OptimizedObjectInputStream in = OptimizedObjectStreamRegistry.in();

        try {
            byte[] arr = new byte[50];

            for (int i = 0; i < arr.length; i++)
                arr[i] = (byte)i;

            in.in().bytes(arr, arr.length);

            byte[] buf = new byte[10];

            assertEquals(10, in.read(buf));

            for (int i = 0; i < buf.length; i++)
                assertEquals(i, buf[i]);

            buf = new byte[30];

            assertEquals(20, in.read(buf, 0, 20));

            for (int i = 0; i < buf.length; i++)
                assertEquals(i < 20 ? 10 + i : 0, buf[i]);

            buf = new byte[30];

            assertEquals(10, in.read(buf, 10, 10));

            for (int i = 0; i < buf.length; i++)
                assertEquals(i >= 10 && i < 20 ? 30 + (i - 10) : 0, buf[i]);

            buf = new byte[20];

            assertEquals(10, in.read(buf));

            for (int i = 0; i < buf.length; i++)
                assertEquals(i < 10 ? 40 + i : 0, buf[i]);
        }
        finally {
            OptimizedObjectStreamRegistry.closeIn(in);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testHandleTableGrow() throws Exception {
        List<String> c = new ArrayList<>();

        for (int i = 0; i < 29; i++)
            c.add("str");

        String str = c.get(28);

        c.add("str");
        c.add(str);

        List<Object> c0 = marshalUnmarshal(c);

        assertTrue(c0.get(28) == c0.get(30));
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncorrectExternalizable() throws Exception {
        GridTestUtils.assertThrows(
            log,
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    return marshalUnmarshal(new IncorrectExternalizable());
                }
            },
            IOException.class,
            null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testExcludedClass() throws Exception {
        Class<?>[] exclClasses = U.staticField(MarshallerExclusions.class, "EXCL_CLASSES");

        assertFalse(F.isEmpty(exclClasses));

        for (Class<?> cls : exclClasses)
            assertEquals(cls, marshalUnmarshal(cls));
    }

    /**
     * @throws Exception If failed.
     */
    public void testInet6Address() throws Exception {
        final InetAddress address = Inet6Address.getByAddress(new byte[16]);

        assertEquals(address, marshalUnmarshal(address));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void testPutFieldsWithDefaultWriteObject() throws Exception {
        try {
            marshalUnmarshal(new CustomWriteObjectMethodObject("test"));
        }
        catch (IOException e) {
            assert e.getCause() instanceof NotActiveException;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableInstanceNeverThrown")
    public void testThrowable() throws Exception {
        Throwable t = new Throwable("Throwable");

        assertEquals(t.getMessage(), ((Throwable)marshalUnmarshal(t)).getMessage());
    }

    /**
     * Marshals and unmarshals object.
     *
     * @param obj Original object.
     * @return Object after marshalling and unmarshalling.
     * @throws Exception In case of error.
     */
    private <T> T marshalUnmarshal(@Nullable Object obj) throws Exception {
        OptimizedObjectOutputStream out = null;
        OptimizedObjectInputStream in = null;

        try {
            out = OptimizedObjectStreamRegistry.out();

            out.context(clsMap, CTX, null, true);

            out.writeObject(obj);

            byte[] arr = out.out().array();

            in = OptimizedObjectStreamRegistry.in();

            in.context(clsMap, CTX, null, getClass().getClassLoader());

            in.in().bytes(arr, arr.length);

            Object obj0 = in.readObject();

            checkHandles(out, in);

            return (T)obj0;
        }
        finally {
            OptimizedObjectStreamRegistry.closeOut(out);
            OptimizedObjectStreamRegistry.closeIn(in);
        }
    }

    /**
     * Checks that handles are equal in output and input streams.
     *
     * @param out Output stream.
     * @param in Input stream.
     * @throws Exception If failed.
     */
    private void checkHandles(OptimizedObjectOutputStream out, OptimizedObjectInputStream in)
        throws Exception {
        Object[] outHandles = out.handledObjects();
        Object[] inHandles = in.handledObjects();

        assertEquals(outHandles.length, inHandles.length);

        for (int i = 0; i < outHandles.length; i++) {
            if (outHandles[i] == null)
                assertTrue(inHandles[i] == null);
            else {
                assertFalse(inHandles[i] == null);

                assertTrue(outHandles[i].getClass() == inHandles[i].getClass());
            }
        }
    }

    /** */
    private static class IncorrectExternalizable implements Externalizable {
        /**
         * Required by {@link Externalizable}.
         */
        public IncorrectExternalizable() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(0);
            out.writeInt(200);
            out.writeObject("str");
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            in.readInt();
            in.readObject();
        }
    }

    /**
     * Test object.
     */
    private static class TestObject implements Serializable {
        /** */
        private Long longVal;

        /** */
        private Double doubleVal;

        /** */
        private Long[] longArr;

        /** */
        private Double[] doubleArr;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject obj = (TestObject)o;

            return longVal != null ? longVal.equals(obj.longVal) : obj.longVal == null &&
                doubleVal != null ? doubleVal.equals(obj.doubleVal) : obj.doubleVal == null &&
                Arrays.equals(longArr, obj.longArr) &&
                Arrays.equals(doubleArr, obj.doubleArr);
        }
    }

    /**
     * Externalizable test object.
     */
    private static class ExternalizableTestObject1 implements Externalizable {
        /** */
        private Long longVal;

        /** */
        private Double doubleVal;

        /** */
        private Long[] longArr;

        /** */
        private Double[] doubleArr;

        /**
         * Required by {@link Externalizable}.
         */
        public ExternalizableTestObject1() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ExternalizableTestObject1 obj = (ExternalizableTestObject1)o;

            return longVal != null ? longVal.equals(obj.longVal) : obj.longVal == null &&
                doubleVal != null ? doubleVal.equals(obj.doubleVal) : obj.doubleVal == null &&
                Arrays.equals(longArr, obj.longArr) &&
                Arrays.equals(doubleArr, obj.doubleArr);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(longVal);
            out.writeDouble(doubleVal);
            U.writeArray(out, longArr);
            U.writeArray(out, doubleArr);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            longVal = in.readLong();
            doubleVal = in.readDouble();

            Object[] arr = U.readArray(in);

            longArr = Arrays.copyOf(arr, arr.length, Long[].class);

            arr = U.readArray(in);

            doubleArr = Arrays.copyOf(arr, arr.length, Double[].class);
        }
    }

    /**
     * Externalizable test object.
     */
    private static class ExternalizableTestObject2 implements Externalizable {
        /** */
        private Long longVal;

        /** */
        private Double doubleVal;

        /** */
        private Long[] longArr;

        /** */
        private Double[] doubleArr;

        /**
         * Required by {@link Externalizable}.
         */
        public ExternalizableTestObject2() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ExternalizableTestObject2 obj = (ExternalizableTestObject2)o;

            return longVal != null ? longVal.equals(obj.longVal) : obj.longVal == null &&
                doubleVal != null ? doubleVal.equals(obj.doubleVal) : obj.doubleVal == null &&
                Arrays.equals(longArr, obj.longArr) &&
                Arrays.equals(doubleArr, obj.doubleArr);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(longVal);
            U.writeArray(out, longArr);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            longVal = in.readLong();

            Object[] arr = U.readArray(in);

            longArr = Arrays.copyOf(arr, arr.length, Long[].class);
        }
    }

    /**
     * Test object.
     */
    private static class LinkTestObject1 implements Serializable {
        /** */
        private int val;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            LinkTestObject1 obj = (LinkTestObject1)o;

            return val == obj.val;
        }
    }

    /**
     * Test object.
     */
    private static class LinkTestObject2 implements Serializable {
        /** */
        private LinkTestObject1 ref;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            LinkTestObject2 obj = (LinkTestObject2)o;

            return ref != null ? ref.equals(obj.ref) : obj.ref == null;
        }
    }

    /**
     * Cycle link test object.
     */
    private static class CycleLinkTestObject implements Serializable {
        /** */
        private int val;

        /** */
        private CycleLinkTestObject ref;

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            CycleLinkTestObject obj = (CycleLinkTestObject)o;

            return val == obj.val && ref != null ? ref.val == val : obj.ref == null;
        }
    }

    /**
     * Test object without default constructor.
     */
    private static class NoDefaultConstructorTestObject implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private NoDefaultConstructorTestObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            NoDefaultConstructorTestObject obj = (NoDefaultConstructorTestObject)o;

            return val == obj.val;
        }
    }

    /**
     * Test object with transient fields.
     */
    @SuppressWarnings("TransientFieldNotInitialized")
    private static class TransientTestObject implements Serializable {
        /** */
        private int val1;

        /** */
        private transient int val2;

        /** */
        private String str1;

        /** */
        private transient String str2;

        /**
         * @param val1 Value 1.
         * @param val2 Value 2.
         * @param str1 String 1.
         * @param str2 String 2.
         */
        private TransientTestObject(int val1, int val2, String str1, String str2) {
            this.val1 = val1;
            this.val2 = val2;
            this.str1 = str1;
            this.str2 = str2;
        }
    }

    /**
     * Test object with {@code writeObject} and {@code readObject} methods.
     */
    @SuppressWarnings("TransientFieldNotInitialized")
    private static class WriteReadTestObject implements Serializable {
        /** */
        private int val;

        /** */
        private transient String str;

        /**
         * @param val Value.
         * @param str String.
         */
        private WriteReadTestObject(int val, String str) {
            this.val = val;
            this.str = str;
        }

        /**
         * @param out Output stream.
         * @throws IOException In case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();

            out.writeUTF("Optional data");
        }

        /**
         * @param in Input stream.
         * @throws IOException In case of error.
         * @throws ClassNotFoundException If class not found.
         */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();

            str = in.readUTF();
        }
    }

    /**
     * Test object that uses {@code writeFields} and {@code readFields} methods.
     */
    private static class WriteReadFieldsTestObject implements Serializable {
        /** */
        private int val;

        /** */
        private String str;

        /**
         * @param val Value.
         * @param str String.
         */
        private WriteReadFieldsTestObject(int val, String str) {
            this.val = val;
            this.str = str;
        }

        /**
         * @param out Output stream.
         * @throws IOException In case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            ObjectOutputStream.PutField fields = out.putFields();

            fields.put("val", val);
            fields.put("str", "Optional data");

            out.writeFields();
        }

        /**
         * @param in Input stream.
         * @throws IOException In case of error.
         * @throws ClassNotFoundException If class not found.
         */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            ObjectInputStream.GetField fields = in.readFields();

            val = fields.get("val", 0);
            str = (String)fields.get("str", null);
        }
    }

    /**
     * Test object that uses {@code writeFields} and {@code readFields} methods.
     */
    private static class WriteFieldsTestObject implements Serializable {
        /** */
        private int val;

        /** */
        @SuppressWarnings("UnusedDeclaration")
        private String str;

        /**
         * @param val Value.
         * @param str String.
         */
        private WriteFieldsTestObject(int val, String str) {
            this.val = val;
            this.str = str;
        }

        /**
         * @param out Output stream.
         * @throws IOException In case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            ObjectOutputStream.PutField fields = out.putFields();

            fields.put("val", val);
            fields.put("str", "Optional data");

            out.writeFields();
        }
    }

    /**
     * Base object with {@code writeReplace} method.
     */
    private abstract static class ReplaceTestBaseObject implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private ReplaceTestBaseObject(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /**
         * @return Replaced object.
         * @throws ObjectStreamException In case of error.
         */
        protected Object writeReplace() throws ObjectStreamException {
            return new ReplaceTestObject(val * 2);
        }
    }

    /**
     * Test object for {@code writeReplace} method.
     */
    private static class ReplaceTestObject extends ReplaceTestBaseObject {
        /**
         * @param val Value.
         */
        private ReplaceTestObject(int val) {
            super(val);
        }
    }

    /**
     * Test object with {@code writeReplace} method.
     */
    private static class ReplaceNullTestObject implements Serializable {
        /**
         * @return Replaced object.
         * @throws ObjectStreamException In case of error.
         */
        protected Object writeReplace() throws ObjectStreamException {
            return null;
        }
    }

    /**
     * Base object with {@code readResolve} method.
     */
    private abstract static class ResolveTestBaseObject implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private ResolveTestBaseObject(int val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public int value() {
            return val;
        }

        /**
         * @return Replaced object.
         * @throws ObjectStreamException In case of error.
         */
        protected Object readResolve() throws ObjectStreamException {
            return new ResolveTestObject(val * 2);
        }
    }

    /**
     * Test object for {@code readResolve} method.
     */
    private static class ResolveTestObject extends ResolveTestBaseObject {
        /**
         * @param val Value.
         */
        private ResolveTestObject(int val) {
            super(val);
        }
    }

    /**
     * Class A.
     */
    private static class A implements Serializable {
        /** */
        private int valA;

        /** */
        private transient String strA;

        /**
         * @param valA Value A.
         * @param strA String A.
         */
        A(int valA, String strA) {
            this.valA = valA;
            this.strA = strA;
        }

        /**
         * @return Value.
         */
        int valueA() {
            return valA;
        }

        /**
         * @return String.
         */
        String stringA() {
            return strA;
        }

        /**
         * @param out Output stream.
         * @throws IOException In case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();

            out.writeUTF("Optional data");
        }

        /**
         * @param in Input stream.
         * @throws IOException In case of error.
         * @throws ClassNotFoundException If class not found.
         */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();

            strA = in.readUTF();
        }
    }

    /**
     * Class B.
     */
    private static class B extends A {
        /** */
        private int valB;

        /** */
        @SuppressWarnings("TransientFieldNotInitialized")
        private transient String strB;

        /**
         * @param valA Value A.
         * @param strA String A.
         * @param valB Value B.
         * @param strB String B.
         */
        B(int valA, String strA, int valB, String strB) {
            super(valA, strA);

            this.valB = valB;
            this.strB = strB;
        }

        /**
         * @return Value.
         */
        int valueB() {
            return valB;
        }

        /**
         * @return String.
         */
        String stringB() {
            return strB;
        }
    }

    /**
     * Class C.
     */
    @SuppressWarnings("MethodOverridesPrivateMethodOfSuperclass")
    private static class C extends B {
        /** */
        @SuppressWarnings("InstanceVariableMayNotBeInitializedByReadObject")
        private int valC;

        /** */
        private transient String strC;

        /**
         * @param valA Value A.
         * @param strA String A.
         * @param valB Value B.
         * @param strB String B.
         * @param valC Value C.
         * @param strC String C.
         */
        C(int valA, String strA, int valB, String strB, int valC, String strC) {
            super(valA, strA, valB, strB);

            this.valC = valC;
            this.strC = strC;
        }

        /**
         * @return Value.
         */
        int valueC() {
            return valC;
        }

        /**
         * @return String.
         */
        String stringC() {
            return strC;
        }

        /**
         * @param out Output stream.
         * @throws IOException In case of error.
         */
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.writeUTF("Optional data");
        }

        /**
         * @param in Input stream.
         * @throws IOException In case of error.
         * @throws ClassNotFoundException If class not found.
         */
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            strC = in.readUTF();
        }
    }

    /**
     * Complex test object.
     */
    private static class ComplexTestObject implements Serializable {
        /** */
        private byte byteVal1;

        /** */
        private short shortVal1;

        /** */
        private int intVal1;

        /** */
        private long longVal1;

        /** */
        private float floatVal1;

        /** */
        private double doubleVal1;

        /** */
        private char cVal1;

        /** */
        private boolean boolVal1;

        /** */
        private Byte byteVal2;

        /** */
        private Short shortVal2;

        /** */
        private Integer intVal2;

        /** */
        private Long longVal2;

        /** */
        private Float floatVal2;

        /** */
        private Double doubleVal2;

        /** */
        private Character cVal2;

        /** */
        private Boolean boolVal2;

        /** */
        private byte[] byteArr;

        /** */
        private short[] shortArr;

        /** */
        private int[] intArr;

        /** */
        private long[] longArr;

        /** */
        private float[] floatArr;

        /** */
        private double[] doubleArr;

        /** */
        private char[] cArr;

        /** */
        private boolean[] boolArr;

        /** */
        private ExternalizableTestObject1[] objArr;

        /** */
        private String str;

        /** */
        private TestEnum enumVal;

        /** */
        private UUID uuid;

        /** */
        private Properties props;

        /** */
        private ArrayList<Integer> arrList;

        /** */
        private HashMap<Integer, String> hashMap;

        /** */
        private HashSet<Integer> hashSet;

        /** */
        private LinkedList<Integer> linkedList;

        /** */
        private LinkedHashMap<Integer, String> linkedHashMap;

        /** */
        private LinkedHashSet<Integer> linkedHashSet;

        /** */
        private Date date;

        /** */
        private Class<?> cls;

        /** */
        private ComplexTestObject self;

        /** */
        private ComplexTestObject() {
            self = this;
        }

        /**
         * @param byteVal1 Byte value.
         * @param shortVal1 Short value.
         * @param intVal1 Integer value.
         * @param longVal1 Long value.
         * @param floatVal1 Float value.
         * @param doubleVal1 Double value.
         * @param cVal1 Char value.
         * @param boolVal1 Boolean value.
         * @param byteVal2 Byte value.
         * @param shortVal2 Short value.
         * @param intVal2 Integer value.
         * @param longVal2 Long value.
         * @param floatVal2 Float value.
         * @param doubleVal2 Double value.
         * @param cVal2 Char value.
         * @param boolVal2 Boolean value.
         * @param byteArr Bytes array.
         * @param shortArr Shorts array.
         * @param intArr Integers array.
         * @param longArr Longs array.
         * @param floatArr Floats array.
         * @param doubleArr Doubles array.
         * @param cArr Chars array.
         * @param boolArr Booleans array.
         * @param objArr Objects array.
         * @param str String.
         * @param enumVal Enum.
         * @param uuid UUID.
         * @param props Properties.
         * @param arrList ArrayList.
         * @param hashMap HashMap.
         * @param hashSet HashSet.
         * @param linkedList LinkedList.
         * @param linkedHashMap LinkedHashMap.
         * @param linkedHashSet LinkedHashSet.
         * @param date Date.
         * @param cls Class.
         */
        private ComplexTestObject(byte byteVal1, short shortVal1, int intVal1, long longVal1, float floatVal1,
            double doubleVal1, char cVal1, boolean boolVal1, Byte byteVal2, Short shortVal2, Integer intVal2,
            Long longVal2, Float floatVal2, Double doubleVal2, Character cVal2, Boolean boolVal2, byte[] byteArr,
            short[] shortArr, int[] intArr, long[] longArr, float[] floatArr, double[] doubleArr, char[] cArr,
            boolean[] boolArr, ExternalizableTestObject1[] objArr, String str, TestEnum enumVal, UUID uuid,
            Properties props, ArrayList<Integer> arrList, HashMap<Integer, String> hashMap, HashSet<Integer> hashSet,
            LinkedList<Integer> linkedList, LinkedHashMap<Integer, String> linkedHashMap,
            LinkedHashSet<Integer> linkedHashSet, Date date, Class<?> cls) {
            this.byteVal1 = byteVal1;
            this.shortVal1 = shortVal1;
            this.intVal1 = intVal1;
            this.longVal1 = longVal1;
            this.floatVal1 = floatVal1;
            this.doubleVal1 = doubleVal1;
            this.cVal1 = cVal1;
            this.boolVal1 = boolVal1;
            this.byteVal2 = byteVal2;
            this.shortVal2 = shortVal2;
            this.intVal2 = intVal2;
            this.longVal2 = longVal2;
            this.floatVal2 = floatVal2;
            this.doubleVal2 = doubleVal2;
            this.cVal2 = cVal2;
            this.boolVal2 = boolVal2;
            this.byteArr = byteArr;
            this.shortArr = shortArr;
            this.intArr = intArr;
            this.longArr = longArr;
            this.floatArr = floatArr;
            this.doubleArr = doubleArr;
            this.cArr = cArr;
            this.boolArr = boolArr;
            this.objArr = objArr;
            this.str = str;
            this.enumVal = enumVal;
            this.uuid = uuid;
            this.props = props;
            this.arrList = arrList;
            this.hashMap = hashMap;
            this.hashSet = hashSet;
            this.linkedList = linkedList;
            this.linkedHashMap = linkedHashMap;
            this.linkedHashSet = linkedHashSet;
            this.date = date;
            this.cls = cls;

            self = this;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("RedundantIfStatement")
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ComplexTestObject obj = (ComplexTestObject)o;

            if (boolVal1 != obj.boolVal1)
                return false;

            if (byteVal1 != obj.byteVal1)
                return false;

            if (cVal1 != obj.cVal1)
                return false;

            if (Double.compare(obj.doubleVal1, doubleVal1) != 0)
                return false;

            if (Float.compare(obj.floatVal1, floatVal1) != 0)
                return false;

            if (intVal1 != obj.intVal1)
                return false;

            if (longVal1 != obj.longVal1)
                return false;

            if (shortVal1 != obj.shortVal1)
                return false;

            if (arrList != null ? !arrList.equals(obj.arrList) : obj.arrList != null)
                return false;

            if (!Arrays.equals(boolArr, obj.boolArr))
                return false;

            if (boolVal2 != null ? !boolVal2.equals(obj.boolVal2) : obj.boolVal2 != null)
                return false;

            if (!Arrays.equals(byteArr, obj.byteArr))
                return false;

            if (byteVal2 != null ? !byteVal2.equals(obj.byteVal2) : obj.byteVal2 != null)
                return false;

            if (!Arrays.equals(cArr, obj.cArr))
                return false;

            if (cVal2 != null ? !cVal2.equals(obj.cVal2) : obj.cVal2 != null)
                return false;

            if (cls != null ? !cls.equals(obj.cls) : obj.cls != null)
                return false;

            if (date != null ? !date.equals(obj.date) : obj.date != null)
                return false;

            if (!Arrays.equals(doubleArr, obj.doubleArr))
                return false;

            if (doubleVal2 != null ? !doubleVal2.equals(obj.doubleVal2) : obj.doubleVal2 != null)
                return false;

            if (enumVal != obj.enumVal)
                return false;

            if (!Arrays.equals(floatArr, obj.floatArr))
                return false;

            if (floatVal2 != null ? !floatVal2.equals(obj.floatVal2) : obj.floatVal2 != null)
                return false;

            if (hashMap != null ? !hashMap.equals(obj.hashMap) : obj.hashMap != null)
                return false;

            if (hashSet != null ? !hashSet.equals(obj.hashSet) : obj.hashSet != null)
                return false;

            if (!Arrays.equals(intArr, obj.intArr))
                return false;

            if (intVal2 != null ? !intVal2.equals(obj.intVal2) : obj.intVal2 != null)
                return false;

            if (linkedHashMap != null ? !linkedHashMap.equals(obj.linkedHashMap) : obj.linkedHashMap != null)
                return false;

            if (linkedHashSet != null ? !linkedHashSet.equals(obj.linkedHashSet) : obj.linkedHashSet != null)
                return false;

            if (linkedList != null ? !linkedList.equals(obj.linkedList) : obj.linkedList != null)
                return false;

            if (!Arrays.equals(longArr, obj.longArr))
                return false;

            if (longVal2 != null ? !longVal2.equals(obj.longVal2) : obj.longVal2 != null)
                return false;

            if (!Arrays.equals(objArr, obj.objArr))
                return false;

            if (props != null ? !props.equals(obj.props) : obj.props != null)
                return false;

            if (!Arrays.equals(shortArr, obj.shortArr))
                return false;

            if (shortVal2 != null ? !shortVal2.equals(obj.shortVal2) : obj.shortVal2 != null)
                return false;

            if (str != null ? !str.equals(obj.str) : obj.str != null)
                return false;

            if (uuid != null ? !uuid.equals(obj.uuid) : obj.uuid != null)
                return false;

            if (self != this)
                return false;

            return true;
        }
    }

    /**
     * Test enum.
     */
    @SuppressWarnings("JavaDoc")
    private enum TestEnum {
        /** */
        A,

        /** */
        B,

        /** */
        C
    }

    /**
     * Class with custom serialization method which at the beginning invokes
     * {@link ObjectOutputStream#defaultWriteObject()} and {@link ObjectOutputStream#putFields()} then.
     */
    public static class CustomWriteObjectMethodObject implements Serializable {
        /** */
        private final String name;

        /**
         * Creates new instance.
         * @param name Object name.
         */
        public CustomWriteObjectMethodObject(String name) {
            this.name = name;
        }

        /** {@inheritDoc} */
        private void writeObject(ObjectOutputStream stream) throws IOException {
            stream.defaultWriteObject();

            ObjectOutputStream.PutField fields = stream.putFields();
            fields.put("name", "test");

            stream.writeFields();
        }
    }
}