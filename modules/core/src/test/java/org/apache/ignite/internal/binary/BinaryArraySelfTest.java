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

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryMarshallerSelfTest.TestClass1;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/** */
public class BinaryArraySelfTest extends AbstractTypedArrayTest {
    /** */
    private static Ignite server;

    /** */
    private static Ignite client;

    /** */
    private static IgniteCache<Object, Object> srvCache;

    /** */
    private static IgniteCache<Object, Object> cliCache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        server = startGrid(0);
        client = startClientGrid(1);

        srvCache = server.createCache("my-cache");
        cliCache = client.getOrCreateCache("my-cache");
    }

    /** */
    @Test
    public void testArrayKey() {
        doTestArrayKey(srvCache);
        doTestArrayKey(cliCache);
    }

    /** */
    @Test
    public void testArrayValue() {
        doTestArrayValue(srvCache);
        doTestArrayValue(cliCache);
    }

    /** */
    @Test
    public void testArrayFieldInKey() {
        doTestArrayFieldInKey(srvCache);
        doTestArrayFieldInKey(cliCache);
    }

    /** */
    @Test
    public void testArrayFieldInValue() {
        doTestArrayFieldInValue(srvCache);
        doTestArrayFieldInValue(cliCache);
    }

    /** */
    @Test
    public void testBoxedPrimitivesArrays() {
        doTestBoxedPrimitivesArrays(srvCache);
        doTestBoxedPrimitivesArrays(cliCache);
    }

    /** */
    @Test
    public void testBinaryModeArray() {
        doTestBinaryModeArray(srvCache);
        doTestBinaryModeArray(cliCache);
    }

    /** */
    private void doTestArrayKey(IgniteCache<Object, Object> c) {
        TestClass1[] key1 = {new TestClass1(), new TestClass1()};
        TestClass1[] key2 = {};
        TestClass1[] key3 = new TestClass1[] {new TestClass1() {}};

        c.put(key1, 1);
        c.put(key2, 2);
        c.put(key3, 3);

        assertEquals(1, c.get(key1));
        assertEquals(2, c.get(key2));
        assertEquals(3, c.get(key3));

        assertTrue(c.replace(key1, 1, 2));
        assertTrue(c.replace(key2, 2, 3));
        assertTrue(c.replace(key3, 3, 4));

        assertTrue(c.remove(key1));
        assertTrue(c.remove(key2));
        assertTrue(c.remove(key3));
    }

    /** */
    private void doTestArrayFieldInKey(IgniteCache<Object, Object> c) {
        TestClass key1 = new TestClass(new TestClass1[] {new TestClass1(), new TestClass1()});
        TestClass key2 = new TestClass(new TestClass1[] {});
        TestClass key3 = new TestClass(new TestClass1[] {new TestClass1() {}});

        c.put(key1, 1);
        c.put(key2, 2);
        c.put(key3, 3);

        assertEquals(1, c.get(key1));
        assertEquals(2, c.get(key2));
        assertEquals(3, c.get(key3));

        assertTrue(c.replace(key1, 1, 2));
        assertTrue(c.replace(key2, 2, 3));
        assertTrue(c.replace(key3, 3, 4));

        assertTrue(c.remove(key1));
        assertTrue(c.remove(key2));
        assertTrue(c.remove(key3));
    }

    /** */
    private void doTestArrayValue(IgniteCache<Object, Object> c) {
        c.put(1, new TestClass1[] {new TestClass1(), new TestClass1()});
        c.put(2, new TestClass1[] {});
        c.put(3, new TestClass1[] {new TestClass1() {}});

        Object val1 = c.get(1);
        Object val2 = c.get(2);
        Object val3 = c.get(3);

        assertEquals(useTypedArrays ? TestClass1[].class : Object[].class, val1.getClass());
        assertEquals(useTypedArrays ? TestClass1[].class : Object[].class, val2.getClass());
        assertEquals(useTypedArrays ? TestClass1[].class : Object[].class, val3.getClass());

        if (useTypedArrays) {
            assertTrue(c.replace(1, val1, val2));
            assertTrue(c.replace(2, val2, val3));
            assertTrue(c.replace(3, val3, val1));
        }

        assertTrue(c.remove(1));
        assertTrue(c.remove(2));
        assertTrue(c.remove(3));
    }

    /** */
    private void doTestArrayFieldInValue(IgniteCache<Object, Object> c) {
        c.put(1, new TestClass(new TestClass1[] {new TestClass1(), new TestClass1()}));
        c.put(2, new TestClass(new TestClass1[] {}));
        c.put(3, new TestClass(new TestClass1[] {new TestClass1() {}}));

        TestClass val1 = (TestClass)c.get(1);
        TestClass val2 = (TestClass)c.get(2);
        TestClass val3 = (TestClass)c.get(3);

        assertEquals(2, val1.getArr().length);
        assertEquals(0, val2.getArr().length);
        assertEquals(1, val3.getArr().length);

        if (useTypedArrays) {
            assertTrue(c.replace(1, val1, val2));
            assertTrue(c.replace(2, val2, val3));
            assertTrue(c.replace(3, val3, val1));
        }

        assertTrue(c.remove(1));
        assertTrue(c.remove(2));
        assertTrue(c.remove(3));
    }

    /** */
    private void doTestBinaryModeArray(IgniteCache<Object, Object> c) {
        c.put(1, new TestClass1[] {new TestClass1(), new TestClass1()});
        Object obj = c.withKeepBinary().get(1);

        assertEquals(useTypedArrays ? BinaryArray.class : Object[].class, obj.getClass());

        if (useTypedArrays)
            assertEquals(TestClass1[].class, ((BinaryObject)obj).deserialize().getClass());

        assertTrue(c.remove(1));
    }

    /** */
    private void doTestBoxedPrimitivesArrays(IgniteCache<Object, Object> c) {
        Object[] data = new Object[] {
            new Byte[] {1, 2, 3},
            new Short[] {1, 2, 3},
            new Integer[] {1, 2, 3},
            new Long[] {1L, 2L, 3L},
            new Float[] {1f, 2f, 3f},
            new Double[] {1d, 2d, 3d},
            new Character[] {'a', 'b', 'c'},
            new Boolean[] {true, false},
        };

        for (Object item : data) {
            c.put(1, item);

            Object item0 = c.get(1);

            if (useTypedArrays)
                assertTrue(c.replace(1, item, item));

            assertTrue(c.remove(1));

            if (useTypedArrays)
                assertEquals(item.getClass(), item0.getClass());

            assertTrue(Arrays.equals((Object[])item, (Object[])item0));

            c.put(item, 1);

            assertEquals(1, c.get(item));

            if (useTypedArrays)
                assertTrue(c.replace(item, 1, 2));

            assertTrue(c.remove(item));
        }
    }

    /** */
    @Test
    public void testArrayOfBinariesSerDe() {
        BinaryObject[] arr = new BinaryObject[] {
            server.binary().toBinary(new TestClass1()),
            server.binary().toBinary(new TestClass1())
        };

        Object obj = server.binary().toBinary(arr);

        Object deser;

        if (useTypedArrays) {
            assertTrue(obj instanceof BinaryArray);

            deser = ((BinaryArray)obj).deserialize();
        }
        else {
            assertTrue(obj instanceof Object[]);

            deser = PlatformUtils.unwrapBinariesInArray((Object[])obj);
        }

        assertEquals(Object[].class, deser.getClass());

        Object[] res = ((Object[])deser);

        assertEquals(2, res.length);
        assertTrue(res[0] instanceof TestClass1);
        assertTrue(res[1] instanceof TestClass1);

    }

    /** */
    @Test
    public void testBinaryArraySerDe() {
        TestClass1[] arr = {new TestClass1(), new TestClass1()};

        Object obj = server.binary().toBinary(arr);

        Object deser;

        if (useTypedArrays) {
            assertEquals(BinaryArray.class, obj.getClass());

            deser = ((BinaryArray)obj).deserialize();

            assertEquals(TestClass1[].class, deser.getClass());
        }
        else {
            assertTrue(obj instanceof Object[]);

            deser = PlatformUtils.unwrapBinariesInArray((Object[])obj);
        }

        assertEquals(2, ((Object[])deser).length);
    }

    /** */
    @Test
    public void testBinaryArrayFieldSerDe() {
        TestClass1 src = new TestClass1();

        BinaryMarshallerSelfTest.SimpleObject sobj = GridTestUtils.getFieldValue(src, "obj");

        GridTestUtils.setFieldValue(sobj, "objArr", new Object[] {"string", 1L, null});

        BinaryObject obj = server.binary().toBinary(src);

        BinaryObject simpleObj = obj.field("obj");
        Object objArr = simpleObj.field("objArr");

        assertEquals(useTypedArrays ? BinaryArray.class : Object[].class, objArr.getClass());

        Object deser = obj.deserialize();

        assertEquals(TestClass1.class, deser.getClass());

        sobj = GridTestUtils.getFieldValue(deser, "obj");

        Object[] arr = GridTestUtils.getFieldValue(sobj, "objArr");

        assertNotNull(arr);
        assertEquals(3, arr.length);
        assertEquals("string", arr[0]);
        assertEquals(1L, arr[1]);
        assertNull(arr[2]);
    }

    /** */
    public static class TestClass {
        /** */
        private final TestClass1[] arr;

        /** */
        public TestClass(TestClass1[] arr) {
            this.arr = arr;
        }

        /** */
        public TestClass1[] getArr() {
            return arr;
        }
    }
}
