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
    private static IgniteCache<TestClass1[], Integer> srvCache0;

    /** */
    private static IgniteCache<TestClass1[], Integer> cliCache0;

    /** */
    private static IgniteCache<Integer, TestClass1[]> srvCache1;

    /** */
    private static IgniteCache<Integer, TestClass1[]> cliCache1;

    /** */
    private static IgniteCache<TestClass, Integer> srvCache2;

    /** */
    private static IgniteCache<TestClass, Integer> cliCache2;

    /** */
    private static IgniteCache<Integer, TestClass> srvCache3;

    /** */
    private static IgniteCache<Integer, TestClass> cliCache3;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        server = startGrid(0);
        client = startClientGrid(1);

        srvCache0 = server.createCache("my-cache-1");
        cliCache0 = client.getOrCreateCache("my-cache-1");

        srvCache1 = server.createCache("my-cache");
        cliCache1 = client.getOrCreateCache("my-cache");

        srvCache2 = server.createCache("my-cache-2");
        cliCache2 = client.getOrCreateCache("my-cache-2");

        srvCache3 = server.createCache("my-cache-3");
        cliCache3 = client.getOrCreateCache("my-cache-3");
    }

    /** */
    @Test
    public void testArrayKey() {
        doTestArrayKey(srvCache0);
        doTestArrayKey(cliCache0);
    }

    /** */
    @Test
    public void testArrayValue() {
        doTestArrayValue(srvCache1);
        doTestArrayValue(cliCache1);
    }

    /** */
    @Test
    public void testArrayFieldInKey() {
        doTestArrayFieldInKey(srvCache2);
        doTestArrayFieldInKey(cliCache2);
    }

    /** */
    @Test
    public void testArrayFieldInValue() {
        doTestArrayFieldInValue(srvCache3);
        doTestArrayFieldInValue(cliCache3);
    }

    /** */
    private void doTestArrayKey(IgniteCache<TestClass1[], Integer> c) {
        TestClass1[] key1 = {new TestClass1(), new TestClass1()};
        TestClass1[] key2 = {};
        TestClass1[] key3 = new TestClass1[] {new TestClass1() {}};

        c.put(key1, 1);
        c.put(key2, 2);
        c.put(key3, 3);

        assertEquals((Integer)1, c.get(key1));
        assertEquals((Integer)2, c.get(key2));
        assertEquals((Integer)3, c.get(key3));

        assertTrue(c.remove(key1));
        assertTrue(c.remove(key2));
        assertTrue(c.remove(key3));
    }

    /** */
    private void doTestArrayFieldInKey(IgniteCache<TestClass, Integer> c) {
        TestClass key1 = new TestClass(new TestClass1[] {new TestClass1(), new TestClass1()});
        TestClass key2 = new TestClass(new TestClass1[] {});
        TestClass key3 = new TestClass(new TestClass1[] {new TestClass1() {}});

        c.put(key1, 1);
        c.put(key2, 2);
        c.put(key3, 3);

        assertEquals((Integer)1, c.get(key1));
        assertEquals((Integer)2, c.get(key2));
        assertEquals((Integer)3, c.get(key3));

        assertTrue(c.remove(key1));
        assertTrue(c.remove(key2));
        assertTrue(c.remove(key3));
    }

    /** */
    private void doTestArrayValue(IgniteCache<Integer, TestClass1[]> c) {
        c.put(1, new TestClass1[] {new TestClass1(), new TestClass1()});
        c.put(2, new TestClass1[] {});
        c.put(3, new TestClass1[] {new TestClass1() {}});

        Object val1 = c.get(1);
        Object val2 = c.get(2);
        Object val3 = c.get(3);

        assertEquals(useTypedArrays ? TestClass1[].class : Object[].class, val1.getClass());
        assertEquals(useTypedArrays ? TestClass1[].class : Object[].class, val2.getClass());
        assertEquals(useTypedArrays ? TestClass1[].class : Object[].class, val3.getClass());

        assertTrue(c.remove(1));
        assertTrue(c.remove(2));
        assertTrue(c.remove(3));
    }

    /** */
    private void doTestArrayFieldInValue(IgniteCache<Integer, TestClass> c) {
        c.put(1, new TestClass(new TestClass1[] {new TestClass1(), new TestClass1()}));
        c.put(2, new TestClass(new TestClass1[] {}));
        c.put(3, new TestClass(new TestClass1[] {new TestClass1() {}}));

        TestClass val1 = c.get(1);
        TestClass val2 = c.get(2);
        TestClass val3 = c.get(3);

        assertEquals(2, val1.getArr().length);
        assertEquals(0, val2.getArr().length);
        assertEquals(1, val3.getArr().length);

        assertTrue(c.remove(1));
        assertTrue(c.remove(2));
        assertTrue(c.remove(3));

    }

    /** */
    @Test
    public void testBinaryModeArray() {
        doTestBinaryModeArray(srvCache1);
        doTestBinaryModeArray(cliCache1);
    }

    /** */
    private void doTestBinaryModeArray(IgniteCache<Integer, TestClass1[]> c) {
        c.put(1, new TestClass1[] {new TestClass1(), new TestClass1()});
        Object obj = c.withKeepBinary().get(1);

        assertEquals(useTypedArrays ? BinaryArray.class : Object[].class, obj.getClass());

        if (useTypedArrays)
            assertEquals(TestClass1[].class, ((BinaryObject)obj).deserialize().getClass());

        assertTrue(c.remove(1));
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
