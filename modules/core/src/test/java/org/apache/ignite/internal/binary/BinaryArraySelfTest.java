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
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class BinaryArraySelfTest extends GridCommonAbstractTest {
    /** */
    private static Ignite server;

    /** */
    private static Ignite client;

    /** */
    private static IgniteCache<Integer, TestClass1[]> srvCache0;

    /** */
    private static IgniteCache<Integer, TestClass1[]> cliCache0;

    /** */
    private static IgniteCache<TestClass1[], Integer> srvCache1;

    /** */
    private static IgniteCache<TestClass1[], Integer> cliCache1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        server = startGrid(0);
        client = startClientGrid(1);

        srvCache0 = server.createCache("my-cache");
        cliCache0 = client.getOrCreateCache("my-cache");
        srvCache1 = server.createCache("my-cache-1");
        cliCache1 = client.getOrCreateCache("my-cache-1");
    }

    /** */
    @Test
    public void testArrayKey() {
        doTestArrayKey(srvCache1);
        doTestArrayKey(cliCache1);
    }

    /** */
    @Test
    public void testArrayValue() {
        doTestArrayValue(srvCache0);
        doTestArrayValue(cliCache0);
    }

    /** */
    private void doTestArrayKey(IgniteCache<TestClass1[], Integer> c) {
        TestClass1[] arr = {new TestClass1(), new TestClass1()};
        TestClass1[] emptyArr = {};

        c.put(arr, 1);
        c.put(emptyArr, 2);

        assertEquals((Integer)1, c.get(arr));
        assertEquals((Integer)2, c.get(emptyArr));

        assertTrue(c.remove(arr));
        assertTrue(c.remove(emptyArr));
    }

    /** */
    private void doTestArrayValue(IgniteCache<Integer, TestClass1[]> c) {
        c.put(1, new TestClass1[] {new TestClass1(), new TestClass1()});
        c.put(2, new TestClass1[] {});
        TestClass1[] obj = c.get(1);
        TestClass1[] emptyObj = c.get(2);

        assertEquals(TestClass1[].class, obj.getClass());
        assertEquals(TestClass1[].class, emptyObj.getClass());

        assertTrue(c.remove(1));
        assertTrue(c.remove(2));
    }

    /** */
    @Test
    public void testBinaryModeArray() {
        doTestBinaryModeArray(srvCache0);
        doTestBinaryModeArray(cliCache0);
    }

    /** */
    private void doTestBinaryModeArray(IgniteCache<Integer, TestClass1[]> c) {
        c.put(1, new TestClass1[] {new TestClass1(), new TestClass1()});
        Object obj = c.withKeepBinary().get(1);

        assertEquals(BinaryArray.class, obj.getClass());
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

        BinaryObject obj = server.binary().toBinary(arr);

        Object deser = obj.deserialize();

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

        BinaryObject obj = server.binary().toBinary(arr);

        assertEquals(BinaryArray.class, obj.getClass());

        Object deser = obj.deserialize();

        assertEquals(TestClass1[].class, deser.getClass());
        assertEquals(2, ((TestClass1[])deser).length);
    }

    /** */
    @Test
    public void testBinaryArrayFieldSerDe() {
        TestClass1 src = new TestClass1();

        BinaryMarshallerSelfTest.SimpleObject sobj = GridTestUtils.getFieldValue(src, "obj");

        GridTestUtils.setFieldValue(sobj, "objArr", new Object[] {"string", 1L, null});

        BinaryObject obj = server.binary().toBinary(src);

        BinaryObject simpleObj = obj.field("obj");
        BinaryObject objArr = simpleObj.field("objArr");

        assertEquals(BinaryArray.class, objArr.getClass());

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
}
