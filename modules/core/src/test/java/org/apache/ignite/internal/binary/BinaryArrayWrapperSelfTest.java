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
public class BinaryArrayWrapperSelfTest extends GridCommonAbstractTest {
    /** */
    private static Ignite server;

    /** */
    private static Ignite client;

    /** */
    private static IgniteCache<Integer, TestClass1[]> srvCache;

    /** */
    private static IgniteCache<Integer, TestClass1[]> cliCache;

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
    public void testArray() throws Exception {
        doTestArray(srvCache);
        doTestArray(cliCache);

    }

    /** */
    private void doTestArray(IgniteCache<Integer, TestClass1[]> c) {
        c.put(1, new TestClass1[] {new TestClass1(), new TestClass1()});
        TestClass1[] obj = c.get(1);

        assertEquals(TestClass1[].class, obj.getClass());

        assertTrue(c.remove(1));
    }

    /** */
    @Test
    public void testBinaryModeArray() {
        doTestBinaryModeArray(srvCache);
        doTestBinaryModeArray(cliCache);
    }

    /** */
    private void doTestBinaryModeArray(IgniteCache<Integer, TestClass1[]> c) {
        c.put(1, new TestClass1[] {new TestClass1(), new TestClass1()});
        Object obj = c.withKeepBinary().get(1);

        assertEquals(BinaryArrayWrapper.class, obj.getClass());
        assertEquals(TestClass1[].class, ((BinaryObject)obj).deserialize().getClass());

        assertTrue(c.remove(1));
    }

    /** */
    @Test
    public void testBinaryArraySerDe() {
        TestClass1[] arr = {new TestClass1(), new TestClass1()};

        BinaryObject obj = server.binary().toBinary(arr);

        assertEquals(BinaryArrayWrapper.class, obj.getClass());

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

        assertEquals(BinaryArrayWrapper.class, objArr.getClass());

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
