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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.binary.BinaryMarshallerSelfTest.TestClass1;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectContainer;
import org.apache.ignite.internal.processors.cache.CacheEnumOperationsAbstractTest.TestEnum;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheEnumOperationsAbstractTest.TestEnum.VAL1;
import static org.apache.ignite.internal.processors.cache.CacheEnumOperationsAbstractTest.TestEnum.VAL2;
import static org.apache.ignite.internal.processors.cache.CacheEnumOperationsAbstractTest.TestEnum.VAL3;
import static org.junit.Assert.assertArrayEquals;

/** */
public class BinaryArraySelfTest extends AbstractBinaryArraysTest {
    /** */
    private static Ignite server;

    /** */
    private static Ignite client;

    /** */
    private static CacheAdapter<Object, Object> srvCache;

    /** */
    private static CacheAdapter<Object, Object> cliCache;

    /** */
    private static final Function<Object, Object> TO_TEST_CLS = arr -> arr instanceof TestClass1[][]
        ? new TestClass2(null, (TestClass1[][])arr)
        : new TestClass2((TestClass1[])arr, null);

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        server = startGrid(0);
        client = startClientGrid(1);

        srvCache = new IgniteCacheAdapter<>(server.createCache(DEFAULT_CACHE_NAME));
        cliCache = new IgniteCacheAdapter<>(client.getOrCreateCache(DEFAULT_CACHE_NAME));
    }

    /** */
    @Test
    public void testArrayKey() {
        doTestKeys(srvCache, arr -> arr);
        doTestKeys(cliCache, arr -> arr);
        try (IgniteClient thinClient = thinClient()) {
            // Using other cache because
            // 1. Removed entry store in `GridCacheAdapter#map`
            // 2. Bytes representation of multidimensional BinaryArray from thin client and Ignite node differ.
            // 2a. Thin client reads array from byte stream and preserve pointer equality (look at #dataToTest() -> arr6).
            // 2b. Client node invoke `CacheObjectBinaryProcessorImpl#marshallToBinary` before storing
            // which breaks link equality of array
            // 3. During invocation of `put` node inserts key representation from previous invocation of methods from client node.
            // 4. This lead to `ClientCache#containsKey` can't find key because
            // it invokes search based equality on `byte[]` key representaion.
            //
            // This doesn't happen in `useBinaryArrays=false` becuase Ignite node obtain `Object[]`
            // from byte stream and invoke marshallToBinary for it which also breaks pointer equality
            // therefore during serialization handle will NOT be used.
            // In `useBinaryArrays=true` node read `BinaryArray` from stream which mean no need to marshall to binary
            // therefore link equality preserved which mean during serialization handle will be used.
            doTestKeys(
                new ClientCacheAdapter<>(thinClient.getOrCreateCache(DEFAULT_CACHE_NAME + (useBinaryArrays ? "2" : ""))),
                arr -> arr
            );
        }
    }

    /** */
    @Test
    public void testArrayFieldInKey() {
        doTestKeys(srvCache, TO_TEST_CLS);
        doTestKeys(cliCache, TO_TEST_CLS);
        try (IgniteClient thinClient = thinClient()) {
            doTestKeys(new ClientCacheAdapter<>(thinClient.getOrCreateCache(DEFAULT_CACHE_NAME)), TO_TEST_CLS);
        }
    }

    /** */
    @Test
    public void testArrayValue() {
        doTestValue(srvCache, arr -> arr, false, false);
        doTestValue(cliCache, arr -> arr, false, false);
        doTestValue(srvCache, arr -> arr, true, false);
        doTestValue(cliCache, arr -> arr, true, false);

        try (IgniteClient thinClient = thinClient()) {
            ClientCacheAdapter<Object, Object> c =
                new ClientCacheAdapter<>(thinClient.getOrCreateCache(DEFAULT_CACHE_NAME));

            doTestValue(c, arr -> arr, false, false);
            doTestValue(c, arr -> arr, true, false);
        }
    }

    /** */
    @Test
    public void testArrayFieldInValue() {
        doTestValue(srvCache, TO_TEST_CLS, false, true);
        doTestValue(cliCache, TO_TEST_CLS, false, true);
        doTestValue(srvCache, TO_TEST_CLS, true, true);
        doTestValue(cliCache, TO_TEST_CLS, true, true);
        try (IgniteClient thinClient = thinClient()) {
            ClientCacheAdapter<Object, Object> c =
                new ClientCacheAdapter<>(thinClient.getOrCreateCache(DEFAULT_CACHE_NAME));

            doTestValue(c, TO_TEST_CLS, false, true);
            doTestValue(c, TO_TEST_CLS, true, true);
        }
    }

    /** */
    @Test
    public void testArraySerDe() {
        checkArraySerDe(arr -> {
            Object obj = server.binary().toBinary(arr);

            return useBinaryArrays
                ? ((BinaryObject)obj).deserialize()
                : PlatformUtils.unwrapBinariesInArray((Object[])obj);
        }, false);
    }

    /** */
    @Test
    public void testArrayFieldSerDe() {
        checkArraySerDe(arr -> {
            BinaryObject bObj = server.binary().toBinary(new TestObjectContainer(arr));

            return ((TestObjectContainer)bObj.deserialize()).foo;
        }, true);
    }

    /** */
    @Test
    public void testBinaryModeArray() {
        putInBinaryGetRegular(srvCache);
        putInBinaryGetRegular(cliCache);
        try (IgniteClient thinClient = thinClient()) {
            putInBinaryGetRegular(new ClientCacheAdapter<>(thinClient.getOrCreateCache(DEFAULT_CACHE_NAME)));
        }
    }

    /** */
    @Test
    public void testPrimitivesArrays() {
        doTestBoxedPrimitivesArrays(srvCache);
        doTestBoxedPrimitivesArrays(cliCache);

        doTestPrimitivesArrays(srvCache);
        doTestPrimitivesArrays(cliCache);

        try (IgniteClient thinClient = thinClient()) {
            ClientCacheAdapter<Object, Object> c = new
                ClientCacheAdapter<>(thinClient.getOrCreateCache(DEFAULT_CACHE_NAME));

            doTestBoxedPrimitivesArrays(c);
            doTestPrimitivesArrays(c);
        }
    }

    /** */
    @Test
    public void testSimpleBinaryArrayFieldSerDe() {
        TestClass1 src = new TestClass1();

        BinaryMarshallerSelfTest.SimpleObject sobj = GridTestUtils.getFieldValue(src, "obj");

        GridTestUtils.setFieldValue(sobj, "objArr", new Object[] {"string", 1L, null});

        BinaryObject obj = server.binary().toBinary(src);

        BinaryObject simpleObj = obj.field("obj");
        Object objArr = simpleObj.field("objArr");

        assertEquals(useBinaryArrays ? BinaryArray.class : Object[].class, objArr.getClass());

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
    @Test
    public void testArrayOfCollectionSerDe() {
        List<TestClass1> l1 = new ArrayList<>(F.asList(new TestClass1(), new TestClass1()));
        List<TestClass1> l2 = new ArrayList<>(F.asList(new TestClass1(), new TestClass1()));
        List<TestClass1> l3 = new ArrayList<>(F.asList(new TestClass1(), new TestClass1()));

        List[] arr = new List[] { l1, l2, l3 };

        Object res = server.binary().toBinary(arr);
        Object[] res0;

        if (useBinaryArrays) {
            assertTrue(BinaryUtils.isBinaryArray(res));

            res0 = ((BinaryArray)res).deserialize();
        }
        else {
            assertTrue(res instanceof Object[]);

            res0 = PlatformUtils.unwrapBinariesInArray((Object[])res);
        }

        assertEquals(arr.length, res0.length);

        for (int i = 0; i < arr.length; i++)
            assertEqualsCollections(arr[i], (Collection<?>)res0[i]);
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

        if (useBinaryArrays) {
            assertTrue(BinaryUtils.isBinaryArray(obj));

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
    private void doTestKeys(CacheAdapter<Object, Object> c, Function<Object, Object> wrap) {
        List<?> keys = dataToTest();

        for (int i = 0; i < keys.size(); i++) {
            Object key = wrap.apply(keys.get(i));

            assertFalse(c.containsKey(key));

            c.put(key, i);

            assertTrue(c.containsKey(key));
            assertEquals(i, c.get(key));
            assertTrue(c.replace(key, i, i + 1));
            assertEquals(i + 1, c.get(key));
            assertTrue(c.remove(key));
        }
    }

    /** */
    private void doTestValue(
        CacheAdapter<Object, Object> c,
        Function<Object, Object> wrap,
        boolean keepBinary,
        boolean alwaysSameType
    ) {
        AtomicInteger cntr = new AtomicInteger();

        checkArraySerDe(arr -> {
            c.put(cntr.getAndIncrement(), wrap.apply(arr));

            Object obj;

            if (keepBinary) {
                obj = c.withKeepBinary().get(cntr.get() - 1);

                if (obj instanceof BinaryObject)
                    obj = ((BinaryObject)obj).deserialize();
                else
                    obj = PlatformUtils.unwrapBinariesInArray((Object[])obj);
            }
            else
                obj = c.get(cntr.get() - 1);

            if (obj instanceof Object[])
                return obj;

            TestClass2 cached = (TestClass2)obj;

            return cached.arr != null ? cached.arr : cached.arr2;
        }, alwaysSameType);

        List<?> vals = dataToTest();

        for (int i = 0; i < vals.size(); i++)
            assertTrue(c.replace(i, wrap.apply(vals.get(i)), wrap.apply(vals.get((i + 1) % vals.size()))));

        for (int i = 0; i < cntr.get(); i++)
            assertTrue(c.remove(i));
    }

    /** */
    private List<?> dataToTest() {
        TestClass1[][] arr5 = new TestClass1[3][2];
        TestClass1[][] arr6 = new TestClass1[2][2];

        arr5[0] = new TestClass1[] {new TestClass1()};
        arr5[1] = new TestClass1[] {new TestClass1()};

        // arr6[0] == arr6[1]
        arr6[0] = new TestClass1[] {new TestClass1()};
        arr6[1] = arr6[0];

        return F.asList(
            new TestClass1[0],
            new TestClass1[1][2],
            new TestClass1[] {new TestClass1(), new TestClass1()},
            arr5,
            arr6
        );
    }

    /** */
    private void putRegularGetInBinary(IgniteCache<Object, Object> c) {
        List<?> vals = dataToTest();

        for (Object val : vals) {
            c.put(1, val);

            Object obj = c.withKeepBinary().get(1);

            assertEquals(useBinaryArrays ? BinaryArray.class : Object[].class, obj.getClass());

            if (useBinaryArrays)
                assertEquals(val.getClass(), ((BinaryObject)obj).deserialize().getClass());

            assertTrue(c.remove(1));
        }
    }

    /** */
    private void putInBinaryGetRegular(CacheAdapter<Object, Object> c) {
        Runnable checker = () -> {
            Object[] arr = (Object[])c.get(1);

            assertTrue(arr[0] instanceof TestClass1);
            assertTrue(arr[1] instanceof TestClass1);

            assertTrue(c.withKeepBinary().remove(1));

            assertNull(c.withKeepBinary().get(1));
            assertNull(c.get(1));
        };

        {
            BinaryObject[] src = new BinaryObject[] {
                server.binary().toBinary(new TestClass1()),
                server.binary().toBinary(new TestClass1())
            };

            c.withKeepBinary().put(1, src);

            checker.run();
        }

        {
            Object src = server.binary().toBinary(
                new Object[] {
                    server.binary().toBinary(new TestClass1()),
                    server.binary().toBinary(new TestClass1())
                }
            );

            assertEquals(useBinaryArrays ? BinaryArray.class : Object[].class, src.getClass());

            c.withKeepBinary().put(1, src);

            checker.run();
        }
    }

    /** */
    private void doTestPrimitivesArrays(CacheAdapter<Object, Object> c) {
        Object[] data = new Object[] {
            new byte[] {1, 2, 3},
            new short[] {1, 2, 3},
            new int[] {1, 2, 3},
            new long[] {1L, 2L, 3L},
            new float[] {1f, 2f, 3f},
            new double[] {1d, 2d, 3d},
            new char[] {'a', 'b', 'c'},
            new boolean[] {true, false}
        };

        for (Object item : data) {
            c.put(1, item);

            Object item0 = c.get(1);

            assertTrue(c.replace(1, item, item));
            assertTrue(c.remove(1));
            assertEquals(item.getClass(), item0.getClass());
            assertEqualsArraysAware(item, item0);

            c.put(item, 1);

            assertTrue(c.containsKey(item));
            assertEquals(1, c.get(item));
            assertTrue(c.replace(item, 1, 2));
            assertTrue(c.remove(item));
        }
    }

    /** */
    private void doTestBoxedPrimitivesArrays(CacheAdapter<Object, Object> c) {
        Object[] data = new Object[] {
            new Byte[] {1, 2, 3},
            new Short[] {1, 2, 3},
            new Integer[] {1, 2, 3},
            new Long[] {1L, 2L, 3L},
            new Float[] {1f, 2f, 3f},
            new Double[] {1d, 2d, 3d},
            new Character[] {'a', 'b', 'c'},
            new Boolean[] {true, false},
            new TestEnum[] {VAL1, VAL2, VAL3}
        };

        for (Object item : data) {
            c.put(1, item);

            Object item0 = c.get(1);

            assertTrue(c.replace(1, item, item));
            assertTrue(c.remove(1));

            if (useBinaryArrays)
                assertEquals(item.getClass(), item0.getClass());

            assertTrue(Arrays.equals((Object[])item, (Object[])item0));

            c.put(item, 1);

            assertTrue(c.containsKey(item));
            assertEquals(1, c.get(item));
            assertTrue(c.replace(item, 1, 2));
            assertTrue(c.remove(item));

        }
    }

    /** */
    public void checkArraySerDe(Function<Object[], Object> serde, boolean sameArr) {
        for (Object[] arr : (List<Object[]>)dataToTest()) {
            Object deser = serde.apply(arr);

            assertEquals((useBinaryArrays || sameArr) ? arr.getClass() : Object[].class, deser.getClass());

            assertEquals(arr.length, ((Object[])deser).length);
            assertArrayEquals(arr, ((Object[])deser));

            if (arr instanceof TestClass1[][] && arr.length == 2 && sameArr) {
                Object[] val = (Object[])deser;

                // See dataToTest -> dataToTest -> arr6[0] == arr6[1]
                // Check the sanity of test data.
                assertSame(((TestClass1[][])arr)[0], ((TestClass1[][])arr)[1]);
                assertSame(val[0], val[1]);
            }
        }
    }

    /** */
    private IgniteClient thinClient() {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses(Config.SERVER)
            .setBinaryConfiguration(new BinaryConfiguration().setCompactFooter(true))
        );
    }

    /** */
    public static class TestClass2 {
        /** */
        final TestClass1[] arr;

        /** */
        final TestClass1[][] arr2;

        /** */
        public TestClass2(TestClass1[] arr, TestClass1[][] arr2) {
            this.arr = arr;
            this.arr2 = arr2;
        }
    }

    /** */
    public interface CacheAdapter<K, V> {
        /** */
        public V get(K key);

        /** */
        public void put(K key, V val);

        /** */
        public boolean replace(K key, V oldVal, V newVal);

        /** */
        public boolean remove(K key);

        /** */
        public boolean containsKey(K key);

        /** */
        public <K1, V1> CacheAdapter<K1, V1> withKeepBinary();
    }

    /** */
    private static class IgniteCacheAdapter<K, V> implements CacheAdapter<K, V> {
        /** */
        private final IgniteCache<K, V> c;

        /** */
        public IgniteCacheAdapter(IgniteCache<K, V> c) {
            this.c = c;
        }

        /** {@inheritDoc} */
        @Override public V get(K key) {
            return c.get(key);
        }

        /** {@inheritDoc} */
        @Override public void put(K key, V val) {
            c.put(key, val);

        }

        /** {@inheritDoc} */
        @Override public boolean replace(K key, V oldVal, V newVal) {
            return c.replace(key, oldVal, newVal);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(K key) {
            return c.remove(key);
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(K key) {
            return c.containsKey(key);
        }

        /** {@inheritDoc} */
        @Override public <K1, V1> CacheAdapter<K1, V1> withKeepBinary() {
            return new IgniteCacheAdapter<>(c.withKeepBinary());
        }
    }

    /** */
    private static class ClientCacheAdapter<K, V> implements CacheAdapter<K, V> {
        /** */
        private final ClientCache<K, V> c;

        /** */
        public ClientCacheAdapter(ClientCache<K, V> c) {
            this.c = c;
        }

        /** {@inheritDoc} */
        @Override public V get(K key) {
            return c.get(key);
        }

        /** {@inheritDoc} */
        @Override public void put(K key, V val) {
            c.put(key, val);

        }

        /** {@inheritDoc} */
        @Override public boolean replace(K key, V oldVal, V newVal) {
            return c.replace(key, oldVal, newVal);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(K key) {
            return c.remove(key);
        }

        /** {@inheritDoc} */
        @Override public boolean containsKey(K key) {
            return c.containsKey(key);
        }

        /** {@inheritDoc} */
        @Override public <K1, V1> CacheAdapter<K1, V1> withKeepBinary() {
            return new ClientCacheAdapter<>(c.withKeepBinary());
        }
    }
}
