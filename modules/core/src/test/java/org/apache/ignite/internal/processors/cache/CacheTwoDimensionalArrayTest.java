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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.junit.Assert.assertArrayEquals;

/**
 *
 */
public class CacheTwoDimensionalArrayTest extends GridCommonAbstractTest {
    /** */
    private static int NODES = 3;

    /** */
    private static int KEYS = 100;

    /** */
    private static int size = 5;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimpleModel() throws Exception {
        doTestSimpleModel(ATOMIC, PARTITIONED);
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param cacheMode Cache mode.
     *
     * @throws Exception If failed.
     */
    private void doTestSimpleModel(CacheAtomicityMode atomicityMode, CacheMode cacheMode) throws Exception {
        CacheConfiguration ccfg = getConfiguration(atomicityMode, cacheMode);

        ignite(0).getOrCreateCache(ccfg);

        int n = size, m = size -1;

        // Object array with primitives.
        {
            IgniteCache<Integer, Object[]> cache = ignite(0).cache(ccfg.getName());

            for (int key = 0; key < KEYS; key++)
                cache.put(key, new Object[]{1});

            for (int key = 0; key < KEYS; key++) {
                Object[] exp = new Object[]{1};

                Object[] act = cache.get(key);

                assertArrayEquals(exp, act);
            }

            cache.removeAll();
        }

        // Primitive empty array.
        {
            IgniteCache<Integer, int[][]> cache = ignite(0).cache(ccfg.getName());

            for (int key = 0; key < KEYS; key++)
                cache.put(key, new int[n][m]);

            for (int key = 0; key < KEYS; key++) {
                int[][] exp = new int[n][m];

                int[][] act = cache.get(key);

                assertArrayEquals(exp, act);
            }

            cache.removeAll();
        }

        // Object empty array.
        {
            IgniteCache<Integer, Object[][]> cache = ignite(0).cache(ccfg.getName());

            for (int key = 0; key < KEYS; key++)
                cache.put(key, new Object[n][m]);

            for (int key = 0; key < KEYS; key++) {
                Object[][] exp = new Object[n][m];

                Object[][] act = cache.get(key);

                assertArrayEquals(exp, act);
            }

            cache.removeAll();
        }

        {
            IgniteCache<Integer, int[][]> cache = ignite(0).cache(ccfg.getName());

            for (int key = 0; key < KEYS; key++)
                cache.put(key, intArray(n, m, key));

            for (int key = 0; key < KEYS; key++) {
                int[][] exp = intArray(n, m, key);

                int[][] act = cache.get(key);

                assertArrayEquals(exp, act);
            }

            cache.removeAll();
        }

        {
            IgniteCache<Integer, int[][][]> cache = ignite(0).cache(ccfg.getName());

            for (int key = 0; key < KEYS; key++)
                cache.put(key, new int[5][6][7]);

            for (int key = 0; key < KEYS; key++) {
                int[][][] exp = new int[5][6][7];

                int[][][] act = cache.get(key);

                assertArrayEquals(exp, act);
            }

            cache.removeAll();
        }

        {
            IgniteCache<Integer, Object[][]> cache = ignite(0).cache(ccfg.getName());

            for (int key = 0; key < KEYS; key++)
                cache.put(key, objectArray(n, m, key));

            for (int key = 0; key < KEYS; key++) {
                Object[][] exp = objectArray(n, m, key);

                Object[][] act = cache.get(key);

                assertArrayEquals(exp, act);
            }

            cache.removeAll();
        }

        {
            IgniteCache<Integer, TestObject[][]> cache = ignite(0).cache(ccfg.getName());

            for (int key = 0; key < KEYS; key++)
                cache.put(key, testObjectArray(n, m, key));

            for (int key = 0; key < KEYS; key++) {
                TestObject[][] exp = testObjectArray(n, m, key);

                TestObject[][] act = cache.get(key);

                assertArrayEquals(exp, act);
            }

            cache.removeAll();
        }

        {
            IgniteCache<Integer, TestObject[][]> cache = ignite(0).cache(ccfg.getName());

            for (int key = 0; key < KEYS; key++)
                cache.put(key, testObjectArray(n, m, key));

            for (int key = 0; key < KEYS; key++) {
                TestObject[][] exp = testObjectArray(n, m, key);

                TestObject[][] act = cache.get(key);

                assertArrayEquals(exp, act);
            }

            cache.removeAll();
        }
    }

    /**
     * @return Array.
     */
    private int[][] intArray(int n, int m,int K) {
        int[][] arr = new int[n][m];

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++)
                arr[i][j] = (i + j) * K;
        }

        return arr;
    }

    /**
     * @return Array.
     */
    private Object[][] objectArray(int n, int m, int K) {
        Object[][] arr = new Object[n][m];

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++)
                arr[i][j] = ((n + m) % 2 == 0) ? (i + j) * K : new TestObject((i + j) * K);
        }

        return arr;
    }

    /**
     * @return Array.
     */
    private TestObject[][] testObjectArray(int n, int m, int K) {
        TestObject[][] arr = new TestObject[n][m];

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < m; j++)
                arr[i][j] = new TestObject((i + j) * K);
        }

        return arr;
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @param cacheMode Cache mode.
     *
     * @return Cache configuration.
     */
    @NotNull private CacheConfiguration getConfiguration(CacheAtomicityMode atomicityMode,
        CacheMode cacheMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     *
     */
    private static class TestObject {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public TestObject(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestObject object = (TestObject)o;

            return val == object.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }
}
