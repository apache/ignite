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

package org.apache.ignite.cache.store;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import javax.cache.integration.CacheLoaderException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractSelfTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridCacheLoadOnlyStoreAdapterSelfTest extends GridCacheAbstractSelfTest {
    /** Expected loadAll arguments, hardcoded on call site for convenience. */
    private static final Integer[] EXP_ARGS = {1, 2, 3};

    /** Store to use. */
    private CacheLoadOnlyStoreAdapter store;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {

    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {

    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {

    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {

    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        assertNotNull(store);

        cfg.setCacheStoreFactory(singletonFactory(store));
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setLoadPreviousValue(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStore() throws Exception {
        try {
            int inputSize = 100;

            store = new TestStore(inputSize);

            startGrids(gridCount());

            awaitPartitionMapExchange();

            jcache().localLoadCache(null, 1, 2, 3);

            int cnt = 0;

            for (int i = 0; i < gridCount(); i++)
                cnt += jcache(i).localSize();

            assertEquals(inputSize - (inputSize / 10), cnt);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoreSmallQueueSize() throws Exception {
        try {
            int inputSize = 1500;

            store = new ParallelTestStore(inputSize);

            store.setBatchSize(1);
            store.setBatchQueueSize(1);
            store.setThreadsCount(2);

            startGrids(gridCount());

            awaitPartitionMapExchange();

            jcache().localLoadCache(null, 1, 2, 3);

            int cnt = 0;

            for (int i = 0; i < gridCount(); i++)
                cnt += jcache(i).localSize();

            assertEquals(inputSize, cnt);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheLoadOnlyStoreAdapter<Integer, String, String> {
        /** */
        private final int inputSize;

        /**
         * @param inputSize Input size.
         */
        public TestStore(int inputSize) {
            this.inputSize = inputSize;
        }

        /** {@inheritDoc} */
        @Override protected Iterator<String> inputIterator(@Nullable Object... args) {
            assertNotNull(args);
            assertTrue(Arrays.equals(EXP_ARGS, args));

            return new Iterator<String>() {
                private int i = -1;

                @Override public boolean hasNext() {
                    return i < inputSize;
                }

                @Override public String next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    i++;

                    return i + "=str" + i;
                }

                @Override public void remove() {
                    // No-op.
                }
            };
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Integer, String> parse(String rec, @Nullable Object... args) {
            assertNotNull(args);
            assertTrue(Arrays.equals(EXP_ARGS, args));

            String[] p = rec.split("=");

            int i = Integer.parseInt(p[0]);

            return i % 10 == 0 ? null : new T2<>(i, p[1]);
        }
    }

    /**
     *
     */
    private static class ParallelTestStore extends CacheLoadOnlyStoreAdapter<Integer, String, String> {
        /** */
        private final int inputSize;

        /**
         * @param inputSize Input size.
         */
        public ParallelTestStore(int inputSize) {
            this.inputSize = inputSize;
        }

        /** {@inheritDoc} */
        @Override protected Iterator<String> inputIterator(@Nullable Object... args) throws CacheLoaderException {
            return new Iterator<String>() {
                private int i;

                @Override public boolean hasNext() {
                    return i < inputSize;
                }

                @Override public String next() {
                    if (!hasNext())
                        throw new NoSuchElementException();

                    String res = i + "=str" + i;

                    i++;

                    return res;
                }

                @Override public void remove() {
                    // No-op.
                }
            };
        }

        /** {@inheritDoc} */
        @Nullable @Override protected IgniteBiTuple<Integer, String> parse(String rec, @Nullable Object... args) {
            String[] p = rec.split("=");

            return new T2<>(Integer.parseInt(p[0]), p[1]);
        }
    }
}
