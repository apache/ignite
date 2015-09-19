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

    /** Test input size. */
    private static final int INPUT_SIZE = 100;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheStoreFactory(singletonFactory(new TestStore()));
        cfg.setReadThrough(true);
        cfg.setWriteThrough(true);
        cfg.setLoadPreviousValue(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStore() throws Exception {
        jcache().localLoadCache(null, 1, 2, 3);

        int cnt = 0;

        for (int i = 0; i < gridCount(); i++)
            cnt += jcache(i).localSize();

        assertEquals(INPUT_SIZE - (INPUT_SIZE/10), cnt);
    }

    /**
     *
     */
    private static class TestStore extends CacheLoadOnlyStoreAdapter<Integer, String, String> {
        /** {@inheritDoc} */
        @Override protected Iterator<String> inputIterator(@Nullable Object... args) {
            assertNotNull(args);
            assertTrue(Arrays.equals(EXP_ARGS, args));

            return new Iterator<String>() {
                private int i = -1;

                @Override public boolean hasNext() {
                    return i < INPUT_SIZE;
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
}