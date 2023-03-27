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

import java.io.Serializable;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheObjectPutSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String CACHE_NAME = "partitioned";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setOnheapCacheEnabled(true);

        cfg.setCacheConfiguration(ccfg);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPrimitiveValues() throws Exception {
        IgniteEx ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, String.valueOf(i));

        IgniteCacheObjectProcessor co = ignite.context().cacheObjects();
        GridCacheAdapter<Object, Object> iCache = ignite.context().cache().internalCache(CACHE_NAME);
        GridCacheContext<Object, Object> cacheCtx = iCache.context();
        CacheObjectContext coCtx = cacheCtx.cacheObjectContext();

        ByteBuffer buf = ByteBuffer.allocate(2048);

        for (int i = 0; i < 10; i++) {
            KeyCacheObject key = co.toCacheKeyObject(coCtx, cacheCtx, i, false);

            GridCacheEntryEx entry = iCache.peekEx(key);

            assertNotNull(entry);

            assertTrue(entry.key().putValue(buf));
            assertTrue(entry.valueBytes().putValue(buf));
        }

        buf.flip();

        for (int i = 0; i < 10; i++) {
            CacheObject co1 = co.toCacheObject(coCtx, buf);

            assertEquals((Integer)i, co1.value(coCtx, false));

            CacheObject co2 = co.toCacheObject(coCtx, buf);

            assertEquals(String.valueOf(i), co2.value(coCtx, false));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClassValues() throws Exception {
        IgniteEx ignite = grid(0);

        IgniteCache<Object, Object> cache = ignite.cache(CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(new TestValue(i), new TestValue(i));

        IgniteCacheObjectProcessor co = ignite.context().cacheObjects();
        GridCacheAdapter<Object, Object> iCache = ignite.context().cache().internalCache(CACHE_NAME);
        GridCacheContext<Object, Object> cacheCtx = iCache.context();
        CacheObjectContext coCtx = cacheCtx.cacheObjectContext();

        ByteBuffer buf = ByteBuffer.allocate(2048);

        for (int i = 0; i < 10; i++) {
            KeyCacheObject key = co.toCacheKeyObject(coCtx, cacheCtx, new TestValue(i), false);

            GridCacheEntryEx entry = iCache.peekEx(key);

            assertNotNull(entry);

            assertTrue(entry.key().putValue(buf));
            assertTrue(entry.valueBytes().putValue(buf));
        }

        buf.flip();

        for (int i = 0; i < 10; i++) {
            CacheObject co1 = co.toCacheObject(coCtx, buf);

            assertEquals(new TestValue(i), co1.value(coCtx, false));

            CacheObject co2 = co.toCacheObject(coCtx, buf);

            assertEquals(new TestValue(i), co2.value(coCtx, false));
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        private TestValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof TestValue))
                return false;

            TestValue value = (TestValue)o;

            return val == value.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }
}
