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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.store.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;

import javax.cache.*;
import javax.cache.integration.*;
import javax.cache.processor.*;
import java.io.*;
import java.lang.ref.*;
import java.util.*;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
public abstract class IgniteCacheStoreValueAbstractTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        if (ccfg.getCacheMode() != CacheMode.LOCAL)
            assertEquals(1, ccfg.getBackups());

        assertTrue(ccfg.isCopyOnRead());

        assertEquals(FULL_SYNC, ccfg.getWriteSynchronizationMode());

        ccfg.setCacheStoreFactory(singletonFactory(new CacheStoreAdapter() {
            @Override public void loadCache(IgniteBiInClosure clo, Object... args) {
                clo.apply(new TestKey(100_000), new TestValue(30_000));
            }

            @Override public Object load(Object key) throws CacheLoaderException {
                return null;
            }

            @Override public void write(Cache.Entry entry) throws CacheWriterException {
                // No-op.
            }

            @Override public void delete(Object key) throws CacheWriterException {
                // No-op.
            }
        }));

        ccfg.setInterceptor(new TestInterceptor());

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean swapEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testValueNotStored() throws Exception {
        IgniteCache<TestKey, TestValue> cache = grid(0).cache(null);

        Affinity<Object> aff = grid(0).affinity(null);

        final List<WeakReference<Object>> refs = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            TestKey key = new TestKey(i);
            TestValue val = new TestValue(i);

            refs.add(new WeakReference<Object>(val));

            cache.put(key, val);

            checkNoValue(aff, key);

            for (int g = 0; g < gridCount(); g++)
                assertNotNull(grid(g).cache(null).get(key));

            checkNoValue(aff, key);

            cache.invoke(key, new CacheEntryProcessor<TestKey, TestValue, Object>() {
                @Override public Object process(MutableEntry<TestKey, TestValue> entry, Object... args) {
                    assertNotNull(entry.getValue());

                    entry.setValue(new TestValue(10_000));

                    return new TestValue(20_000);
                }
            });

            checkNoValue(aff, key);

            for (int g = 0; g < gridCount(); g++)
                assertNotNull(grid(g).cache(null).get(key));

            checkNoValue(aff, key);

            cache.remove(key);

            for (int g = 0; g < gridCount(); g++)
                assertNull(grid(g).cache(null).get(key));

            try (IgniteDataStreamer<TestKey, TestValue> streamer  = grid(0).dataStreamer(null)) {
                streamer.addData(key, val);
            }

            checkNoValue(aff, key);

            cache.remove(key);

            atomicClockModeDelay(cache);

            try (IgniteDataStreamer<TestKey, TestValue> streamer  = grid(0).dataStreamer(null)) {
                streamer.allowOverwrite(true);

                streamer.addData(key, val);
            }

            checkNoValue(aff, key);

            if (aff.isPrimaryOrBackup(grid(0).localNode(), key)) {
                cache.localEvict(Collections.singleton(key));

                assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

                cache.localPromote(Collections.singleton(key));

                assertNotNull(cache.localPeek(key, CachePeekMode.ONHEAP));

                checkNoValue(aff, key);
            }
        }

        cache.loadCache(null); // Should load TestKey(100_000).

        TestKey key = new TestKey(100_000);

        checkNoValue(aff, key);

        for (int g = 0; g < gridCount(); g++)
            assertNotNull(grid(g).cache(null).get(key));

        checkNoValue(aff, key);

        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                System.gc();

                boolean pass = true;

                for (Iterator<WeakReference<Object>> it = refs.iterator(); it.hasNext();) {
                    WeakReference<Object> ref = it.next();

                    if (ref.get() == null)
                        it.remove();
                    else {
                        pass = false;

                        log.info("Not collected value: " + ref.get());
                    }
                }

                return pass;
            }
        }, 60_000);

        assertTrue("Failed to wait for when values are collected", wait);
    }

    /**
     * @param aff Affinity.
     * @param key Key.
     */
    private void checkNoValue(Affinity<Object> aff, Object key) {
        for (int g = 0; g < gridCount(); g++) {
            GridCacheAdapter cache0 = internalCache(grid(g), null);

            GridCacheEntryEx e = cache0.peekEx(key);

            if (e == null && cache0.isNear())
                e = ((GridNearCacheAdapter)cache0).dht().peekEx(key);

            if (e != null) {
                CacheObject obj = e.rawGet();

                if (obj != null) {
                    assertNotNull(obj);
                    assertEquals(CacheObjectImpl.class, obj.getClass());

                    assertNull("Unexpected value, node: " + g,
                        GridTestUtils.getFieldValue(obj, CacheObjectAdapter.class, "val"));

                    assertNotNull(obj.value(cache0.context().cacheObjectContext(), true));

                    assertNull("Unexpected value after value() requested1: " + g,
                        GridTestUtils.getFieldValue(obj, CacheObjectAdapter.class, "val"));

                    assertNotNull(obj.value(cache0.context().cacheObjectContext(), false));

                    assertNull("Unexpected value after value() requested2: " + g,
                        GridTestUtils.getFieldValue(obj, CacheObjectAdapter.class, "val"));
                }
                else
                    assertFalse(aff.isPrimaryOrBackup(grid(g).localNode(), key));
            }
            else
                assertFalse("Entry not found, node: " + g, aff.isPrimaryOrBackup(grid(g).localNode(), key));
        }
    }

    /**
     *
     */
    static class TestKey implements Serializable {
        /** */
        private int key;

        /**
         * @param key Key.
         */
        public TestKey(int key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            TestKey that = (TestKey)o;

            return key == that.key;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }

    /**
     *
     */
    static class TestValue implements Serializable {
        /** */
        private int val;

        /**
         *
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        public String toString() {
            return "TestValue [val=" + val + ']';
        }
    }

    /**
     *
     */
    static class TestInterceptor extends CacheInterceptorAdapter {
        // No-op.
    }
}
