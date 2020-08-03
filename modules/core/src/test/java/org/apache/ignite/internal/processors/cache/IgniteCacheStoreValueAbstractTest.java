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
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import javax.cache.Cache;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class IgniteCacheStoreValueAbstractTest extends IgniteCacheAbstractTest {
    /** */
    private boolean cpyOnRead;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean onheapCacheEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        if (ccfg.getCacheMode() != CacheMode.LOCAL)
            assertEquals(1, ccfg.getBackups());

        assertTrue(ccfg.isCopyOnRead());

        ccfg.setCopyOnRead(cpyOnRead);

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
    @Override protected long getTestTimeout() {
        return 3 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testValueNotStored() throws Exception {
        cpyOnRead = true;

        startGrids();

        IgniteCache<TestKey, TestValue> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Affinity<Object> aff = grid(0).affinity(DEFAULT_CACHE_NAME);

        final List<WeakReference<Object>> refs = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            TestKey key = new TestKey(i);
            TestValue val = new TestValue(i);

            refs.add(new WeakReference<Object>(val));

            cache.put(key, val);

            checkNoValue(aff, key);

            for (int g = 0; g < gridCount(); g++)
                assertNotNull(grid(g).cache(DEFAULT_CACHE_NAME).get(key));

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
                assertNotNull(grid(g).cache(DEFAULT_CACHE_NAME).get(key));

            checkNoValue(aff, key);

            cache.remove(key);

            for (int g = 0; g < gridCount(); g++)
                assertNull(grid(g).cache(DEFAULT_CACHE_NAME).get(key));

            try (IgniteDataStreamer<TestKey, TestValue> streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
                streamer.addData(key, val);
            }

            checkNoValue(aff, key);

            cache.remove(key);

            try (IgniteDataStreamer<TestKey, TestValue> streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
                streamer.allowOverwrite(true);

                streamer.addData(key, val);
            }

            checkNoValue(aff, key);

            if (aff.isPrimaryOrBackup(grid(0).localNode(), key)) {
                cache.localEvict(Collections.singleton(key));

                assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

                cache.get(key);

                assertNotNull(cache.localPeek(key, CachePeekMode.ONHEAP));

                checkNoValue(aff, key);
            }
        }

        cache.loadCache(null); // Should load TestKey(100_000).

        TestKey key = new TestKey(100_000);

        checkNoValue(aff, key);

        for (int g = 0; g < gridCount(); g++)
            assertNotNull(grid(g).cache(DEFAULT_CACHE_NAME).get(key));

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
            IgniteEx ig = grid(g);

            GridCacheAdapter cache0 = internalCache(ig, DEFAULT_CACHE_NAME);

            GridCacheEntryEx e = cache0.peekEx(key);

            if (e == null && cache0.isNear())
                e = ((GridNearCacheAdapter)cache0).dht().peekEx(key);

            if (e != null) {
                KeyCacheObject keyObj = e.key();

                assertNotNull(keyObj);

                if (!isBinaryMarshallerUsed(ig)) {
                    assertNotNull("Unexpected value, node: " + g,
                        GridTestUtils.getFieldValue(keyObj, CacheObjectAdapter.class, "val"));

                    Object key0 = keyObj.value(cache0.context().cacheObjectContext(), true);
                    Object key1 = keyObj.value(cache0.context().cacheObjectContext(), false);
                    Object key2 = keyObj.value(cache0.context().cacheObjectContext(), true);
                    Object key3 = keyObj.value(cache0.context().cacheObjectContext(), false);

                    assertSame(key0, key1);
                    assertSame(key1, key2);
                    assertSame(key2, key3);
                }

                CacheObject obj = e.rawGet();

                if (obj != null) {
                    assertNull("Unexpected value, node: " + g, reflectiveValue(obj));

                    assertNotNull(obj.value(cache0.context().cacheObjectContext(), true));

                    assertNull("Unexpected value after value() requested1: " + g, reflectiveValue(obj));

                    assertNotNull(obj.value(cache0.context().cacheObjectContext(), false));

                    assertNull("Unexpected value after value() requested2: " + g, reflectiveValue(obj));
                }
                else
                    assertFalse(aff.isPrimaryOrBackup(ig.localNode(), key));
            }
            else
                assertFalse("Entry not found, node: " + g, aff.isPrimaryOrBackup(ig.localNode(), key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testValueStored() throws Exception {
        cpyOnRead = false;

        startGrids();

        IgniteCache<TestKey, TestValue> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        Affinity<Object> aff = grid(0).affinity(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 100; i++) {
            TestKey key = new TestKey(i);
            TestValue val = new TestValue(i);

            cache.put(key, val);

            checkHasValue(aff, key);

            for (int g = 0; g < gridCount(); g++)
                assertNotNull(grid(g).cache(DEFAULT_CACHE_NAME).get(key));

            checkHasValue(aff, key);

            cache.invoke(key, new CacheEntryProcessor<TestKey, TestValue, Object>() {
                @Override public Object process(MutableEntry<TestKey, TestValue> entry, Object... args) {
                    assertNotNull(entry.getValue());

                    entry.setValue(new TestValue(10_000));

                    return new TestValue(20_000);
                }
            });

            checkHasValue(aff, key);

            for (int g = 0; g < gridCount(); g++)
                assertNotNull(grid(g).cache(DEFAULT_CACHE_NAME).get(key));

            checkHasValue(aff, key);

            cache.remove(key);

            for (int g = 0; g < gridCount(); g++)
                assertNull(grid(g).cache(DEFAULT_CACHE_NAME).get(key));

            try (IgniteDataStreamer<TestKey, TestValue> streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
                streamer.addData(key, val);
            }

            checkHasValue(aff, key);

            cache.remove(key);

            try (IgniteDataStreamer<TestKey, TestValue> streamer = grid(0).dataStreamer(DEFAULT_CACHE_NAME)) {
                streamer.allowOverwrite(true);

                streamer.addData(key, val);
            }

            checkHasValue(aff, key);

            if (aff.isPrimaryOrBackup(grid(0).localNode(), key)) {
                cache.localEvict(Collections.singleton(key));

                assertNull(cache.localPeek(key, CachePeekMode.ONHEAP));

                cache.get(key);

                assertNotNull(cache.localPeek(key, CachePeekMode.ONHEAP));

                checkHasValue(aff, key);
            }
        }
    }

    /**
     * @param aff Affinity.
     * @param key Key.
     */
    private void checkHasValue(Affinity<Object> aff, Object key) {
        for (int g = 0; g < gridCount(); g++) {
            IgniteEx ig = grid(g);

            GridCacheAdapter cache0 = internalCache(ig, DEFAULT_CACHE_NAME);

            GridCacheEntryEx e = cache0.peekEx(key);

            if (e == null && cache0.isNear())
                e = ((GridNearCacheAdapter)cache0).dht().peekEx(key);

            if (e != null) {
                KeyCacheObject keyObj = e.key();

                assertNotNull(keyObj);

                if (!isBinaryMarshallerUsed(ig)) {
                    assertNotNull("Unexpected value, node: " + g,
                        GridTestUtils.getFieldValue(keyObj, CacheObjectAdapter.class, "val"));

                    Object key0 = keyObj.value(cache0.context().cacheObjectContext(), true);
                    Object key1 = keyObj.value(cache0.context().cacheObjectContext(), false);
                    Object key2 = keyObj.value(cache0.context().cacheObjectContext(), true);
                    Object key3 = keyObj.value(cache0.context().cacheObjectContext(), false);

                    assertSame(key0, key1);
                    assertSame(key1, key2);
                    assertSame(key2, key3);
                }

                CacheObject obj = e.rawGet();

                if (obj != null) {
                    if (!isBinaryMarshallerUsed(ig))
                        assertNotNull("Unexpected value, node: " + g, reflectiveValue(obj));

                    Object val0 = obj.value(cache0.context().cacheObjectContext(), true);

                    assertNotNull("Unexpected value after value() requested1: " + g, reflectiveValue(obj));

                    Object val1 = obj.value(cache0.context().cacheObjectContext(), true);

                    assertNotNull("Unexpected value after value() requested2: " + g, reflectiveValue(obj));

                    assertSame(val0, val1);

                    Object val2 = obj.value(cache0.context().cacheObjectContext(), false);

                    assertNotNull("Unexpected value after value() requested3: " + g, reflectiveValue(obj));

                    assertSame(val1, val2);
                }
                else
                    assertFalse(aff.isPrimaryOrBackup(ig.localNode(), key));
            }
            else
                assertFalse("Entry not found, node: " + g, aff.isPrimaryOrBackup(ig.localNode(), key));
        }
    }

    /**
     * @param ig Ignite.
     * @return If binary marshaller is used.
     */
    private boolean isBinaryMarshallerUsed(Ignite ig) {
        return ig.configuration().getMarshaller() == null ||
            ig.configuration().getMarshaller() instanceof BinaryMarshaller;
    }

    /**
     * @param obj Object to extract value from.
     * @return Cache object.
     */
    @SuppressWarnings("IfMayBeConditional")
    private Object reflectiveValue(CacheObject obj) {
        if (obj instanceof BinaryObjectImpl)
            return GridTestUtils.getFieldValue(obj, BinaryObjectImpl.class, "obj");
        else
            return GridTestUtils.getFieldValue(obj, CacheObjectAdapter.class, "val");
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
        @Override public String toString() {
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
