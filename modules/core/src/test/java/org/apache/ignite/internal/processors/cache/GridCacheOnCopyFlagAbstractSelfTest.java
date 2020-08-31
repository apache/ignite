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
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.cache.CacheInterceptorAdapter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

/**
 * Tests that cache value is copied for get, interceptor and invoke closure.
 */
public abstract class GridCacheOnCopyFlagAbstractSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int ITER_CNT = 1000;

    /** */
    public static final int WRONG_VALUE = -999999;

    /** */
    private static Interceptor interceptor;

    /** */
    private static boolean noInterceptor;

    /** p2p enabled. */
    private boolean p2pEnabled;

    /**
     * Returns cache mode for tests.
     * @return cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * Returns cache atomicity mode for cache.
     * @return cache atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.setPeerClassLoadingEnabled(p2pEnabled);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /** */
    @SuppressWarnings("unchecked")
    protected CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration ccfg = defaultCacheConfiguration();

        assertTrue(ccfg.isCopyOnRead());

        interceptor = new Interceptor();

        ccfg.setInterceptor(interceptor);

        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setCacheMode(cacheMode());
        ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCopyOnReadFlagP2PEnabled() throws Exception {
        doTest(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCopyOnReadFlagP2PDisbaled() throws Exception {
        doTest(false);
    }

    /**
     * @param p2pEnabled P 2 p enabled.
     */
    private void doTest(boolean p2pEnabled) throws Exception {
        this.p2pEnabled = p2pEnabled;

        IgniteEx grid = startGrid(0);

        assertEquals(p2pEnabled, grid.configuration().isPeerClassLoadingEnabled());

        try {
            interceptor();
            invokeAndInterceptor();
            putGet();
            putGetByteArray();
            putGetKnownImmutable();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void interceptor() throws Exception {
        noInterceptor = false;

        IgniteCache<TestKey, TestValue> cache = grid(0).createCache(cacheConfiguration());

        try {
            for (int i = 0; i < ITER_CNT; i++) {
                final TestValue val = new TestValue(i);
                final TestKey key = new TestKey(i, i);

                interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>() {
                    @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                        assertNotSame(key, entry.getKey());

                        assertSame(entry.getValue(), entry.getValue());
                        assertSame(entry.getKey(), entry.getKey());

                        // Try change value.
                        entry.getValue().val(WRONG_VALUE);
                    }
                });

                cache.put(key, val);

                CacheObject obj =
                    ((GridCacheAdapter)((IgniteCacheProxy)cache).internalProxy().delegate()).peekEx(key).peekVisibleValue();

                // Check thar internal entry wasn't changed.
                assertEquals(i, getValue(obj, cache));

                final TestValue newTestVal = new TestValue(-i);

                interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>() {
                    @Override public TestValue onBeforePut(Cache.Entry<TestKey, TestValue> entry, TestValue newVal) {
                        assertNotSame(key, entry.getKey());
                        assertNotSame(val, entry.getValue());

                        assertEquals(newTestVal, newVal);

                        // Try change value.
                        entry.getValue().val(WRONG_VALUE);

                        return newVal;
                    }

                    @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                        assertNotSame(key, entry.getKey());

                        assertSame(entry.getValue(), entry.getValue());
                        assertSame(entry.getKey(), entry.getKey());

                        // Try change value.
                        entry.getValue().val(WRONG_VALUE);
                    }
                });

                cache.put(key, newTestVal);

                obj = ((GridCacheAdapter)((IgniteCacheProxy)cache).internalProxy().delegate()).peekEx(key).peekVisibleValue();

                // Check thar internal entry wasn't changed.
                assertEquals(-i, getValue(obj, cache));

                interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>() {
                    @Override public IgniteBiTuple onBeforeRemove(Cache.Entry<TestKey, TestValue> entry) {
                        assertNotSame(key, entry.getKey());
                        assertNotSame(newTestVal, entry.getValue());

                        return super.onBeforeRemove(entry);
                    }

                    @Override public void onAfterRemove(Cache.Entry<TestKey, TestValue> entry) {
                        assertNotSame(key, entry.getKey());
                        assertNotSame(newTestVal, entry.getValue());
                    }
                });

                cache.remove(key);
            }
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void invokeAndInterceptor() throws Exception {
        noInterceptor = false;

        IgniteCache<TestKey, TestValue> cache = grid(0).createCache(cacheConfiguration());

        try {
            for (int i = 0; i < ITER_CNT; i++)
                cache.put(new TestKey(i, i), new TestValue(i));

            interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>() {
                @Override public TestValue onBeforePut(Cache.Entry<TestKey, TestValue> entry, TestValue newVal) {
                    // Check that we have correct value and key.
                    assertEquals(entry.getKey().key(), entry.getKey().field());

                    // Try changed entry.
                    entry.getValue().val(WRONG_VALUE);

                    return super.onBeforePut(entry, newVal);
                }

                @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                    assertEquals(entry.getKey().key(), entry.getKey().field());

                    entry.getValue().val(WRONG_VALUE);

                    super.onAfterPut(entry);
                }
            });

            for (int i = 0; i < ITER_CNT; i++) {
                TestKey key = new TestKey(i, i);

                cache.invoke(key, new EntryProcessor<TestKey, TestValue, Object>() {
                    @Override public Object process(MutableEntry<TestKey, TestValue> entry, Object... arguments)
                        throws EntryProcessorException {
                        TestValue val = entry.getValue();

                        // Check that we have correct value.
                        assertEquals(entry.getKey().key(), val.val());

                        // Try changed entry.
                        val.val(WRONG_VALUE);

                        return -1;
                    }
                });

                CacheObject obj =
                    ((GridCacheAdapter)((IgniteCacheProxy)cache).internalProxy().delegate()).peekEx(key).peekVisibleValue();

                assertNotEquals(WRONG_VALUE, getValue(obj, cache));
            }
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void putGet() throws Exception {
        noInterceptor = true;

        IgniteCache<TestKey, TestValue> cache = grid(0).createCache(cacheConfiguration());

        try {
            Map<TestKey, TestValue> map = new HashMap<>();

            for (int i = 0; i < ITER_CNT; i++) {
                TestKey key = new TestKey(i, i);
                TestValue val = new TestValue(i);

                cache.put(key, val);

                map.put(key, val);
            }

            GridCacheAdapter cache0 = internalCache(cache);

            GridCacheContext cctx = cache0.context();

            boolean binary = cctx.cacheObjects().isBinaryEnabled(null);

            for (Map.Entry<TestKey, TestValue> e : map.entrySet()) {
                GridCacheEntryEx entry = cache0.peekEx(e.getKey());

                assertNotNull("No entry for key: " + e.getKey(), entry);

                TestKey key0 = entry.key().value(cctx.cacheObjectContext(), false);

                assertNotSame(key0, e.getKey());

                TestKey key1 = entry.key().value(cctx.cacheObjectContext(), true);

                if (!binary)
                    assertSame(key0, key1);
                else
                    assertNotSame(key0, key1);

                TestValue val0 = entry.rawGet().value(cctx.cacheObjectContext(), false);

                assertNotSame(val0, e.getValue());

                TestValue val1 = entry.rawGet().value(cctx.cacheObjectContext(), true);

                assertNotSame(val0, val1);
            }
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void putGetByteArray() throws Exception {
        noInterceptor = true;

        IgniteCache<TestKey, byte[]> cache = grid(0).createCache(cacheConfiguration());

        try {
            Map<TestKey, byte[]> map = new HashMap<>();

            for (int i = 0; i < ITER_CNT; i++) {
                TestKey key = new TestKey(i, i);
                byte[] val = new byte[10];

                cache.put(key, val);

                map.put(key, val);
            }

            GridCacheAdapter cache0 = internalCache(cache);

            GridCacheContext cctx = cache0.context();

            boolean binary = cctx.cacheObjects().isBinaryEnabled(null);

            for (Map.Entry<TestKey, byte[]> e : map.entrySet()) {
                GridCacheEntryEx entry = cache0.peekEx(e.getKey());

                assertNotNull("No entry for key: " + e.getKey(), entry);

                TestKey key0 = entry.key().value(cctx.cacheObjectContext(), false);

                assertNotSame(key0, e.getKey());

                TestKey key1 = entry.key().value(cctx.cacheObjectContext(), true);

                if (!binary)
                    assertSame(key0, key1);
                else
                    assertNotSame(key0, key1);

                byte[] val0 = entry.rawGet().value(cctx.cacheObjectContext(), false);

                assertNotSame(val0, e.getValue());

                byte[] val1 = entry.rawGet().value(cctx.cacheObjectContext(), true);

                assertNotSame(val0, val1);
            }
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void putGetKnownImmutable() throws Exception {
        noInterceptor = true;

        IgniteCache<String, Long> cache = grid(0).createCache(cacheConfiguration());

        try {
            Map<String, Long> map = new HashMap<>();

            for (int i = 0; i < ITER_CNT; i++) {
                String key = String.valueOf(i);
                Long val = Long.MAX_VALUE - i;

                cache.put(key, val);

                map.put(key, val);
            }

            GridCacheAdapter cache0 = internalCache(cache);

            GridCacheContext cctx = cache0.context();

            for (Map.Entry<String, Long> e : map.entrySet()) {
                GridCacheEntryEx entry = cache0.peekEx(e.getKey());

                assertNotNull("No entry for key: " + e.getKey(), entry);

                String key0 = entry.key().value(cctx.cacheObjectContext(), false);

                assertSame(key0, e.getKey());

                String key1 = entry.key().value(cctx.cacheObjectContext(), true);

                assertSame(key0, key1);

                if (!storeValue(cache)) {
                    Long val0 = entry.rawGet().value(cctx.cacheObjectContext(), false);

                    assertNotSame(val0, e.getValue());

                    Long val1 = entry.rawGet().value(cctx.cacheObjectContext(), true);

                    assertNotSame(val0, val1);

                    assertNotSame(e.getValue(), cache.get(e.getKey()));
                }
            }
        }
        finally {
            if (cache != null)
                cache.destroy();
        }
    }

    /**
     *
     */
    public static class TestKey implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int key;

        /** */
        private int field;

        /**
         * Constructor.
         *
         * @param key Key.
         * @param field Field.
         */
        public TestKey(int key, int field) {
            this.key = key;
            this.field = field;
        }

        /**
         * Default constructor.
         */
        public TestKey() {
            // No-op.
        }

        /**
         * @return key Key.
         */
        public int key() {
            return key;
        }

        /**
         * @return Test field.
         */
        public int field() {
            return field;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            TestKey testKey = (TestKey) o;

            return key == testKey.key;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestKey [field=" + field + ", key=" + key + ']';
        }
    }

    /**
     * @param cache Cache.
     */
    private static boolean storeValue(IgniteCache cache) {
        return ((IgniteCacheProxy)cache).context().cacheObjectContext().storeValue();
    }

    /**
     * @param obj Object.
     * @param cache Cache.
     */
    private static Object getValue(CacheObject obj, IgniteCache cache) {
        if (obj instanceof BinaryObject)
            return ((BinaryObject)obj).field("val");
        else {
            if (storeValue(cache))
                return ((TestValue)U.field(obj, "val")).val();
            else
                return CU.<TestValue>value(obj, ((IgniteCacheProxy)cache).context(), false).val();
        }
    }

    /**
     *
     */
    public static class TestValue implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int val;

        /**
         * Constructor.
         *
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }

        /**
         * Default constructor.
         */
        public TestValue() {
            // No-op.
        }

        /**
         * @return Value.
         */
        public int val() {
            return val;
        }

        /**
         * @param val Value.
         */
        public void val(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;

            if (o == null || getClass() != o.getClass()) return false;

            TestValue testKey = (TestValue)o;

            return val == testKey.val;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     *
     */
    private static class Interceptor implements CacheInterceptor<Object, Object> {
        /** */
        CacheInterceptor<TestKey, TestValue> delegate = new CacheInterceptorAdapter<>();

        /** {@inheritDoc} */
        @Override public Object onGet(Object key, @Nullable Object val) {
            if (!noInterceptor)
                return delegate.onGet((TestKey)key, (TestValue)val);

            return val;
        }

        /** {@inheritDoc} */
        @Override public Object onBeforePut(Cache.Entry<Object, Object> entry, Object newVal) {
            if (!noInterceptor)
                return delegate.onBeforePut((Cache.Entry)entry, (TestValue)newVal);

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Object, Object> entry) {
            if (!noInterceptor)
                delegate.onAfterPut((Cache.Entry)entry);
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<Boolean, Object> onBeforeRemove(Cache.Entry<Object, Object> entry) {
            if (!noInterceptor)
                return (IgniteBiTuple)delegate.onBeforeRemove((Cache.Entry)entry);

            return new IgniteBiTuple<>(false, entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Object, Object> entry) {
            if (!noInterceptor)
                delegate.onAfterRemove((Cache.Entry)entry);
        }

        /**
         * @param delegate Cache interceptor delegate.
         */
        public void delegate(CacheInterceptor<TestKey, TestValue> delegate) {
            this.delegate = delegate;
        }
    }
}
