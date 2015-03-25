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
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Tests that cache value is copied for get, interceptor and invoke closure.
 */
public abstract class GridCacheOnCopyFlagAbstractSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    public static final int ITER_CNT = 1000;

    /** */
    public static final int WRONG_VALUE = -999999;

    /** */
    private static Interceptor interceptor;

    /** */
    private static boolean noInterceptor;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        interceptor = new Interceptor();

        super.beforeTestsStarted();

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        noInterceptor = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>());

        for (int i = 0; i < gridCount(); i++)
            cache(i, null).clearLocally();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(spi);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        return c;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        assertTrue(ccfg.isCopyOnRead());

        assertNotNull(interceptor);

        ccfg.setInterceptor(interceptor);

        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setCacheMode(cacheMode());
        ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @throws Exception If failed.
     */
    public void testInterceptor() throws Exception {
        IgniteCache<TestKey, TestValue> cache = grid(0).cache(null);

        for (int i = 0; i < ITER_CNT; i++) {
            final TestValue val = new TestValue(i);
            final TestKey key = new TestKey(i, i);

            interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>() {
                @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                    assertNotSame(key, entry.getKey());

                    assertSame(entry.getValue(), entry.getValue());
                    assertSame(entry.getKey(), entry.getKey());

                    // Try change key and value.
                    entry.getKey().field(WRONG_VALUE);
                    entry.getValue().val(WRONG_VALUE);
                }
            });

            cache.put(key, val);

            Cache.Entry<Object, Object> entry = internalCache(0).entrySet().iterator().next();

            // Check thar internal entry wasn't changed.
            assertEquals(i, ((TestKey)entry.getKey()).field());
            assertEquals(i, ((TestValue)entry.getValue()).val());

            final TestValue newTestVal = new TestValue(-i);

            interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>() {
                @Override public TestValue onBeforePut(Cache.Entry<TestKey, TestValue> entry, TestValue newVal) {
                    assertNotSame(key, entry.getKey());
                    assertNotSame(val, entry.getValue());

                    assertEquals(newTestVal, newVal);

                    // Try change key and value.
                    entry.getKey().field(WRONG_VALUE);
                    entry.getValue().val(WRONG_VALUE);

                    return newVal;
                }

                @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                    assertNotSame(key, entry.getKey());

                    assertSame(entry.getValue(), entry.getValue());
                    assertSame(entry.getKey(), entry.getKey());

                    // Try change key and value.
                    entry.getKey().field(WRONG_VALUE);
                    entry.getValue().val(WRONG_VALUE);
                }
            });

            cache.put(key, newTestVal);

            entry = internalCache(0).entrySet().iterator().next();

            // Check thar internal entry wasn't changed.
            assertEquals(i, ((TestKey)entry.getKey()).field());
            assertEquals(-i, ((TestValue)entry.getValue()).val());

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

    /**
     * @throws Exception If failed.
     */
    public void testInvokeAndInterceptor() throws Exception {
        IgniteCache<TestKey, TestValue> cache = grid(0).cache(null);

        for (int i = 0; i < ITER_CNT; i++)
            cache.put(new TestKey(i, i), new TestValue(i));

        interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>(){
            @Override public TestValue onBeforePut(Cache.Entry<TestKey, TestValue> entry, TestValue newVal) {
                // Check that we have correct value and key.
                assertEquals(entry.getKey().key(), entry.getKey().field());

                // Try changed entry.
                entry.getKey().field(WRONG_VALUE);
                entry.getValue().val(WRONG_VALUE);

                return super.onBeforePut(entry, newVal);
            }

            @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                assertEquals(entry.getKey().key(), entry.getKey().field());

                entry.getValue().val(WRONG_VALUE);
                entry.getKey().field(WRONG_VALUE);

                super.onAfterPut(entry);
            }
        });

        for (int i = 0; i < ITER_CNT; i++)
            cache.invoke(new TestKey(i, i), new EntryProcessor<TestKey, TestValue, Object>() {
                @Override public Object process(MutableEntry<TestKey, TestValue> entry, Object... arguments)
                    throws EntryProcessorException {
                    // Check that we have correct value and key.
                    assertEquals(entry.getKey().key(), entry.getKey().field());
                    assertEquals(entry.getKey().key(), entry.getValue().val());

                    // Try changed entry.
                    entry.getKey().field(WRONG_VALUE);
                    entry.getValue().val(WRONG_VALUE);

                    return -1;
                }
            });

        // Check that entries weren't changed.
        for (Cache.Entry<Object, Object> e : internalCache(0).entrySet()) {
            assertNotEquals(WRONG_VALUE, ((TestKey)e.getKey()).field());
            assertNotEquals(WRONG_VALUE, ((TestValue)e.getValue()).val());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGet() throws Exception {
        noInterceptor = true;

        IgniteCache<TestKey, TestValue> cache = grid(0).cache(null);

        Map<TestKey, TestValue> map = new HashMap<>();

        for (int i = 0; i < ITER_CNT; i++) {
            TestKey key = new TestKey(i, i);
            TestValue val = new TestValue(i);

            cache.put(key, val);

            map.put(key, val);
        }

        GridCacheAdapter cache0 = internalCache(cache);

        GridCacheContext cctx = cache0.context();

        for (Map.Entry<TestKey, TestValue> e : map.entrySet()) {
            GridCacheEntryEx entry = cache0.peekEx(e.getKey());

            assertNotNull("No entry for key: " + e.getKey(), entry);

            TestKey key0 = entry.key().value(cctx.cacheObjectContext(), false);

            assertNotSame(key0, e.getKey());

            TestKey key1 = entry.key().value(cctx.cacheObjectContext(), true);

            assertNotSame(key0, key1);

            TestValue val0 = entry.rawGet().value(cctx.cacheObjectContext(), false);

            assertNotSame(val0, e.getValue());

            TestValue val1 = entry.rawGet().value(cctx.cacheObjectContext(), true);

            assertNotSame(val0, val1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetByteArray() throws Exception {
        noInterceptor = true;

        IgniteCache<TestKey, byte[]> cache = grid(0).cache(null);

        Map<TestKey, byte[]> map = new HashMap<>();

        for (int i = 0; i < ITER_CNT; i++) {
            TestKey key = new TestKey(i, i);
            byte[] val = new byte[10];

            cache.put(key, val);

            map.put(key, val);
        }

        GridCacheAdapter cache0 = internalCache(cache);

        GridCacheContext cctx = cache0.context();

        for (Map.Entry<TestKey, byte[]> e : map.entrySet()) {
            GridCacheEntryEx entry = cache0.peekEx(e.getKey());

            assertNotNull("No entry for key: " + e.getKey(), entry);

            TestKey key0 = entry.key().value(cctx.cacheObjectContext(), false);

            assertNotSame(key0, e.getKey());

            TestKey key1 = entry.key().value(cctx.cacheObjectContext(), true);

            assertNotSame(key0, key1);

            byte[] val0 = entry.rawGet().value(cctx.cacheObjectContext(), false);

            assertNotSame(val0, e.getValue());

            byte[] val1 = entry.rawGet().value(cctx.cacheObjectContext(), true);

            assertNotSame(val0, val1);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetImmutable() throws Exception {
        noInterceptor = true;

        IgniteCache<TestImmutableKey, TestImmutableValue> cache = grid(0).cache(null);

        Map<TestImmutableKey, TestImmutableValue> map = new HashMap<>();

        for (int i = 0; i < ITER_CNT; i++) {
            TestImmutableKey key = new TestImmutableKey(i, i);
            TestImmutableValue val = new TestImmutableValue(i);

            cache.put(key, val);

            map.put(key, val);
        }

        GridCacheAdapter cache0 = internalCache(cache);

        GridCacheContext cctx = cache0.context();

        for (Map.Entry<TestImmutableKey, TestImmutableValue> e : map.entrySet()) {
            GridCacheEntryEx entry = cache0.peekEx(e.getKey());

            assertNotNull("No entry for key: " + e.getKey(), entry);

            TestKey key0 = entry.key().value(cctx.cacheObjectContext(), false);

            assertSame(key0, e.getKey());

            TestKey key1 = entry.key().value(cctx.cacheObjectContext(), true);

            assertSame(key0, key1);

            TestImmutableValue val0 = entry.rawGet().value(cctx.cacheObjectContext(), false);

            assertSame(val0, e.getValue());

            TestImmutableValue val1 = entry.rawGet().value(cctx.cacheObjectContext(), true);

            assertSame(val0, val1);

            assertSame(e.getValue(), cache.get(e.getKey()));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutGetKnownImmutable() throws Exception {
        noInterceptor = true;

        IgniteCache<String, Long> cache = grid(0).cache(null);

        Map<String, Long> map = new HashMap<>();

        for (int i = 0; i < ITER_CNT; i++) {
            String key = String.valueOf(i);
            Long val = (long)i;

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

            Long val0 = entry.rawGet().value(cctx.cacheObjectContext(), false);

            assertSame(val0, e.getValue());

            Long val1 = entry.rawGet().value(cctx.cacheObjectContext(), true);

            assertSame(val0, val1);

            assertSame(e.getValue(), cache.get(e.getKey()));
        }
    }

    /**
     *
     */
    public static class TestKey implements Externalizable {
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
        public int key(){
            return key;
        }

        /**
         * @return Test field.
         */
        public int field() {
            return field;
        }

        /**
         * *
         * @param field Test field.
         */
        public void field(int field) {
            this.field = field;
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
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(key);
            out.writeInt(field);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            key = in.readInt();
            field = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestKey [field=" + field + ", key=" + key + ']';
        }
    }

    /**
     *
     */
    public static class TestValue implements Externalizable {
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

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(val);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            val = in.readInt();
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

    /**
     *
     */
    @IgniteImmutable
    public static class TestImmutableKey extends TestKey {
        /**
         *
         */
        public TestImmutableKey() {
            // No-op.
        }

        /**
         * @param key Key.
         * @param field Field.
         */
        public TestImmutableKey(int key, int field) {
            super(key, field);
        }
    }

    /**
     *
     */
    @IgniteImmutable
    public static class TestImmutableValue {
        /** */
        public int val;

        /**
         * @param val Value.
         */
        public TestImmutableValue(int val) {
            this.val = val;
        }
    }
}
