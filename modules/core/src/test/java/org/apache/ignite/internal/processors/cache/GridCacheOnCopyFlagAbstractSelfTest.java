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
 * Tests {@link org.apache.ignite.cache.CacheInterceptor}.
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
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        interceptor = new Interceptor();
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
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        assertNotNull(interceptor);

        ccfg.setInterceptor(interceptor);
        
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setDistributionMode(distributionMode());
        ccfg.setCacheMode(cacheMode());

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @throws Exception If failed.
     */
    public void testInterceptor() throws Exception {
        IgniteCache<TestKey, TestValue> cache = grid(0).jcache(null);

        for (int i = 0; i < ITER_CNT; i++) {
            final TestValue val = new TestValue(i);
            final TestKey key = new TestKey(i, i);

            interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>() {
                @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                    assertNotSame(key, entry.getKey());
                    assertNotSame(val, entry.getValue());

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
                    assertNotSame(newTestVal, entry.getValue());

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
    public void testEntryProcessor() throws Exception {
        IgniteCache<TestKey, TestValue> cache = grid(0).jcache(null);

        Set<TestKey> keys = new LinkedHashSet<>();

        for (int i = 0; i < ITER_CNT; i++) {
            TestKey key = new TestKey(i, i);
            keys.add(key);

            cache.put(key, new TestValue(i));
        }

        for (int i = 0; i < ITER_CNT; i++) {
            cache.invoke(new TestKey(i, i), new EntryProcessor<TestKey, TestValue, Object>() {
                @Override public Object process(MutableEntry<TestKey, TestValue> entry, Object... arguments)
                    throws EntryProcessorException {
                    // Try change entry.
                    entry.getKey().field(WRONG_VALUE);
                    entry.getValue().val(WRONG_VALUE);
   
                    return -1;
                }
            });

            // Check that internal entry isn't changed.
            Cache.Entry<Object, Object> e = internalCache(0).entry(new TestKey(i, i));

            assertEquals(i, ((TestKey)e.getKey()).field());
            assertEquals(i, ((TestValue)e.getValue()).val());
        }
        
        cache.invokeAll(keys, new EntryProcessor<TestKey, TestValue, Object>() {
            @Override public Object process(MutableEntry<TestKey, TestValue> entry, Object... arguments) 
                throws EntryProcessorException {
                // Try change entry.
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
    public void testInvokeAndInterceptor() throws Exception {
        IgniteCache<TestKey, TestValue> cache = grid(0).jcache(null);

        for (int i = 0; i < ITER_CNT; i++)
            cache.put(new TestKey(i, i), new TestValue(i));

        interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>(){
            @Override public TestValue onBeforePut(Cache.Entry<TestKey, TestValue> entry, TestValue newVal) {
                // Check that we have correct value and key.
                assertEquals(entry.getKey().key(), entry.getKey().field());
                assertEquals(entry.getKey().key(), entry.getValue().val());

                // Try changed entry.
                entry.getKey().field(WRONG_VALUE);
                entry.getValue().val(WRONG_VALUE);

                return super.onBeforePut(entry, newVal);
            }

            @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                assertEquals(entry.getKey().key(), entry.getKey().field());
                assertEquals(entry.getKey().key(), entry.getValue().val());

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
    public void testInvokeAllAndInterceptor() throws Exception {
        IgniteCache<TestKey, TestValue> cache = grid(0).jcache(null);

        Set<TestKey> keys = new LinkedHashSet<>();
        
        for (int i = 0; i < ITER_CNT; i++) {
            TestKey key = new TestKey(i, i);

            keys.add(key);

            cache.put(key, new TestValue(i));
        }

        interceptor.delegate(new CacheInterceptorAdapter<TestKey, TestValue>(){
            @Override public TestValue onBeforePut(Cache.Entry<TestKey, TestValue> entry, TestValue newVal) {
                // Check that we have correct value and key.
                assertEquals(entry.getKey().key(), entry.getKey().field());
                assertEquals(entry.getKey().key(), entry.getValue().val());

                // Try changed entry.
                entry.getKey().field(WRONG_VALUE);
                entry.getValue().val(WRONG_VALUE);

                return super.onBeforePut(entry, newVal);
            }

            @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
                // Check that we have correct value and key.
                assertEquals(entry.getKey().key(), entry.getKey().field());
                assertEquals(entry.getKey().key(), entry.getValue().val());

                // Try changed entry.
                entry.getValue().val(WRONG_VALUE);
                entry.getKey().field(WRONG_VALUE);

                super.onAfterPut(entry);
            }
        });

        cache.invokeAll(keys, new EntryProcessor<TestKey, TestValue, Object>() {
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
    private class Interceptor implements CacheInterceptor<TestKey, TestValue> {
        /** */
        CacheInterceptor<TestKey, TestValue> delegate = new CacheInterceptorAdapter<>();

        /** {@inheritDoc} */
        @Override public TestValue onGet(TestKey key, @Nullable TestValue val) {
            return delegate.onGet(key, val);
        }

        /** {@inheritDoc} */
        @Override public TestValue onBeforePut(Cache.Entry<TestKey, TestValue> entry, TestValue newVal) {
            return delegate.onBeforePut(entry, newVal);
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<TestKey, TestValue> entry) {
            delegate.onAfterPut(entry);
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<Boolean, TestValue> onBeforeRemove(Cache.Entry<TestKey, TestValue> entry) {
            return delegate.onBeforeRemove(entry);
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<TestKey, TestValue> entry) {
            delegate.onAfterRemove(entry);
        }

        /**
         * @param delegate Cache interceptor delegate.
         */
        public void delegate(CacheInterceptor<TestKey, TestValue> delegate) {
            this.delegate = delegate;
        }
    }
}
