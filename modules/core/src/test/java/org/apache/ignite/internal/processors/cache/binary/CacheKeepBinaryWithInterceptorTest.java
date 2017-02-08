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

package org.apache.ignite.internal.processors.cache.binary;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheInterceptor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import javax.cache.Cache;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 *
 */
@SuppressWarnings("unchecked")
public class CacheKeepBinaryWithInterceptorTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testKeepBinaryWithInterceptor() throws Exception {
        keepBinaryWithInterceptor(cacheConfiguration(ATOMIC, false));
        keepBinaryWithInterceptor(cacheConfiguration(TRANSACTIONAL, false));

        keepBinaryWithInterceptorPrimitives(cacheConfiguration(ATOMIC, true));
        keepBinaryWithInterceptorPrimitives(cacheConfiguration(TRANSACTIONAL, true));

        startGridsMultiThreaded(1, 3);

        keepBinaryWithInterceptor(cacheConfiguration(ATOMIC, false));
        keepBinaryWithInterceptor(cacheConfiguration(TRANSACTIONAL, false));

        keepBinaryWithInterceptorPrimitives(cacheConfiguration(ATOMIC, true));
        keepBinaryWithInterceptorPrimitives(cacheConfiguration(TRANSACTIONAL, true));
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void keepBinaryWithInterceptor(CacheConfiguration ccfg) {
        ignite(0).createCache(ccfg);

        try {
            TestInterceptor1.onAfterRmv = 0;
            TestInterceptor1.onBeforeRmv = 0;
            TestInterceptor1.onAfterPut = 0;
            TestInterceptor1.onBeforePut = 0;
            TestInterceptor1.onGet = 0;

            IgniteCache cache = ignite(0).cache(null).withKeepBinary();

            IgniteCache asyncCache = cache.withAsync();

            cache.put(new TestKey(1), new TestValue(10));

            cache.put(new TestKey(1), new TestValue(10));

            BinaryObject obj = (BinaryObject)cache.get(new TestKey(1));
            assertEquals(10, (int)obj.field("val"));

            asyncCache.get(new TestKey(1));
            obj = (BinaryObject)asyncCache.future().get();
            assertEquals(10, (int)obj.field("val"));

            Cache.Entry<BinaryObject, BinaryObject> e = (Cache.Entry)cache.getEntry(new TestKey(1));
            assertEquals(1, (int)e.getKey().field("key"));
            assertEquals(10, (int)e.getValue().field("val"));

            asyncCache.getEntry(new TestKey(1));
            e = (Cache.Entry)asyncCache.future().get();
            assertEquals(1, (int)e.getKey().field("key"));
            assertEquals(10, (int)e.getValue().field("val"));

            obj = (BinaryObject)cache.getAndRemove(new TestKey(1));
            assertEquals(10, (int)obj.field("val"));

            cache.put(new TestKey(1), new TestValue(10));

            assertTrue(cache.remove(new TestKey(1)));

            assertTrue(TestInterceptor1.onAfterRmv > 0);
            assertTrue(TestInterceptor1.onBeforeRmv > 0);
            assertTrue(TestInterceptor1.onAfterPut > 0);
            assertTrue(TestInterceptor1.onBeforePut > 0);
            assertTrue(TestInterceptor1.onGet > 0);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param ccfg Cache configuration.
     */
    private void keepBinaryWithInterceptorPrimitives(CacheConfiguration ccfg) {
        ignite(0).createCache(ccfg);

        try {
            TestInterceptor2.onAfterRmv = 0;
            TestInterceptor2.onBeforeRmv = 0;
            TestInterceptor2.onAfterPut = 0;
            TestInterceptor2.onBeforePut = 0;
            TestInterceptor2.onGet = 0;

            IgniteCache cache = ignite(0).cache(null).withKeepBinary();

            IgniteCache asyncCache = cache.withAsync();

            cache.put(1, 10);

            cache.put(1, 10);

            Integer obj = (Integer)cache.get(1);
            assertEquals((Integer)10, obj);

            asyncCache.get(1);
            obj = (Integer)asyncCache.future().get();
            assertEquals((Integer)10, obj);

            Cache.Entry<Integer, Integer> e = (Cache.Entry)cache.getEntry(1);
            assertEquals((Integer)1, e.getKey());
            assertEquals((Integer)10, e.getValue());

            asyncCache.getEntry(1);
            e = (Cache.Entry)asyncCache.future().get();
            assertEquals((Integer)1, e.getKey());
            assertEquals((Integer)10, e.getValue());

            obj = (Integer)cache.getAndRemove(1);
            assertEquals((Integer)10, obj);

            cache.put(1, 10);

            assertTrue(cache.remove(1));

            assertTrue(TestInterceptor2.onAfterRmv > 0);
            assertTrue(TestInterceptor2.onBeforeRmv > 0);
            assertTrue(TestInterceptor2.onAfterPut > 0);
            assertTrue(TestInterceptor2.onBeforePut > 0);
            assertTrue(TestInterceptor2.onGet > 0);
        }
        finally {
            ignite(0).destroyCache(ccfg.getName());
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param testPrimitives {@code True} if test interceptor with primitive values.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(CacheAtomicityMode atomicityMode, boolean testPrimitives) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setInterceptor(testPrimitives ? new TestInterceptor2() : new TestInterceptor1());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     *
     */
    static class TestInterceptor1 implements CacheInterceptor<BinaryObject, BinaryObject> {
        /** */
        static int onGet;

        /** */
        static int onBeforePut;

        /** */
        static int onAfterPut;

        /** */
        static int onBeforeRmv;

        /** */
        static int onAfterRmv;

        /** {@inheritDoc} */
        @Nullable @Override public BinaryObject onGet(BinaryObject key, @Nullable BinaryObject val) {
            System.out.println("Get [key=" + key + ", val=" + val + ']');

            onGet++;

            assertEquals(1, (int)key.field("key"));
            assertEquals(10, (int)val.field("val"));

            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public BinaryObject onBeforePut(Cache.Entry<BinaryObject, BinaryObject> entry, BinaryObject newVal) {
            System.out.println("Before put [e=" + entry + ", newVal=" + newVal + ']');

            onBeforePut++;

            if (entry.getValue() != null)
                assertEquals(10, (int)entry.getValue().field("val"));

            assertEquals(1, (int)entry.getKey().field("key"));
            assertEquals(10, (int)newVal.field("val"));

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<BinaryObject, BinaryObject> entry) {
            System.out.println("After put [e=" + entry + ']');

            onAfterPut++;

            assertEquals(1, (int)entry.getKey().field("key"));
            assertEquals(10, (int)entry.getValue().field("val"));
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, BinaryObject> onBeforeRemove(Cache.Entry<BinaryObject, BinaryObject> entry) {
            assertEquals(1, (int)entry.getKey().field("key"));
            assertEquals(10, (int)entry.getValue().field("val"));

            onBeforeRmv++;

            return new IgniteBiTuple<>(false, entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<BinaryObject, BinaryObject> entry) {
            System.out.println("After remove [e=" + entry + ']');

            onAfterRmv++;

            assertEquals(1, (int)entry.getKey().field("key"));
            assertEquals(10, (int)entry.getValue().field("val"));
        }
    }

    /**
     *
     */
    static class TestInterceptor2 implements CacheInterceptor<Integer, Integer> {
        /** */
        static int onGet;

        /** */
        static int onBeforePut;

        /** */
        static int onAfterPut;

        /** */
        static int onBeforeRmv;

        /** */
        static int onAfterRmv;

        /** {@inheritDoc} */
        @Nullable @Override public Integer onGet(Integer key, @Nullable Integer val) {
            System.out.println("Get [key=" + key + ", val=" + val + ']');

            onGet++;

            assertEquals((Integer)1, key);
            assertEquals((Integer)10, val);

            return val;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer onBeforePut(Cache.Entry<Integer, Integer> entry, Integer newVal) {
            System.out.println("Before put [e=" + entry + ", newVal=" + newVal + ']');

            onBeforePut++;

            if (entry.getValue() != null)
                assertEquals((Integer)10, entry.getValue());

            assertEquals((Integer)1, entry.getKey());
            assertEquals((Integer)10, newVal);

            return newVal;
        }

        /** {@inheritDoc} */
        @Override public void onAfterPut(Cache.Entry<Integer, Integer> entry) {
            System.out.println("After put [e=" + entry + ']');

            onAfterPut++;

            assertEquals((Integer)1, entry.getKey());
            assertEquals((Integer)10, entry.getValue());
        }

        /** {@inheritDoc} */
        @Nullable @Override public IgniteBiTuple<Boolean, Integer> onBeforeRemove(Cache.Entry<Integer, Integer> entry) {
            assertEquals((Integer)1, entry.getKey());
            assertEquals((Integer)10, entry.getValue());

            onBeforeRmv++;

            return new IgniteBiTuple<>(false, entry.getValue());
        }

        /** {@inheritDoc} */
        @Override public void onAfterRemove(Cache.Entry<Integer, Integer> entry) {
            System.out.println("After remove [e=" + entry + ']');

            onAfterRmv++;

            assertEquals((Integer)1, entry.getKey());
            assertEquals((Integer)10, entry.getValue());
        }
    }

    /**
     *
     */
    static class TestKey {
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

            TestKey testKey = (TestKey)o;

            return key == testKey.key;

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return key;
        }
    }

    /**
     *
     */
    static class TestValue {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public TestValue(int val) {
            this.val = val;
        }
    }
}
