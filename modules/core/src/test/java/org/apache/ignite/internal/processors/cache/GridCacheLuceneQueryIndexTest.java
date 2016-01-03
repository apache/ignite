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
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class GridCacheLuceneQueryIndexTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheLuceneQueryIndexTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setIncludeEventTypes();
        cfg.setConnectorConfiguration(null);

        CacheConfiguration cacheCfg1 = defaultCacheConfiguration();

        cacheCfg1.setName("local1");
        cacheCfg1.setCacheMode(LOCAL);
        cacheCfg1.setWriteSynchronizationMode(FULL_SYNC);

        CacheConfiguration cacheCfg2 = defaultCacheConfiguration();

        cacheCfg2.setName("local2");
        cacheCfg2.setCacheMode(LOCAL);
        cacheCfg2.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cacheCfg1, cacheCfg2);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 10 * 60 * 1000;
    }

    /**
     * Tests puts one by one.
     *
     * @throws Exception In case of error.
     */
    public void testLuceneIndex() throws Exception {
        final Ignite g = startGrid(0);

        final IgniteCache<Integer, ObjectValue> cache1 = g.cache("local1");
        final IgniteCache<Integer, ObjectValue> cache2 = g.cache("local2");

        final AtomicInteger threadIdxGen = new AtomicInteger();

        final int keyCnt = 10000;

        final IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    int threadIdx = threadIdxGen.getAndIncrement() % 2;

                    for (int i = 0; i < keyCnt; i++) {
                        if (threadIdx == 0)
                            cache1.put(i, new ObjectValue("test full text more" + i));
                        else
                            cache2.put(i, new ObjectValue("test full text more" + i));

                        if (i % 200 == 0)
                            info("Put entries count: " + i);
                    }

                    return null;
                }
            },
            10);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (!fut.isDone()) {
                        Thread.sleep(10000);

//                        ((GridKernal)g).internalCache("local1").context().queries().index().printH2Stats();
//                        ((GridKernal)g).internalCache("local2").context().queries().index().printH2Stats();
                    }

                    return null;
                }
            },
            1);

        fut.get();
        fut1.get();

        assert cache1.size() == keyCnt;
        assert cache2.size() == keyCnt;
    }

    /**
     * Tests with putAll.
     *
     * @throws Exception In case of error.
     */
    public void testLuceneIndex1() throws Exception {
        final Ignite g = startGrid(0);

        final IgniteCache<Integer, ObjectValue> cache1 = g.cache("local1");
        final IgniteCache<Integer, ObjectValue> cache2 = g.cache("local2");

        final AtomicInteger threadIdxGen = new AtomicInteger();

        final int keyCnt = 10000;

        final IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    int threadIdx = threadIdxGen.getAndIncrement() % 2;

                    Map<Integer, ObjectValue> map = new HashMap<>();

                    for (int i = 0; i < keyCnt; i++) {
                        if (i % 200 == 0 && !map.isEmpty()) {
                            if (threadIdx == 0)
                                cache1.putAll(map);
                            else
                                cache2.putAll(map);

                            info("Put entries count: " + i);

                            map = new HashMap<>();
                        }

                        map.put(i, new ObjectValue("String value " + i));
                    }

                    if (!map.isEmpty()) {
                        if (threadIdx == 0)
                            cache1.putAll(map);
                        else
                            cache2.putAll(map);
                    }

                    return null;
                }
            },
            10);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (!fut.isDone()) {
                        Thread.sleep(10000);

//                        ((GridKernal)g).internalCache("local1").context().queries().index().printH2Stats();
//                        ((GridKernal)g).internalCache("local2").context().queries().index().printH2Stats();
                    }

                    return null;
                }
            },
            1);

        fut.get();
        fut1.get();

        assert cache1.size() == keyCnt;
        assert cache2.size() == keyCnt;
    }

    /**
     * Test same value with putAll.
     *
     * @throws Exception In case of error.
     */
    public void testLuceneIndex2() throws Exception {
        final Ignite g = startGrid(0);

        final IgniteCache<Integer, ObjectValue> cache1 = g.cache("local1");
        final IgniteCache<Integer, ObjectValue> cache2 = g.cache("local2");

        final AtomicInteger threadIdxGen = new AtomicInteger();

        final int keyCnt = 10000;

        final ObjectValue val = new ObjectValue("String value");

        final IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    int threadIdx = threadIdxGen.getAndIncrement() % 2;

                    Map<Integer, ObjectValue> map = new HashMap<>();

                    for (int i = 0; i < keyCnt; i++) {
                        if (i % 200 == 0 && !map.isEmpty()) {
                            if (threadIdx == 0)
                                cache1.putAll(map);
                            else
                                cache2.putAll(map);

                            info("Put entries count: " + i);

                            map = new HashMap<>();
                        }

                        map.put(i, val);
                    }

                    if (!map.isEmpty()) {
                        if (threadIdx == 0)
                            cache1.putAll(map);
                        else
                            cache2.putAll(map);
                    }

                    return null;
                }
            },
            10);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (!fut.isDone()) {
                        Thread.sleep(10000);

//                        ((GridKernal)g).internalCache("local1").context().queries().index().printH2Stats();
//                        ((GridKernal)g).internalCache("local2").context().queries().index().printH2Stats();
                    }

                    return null;
                }
            },
            1);

        fut.get();
        fut1.get();

        assert cache1.size() == keyCnt;
        assert cache2.size() == keyCnt;
    }

    /**
     * Test limited values set and custom keys with putAll.
     *
     * @throws Exception In case of error.
     */
    public void testLuceneIndex3() throws Exception {
        final Ignite g = startGrid(0);

        final IgniteCache<ObjectKey, ObjectValue> cache1 = g.cache("local1");
        final IgniteCache<ObjectKey, ObjectValue> cache2 = g.cache("local2");

        final AtomicInteger threadIdxGen = new AtomicInteger();

        final int keyCnt = 10000;

        final ObjectValue[] vals = new ObjectValue[10];

        for (int i = 0; i < vals.length; i++)
            vals[i] = new ObjectValue("Object value " + i);

        final IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    int threadIdx = threadIdxGen.getAndIncrement() % 2;

                    Map<ObjectKey, ObjectValue> map = new HashMap<>();

                    for (int i = 0; i < keyCnt; i++) {
                        if (i % 200 == 0 && !map.isEmpty()) {
                            if (threadIdx == 0)
                                cache1.putAll(map);
                            else
                                cache2.putAll(map);

                            info("Put entries count: " + i);

                            map = new HashMap<>();
                        }

                        map.put(new ObjectKey(String.valueOf(i)), F.rand(vals));
                    }

                    if (!map.isEmpty()) {
                        if (threadIdx == 0)
                            cache1.putAll(map);
                        else
                            cache2.putAll(map);
                    }

                    return null;
                }
            },
            1);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    while (!fut.isDone()) {
                        Thread.sleep(10000);

//                        ((GridKernal)g).internalCache("local1").context().queries().index().printH2Stats();
//                        ((GridKernal)g).internalCache("local2").context().queries().index().printH2Stats();
                    }

                    return null;
                }
            },
            1);

        fut.get();
        fut1.get();

        assert cache1.size() == keyCnt;
        assert cache2.size() == keyCnt;
    }

    /**
     * Test value object.
     */
    private static class ObjectValue implements Serializable {
        /** String value. */
        @QueryTextField
        private String strVal;

        /**
         * @param strVal String value.
         */
        ObjectValue(String strVal) {
            this.strVal = strVal;
        }

        /**
         * @return Value.
         */
        public String stringValue() {
            return strVal;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectValue other = (ObjectValue)o;

            return strVal == null ? other.strVal == null : strVal.equals(other.strVal);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return strVal != null ? strVal.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ObjectValue.class, this);
        }
    }

    /**
     * Test value key.
     */
    private static class ObjectKey implements Serializable {
        /** String key. */
        @QueryTextField
        private String strKey;

        /**
         * @param strKey String key.
         */
        ObjectKey(String strKey) {
            this.strKey = strKey;
        }

        /**
         * @return Key.
         */
        public String stringKey() {
            return strKey;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ObjectKey other = (ObjectKey)o;

            return strKey == null ? other.strKey == null : strKey.equals(other.strKey);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return strKey != null ? strKey.hashCode() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ObjectKey.class, this);
        }
    }
}