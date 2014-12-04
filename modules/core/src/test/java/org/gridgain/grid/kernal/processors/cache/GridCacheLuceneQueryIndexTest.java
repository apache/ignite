/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCacheLuceneQueryIndexTest extends GridCommonAbstractTest {
    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     *
     */
    public GridCacheLuceneQueryIndexTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setIncludeEventTypes();
        cfg.setRestEnabled(false);

        GridCacheConfiguration cacheCfg1 = defaultCacheConfiguration();

        cacheCfg1.setName("local1");
        cacheCfg1.setCacheMode(LOCAL);
        cacheCfg1.setWriteSynchronizationMode(FULL_SYNC);

        GridCacheConfiguration cacheCfg2 = defaultCacheConfiguration();

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

        final GridCache<Integer, ObjectValue> cache1 = g.cache("local1");
        final GridCache<Integer, ObjectValue> cache2 = g.cache("local2");

        final AtomicInteger threadIdxGen = new AtomicInteger();

        final int keyCnt = 10000;

        final GridFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    int threadIdx = threadIdxGen.getAndIncrement() % 2;

                    for (int i = 0; i < keyCnt; i++) {
                        if (threadIdx == 0)
                            cache1.putx(i, new ObjectValue("test full text more" + i));
                        else
                            cache2.putx(i, new ObjectValue("test full text more" + i));

                        if (i % 200 == 0)
                            info("Put entries count: " + i);
                    }

                    return null;
                }
            },
            10);

        GridFuture<?> fut1 = multithreadedAsync(
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

        final GridCache<Integer, ObjectValue> cache1 = g.cache("local1");
        final GridCache<Integer, ObjectValue> cache2 = g.cache("local2");

        final AtomicInteger threadIdxGen = new AtomicInteger();

        final int keyCnt = 10000;

        final GridFuture<?> fut = multithreadedAsync(
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

        GridFuture<?> fut1 = multithreadedAsync(
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

        final GridCache<Integer, ObjectValue> cache1 = g.cache("local1");
        final GridCache<Integer, ObjectValue> cache2 = g.cache("local2");

        final AtomicInteger threadIdxGen = new AtomicInteger();

        final int keyCnt = 10000;

        final ObjectValue val = new ObjectValue("String value");

        final GridFuture<?> fut = multithreadedAsync(
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

        GridFuture<?> fut1 = multithreadedAsync(
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

        final GridCache<ObjectKey, ObjectValue> cache1 = g.cache("local1");
        final GridCache<ObjectKey, ObjectValue> cache2 = g.cache("local2");

        final AtomicInteger threadIdxGen = new AtomicInteger();

        final int keyCnt = 10000;

        final ObjectValue[] vals = new ObjectValue[10];

        for (int i = 0; i < vals.length; i++)
            vals[i] = new ObjectValue("Object value " + i);

        final GridFuture<?> fut = multithreadedAsync(
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

        GridFuture<?> fut1 = multithreadedAsync(
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
        @GridCacheQueryTextField
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
        @GridCacheQueryTextField
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
