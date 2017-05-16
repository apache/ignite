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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheGroupsTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String GROUP1 = "grp1";

    /** */
    private static final String GROUP2 = "grp2";

    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        //cfg.setLateAffinityAssignment(false);

        cfg.setClientMode(client);

        cfg.setMarshaller(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCloseCache1() throws Exception {
        startGrid(0);

        client = true;

        Ignite client = startGrid(1);

        IgniteCache c1 = client.createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 0, false));

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(0, GROUP1, true);

        checkCache(0, "c1");
        checkCache(1, "c1");

        c1.close();

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(1, GROUP1, false);

        checkCache(0, "c1");

        assertNotNull(client.cache("c1"));

        checkCacheGroup(0, GROUP1, true);
        checkCacheGroup(1, GROUP1, true);

        checkCache(0, "c1");
        checkCache(1, "c1");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCaches1() throws Exception {
        createDestroyCaches(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateDestroyCaches2() throws Exception {
        createDestroyCaches(5);
    }

    /**
     * @param srvs Number of server nodes.
     * @throws Exception If failed.
     */
    private void createDestroyCaches(int srvs) throws Exception {
        startGridsMultiThreaded(srvs);

        Ignite srv0 = ignite(0);

        for (int i = 0; i < srvs; i++)
            checkCacheGroup(i, GROUP1, false);

        for (int iter = 0; iter < 3; iter++) {
            log.info("Iteration: " + iter);

            srv0.createCache(cacheConfiguration(GROUP1, "cache1", PARTITIONED, ATOMIC, 2, false));

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, "cache1");
            }

            srv0.createCache(cacheConfiguration(GROUP1, "cache2", PARTITIONED, ATOMIC, 2, false));

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, "cache2");
            }

            srv0.destroyCache("cache1");

            for (int i = 0; i < srvs; i++) {
                checkCacheGroup(i, GROUP1, true);

                checkCache(i, "cache2");
            }

            srv0.destroyCache("cache2");

            for (int i = 0; i < srvs; i++)
                checkCacheGroup(i, GROUP1, false);
        }
    }

    /**
     * @param idx Node index.
     * @param cacheName Cache name.
     */
    private void checkCache(int idx, String cacheName) {
        IgniteCache cache = ignite(idx).cache(cacheName);

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10; i++) {
            Integer key = rnd.nextInt();

            cache.put(key, i);

            assertEquals(i, cache.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCache1() throws Exception {
        Ignite srv0 = startGrid(0);

        {
            IgniteCache<Object, Object> cache1 =
                srv0.createCache(cacheConfiguration("grp1", "cache1", PARTITIONED, ATOMIC, 2, false));
            IgniteCache<Object, Object> cache2 =
                srv0.createCache(cacheConfiguration("grp1", "cache2", PARTITIONED, ATOMIC, 2, false));

            cache1.put(new Key1(1), 1);
            assertEquals(1, cache1.get(new Key1(1)));

            assertEquals(1, cache1.size());
            assertEquals(0, cache2.size());
            //assertFalse(cache2.iterator().hasNext());

            cache2.put(new Key2(1), 2);
            assertEquals(2, cache2.get(new Key2(1)));

            assertEquals(1, cache1.size());
            assertEquals(1, cache2.size());
        }

        Ignite srv1 = startGrid(1);

        awaitPartitionMapExchange();

        IgniteCache<Object, Object> cache1 = srv1.cache("cache1");
        IgniteCache<Object, Object> cache2 = srv1.cache("cache2");

        assertEquals(1, cache1.localPeek(new Key1(1)));
        assertEquals(2, cache2.localPeek(new Key2(1)));

        assertEquals(1, cache1.localSize());
        assertEquals(1, cache2.localSize());
    }

    /**
     * @throws Exception If failed.
     */
    public void testCreateCache2() throws Exception {
        Ignite srv0 = startGrid(0);

        {
            IgniteCache<Object, Object> cache1 =
                srv0.createCache(cacheConfiguration(GROUP1, "cache1", PARTITIONED, ATOMIC, 0, false));
            IgniteCache<Object, Object> cache2 =
                srv0.createCache(cacheConfiguration(GROUP1, "cache2", PARTITIONED, ATOMIC, 0, false));

            for (int i = 0; i < 10; i++) {
                cache1.put(new Key1(i), 1);
                cache2.put(new Key2(i), 2);
            }
        }

        Ignite srv1 = startGrid(1);

        awaitPartitionMapExchange();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoKeyIntersectTx() throws Exception {
        testNoKeyIntersect(TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoKeyIntersectAtomic() throws Exception {
        testNoKeyIntersect(ATOMIC);
    }

    /**
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersect(CacheAtomicityMode atomicityMode) throws Exception {
        startGrid(0);

        testNoKeyIntersect(atomicityMode, false);

        testNoKeyIntersect(atomicityMode, true);

        startGridsMultiThreaded(1, 4);

        testNoKeyIntersect(atomicityMode, false);

        testNoKeyIntersect(atomicityMode, true);
    }

    /**
     * @param keys Keys.
     * @param rnd Random.
     * @return Added key.
     */
    private Integer addKey(Set<Integer> keys, ThreadLocalRandom rnd) {
        for (;;) {
            Integer key = rnd.nextInt(100_000);

            if (keys.add(key))
                return key;
        }
    }

    /**
     * @param atomicityMode Cache atomicity mode.
     * @param heapCache On heap cache flag.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersect(CacheAtomicityMode atomicityMode, boolean heapCache) throws Exception {
        Ignite srv0 = ignite(0);

        try {
            IgniteCache cache1 = srv0.
                createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, atomicityMode, 1, heapCache));

            Set<Integer> keys = new LinkedHashSet<>(30);

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < 10; i++) {
                Integer key = addKey(keys, rnd);

                cache1.put(key, key);
                cache1.put(new Key1(key), new Value1(key));
                cache1.put(new Key2(key), new Value2(key));
            }

            assertEquals(30, cache1.size());

            IgniteCache cache2 = srv0.
                createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, atomicityMode, 1, heapCache));

            assertEquals(30, cache1.size());
            assertEquals(0 , cache2.size());

            for (Integer key : keys) {
                assertNull(cache2.get(key));
                assertNull(cache2.get(new Key1(key)));
                assertNull(cache2.get(new Key2(key)));

                cache2.put(key, key + 1);
                cache2.put(new Key1(key), new Value1(key + 1));
                cache2.put(new Key2(key), new Value2(key + 1));
            }

            assertEquals(30, cache1.size());
            assertEquals(30, cache2.size());

            for (int i = 0; i < 10; i++) {
                Integer key = addKey(keys, rnd);

                cache2.put(key, key + 1);
                cache2.put(new Key1(key), new Value1(key + 1));
                cache2.put(new Key2(key), new Value2(key + 1));
            }

            assertEquals(30, cache1.size());
            assertEquals(60, cache2.size());

            int i = 0;

            for (Integer key : keys) {
                if (i++ < 10) {
                    assertEquals(key, cache1.get(key));
                    assertEquals(new Value1(key), cache1.get(new Key1(key)));
                    assertEquals(new Value2(key), cache1.get(new Key2(key)));
                }
                else {
                    assertNull(cache1.get(key));
                    assertNull(cache1.get(new Key1(key)));
                    assertNull(cache1.get(new Key2(key)));
                }

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));
            }

            IgniteCache cache3 = srv0.
                createCache(cacheConfiguration(GROUP1, "c3", PARTITIONED, atomicityMode, 1, heapCache));

            assertEquals(30, cache1.size());
            assertEquals(60, cache2.size());
            assertEquals(0, cache3.size());

            for (Integer key : keys) {
                assertNull(cache3.get(key));
                assertNull(cache3.get(new Key1(key)));
                assertNull(cache3.get(new Key2(key)));
            }

            for (Integer key : keys) {
                cache3.put(key, key);
                cache3.put(new Key1(key), new Value1(key));
                cache3.put(new Key2(key), new Value2(key));
            }

            i = 0;

            for (Integer key : keys) {
                if (i++ < 10) {
                    assertEquals(key, cache1.get(key));
                    assertEquals(new Value1(key), cache1.get(new Key1(key)));
                    assertEquals(new Value2(key), cache1.get(new Key2(key)));
                }
                else {
                    assertNull(cache1.get(key));
                    assertNull(cache1.get(new Key1(key)));
                    assertNull(cache1.get(new Key2(key)));
                }

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            i = 0;

            for (Integer key : keys) {
                if (i++ == 3)
                    break;

                cache1.remove(key);
                cache1.remove(new Key1(key));
                cache1.remove(new Key2(key));

                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            cache1.removeAll();

            for (Integer key : keys) {
                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertEquals(key + 1, cache2.get(key));
                assertEquals(new Value1(key + 1), cache2.get(new Key1(key)));
                assertEquals(new Value2(key + 1), cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            cache2.removeAll();

            for (Integer key : keys) {
                assertNull(cache1.get(key));
                assertNull(cache1.get(new Key1(key)));
                assertNull(cache1.get(new Key2(key)));

                assertNull(cache2.get(key));
                assertNull(cache2.get(new Key1(key)));
                assertNull(cache2.get(new Key2(key)));

                assertEquals(key, cache3.get(key));
                assertEquals(new Value1(key), cache3.get(new Key1(key)));
                assertEquals(new Value2(key), cache3.get(new Key2(key)));
            }

            if (atomicityMode == TRANSACTIONAL)
                testNoKeyIntersectTxLocks(cache1, cache2);
        }
        finally {
            srv0.destroyCaches(Arrays.asList("c1", "c2", "c3"));
        }
    }

    /**
     * @param cache1 Cache1.
     * @param cache2 Cache2.
     * @throws Exception If failed.
     */
    private void testNoKeyIntersectTxLocks(final IgniteCache cache1, final IgniteCache cache2) throws Exception {
        final Ignite node = (Ignite)cache1.unwrap(Ignite.class);

        for (int i = 0; i < 5; i++) {
            final Integer key = ThreadLocalRandom.current().nextInt(1000);

            Lock lock = cache1.lock(key);

            lock.lock();

            try {
                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        Lock lock1 = cache1.lock(key);

                        assertFalse(lock1.tryLock());

                        Lock lock2 = cache2.lock(key);

                        assertTrue(lock2.tryLock());

                        lock2.unlock();

                        return null;
                    }
                }, "lockThread");

                fut.get(10_000);
            }
            finally {
                lock.unlock();
            }

            try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache1.put(key, 1);

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache2.put(key, 2);

                            tx.commit();
                        }

                        assertEquals(2, cache2.get(key));

                        return null;
                    }
                }, "txThread");

                fut.get(10_000);

                tx.commit();
            }

            assertEquals(1, cache1.get(key));
            assertEquals(2, cache2.get(key));

            try (Transaction tx = node.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                Integer val = (Integer)cache1.get(key);

                cache1.put(key, val + 10);

                IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
                    @Override public Object call() throws Exception {
                        try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            cache2.put(key, 3);

                            tx.commit();
                        }

                        assertEquals(3, cache2.get(key));

                        return null;
                    }
                }, "txThread");

                fut.get(10_000);

                tx.commit();
            }

            assertEquals(11, cache1.get(key));
            assertEquals(3, cache2.get(key));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiTxPartitioned() throws Exception {
        cacheApiTest(PARTITIONED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiTxReplicated() throws Exception {
        cacheApiTest(REPLICATED, TRANSACTIONAL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiAtomicPartitioned() throws Exception {
        cacheApiTest(PARTITIONED, ATOMIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheApiAtomicReplicated() throws Exception {
        cacheApiTest(REPLICATED, ATOMIC);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @throws Exception If failed.
     */
    private void cacheApiTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode) throws Exception {
        startGridsMultiThreaded(4);

        client = true;

        startGrid(4);

        int[] backups = cacheMode == REPLICATED ? new int[]{Integer.MAX_VALUE} : new int[]{0, 1, 2, 3};

        for (int backups0 : backups)
            cacheApiTest(cacheMode, atomicityMode, backups0, false);

        int backups0 = cacheMode == REPLICATED ? Integer.MAX_VALUE :
            backups[ThreadLocalRandom.current().nextInt(backups.length)];

        cacheApiTest(cacheMode, atomicityMode, backups0, true);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Number of backups.
     * @param heapCache On heap cache flag.
     */
    private void cacheApiTest(CacheMode cacheMode, CacheAtomicityMode atomicityMode, int backups, boolean heapCache) {
        for (int i = 0; i < 2; i++)
            ignite(0).createCache(cacheConfiguration(GROUP1, "cache-" + i, cacheMode, atomicityMode, backups, heapCache));

        try {
            for (Ignite node : Ignition.allGrids()) {
                for (int i = 0; i < 2; i++) {
                    IgniteCache cache = node.cache("cache-" + i);

                    log.info("Test cache [node=" + node.name() +
                        ", cache=" + cache.getName() +
                        ", mode=" + cacheMode +
                        ", atomicity=" + atomicityMode +
                        ", backups=" + backups +
                        ", heapCache=" + heapCache +
                        ']');

                    cacheApiTest(cache);
                }
            }
        }
        finally {
            for (int i = 0; i < 2; i++)
                ignite(0).destroyCache("cache-" + i);
        }
    }

    /**
     * @param cache Cache.
     */
    private void cacheApiTest(IgniteCache cache) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 10; i++) {
            Integer key = rnd.nextInt(10_000);

            assertNull(cache.get(key));
            assertFalse(cache.containsKey(key));

            Integer val = key + 1;

            cache.put(key, val);

            assertEquals(val, cache.get(key));
            assertTrue(cache.containsKey(key));

            cache.remove(key);

            assertNull(cache.get(key));
            assertFalse(cache.containsKey(key));
        }

        cache.clear();

        cache.removeAll();
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentOperationsSameKeys() throws Exception {
        final int SRVS = 4;
        final int CLIENTS = 4;
        final int NODES = SRVS + CLIENTS;

        startGrid(0);

        Ignite srv0 = startGridsMultiThreaded(1, SRVS - 1);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        srv0.createCache(cacheConfiguration(GROUP1, "a0", PARTITIONED, ATOMIC, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "a1", PARTITIONED, ATOMIC, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "t0", PARTITIONED, TRANSACTIONAL, 1, false));
        srv0.createCache(cacheConfiguration(GROUP1, "t1", PARTITIONED, TRANSACTIONAL, 1, false));

        final List<Integer> keys = new ArrayList<>();

        for (int i = 0; i < 50; i++)
            keys.add(i);

        final AtomicBoolean err = new AtomicBoolean();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture fut1 = updateFuture(NODES, "a0", keys, false, stop, err);
        IgniteInternalFuture fut2 = updateFuture(NODES, "a1", keys, true, stop, err);
        IgniteInternalFuture fut3 = updateFuture(NODES, "t0", keys, false, stop, err);
        IgniteInternalFuture fut4 = updateFuture(NODES, "t1", keys, true, stop, err);

        try {
            for (int i = 0; i < 15 && !stop.get(); i++)
                U.sleep(1_000);
        }
        finally {
            stop.set(true);
        }

        fut1.get();
        fut2.get();
        fut3.get();
        fut4.get();

        assertFalse("Unexpected error, see log for details", err.get());
    }

    /**
     * @param nodes Total number of nodes.
     * @param cacheName Cache name.
     * @param keys Keys to update.
     * @param reverse {@code True} if update in reverse order.
     * @param stop Stop flag.
     * @param err Error flag.
     * @return Update future.
     */
    private IgniteInternalFuture updateFuture(final int nodes,
        final String cacheName,
        final List<Integer> keys,
        final boolean reverse,
        final AtomicBoolean stop,
        final AtomicBoolean err) {
        final AtomicInteger idx = new AtomicInteger();

        return GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                try {
                    Ignite node = ignite(idx.getAndIncrement() % nodes);

                    log.info("Start thread [node=" + node.name() + ']');

                    IgniteCache cache = node.cache(cacheName);

                    Map<Integer, Integer> map = new LinkedHashMap<>();

                    if (reverse) {
                        for (int i = keys.size() - 1; i >= 0; i--)
                            map.put(keys.get(i), 2);
                    }
                    else {
                        for (Integer key : keys)
                            map.put(key, 1);
                    }

                    while (!stop.get())
                        cache.putAll(map);
                }
                catch (Exception e) {
                    err.set(true);

                    log.error("Unexpected error: " + e, e);

                    stop.set(true);
                }

                return null;
            }
        }, nodes * 2, "update-" + cacheName + "-" + reverse);
    }

    /**
     * @throws Exception If failed.
     */
    public void testConcurrentOperationsAndCacheDestroy() throws Exception {
        final int SRVS = 4;
        final int CLIENTS = 4;
        final int NODES = SRVS + CLIENTS;

        startGrid(0);

        Ignite srv0 = startGridsMultiThreaded(1, SRVS - 1);

        client = true;

        startGridsMultiThreaded(SRVS, CLIENTS);

        final int CACHES = 8;

        final int grp1Backups = ThreadLocalRandom.current().nextInt(3);
        final int grp2Backups = ThreadLocalRandom.current().nextInt(3);

        log.info("Start test [grp1Backups=" + grp1Backups + ", grp2Backups=" + grp2Backups + ']');

        for (int i = 0; i < CACHES; i++) {
            srv0.createCache(
                cacheConfiguration(GROUP1, GROUP1 + "-" + i, PARTITIONED, ATOMIC, grp1Backups, i % 2 == 0));

            srv0.createCache(
                cacheConfiguration(GROUP2, GROUP2 + "-" + i, PARTITIONED, TRANSACTIONAL, grp2Backups, i % 2 == 0));
        }

        final AtomicInteger idx = new AtomicInteger();

        final AtomicBoolean err = new AtomicBoolean();

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture opFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                try {
                    Ignite node = ignite(idx.getAndIncrement() % NODES);

                    log.info("Start thread [node=" + node.name() + ']');

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (!stop.get()) {
                        String grp = rnd.nextBoolean() ? GROUP1 : GROUP2;
                        int cacheIdx = rnd.nextInt(CACHES);

                        IgniteCache cache = node.cache(grp + "-" + cacheIdx);

                        for (int i = 0; i < 10; i++)
                            cacheOperation(rnd, cache);
                    }
                }
                catch (Exception e) {
                    err.set(true);

                    log.error("Unexpected error(1): " + e, e);

                    stop.set(true);
                }
            }
        }, (SRVS + CLIENTS) * 2, "op-thread");

        IgniteInternalFuture cacheFut = GridTestUtils.runAsync(new Runnable() {
            @Override public void run() {
                try {
                    int cntr = 0;

                    while (!stop.get()) {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        String grp;
                        int backups;

                        if (rnd.nextBoolean()) {
                            grp = GROUP1;
                            backups = grp1Backups;
                        }
                        else {
                            grp = GROUP2;
                            backups = grp2Backups;
                        }

                        Ignite node = ignite(rnd.nextInt(NODES));

                        log.info("Create cache [node=" + node.name() + ", grp=" + grp + ']');

                        IgniteCache cache = node.createCache(cacheConfiguration(grp, "tmpCache-" + cntr++,
                            PARTITIONED,
                            rnd.nextBoolean() ? ATOMIC : TRANSACTIONAL,
                            backups,
                            rnd.nextBoolean()));

                        for (int i = 0; i < 10; i++)
                            cacheOperation(rnd, cache);

                        log.info("Destroy cache [node=" + node.name() + ", grp=" + grp + ']');

                        node.destroyCache(cache.getName());
                    }
                }
                catch (Exception e) {
                    err.set(true);

                    log.error("Unexpected error(2): " + e, e);

                    stop.set(true);
                }
            }
        }, "cache-destroy-thread");

        try {
            for (int i = 0; i < 30 && !stop.get(); i++)
                U.sleep(1_000);
        }
        finally {
            stop.set(true);
        }

        opFut.get();
        cacheFut.get();

        assertFalse("Unexpected error, see log for details", err.get());
    }

    /**
     * @throws Exception If failed.
     */
    public void testConfigurationConsistencyValidation() throws Exception {
        startGrids(2);

        client = true;

        startGrid(2);

        ignite(0).createCache(cacheConfiguration(GROUP1, "c1", PARTITIONED, ATOMIC, 1, false));

        for (int i = 0; i < 3; i++) {
            try {
                ignite(i).createCache(cacheConfiguration(GROUP1, "c2", REPLICATED, ATOMIC, Integer.MAX_VALUE, false));

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Cache mode mismatch for caches related to the same group [groupName=grp1"));
            }

            try {
                ignite(i).createCache(cacheConfiguration(GROUP1, "c2", PARTITIONED, ATOMIC, 2, false));

                fail();
            }
            catch (CacheException e) {
                assertTrue("Unexpected message: " + e.getMessage(),
                    e.getMessage().contains("Backups mismatch for caches related to the same group [groupName=grp1"));
            }
        }
    }

    /**
     * @param rnd Random.
     * @param cache Cache.
     */
    private void cacheOperation(ThreadLocalRandom rnd, IgniteCache cache) {
        Object key = rnd.nextInt(1000);

        switch (rnd.nextInt(4)) {
            case 0:
                cache.put(key, 1);

                break;

            case 1:
                cache.get(key);

                break;

            case 2:
                cache.remove(key);

                break;

            case 3:
                cache.localPeek(key);

                break;
        }
    }

    /**
     *
     */
    static class Key1 implements Serializable {
        /** */
        private int id;

        /**
         * @param id ID.
         */
        Key1(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key1 key = (Key1)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Key2 implements Serializable {
        /** */
        private int id;

        /**
         * @param id ID.
         */
        Key2(int id) {
            this.id = id;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Key2 key = (Key2)o;

            return id == key.id;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return id;
        }
    }

    /**
     *
     */
    static class Value1 implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public Value1(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value1 val1 = (Value1)o;

            return val == val1.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     *
     */
    static class Value2 implements Serializable {
        /** */
        private int val;

        /**
         * @param val Value.
         */
        public Value2(int val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            Value2 val2 = (Value2)o;

            return val == val2.val;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return val;
        }
    }

    /**
     * @param grpName Cache group name.
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @param backups Backups number.
     * @param heapCache On heap cache flag.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(
        String grpName,
        String name,
        CacheMode cacheMode,
        CacheAtomicityMode atomicityMode,
        int backups,
        boolean heapCache
    ) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setGroupName(grpName);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setOnheapCacheEnabled(heapCache);

        return ccfg;
    }

    /**
     * @param idx Node index.
     * @param grpName Cache group name.
     * @param expGrp {@code True} if cache group should be created.
     * @throws IgniteCheckedException If failed.
     */
    private void checkCacheGroup(int idx, final String grpName, final boolean expGrp) throws IgniteCheckedException {
        final IgniteKernal node = (IgniteKernal)ignite(idx);

        assertTrue(GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return expGrp == (cacheGroup(node, grpName) != null);
            }
        }, 1000));

        assertNotNull(node.context().cache().cache(CU.UTILITY_CACHE_NAME));
    }

    /**
     * @param node Node.
     * @param grpName Cache group name.
     * @return Cache group.
     */
    private CacheGroupInfrastructure cacheGroup(IgniteKernal node, String grpName) {
        for (CacheGroupInfrastructure grp : node.context().cache().cacheGroups()) {
            if (grpName.equals(grp.name()))
                return grp;
        }

        return null;
    }
}
