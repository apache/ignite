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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheAlwaysEvictionPolicy;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheGetInsideLockChangingTopologyTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static ThreadLocal<Boolean> client = new ThreadLocal<>();

    /** */
    private static final int SRVS = 3;

    /** */
    private static final int CLIENTS = 2;

    /** */
    private static final String TX_CACHE1 = "tx1";

    /** */
    private static final String TX_CACHE2 = "tx2";

    /** */
    private static final String ATOMIC_CACHE = "atomic";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        Boolean clientMode = client.get();

        client.set(null);

        if (clientMode != null && clientMode)
            cfg.setClientMode(true);
        else {
            cfg.setCacheConfiguration(cacheConfiguration(TX_CACHE1, TRANSACTIONAL),
                cacheConfiguration(TX_CACHE2, TRANSACTIONAL),
                cacheConfiguration(ATOMIC_CACHE, ATOMIC));
        }

        return cfg;
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode) {
        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setName(name);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(1);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL, "true");

        super.beforeTestsStarted();

        startGridsMultiThreaded(SRVS);

        client.set(true);

        Ignite client1 = startGrid(SRVS);

        assertTrue(client1.configuration().isClientMode());

        client.set(true);

        Ignite client2 = startGrid(SRVS + 1);

        assertTrue(client2.configuration().isClientMode());

        client2.createNearCache(TX_CACHE1,
            new NearCacheConfiguration<>().setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy<>()));

        client2.createNearCache(TX_CACHE2,
            new NearCacheConfiguration<>().setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy<>()));

        client2.createNearCache(ATOMIC_CACHE,
            new NearCacheConfiguration<>().setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy<>()));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IgniteSystemProperties.IGNITE_ENABLE_FORCIBLE_NODE_KILL);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTxGetInsideLockStopPrimary() throws Exception {
        getInsideLockStopPrimary(ignite(SRVS), TX_CACHE1);
        getInsideLockStopPrimary(ignite(SRVS + 1), TX_CACHE1);

        getInsideLockStopPrimary(ignite(SRVS), TX_CACHE2);
        getInsideLockStopPrimary(ignite(SRVS + 1), TX_CACHE2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicGetInsideLockStopPrimary() throws Exception {
        getInsideLockStopPrimary(ignite(SRVS), ATOMIC_CACHE);

        getInsideLockStopPrimary(ignite(SRVS + 1), ATOMIC_CACHE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAtomicGetInsideTxStopPrimary() throws Exception {
        getInsideTxStopPrimary(ignite(SRVS), ATOMIC_CACHE);

        getInsideTxStopPrimary(ignite(SRVS + 1), ATOMIC_CACHE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadCommittedPessimisticStopPrimary() throws Exception {
        getReadCommittedStopPrimary(ignite(SRVS), TX_CACHE1, PESSIMISTIC);
        getReadCommittedStopPrimary(ignite(SRVS + 1), TX_CACHE1, PESSIMISTIC);

        getReadCommittedStopPrimary(ignite(SRVS), TX_CACHE2, PESSIMISTIC);
        getReadCommittedStopPrimary(ignite(SRVS + 1), TX_CACHE2, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReadCommittedOptimisticStopPrimary() throws Exception {
        getReadCommittedStopPrimary(ignite(SRVS), TX_CACHE1, OPTIMISTIC);
        getReadCommittedStopPrimary(ignite(SRVS + 1), TX_CACHE1, OPTIMISTIC);

        getReadCommittedStopPrimary(ignite(SRVS), TX_CACHE2, OPTIMISTIC);
        getReadCommittedStopPrimary(ignite(SRVS + 1), TX_CACHE2, OPTIMISTIC);
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @param concurrency Transaction concurrency.
     * @throws Exception If failed.
     */
    private void getReadCommittedStopPrimary(Ignite ignite,
        String cacheName,
        TransactionConcurrency concurrency) throws Exception {
        IgniteCache<Integer, Integer> txCache = ignite.cache(TX_CACHE1);

        IgniteCache<Integer, Integer> getCache = ignite.cache(cacheName);

        final int NEW_NODE = SRVS + CLIENTS;

        Ignite srv = startGrid(NEW_NODE);

        awaitPartitionMapExchange();

        try {
            Integer key = primaryKey(srv.cache(cacheName));

            Integer txKey = nearKey(srv.cache(cacheName));

            srv.cache(cacheName).put(key, 1);

            IgniteInternalFuture<?> stopFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    U.sleep(500);

                    log.info("Stop node.");

                    stopGrid(NEW_NODE);

                    log.info("Node stopped.");

                    return null;
                }
            }, "stop-thread");

            try (Transaction tx = ignite.transactions().txStart(concurrency, READ_COMMITTED)) {
                txCache.put(txKey, 1);

                while (!stopFut.isDone())
                    assertEquals(1, (Object)getCache.get(key));

                tx.commit();
            }
        }
        finally {
            stopGrid(NEW_NODE);
        }
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getInsideLockStopPrimary(Ignite ignite, String cacheName) throws Exception {
        IgniteCache<Integer, Integer> lockCache = ignite.cache(TX_CACHE1);

        IgniteCache<Integer, Integer> getCache = ignite.cache(cacheName);

        final int NEW_NODE = SRVS + CLIENTS;

        Ignite srv = startGrid(NEW_NODE);

        awaitPartitionMapExchange();

        try {
            Integer key = primaryKey(srv.cache(cacheName));

            getCache.put(key, 1);

            IgniteInternalFuture<?> stopFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    U.sleep(500);

                    log.info("Stop node.");

                    stopGrid(NEW_NODE);

                    log.info("Node stopped.");

                    return null;
                }
            }, "stop-thread");

            Lock lock = lockCache.lock(key + 1);

            lock.lock();

            try {
                while (!stopFut.isDone())
                    assertEquals(1, (Object)getCache.get(key));
            }
            finally {
                lock.unlock();
            }

            stopFut.get();
        }
        finally {
            stopGrid(NEW_NODE);
        }
    }

    /**
     * @param ignite Node.
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void getInsideTxStopPrimary(Ignite ignite, String cacheName) throws Exception {
        IgniteCache<Integer, Integer> txCache = ignite.cache(TX_CACHE1).withAllowAtomicOpsInTx();

        IgniteCache<Integer, Integer> getCache = ignite.cache(cacheName).withAllowAtomicOpsInTx();

        final int NEW_NODE = SRVS + CLIENTS;

        Ignite srv = startGrid(NEW_NODE);

        awaitPartitionMapExchange();

        try {
            Integer key = primaryKey(srv.cache(cacheName));

            getCache.put(key, 1);

            IgniteInternalFuture<?> stopFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    U.sleep(500);

                    log.info("Stop node.");

                    stopGrid(NEW_NODE);

                    log.info("Node stopped.");

                    return null;
                }
            }, "stop-thread");

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                txCache.get(key + 1);

                while (!stopFut.isDone())
                    assertEquals(1, (Object)getCache.get(key));

                tx.commit();
            }
        }
        finally {
            stopGrid(NEW_NODE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultithreaded() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-2204");

        final AtomicBoolean finished = new AtomicBoolean();

        final int NEW_NODE = SRVS + CLIENTS;

        final AtomicInteger stopIdx = new AtomicInteger();

        IgniteInternalFuture<?> restartFut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int idx = stopIdx.getAndIncrement();

                int node = NEW_NODE + idx;

                while (!finished.get()) {
                    log.info("Start node: " + node);

                    startGrid(node);

                    U.sleep(300);

                    log.info("Stop node: " + node);

                    stopGrid(node);
                }

                return null;
            }
        }, 2, "stop-thread");

        try {
            final long stopTime = System.currentTimeMillis() + 60_000;

            final AtomicInteger idx = new AtomicInteger();

            final int KEYS = 100_000;

            GridTestUtils.runMultiThreaded(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int node = idx.getAndIncrement() % (SRVS + CLIENTS);

                    Ignite ignite = ignite(node);

                    IgniteCache<Integer, Integer> txCache1 = ignite.cache(TX_CACHE1).withAllowAtomicOpsInTx();
                    IgniteCache<Integer, Integer> txCache2 = ignite.cache(TX_CACHE2).withAllowAtomicOpsInTx();
                    IgniteCache<Integer, Integer> atomicCache = ignite.cache(ATOMIC_CACHE).withAllowAtomicOpsInTx();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (U.currentTimeMillis() < stopTime) {
                        Integer lockKey = rnd.nextInt(KEYS, KEYS + 1000);

                        Lock lock = txCache1.lock(lockKey);

                        try {
                            lock.lock();

                            try {
                                executeGet(txCache1);

                                executeGet(txCache2);

                                executeGet(atomicCache);
                            } finally {
                                lock.unlock();
                            }

                            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, READ_COMMITTED)) {
                                txCache1.put(lockKey, lockKey);

                                executeGet(txCache1);

                                executeGet(txCache2);

                                executeGet(atomicCache);

                                tx.commit();
                            }
                        }
                        catch (IgniteException | CacheException e) {
                            log.info("Error: " + e);
                        }
                    }

                    return null;
                }

                private void executeGet(IgniteCache<Integer, Integer> cache) {
                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    for (int i = 0; i < 100; i++)
                        cache.get(rnd.nextInt(KEYS));

                    Set<Integer> keys = new HashSet<>();

                    for (int i = 0; i < 100; i++) {
                        keys.add(rnd.nextInt(KEYS));

                        if (keys.size() == 20) {
                            cache.getAll(keys);

                            keys.clear();
                        }
                    }

                    cache.getAll(keys);
                }
            }, 10, "test-thread");

            finished.set(true);

            restartFut.get();
        }
        finally {
            finished.set(true);
        }
    }
}
