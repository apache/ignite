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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
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
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Base test for all multithreaded cache scenarios w/ and w/o failover.
 */
public class GridCacheMultithreadedFailoverAbstractTest extends GridCommonAbstractTest {
    /** Node name prefix. */
    private static final String NODE_PREFIX = "node";

    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Put condition lock (used to suspend put threads when caches are compared). */
    private final Lock lock = new ReentrantLock();

    /** Node kill lock (used to prevent killing while cache data is compared). */
    private final Lock killLock = new ReentrantLock();

    /** Proceed put condition. */
    private final Condition putCond = lock.newCondition();

    /** Shared IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Caches comparison start latch. */
    private CountDownLatch cmpLatch;

    /** Caches comparison request flag. */
    private volatile boolean cmp;

    /**
     * @return Number of threads executing put.
     */
    protected int putThreads() {
        return 15;
    }

    /**
     * @return Test duration in seconds.
     */
    protected int duration() {
        return 3 * 60 * 1000;
    }

    /**
     * @return Frequency of cache data comparison.
     */
    protected int cacheComparisonFrequency() {
        return 20 * 1000;
    }

    /**
     * @return Put key range.
     */
    protected int keyRange() {
        return 10_000;
    }

    /**
     * @return Cache mode.
     */
    protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @return Cache atomicity mode.
     */
    protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /**
     * @return Atomic write order mode.
     */
    protected CacheAtomicWriteOrderMode atomicWriteOrderMode() {
        return null;
    }

    /**
     * @return Number of data nodes.
     */
    protected int dataNodes() {
        return 3;
    }

    /**
     * @return Number of backups.
     */
    protected int backups() {
        return 1;
    }

    /**
     * @return Probability of killing data node.
     */
    protected int nodeKillProbability() {
        return 1;
    }

    /**
     * @return Min and max value for delay between node killings.
     */
    protected T2<Long, Long> killDelay() {
        return new T2<>(5000L, 10000L);
    }

    /**
     * @return Min and max value for delay between node killing and restarting.
     */
    protected T2<Long, Long> restartDelay() {
        return new T2<>(5000L, 10000L);
    }

    /**
     * Get node name by index.
     *
     * @param idx Node index.
     * @return Node name.
     */
    private String nodeName(int idx) {
        return NODE_PREFIX + idx;
    }

    /**
     * Start up routine.
     *
     * @throws Exception If failed.
     */
    private void startUp() throws Exception {
        assert dataNodes() > 0;
        assert cacheMode() != null;
        assert atomicityMode() != null;

        for (int i = 0; i < dataNodes(); i++)
            G.start(configuration(i));
    }

    /**
     * Node configuration.
     *
     * @param idx Node index.
     * @return Node configuration.
     * @throws Exception If failed.
     */
    private IgniteConfiguration configuration(int idx) throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setRebalanceMode(SYNC);
        ccfg.setSwapEnabled(false);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setEvictionPolicy(null);

        if (cacheMode() == PARTITIONED)
            ccfg.setBackups(backups());

        if (atomicityMode() == ATOMIC) {
            assert atomicWriteOrderMode() != null;

            ccfg.setAtomicWriteOrderMode(atomicWriteOrderMode());
        }
        else {
            if (cacheMode() == PARTITIONED)
                ccfg.setNearConfiguration(new NearCacheConfiguration());
        }

        IgniteConfiguration cfg = getConfiguration(nodeName(idx));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);
        cfg.setLocalHost("127.0.0.1");
        cfg.setCacheConfiguration(ccfg);
        cfg.setConnectorConfiguration(null);

        return cfg;
    }

    /**
     * Actual test.
     *
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        startUp();

        final CyclicBarrier startBarrier = new CyclicBarrier(putThreads());

        final Map<Integer, Integer> expVals = new ConcurrentHashMap<>();

        final int keysPerThread = keyRange() / putThreads();

        final AtomicLong ctr = new AtomicLong();
        final AtomicLong errCtr = new AtomicLong();
        final AtomicBoolean stop = new AtomicBoolean();

        assert keysPerThread > 0;

        Thread[] putThreads = new Thread[putThreads()];

        for (int i = 0; i < putThreads(); i++) {
            final int idx = i;

            Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    try {
                        startBarrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException ignore) {
                        return ;
                    }

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    Ignite ignite = G.ignite(nodeName(0));

                    IgniteCache<Integer, Integer> cache = ignite.cache(CACHE_NAME);

                    int startKey = keysPerThread * idx;
                    int endKey = keysPerThread * (idx + 1);

                    Map<Integer, Integer> putMap = new HashMap<>();
                    Set<Integer> rmvSet = new HashSet<>();

                    while (!stop.get()) {
                        for (int i = 0; i < 100; i++) {
                            int key = rnd.nextInt(startKey, endKey);

                            if (rnd.nextInt(0, 10) > 0) {
                                putMap.put(key, i);

                                rmvSet.remove(key);
                            }
                            else {
                                rmvSet.add(key);

                                putMap.remove(key);
                            }
                        }
                        try {
                            Transaction tx = atomicityMode() == TRANSACTIONAL ? ignite.transactions().txStart() : null;

                            try {
                                cache.putAll(putMap);
                                cache.removeAll(rmvSet);

                                if (tx != null)
                                    tx.commit();
                            }
                            finally {
                                if (tx != null)
                                    tx.close();
                            }

                            expVals.putAll(putMap);

                            for (Integer key : rmvSet)
                                expVals.remove(key);
                        }
                        catch (Exception e) {
                            log.error("Cache update failed [putMap=" + putMap+ ", rmvSet=" + rmvSet + ']', e);

                            errCtr.incrementAndGet();
                        }

                        ctr.addAndGet(putMap.size() + rmvSet.size());

                        try {
                            if (cmp) {
                                cmpLatch.countDown();

                                lock.lock();

                                try {
                                    while (cmp)
                                        putCond.await();
                                }
                                finally {
                                    lock.unlock();
                                }
                            }
                        }
                        catch (InterruptedException ignore) {
                            return;
                        }
                    }
                }
            });

            thread.setName("put-thread-" + i);

            thread.start();

            putThreads[i] = thread;
        }

        IgniteInternalFuture<?> killNodeFut = null;

        if (nodeKillProbability() > 0) {
            killNodeFut = GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    while (!stop.get()) {
                        U.sleep(ThreadLocalRandom.current().nextLong(killDelay().get1(), killDelay().get2()));

                        killLock.lock();

                        try {
                            int idx = ThreadLocalRandom.current().nextInt(1, dataNodes());

                            String gridName = nodeName(idx);

                            if (stop.get())
                                return null;

                            log.info("Killing node [gridName=" + gridName + ']');

                            stopGrid(gridName);

                            U.sleep(ThreadLocalRandom.current().nextLong(restartDelay().get1(), restartDelay().get2()));

                            if (stop.get())
                                return null;

                            log.info("Restarting node [gridName=" + gridName + ']');

                            G.start(configuration(idx));
                        }
                        finally {
                            killLock.unlock();
                        }
                    }

                    return null;
                }
            });
        }

        boolean failed = false;

        try {
            long stopTime = U.currentTimeMillis() + duration();

            long nextCmp = U.currentTimeMillis() + cacheComparisonFrequency();

            while (!failed && U.currentTimeMillis() < stopTime) {
                long start = System.nanoTime();

                long ops = ctr.longValue();

                U.sleep(1000);

                long diff = ctr.longValue() - ops;

                double time = (System.nanoTime() - start) / 1_000_000_000d;

                long opsPerSecond = (long)(diff / time);

                log.info("Operations/second: " + opsPerSecond);

                if (U.currentTimeMillis() >= nextCmp) {
                    failed = !compare(expVals);

                    nextCmp = System.currentTimeMillis() + cacheComparisonFrequency();
                }
            }
        }
        finally {
            stop.set(true);
        }

        if (killNodeFut != null)
            killNodeFut.get();

        for (Thread thread : putThreads)
            U.join(thread);

        log.info("Test finished. Put errors: " + errCtr.get());

        assertFalse("Test failed", failed);
    }

    /**
     * Compare cache content.
     *
     * @param expVals Expected values.
     * @return {@code True} if check passed successfully.
     * @throws Exception If failed.
     */
    private boolean compare(Map<Integer, Integer> expVals) throws Exception {
        cmpLatch = new CountDownLatch(putThreads());

        cmp = true;

        killLock.lock();

        try {
            log.info("Comparing cache content.");

            if (!cmpLatch.await(60_000, TimeUnit.MILLISECONDS))
                throw new IgniteCheckedException("Failed to suspend threads executing put.");

            if (compareCaches(expVals)) {
                log.info("Cache comparison succeeded.");

                return true;
            }
            else {
                log.error("Cache comparison failed.");

                return false;
            }
        }
        finally {
            killLock.unlock();

            lock.lock();

            try {
                cmp = false;

                putCond.signalAll();
            }
            finally {
                lock.unlock();
            }

            U.sleep(500);
        }
    }

    /**
     * Compare caches.
     *
     * @param expVals Expected values.
     * @return {@code True} if check passed successfully.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope", "ConstantIfStatement"})
    private boolean compareCaches(Map<Integer, Integer> expVals) throws Exception {
        List<IgniteCache<Integer, Integer>> caches = new ArrayList<>(dataNodes());
        List<GridDhtCacheAdapter<Integer, Integer>> dhtCaches = null;

        for (int i = 0 ; i < dataNodes(); i++) {
            IgniteCache<Integer, Integer> cache = G.ignite(nodeName(i)).cache(CACHE_NAME);

            assert cache != null;

            caches.add(cache);

            GridCacheAdapter<Integer, Integer> cache0 =
                (GridCacheAdapter<Integer, Integer>)((IgniteKernal)cache.unwrap(Ignite.class))
                    .<Integer, Integer>getCache(CACHE_NAME);

            if (cache0.isNear()) {
                if (dhtCaches == null)
                    dhtCaches = new ArrayList<>(dataNodes());

                dhtCaches.add(((GridNearCacheAdapter<Integer, Integer>)cache0).dht());
            }
        }

        // Compare key sets on each cache.
        Collection<Integer> cacheKeys = new HashSet<>();
        Collection<Integer> dhtCacheKeys = new HashSet<>();

        for (int i = 0; i < dataNodes(); i++) {
            for (Cache.Entry<Integer, Integer> entry : caches.get(i))
                cacheKeys.add(entry.getKey());

            if (dhtCaches != null)
                dhtCacheKeys.addAll(dhtCaches.get(i).keySet());
        }

        boolean failed = false;

        if (!F.eq(expVals.keySet(), cacheKeys)) {
            Collection<Integer> expOnly = new HashSet<>();
            Collection<Integer> cacheOnly = new HashSet<>();

            expOnly.addAll(expVals.keySet());
            expOnly.removeAll(cacheKeys);

            cacheOnly.addAll(cacheKeys);
            cacheOnly.removeAll(expVals.keySet());

            if (!expOnly.isEmpty())
                log.error("Cache does not contain expected keys: " + expOnly);

            if (!cacheOnly.isEmpty())
                log.error("Cache does contain unexpected keys: " + cacheOnly);

            failed = true;
        }

        if (dhtCaches != null && !F.eq(expVals.keySet(), dhtCacheKeys)) {
            Collection<Integer> expOnly = new HashSet<>();
            Collection<Integer> cacheOnly = new HashSet<>();

            expOnly.addAll(expVals.keySet());
            expOnly.removeAll(dhtCacheKeys);

            cacheOnly.addAll(dhtCacheKeys);
            cacheOnly.removeAll(expVals.keySet());

            if (!expOnly.isEmpty())
                log.error("DHT cache does not contain expected keys: " + expOnly);

            if (!cacheOnly.isEmpty())
                log.error("DHT cache does contain unexpected keys: " + cacheOnly);

            failed = true;
        }

        // Compare values.
        Collection<Integer> failedKeys = new HashSet<>();

        for (Map.Entry<Integer, Integer> entry : expVals.entrySet()) {
            for (int i = 0; i < dataNodes(); i++) {
                if (!F.eq(caches.get(i).get(entry.getKey()), entry.getValue()))
                    failedKeys.add(entry.getKey());
            }
        }

        if (!failedKeys.isEmpty()) {
            log.error("Cache content is incorrect for " + failedKeys.size() + " keys:");

            for (Integer key : failedKeys) {
                for (int i = 0; i < dataNodes(); i++) {
                    IgniteCache<Integer, Integer> cache = caches.get(i);

                    UUID nodeId = G.ignite(nodeName(i)).cluster().localNode().id();

                    if (!F.eq(cache.get(key), expVals.get(key)))
                        log.error("key=" + key + ", expVal=" + expVals.get(key) + ", nodeId=" + nodeId);
                }
            }

            failed = true;
        }

        return !failed;
    }
}