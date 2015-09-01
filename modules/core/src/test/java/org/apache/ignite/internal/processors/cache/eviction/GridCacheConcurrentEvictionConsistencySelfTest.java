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

package org.apache.ignite.internal.processors.cache.eviction;

import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Callable;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cache.eviction.lru.LruEvictionPolicy;
import org.apache.ignite.cache.eviction.sorted.SortedEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.CacheEvictableEntryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 *
 */
public class GridCacheConcurrentEvictionConsistencySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Default iteration count. */
    private static final int ITERATION_CNT = 50000;

    /** Size of policy internal queue. */
    private static final int POLICY_QUEUE_SIZE = 50;

    /** Tested policy. */
    private EvictionPolicy<?, ?> plc;

    /** Key count to put into the cache. */
    private int keyCnt;

    /** Number of threads. */
    private int threadCnt = Runtime.getRuntime().availableProcessors();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setDefaultTxConcurrency(PESSIMISTIC);
        c.getTransactionConfiguration().setDefaultTxIsolation(READ_COMMITTED);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(LOCAL);

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setNearConfiguration(null);

        cc.setEvictionPolicy(plc);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000; // 5 min.
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyFifoLocalTwoKeys() throws Exception {
        FifoEvictionPolicy<Object, Object> plc = new FifoEvictionPolicy<>();
        plc.setMaxSize(1);

        this.plc = plc;

        keyCnt = 2;
        threadCnt = Runtime.getRuntime().availableProcessors() / 2;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyLruLocalTwoKeys() throws Exception {
        LruEvictionPolicy<Object, Object> plc = new LruEvictionPolicy<>();
        plc.setMaxSize(1);

        this.plc = plc;

        keyCnt = 2;
        threadCnt = Runtime.getRuntime().availableProcessors() / 2;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencySortedLocalTwoKeys() throws Exception {
        SortedEvictionPolicy<Object, Object> plc = new SortedEvictionPolicy<>();
        plc.setMaxSize(1);

        this.plc = plc;

        keyCnt = 2;
        threadCnt = Runtime.getRuntime().availableProcessors() / 2;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyFifoLocalFewKeys() throws Exception {
        FifoEvictionPolicy<Object, Object> plc = new FifoEvictionPolicy<>();
        plc.setMaxSize(POLICY_QUEUE_SIZE);

        this.plc = plc;

        keyCnt = POLICY_QUEUE_SIZE + 5;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyLruLocalFewKeys() throws Exception {
        LruEvictionPolicy<Object, Object> plc = new LruEvictionPolicy<>();
        plc.setMaxSize(POLICY_QUEUE_SIZE);

        this.plc = plc;

        keyCnt = POLICY_QUEUE_SIZE + 5;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencySortedLocalFewKeys() throws Exception {
        SortedEvictionPolicy<Object, Object> plc = new SortedEvictionPolicy<>();
        plc.setMaxSize(POLICY_QUEUE_SIZE);

        this.plc = plc;

        keyCnt = POLICY_QUEUE_SIZE + 5;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyFifoLocal() throws Exception {
        FifoEvictionPolicy<Object, Object> plc = new FifoEvictionPolicy<>();
        plc.setMaxSize(POLICY_QUEUE_SIZE);

        this.plc = plc;

        keyCnt = POLICY_QUEUE_SIZE * 10;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyLruLocal() throws Exception {
        LruEvictionPolicy<Object, Object> plc = new LruEvictionPolicy<>();
        plc.setMaxSize(POLICY_QUEUE_SIZE);

        this.plc = plc;

        keyCnt = POLICY_QUEUE_SIZE * 10;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencySortedLocal() throws Exception {
        SortedEvictionPolicy<Object, Object> plc = new SortedEvictionPolicy<>();
        plc.setMaxSize(POLICY_QUEUE_SIZE);

        this.plc = plc;

        keyCnt = POLICY_QUEUE_SIZE * 10;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPolicyConsistency() throws Exception {
        try {
            final Ignite ignite = startGrid(1);

            final IgniteCache<Integer, Integer> cache = ignite.cache(null);

            long start = System.currentTimeMillis();

            IgniteInternalFuture<?> fut = multithreadedAsync(
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        final Random rnd = new Random();

                        for (int i = 0; i < ITERATION_CNT; i++) {

                            int j = rnd.nextInt(keyCnt);

                            try (Transaction tx = ignite.transactions().txStart()) {
                                // Put or remove?
                                if (rnd.nextBoolean())
                                    cache.put(j, j);
                                else
                                    cache.remove(j);

                                tx.commit();
                            }

                            if (i != 0 && i % 5000 == 0)
                                info("Stats [iterCnt=" + i + ", size=" + cache.size() + ']');
                        }

                        return null;
                    }
                },
                threadCnt
            );

            fut.get();

            Collection<EvictableEntry<Integer, Integer>> queue = internalQueue(plc);

            info("Test results [threadCnt=" + threadCnt + ", iterCnt=" + ITERATION_CNT + ", cacheSize=" + cache.size() +
                ", internalQueueSize" + queue.size() + ", duration=" + (System.currentTimeMillis() - start) + ']');

            boolean detached = false;

            for (Cache.Entry<Integer, Integer> e : queue) {
                Integer rmv = cache.getAndRemove(e.getKey());

                CacheEvictableEntryImpl unwrapped = e.unwrap(CacheEvictableEntryImpl.class);

                if (rmv == null && (unwrapped.meta() != null || unwrapped.isCached())) {
                    U.warn(log, "Detached entry: " + e);

                    detached = true;
                }
                else
                    info("Entry removed: " + rmv);
            }

            if (detached)
                fail("Eviction policy contains keys that are not present in cache");

            if (!(cache.localSize() == 0)) {
                boolean zombies = false;

                for (Cache.Entry<Integer, Integer> e : cache) {
                    U.warn(log, "Zombie entry: " + e);

                    zombies = true;
                }

                if (zombies)
                    fail("Cache contained zombie entries.");
            }
            else
                info("Cache is empty after test.");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Gets internal policy queue.
     *
     * @param plc Policy to get queue from.
     * @return Internal entries collection.
     */
    private Collection<EvictableEntry<Integer, Integer>> internalQueue(EvictionPolicy<?, ?> plc) {
        if (plc instanceof FifoEvictionPolicy) {
            FifoEvictionPolicy<Integer, Integer> plc0 = (FifoEvictionPolicy<Integer, Integer>)plc;

            return plc0.queue();
        }
        else if (plc instanceof LruEvictionPolicy) {
            LruEvictionPolicy<Integer, Integer> plc0 = (LruEvictionPolicy<Integer, Integer>)plc;

            return plc0.queue();
        }
        else if (plc instanceof SortedEvictionPolicy) {
            SortedEvictionPolicy<Integer, Integer> plc0 = (SortedEvictionPolicy<Integer, Integer>)plc;

            return plc0.queue();
        }

        assert false : "Unexpected policy type: " + plc.getClass().getName();

        return Collections.emptyList();
    }
}