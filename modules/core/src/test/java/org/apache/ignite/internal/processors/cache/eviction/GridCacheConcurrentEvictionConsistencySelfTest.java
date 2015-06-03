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

import org.apache.ignite.*;
import org.apache.ignite.cache.eviction.*;
import org.apache.ignite.cache.eviction.fifo.*;
import org.apache.ignite.cache.eviction.lru.*;
import org.apache.ignite.cache.eviction.sorted.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

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

            for (Cache.Entry<Integer, Integer> e : queue) {
                Integer rmv = cache.getAndRemove(e.getKey());

                if (rmv == null)
                    fail("Eviction policy contains key that is not present in cache: " + e);
                else
                    info("Entry removed: " + rmv);
            }

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
