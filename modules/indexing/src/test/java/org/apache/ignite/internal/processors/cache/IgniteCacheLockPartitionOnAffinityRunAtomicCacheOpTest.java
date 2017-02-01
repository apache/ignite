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

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test to validate https://issues.apache.org/jira/browse/IGNITE-2310
 */
public class IgniteCacheLockPartitionOnAffinityRunAtomicCacheOpTest extends IgniteCacheLockPartitionOnAffinityRunAbstractTest {
    /** Atomic cache. */
    private static final String ATOMIC_CACHE = "atomic";
    /** Transact cache. */
    private static final String TRANSACT_CACHE = "transact";
    /** Transact cache. */
    private static final long TEST_TIMEOUT = 10 * 60_000;
    /** Keys count. */
    private static int KEYS_CNT = 100;
    /** Keys count. */
    private static int PARTS_CNT = 16;
    /** Key. */
    private static AtomicInteger key = new AtomicInteger(0);

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected void beginNodesRestart() {
        stopRestartThread.set(false);
        nodeRestartFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stopRestartThread.get() && System.currentTimeMillis() < endTime) {
                    log.info("Restart nodes");
                    for (int i = GRID_CNT - RESTARTED_NODE_CNT; i < GRID_CNT; ++i)
                        stopGrid(i);
                    Thread.sleep(500);
                    for (int i = GRID_CNT - RESTARTED_NODE_CNT; i < GRID_CNT; ++i)
                        startGrid(i);

                    GridTestUtils.waitForCondition(new GridAbsPredicate() {
                        @Override public boolean apply() {
                            return !stopRestartThread.get();
                        }
                    }, RESTART_TIMEOUT);
                }
                return null;
            }
        }, "restart-node");
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);
        ccfg.setBackups(0);

        return  ccfg;
    }

    /**
     * @param cacheName Cache name.
     * @param mode Atomicity mode.
     * @throws Exception If failed.
     */
    private void createCache(String cacheName, CacheAtomicityMode mode) throws Exception {
        CacheConfiguration ccfg = cacheConfiguration(grid(0).name());
        ccfg.setName(cacheName);

        ccfg.setAtomicityMode(mode);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        grid(0).createCache(ccfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        key.set(0);
        createCache(ATOMIC_CACHE, CacheAtomicityMode.ATOMIC);
        createCache(TRANSACT_CACHE, CacheAtomicityMode.TRANSACTIONAL);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).destroyCache(ATOMIC_CACHE);
        grid(0).destroyCache(TRANSACT_CACHE);

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotReservedAtomicCacheOp() throws Exception {
        notReservedCacheOp(ATOMIC_CACHE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNotReservedTxCacheOp() throws Exception {
        notReservedCacheOp(TRANSACT_CACHE);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void notReservedCacheOp(final String cacheName) throws Exception {
        // Workaround for initial update job metadata.
        grid(0).compute().affinityRun(
            Arrays.asList(Person.class.getSimpleName(), Organization.class.getSimpleName()),
            new Integer(orgIds.get(0)),
            new NotReservedCacheOpAffinityRun(0, 0, cacheName));

        // Run restart threads: start re-balancing
        beginNodesRestart();

        grid(0).cache(cacheName).clear();

        IgniteInternalFuture<Long> affFut = null;
        try {
            affFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    for (int i = 0; i < PARTS_CNT; ++i) {
                        grid(0).compute().affinityRun(
                            Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()),
                            new Integer(i),
                            new NotReservedCacheOpAffinityRun(i, key.getAndIncrement() * KEYS_CNT, cacheName));
                    }
                }
            }, AFFINITY_THREADS_CNT, "affinity-run");
        }
        finally {
            if (affFut != null)
                affFut.get();

            stopRestartThread.set(true);
            nodeRestartFut.get();

            Thread.sleep(5000);

            log.info("Final await. Timed out if failed");
            awaitPartitionMapExchange();

            IgniteCache cache = grid(0).cache(cacheName);
            cache.clear();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReservedPartitionCacheOp() throws Exception {
        // Workaround for initial update job metadata.
        grid(0).cache(Person.class.getSimpleName()).clear();
        grid(0).compute().affinityRun(
            Arrays.asList(Person.class.getSimpleName(), Organization.class.getSimpleName()),
            0,
            new ReservedPartitionCacheOpAffinityRun(0, 0));

        // Run restart threads: start re-balancing
        beginNodesRestart();

        IgniteInternalFuture<Long> affFut = null;
        try {
            affFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    for (int i = 0; i < PARTS_CNT; ++i) {
                        if (System.currentTimeMillis() >= endTime)
                            break;

                        grid(0).compute().affinityRun(
                            Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()),
                            new Integer(i),
                            new ReservedPartitionCacheOpAffinityRun(i, key.getAndIncrement() * KEYS_CNT));
                    }
                }
            }, AFFINITY_THREADS_CNT, "affinity-run");
        }
        finally {
            if (affFut != null)
                affFut.get();

            stopRestartThread.set(true);
            nodeRestartFut.get();

            Thread.sleep(5000);

            log.info("Final await. Timed out if failed");
            awaitPartitionMapExchange();

            IgniteCache cache = grid(0).cache(Person.class.getSimpleName());
            cache.clear();
        }
    }

    /** */
    private static class NotReservedCacheOpAffinityRun implements IgniteRunnable {
        /** Org id. */
        int orgId;

        /** Begin of key. */
        int keyBegin;

        /** Cache name. */
        private String cacheName;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        public NotReservedCacheOpAffinityRun() {
            // No-op.
        }

        /**
         * @param orgId Organization.
         * @param keyBegin Begin key value.
         * @param cacheName Cache name.
         */
        public NotReservedCacheOpAffinityRun(int orgId, int keyBegin, String cacheName) {
            this.orgId = orgId;
            this.keyBegin = keyBegin;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            log.info("Begin run " + keyBegin);
            IgniteCache cache = ignite.cache(cacheName);

            for (int i = 0; i < KEYS_CNT; ++i)
                cache.put(i + keyBegin, i + keyBegin);

            log.info("End run " + keyBegin);
        }
    }

    /** */
    private static class ReservedPartitionCacheOpAffinityRun implements IgniteRunnable {
        /** Org id. */
        int orgId;

        /** Begin of key. */
        int keyBegin;

        /** */
        @IgniteInstanceResource
        private IgniteEx ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        public ReservedPartitionCacheOpAffinityRun() {
            // No-op.
        }

        /**
         * @param orgId Organization Id.
         * @param keyBegin Begin key value;
         */
        public ReservedPartitionCacheOpAffinityRun(int orgId, int keyBegin) {
            this.orgId = orgId;
            this.keyBegin = keyBegin;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            log.info("Begin run " + keyBegin);
            IgniteCache cache = ignite.cache(Person.class.getSimpleName());

            for (int i = 0; i < KEYS_CNT; ++i) {
                Person p = new Person(i + keyBegin, orgId);
                cache.put(p.createKey(), p);
            }
        }
    }
}
