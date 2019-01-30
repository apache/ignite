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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;

import static org.apache.ignite.internal.util.typedef.X.hasCause;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TxOptimisticDeadlockDetectionCrossCacheTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        CacheConfiguration ccfg0 = defaultCacheConfiguration();

        ccfg0.setName("cache0");
        ccfg0.setCacheMode(CacheMode.PARTITIONED);
        ccfg0.setBackups(1);
        ccfg0.setNearConfiguration(null);

        CacheConfiguration ccfg1 = defaultCacheConfiguration();

        ccfg1.setName("cache1");
        ccfg1.setCacheMode(CacheMode.PARTITIONED);
        ccfg1.setBackups(1);
        ccfg1.setNearConfiguration(null);

        cfg.setCacheConfiguration(ccfg0, ccfg1);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testDeadlock() throws Exception {
        startGrids(2);

        try {
            doTestDeadlock();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private boolean doTestDeadlock() throws Exception {
        final AtomicInteger threadCnt = new AtomicInteger();

        final AtomicBoolean deadlock = new AtomicBoolean();

        final AtomicInteger commitCnt = new AtomicInteger();

        grid(0).events().localListen(new CacheLocksListener(), EventType.EVT_CACHE_OBJECT_LOCKED);

        AffinityTopologyVersion waitTopVer = new AffinityTopologyVersion(2, 1);

        IgniteInternalFuture<?> exchFut = grid(0).context().cache().context().exchange().affinityReadyFuture(waitTopVer);

        if (exchFut != null && !exchFut.isDone()) {
            log.info("Waiting for topology exchange future [waitTopVer=" + waitTopVer + ", curTopVer="
                + grid(0).context().cache().context().exchange().readyAffinityVersion() + ']');

            exchFut.get();
        }

        log.info("Finished topology exchange future [curTopVer="
            + grid(0).context().cache().context().exchange().readyAffinityVersion() + ']');

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
            @Override public void run() {
                int threadNum = threadCnt.getAndIncrement();

                Ignite ignite = ignite(0);

                IgniteCache<Integer, Integer> cache1 = ignite.cache("cache" + (threadNum == 0 ? 0 : 1));

                IgniteCache<Integer, Integer> cache2 = ignite.cache("cache" + (threadNum == 0 ? 1 : 0));

                try (Transaction tx =
                         ignite.transactions().txStart(OPTIMISTIC, REPEATABLE_READ, 500, 0)
                ) {
                    int key1 = primaryKey(cache1);

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + key1 + ", cache=" + cache1.getName() + ']');

                    cache1.put(key1, 0);

                    int key2 = primaryKey(cache2);

                    log.info(">>> Performs put [node=" + ((IgniteKernal)ignite).localNode() +
                        ", tx=" + tx + ", key=" + key2 + ", cache=" + cache2.getName() + ']');

                    cache2.put(key2, 1);

                    tx.commit();

                    commitCnt.incrementAndGet();
                }
                catch (Throwable e) {
                    // At least one stack trace should contain TransactionDeadlockException.
                    if (hasCause(e, TransactionTimeoutException.class) &&
                        hasCause(e, TransactionDeadlockException.class)
                        ) {
                        if (deadlock.compareAndSet(false, true))
                            log.info("Successfully set deadlock flag");
                        else
                            log.info("Deadlock flag was already set");
                    }
                    else
                        log.warning("Got not deadlock exception", e);
                }
            }
        }, 2, "tx-thread");

        fut.get();

        assertFalse("Commits must fail", commitCnt.get() == 2);

        assertTrue(deadlock.get());

        for (Ignite ignite : G.allGrids()) {
            IgniteTxManager txMgr = ((IgniteKernal)ignite).context().cache().context().tm();

            Collection<IgniteInternalFuture<?>> futs = txMgr.deadlockDetectionFutures();

            assertTrue(futs.isEmpty());
        }

        return true;
    }

    /**
     * Listener for cache lock events.
     *
     * To ensure deadlock this listener blocks transaction thread until both threads acquire first lock.
     */
    private static class CacheLocksListener implements IgnitePredicate<Event> {
        /** Latch. */
        private final CountDownLatch latch = new CountDownLatch(2);

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            latch.countDown();

            try {
                latch.await();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            return true;
        }
    }
}
