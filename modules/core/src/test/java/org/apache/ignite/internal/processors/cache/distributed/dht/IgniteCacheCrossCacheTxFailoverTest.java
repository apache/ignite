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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import javax.cache.processor.*;
import java.io.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 *
 */
public class IgniteCacheCrossCacheTxFailoverTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE1 = "cache1";

    /** */
    private static final String CACHE2 = "cache2";

    /** */
    private static final int GRID_CNT = 4;

    /** */
    private static final int KEY_RANGE = 1000;

    /** */
    private static final long TEST_TIME = 3 * 60_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        if (gridName.equals(getTestGridName(GRID_CNT - 1)))
            cfg.setClientMode(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @param name Cache name.
     * @param cacheMode Cache mode.
     * @param parts Number of partitions.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheMode cacheMode, int parts) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(name);
        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (cacheMode == PARTITIONED)
            ccfg.setBackups(1);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, parts));

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIME + 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCachePessimisticTxFailover() throws Exception {
        crossCacheTxFailover(PARTITIONED, true, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCachePessimisticTxFailoverDifferentAffinity() throws Exception {
        crossCacheTxFailover(PARTITIONED, false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheOptimisticTxFailover() throws Exception {
        crossCacheTxFailover(PARTITIONED, true, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheOptimisticTxFailoverDifferentAffinity() throws Exception {
        crossCacheTxFailover(PARTITIONED, false, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCachePessimisticTxFailoverReplicated() throws Exception {
        crossCacheTxFailover(REPLICATED, true, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCacheOptimisticTxFailoverReplicated() throws Exception {
        crossCacheTxFailover(REPLICATED, true, OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCrossCachePessimisticTxFailoverDifferentAffinityReplicated() throws Exception {
        crossCacheTxFailover(PARTITIONED, false, PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * @param cacheMode Cache mode.
     * @param sameAff If {@code false} uses different number of partitions for caches.
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @throws Exception If failed.
     */
    private void crossCacheTxFailover(CacheMode cacheMode,
        boolean sameAff,
        final TransactionConcurrency concurrency,
        final TransactionIsolation isolation) throws Exception {
        IgniteKernal ignite0 = (IgniteKernal)ignite(0);

        final AtomicBoolean stop = new AtomicBoolean();

        try {
            ignite0.createCache(cacheConfiguration(CACHE1, cacheMode, 256));
            ignite0.createCache(cacheConfiguration(CACHE2, cacheMode, sameAff ? 256 : 128));

            final AtomicInteger threadIdx = new AtomicInteger();

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int idx = threadIdx.getAndIncrement();

                    Ignite ignite = ignite(idx % GRID_CNT);

                    log.info("Started update thread [node=" + ignite.name() +
                        ", client=" + ignite.configuration().isClientMode() + ']');

                    IgniteCache<TestKey, TestValue> cache1 = ignite.cache(CACHE1);
                    IgniteCache<TestKey, TestValue> cache2 = ignite.cache(CACHE2);

                    assertNotSame(cache1, cache2);

                    IgniteTransactions txs = ignite.transactions();

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    long iter = 0;

                    while (!stop.get()) {
                        boolean sameKey = rnd.nextBoolean();

                        try {
                            try (Transaction tx = txs.txStart(concurrency, isolation)) {
                                if (sameKey) {
                                    TestKey key = new TestKey(rnd.nextLong(KEY_RANGE));

                                    cacheOperation(rnd, cache1, key);
                                    cacheOperation(rnd, cache2, key);
                                }
                                else {
                                    TestKey key1 = new TestKey(rnd.nextLong(KEY_RANGE));
                                    TestKey key2 = new TestKey(key1.key() + 1);

                                    cacheOperation(rnd, cache1, key1);
                                    cacheOperation(rnd, cache2, key2);
                                }

                                tx.commit();
                            }
                        }
                        catch (CacheException | IgniteException e) {
                            log.info("Update error: " + e);
                        }

                        if (iter++ % 500 == 0)
                            log.info("Iteration: " + iter);
                    }

                    return null;
                }

                /**
                 * @param rnd Random.
                 * @param cache Cache.
                 * @param key Key.
                 */
                private void cacheOperation(ThreadLocalRandom rnd, IgniteCache<TestKey, TestValue> cache, TestKey key) {
                    switch (rnd.nextInt(4)) {
                        case 0:
                            cache.put(key, new TestValue(rnd.nextLong()));

                            break;

                        case 1:
                            cache.remove(key);

                            break;

                        case 2:
                            cache.invoke(key, new TestEntryProcessor(rnd.nextBoolean() ? 1L : null));

                            break;

                        case 3:
                            cache.get(key);

                            break;

                        default:
                            assert false;
                    }
                }
            }, 10, "tx-thread");

            long stopTime = System.currentTimeMillis() + 3 * 60_000;

            long topVer = ignite0.cluster().topologyVersion();

            boolean failed = false;

            while (System.currentTimeMillis() < stopTime) {
                log.info("Start node.");

                IgniteKernal ignite = (IgniteKernal)startGrid(GRID_CNT);

                assertFalse(ignite.configuration().isClientMode());

                topVer++;

                IgniteInternalFuture<?> affFut = ignite.context().cache().context().exchange().affinityReadyFuture(
                    new AffinityTopologyVersion(topVer));

                try {
                    if (affFut != null)
                        affFut.get(30_000);
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    log.error("Failed to wait for affinity future after start: " + topVer);

                    failed = true;

                    break;
                }

                Thread.sleep(500);

                log.info("Stop node.");

                stopGrid(GRID_CNT);

                topVer++;

                affFut = ignite0.context().cache().context().exchange().affinityReadyFuture(
                    new AffinityTopologyVersion(topVer));

                try {
                    if (affFut != null)
                        affFut.get(30_000);
                }
                catch (IgniteFutureTimeoutCheckedException e) {
                    log.error("Failed to wait for affinity future after stop: " + topVer);

                    failed = true;

                    break;
                }
            }

            stop.set(true);

            fut.get();

            assertFalse("Test failed, see log for details.", failed);
        }
        finally {
            stop.set(true);

            ignite0.destroyCache(CACHE1);
            ignite0.destroyCache(CACHE2);

            awaitPartitionMapExchange();
        }
    }

    /**
     *
     */
    private static class TestKey implements Serializable {
        /** */
        private long key;

        /**
         * @param key Key.
         */
        public TestKey(long key) {
            this.key = key;
        }

        /**
         * @return Key.
         */
        public long key() {
            return key;
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
            return (int)(key ^ (key >>> 32));
        }
    }

    /**
     *
     */
    private static class TestValue implements Serializable {
        /** */
        private long val;

        /**
         * @param val Value.
         */
        public TestValue(long val) {
            this.val = val;
        }

        /**
         * @return Value.
         */
        public long value() {
            return val;
        }
    }

    /**
     *
     */
    private static class TestEntryProcessor implements CacheEntryProcessor<TestKey, TestValue, TestValue> {
        /** */
        private Long val;

        /**
         * @param val Value.
         */
        public TestEntryProcessor(@Nullable Long val) {
            this.val = val;
        }

        /** {@inheritDoc} */
        @Override public TestValue process(MutableEntry<TestKey, TestValue> e, Object... args) {
            TestValue old = e.getValue();

            if (val != null)
                e.setValue(new TestValue(val));

            return old;
        }
    }
}
