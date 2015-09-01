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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheAlwaysEvictionPolicy;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Multithreaded partition cache put get test.
 */
public class GridCachePartitionedMultiThreadedPutGetSelfTest extends GridCommonAbstractTest {
    /** */
    private static final boolean TEST_INFO = true;

    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Number of threads. */
    private static final int THREAD_CNT = 10;

    /** Number of transactions per thread. */
    private static final int TX_CNT = 500;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxMemorySize(1000);

        cc.setEvictionPolicy(plc);
        cc.setSwapEnabled(false);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setEvictSynchronized(false);

        NearCacheConfiguration nearCfg = new NearCacheConfiguration();

        nearCfg.setNearEvictionPolicy(new GridCacheAlwaysEvictionPolicy());
        cc.setNearConfiguration(nearCfg);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (GRID_CNT > 0)
            grid(0).cache(null).removeAll();

        for (int i = 0; i < GRID_CNT; i++) {
            grid(i).cache(null).clear();

            assert grid(i).cache(null).localSize() == 0;
        }
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticReadCommitted() throws Exception {
        doTest(PESSIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableRead() throws Exception {
        doTest(PESSIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testPessimisticSerializable() throws Exception {
        doTest(PESSIMISTIC, SERIALIZABLE);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticReadCommitted() throws Exception {
        doTest(OPTIMISTIC, READ_COMMITTED);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticRepeatableRead() throws Exception {
        doTest(OPTIMISTIC, REPEATABLE_READ);
    }

    /**
     * JUnit.
     *
     * @throws Exception If failed.
     */
    public void testOptimisticSerializable() throws Exception {
        doTest(OPTIMISTIC, SERIALIZABLE);
    }

    /**
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope", "PointlessBooleanExpression"})
    private void doTest(final TransactionConcurrency concurrency, final TransactionIsolation isolation)
        throws Exception {
        final AtomicInteger cntr = new AtomicInteger();

        multithreaded(new CAX() {
            @SuppressWarnings({"BusyWait"})
            @Override public void applyx() {
                IgniteCache<Integer, Integer> c = grid(0).cache(null);

                for (int i = 0; i < TX_CNT; i++) {
                    int kv = cntr.incrementAndGet();

                    try (Transaction tx = grid(0).transactions().txStart(concurrency, isolation)) {
                        assertNull(c.get(kv));

                        c.put(kv, kv);

                        assertEquals(Integer.valueOf(kv), c.get(kv));

                        // Again.
                        c.put(kv, kv);

                        assertEquals(Integer.valueOf(kv), c.get(kv));

                        tx.commit();
                    }

                    if (TEST_INFO && kv % 1000 == 0)
                        info("Transactions: " + kv);
                }
            }
        }, THREAD_CNT);
    }
}