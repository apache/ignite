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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static java.lang.Thread.yield;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests an ability to rollback transactions on topology change.
 */
public class TxRollbackOnTopologyChangeTest extends GridCommonAbstractTest {
    /** */
    public static final int ROLLBACK_TIMEOUT = 500;

    /** */
    private static final String CACHE_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int SRV_CNT = 6;

    /** */
    private static final int CLNT_CNT = 2;

    /** */
    private static final int TOTAL_CNT = SRV_CNT + CLNT_CNT;

    /** */
    public static final int ITERATIONS = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setTransactionConfiguration(new TransactionConfiguration().
            setTxTimeoutOnPartitionMapExchange(ROLLBACK_TIMEOUT));

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setClientMode(getTestIgniteInstanceIndex(igniteInstanceName) >= SRV_CNT);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(2);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGridsMultiThreaded(TOTAL_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests rollbacks on topology change.
     */
    public void testRollbackOnTopologyChange() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        final long seed = System.currentTimeMillis();

        final Random r = new Random(seed);

        log.info("Using seed: " + seed);

        AtomicIntegerArray reservedIdx = new AtomicIntegerArray(TOTAL_CNT);

        final int keysCnt = SRV_CNT - 1;

        for (int k = 0; k < keysCnt; k++)
            grid(0).cache(CACHE_NAME).put(k, (long)0);

        final CyclicBarrier b = new CyclicBarrier(keysCnt);

        AtomicInteger idGen = new AtomicInteger();

        final IgniteInternalFuture<?> txFut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                int key = idGen.getAndIncrement();

                List<Integer> keys = new ArrayList<>();

                for (int k = 0; k < keysCnt; k++)
                    keys.add(k);

                int cntr = 0;

                for (int i = 0; i < ITERATIONS; i++) {
                    cntr++;

                    int nodeId;

                    while(!reservedIdx.compareAndSet((nodeId = r.nextInt(TOTAL_CNT)), 0, 1))
                        doSleep(10);

                    U.awaitQuiet(b);

                    final IgniteEx grid = grid(nodeId);

                    try (final Transaction tx = grid.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 0, 0)) {
                        reservedIdx.set(nodeId, 0);

                        // Construct deadlock
                        grid.cache(CACHE_NAME).get(keys.get(key));

                        // Should block.
                        grid.cache(CACHE_NAME).get(keys.get((key + 1) % keysCnt));

                        fail("Deadlock expected");
                    }
                    catch (Throwable t) {
                        // Expected.
                    }

                    if (key == 0)
                        log.info("Rolled back: " + cntr);
                }
            }
        }, keysCnt, "tx-lock-thread");

        final IgniteInternalFuture<?> restartFut = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while(!stop.get()) {
                    final int nodeId = r.nextInt(TOTAL_CNT);

                    if (!reservedIdx.compareAndSet(nodeId, 0, 1)) {
                        yield();

                        continue;
                    }

                    stopGrid(nodeId);

                    doSleep(500 + r.nextInt(1000));

                    startGrid(nodeId);

                    reservedIdx.set(nodeId, 0);
                }

                return null;
            }
        }, 1, "tx-restart-thread");

        txFut.get(); // Wait for iterations to complete.

        stop.set(true);

        restartFut.get();

        checkFutures();
    }
}
