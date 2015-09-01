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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_TX_SALVAGE_TIMEOUT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test tx salvage.
 */
public class GridCachePartitionedTxSalvageSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 5;

    /** Key count. */
    private static final int KEY_CNT = 10;

    /** Salvage timeout system property value. */
    private static final Integer SALVAGE_TIMEOUT = 5000;

    /** Difference between salvage timeout and actual wait time when performing "before salvage" tests. */
    private static final int DELTA_BEFORE = 1000;

    /** How much time to wait after salvage timeout when performing "after salvage" tests. */
    private static final int DELTA_AFTER = 1000;

    /** Salvage timeout system property value before alteration. */
    private static String salvageTimeoutOld;

    /** Standard VM IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        // Discovery.
        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setAffinity(new RendezvousAffinityFunction(false, 18));
        cc.setBackups(1);
        cc.setRebalanceMode(SYNC);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // Set salvage timeout system property.
        salvageTimeoutOld = System.setProperty(IGNITE_TX_SALVAGE_TIMEOUT, SALVAGE_TIMEOUT.toString());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // Restore salvage timeout system property to its initial state.
        if (salvageTimeoutOld != null)
            System.setProperty(IGNITE_TX_SALVAGE_TIMEOUT, salvageTimeoutOld);
        else
            System.clearProperty(IGNITE_TX_SALVAGE_TIMEOUT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        // Start the grid.
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        System.gc();
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxSalvageBeforeTimeout() throws Exception {
        checkSalvageBeforeTimeout(OPTIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticcTxSalvageBeforeTimeout() throws Exception {
        checkSalvageBeforeTimeout(PESSIMISTIC, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticTxSalvageAfterTimeout() throws Exception {
        checkSalvageAfterTimeout(OPTIMISTIC, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticTxSalvageAfterTimeout() throws Exception {
        checkSalvageAfterTimeout(PESSIMISTIC, false);
    }

    /**
     * Check whether caches has no transactions after salvage timeout.
     *
     * @param mode Transaction mode (PESSIMISTIC, OPTIMISTIC).
     * @param prepare Whether to prepare transaction state (i.e. call {@link IgniteInternalTx#prepare()}).
     * @throws Exception If failed.
     */
    private void checkSalvageAfterTimeout(TransactionConcurrency mode, boolean prepare) throws Exception {
        startTxAndPutKeys(mode, prepare);

        stopNodeAndSleep(SALVAGE_TIMEOUT + DELTA_AFTER);

        for (int i = 1; i < GRID_CNT; i++) {
            checkTxsEmpty(near(i).context());
            checkTxsEmpty(dht(i).context());
        }
    }

    /**
     * Check whether caches still has all transactions before salvage timeout.
     *
     * @param mode Transaction mode (PESSIMISTIC, OPTIMISTIC).
     * @param prepare Whether to prepare transaction state
     *                (i.e. call {@link IgniteInternalTx#prepare()}).
     * @throws Exception If failed.
     */
    private void checkSalvageBeforeTimeout(TransactionConcurrency mode, boolean prepare) throws Exception {
        startTxAndPutKeys(mode, prepare);

        List<Integer> nearSizes = new ArrayList<>(GRID_CNT - 1);
        List<Integer> dhtSizes = new ArrayList<>(GRID_CNT - 1);

        for (int i = 1; i < GRID_CNT; i++) {
            nearSizes.add(near(i).context().tm().txs().size());
            dhtSizes.add(dht(i).context().tm().txs().size());
        }

        stopNodeAndSleep(SALVAGE_TIMEOUT - DELTA_BEFORE);

        for (int i = 1; i < GRID_CNT; i++) {
            checkTxsNotEmpty(near(i).context(), nearSizes.get(i - 1));
            checkTxsNotEmpty(dht(i).context(), dhtSizes.get(i - 1));
        }
    }

    /**
     * Start new transaction on the grid(0) and put some keys to it.
     *
     * @param mode Transaction mode (PESSIMISTIC, OPTIMISTIC).
     * @param prepare Whether to prepare transaction state (i.e. call {@link IgniteInternalTx#prepare()}).
     * @throws Exception If failed.
     */
    private void startTxAndPutKeys(final TransactionConcurrency mode, final boolean prepare) throws Exception {
        Ignite ignite = grid(0);

        final Collection<Integer> keys = nearKeys(ignite.cache(null), KEY_CNT, 0);

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                IgniteCache<Object, Object> c = jcache(0);

                try {
                    Transaction tx = grid(0).transactions().txStart(mode, REPEATABLE_READ);

                    for (Integer key : keys)
                        c.put(key, "val" + key);

                    if (prepare)
                        ((TransactionProxyImpl)tx).tx().prepare();
                }
                catch (IgniteCheckedException e) {
                    info("Failed to put keys to cache: " + e.getMessage());
                }
            }
        }, 1);

        fut.get();
    }

    /**
     * Stop the very first grid node (the one with 0 index) and sleep for the given amount of time.
     *
     * @param timeout Sleep timeout in milliseconds.
     * @throws Exception If failed.
     */
    private void stopNodeAndSleep(long timeout) throws Exception {
        stopGrid(0);

        info("Stopped grid.");

        U.sleep(timeout);
    }

    /**
     * Checks that transaction manager for cache context does not have any pending transactions.
     *
     * @param ctx Cache context.
     */
    private void checkTxsEmpty(GridCacheContext ctx) {
        Collection txs = ctx.tm().txs();

        assert txs.isEmpty() : "Not all transactions were salvaged: " + txs;
    }

    /**
     * Checks that transaction manager for cache context has expected number of pending transactions.
     *
     * @param ctx Cache context.
     * @param exp Expected amount of transactions.
     */
    private void checkTxsNotEmpty(GridCacheContext ctx, int exp) {
        int size = ctx.tm().txs().size();

        assertEquals("Some transactions were salvaged unexpectedly", exp, size);
    }
}