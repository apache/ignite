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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.CAX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionDeadlockException;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionTimeoutException;
import org.junit.Test;

/**
 *
 */
public class TxDeadlockCauseTest extends GridCommonAbstractTest {
    /** */
    private CacheConfiguration ccfg;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (isDebug()) {
            TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

            discoSpi.failureDetectionTimeoutEnabled(false);

            cfg.setDiscoverySpi(discoSpi);
        }

        DataStorageConfiguration memCfg = new DataStorageConfiguration().setDefaultDataRegionConfiguration(
            new DataRegionConfiguration().setMaxSize(100L * 1024 * 1024));

        cfg.setDataStorageConfiguration(memCfg);

        CacheConfiguration ccfg0 = ccfg == null ? new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL) : ccfg;

        cfg.setCacheConfiguration(ccfg0);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCause() throws Exception {
        startGrids(1);

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            testCauseObject(1, 2, 1000, isolation, true);
            testCauseObject(1, 2, 1000, isolation, false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCauseSeveralNodes() throws Exception {
        startGrids(2);

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            testCauseObject(2, 2, 1500, isolation, true);
            testCauseObject(2, 2, 1500, isolation, false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCauseNear() throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setNearConfiguration(new NearCacheConfiguration());

        startGrids(1);

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            testCauseObject(1, 2, 1000, isolation, true);
            testCauseObject(1, 2, 1000, isolation, false);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCauseSeveralNodesNear() throws Exception {
        ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setNearConfiguration(new NearCacheConfiguration());

        startGrids(4);

        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            testCauseObject(2, 2, 2000, isolation, true);
            testCauseObject(2, 2, 2000, isolation, false);
        }
    }

    /**
     * @param nodes Nodes count.
     * @param keysCnt Keys count.
     * @param timeout Timeout.
     * @param isolation TransactionIsolation.
     * @param oneOp Determines whether {@link IgniteCache#getAndPut(java.lang.Object, java.lang.Object)}
     *              instead of {@link IgniteCache#get(java.lang.Object)} and {@link IgniteCache#put(java.lang.Object, java.lang.Object)} operations sequence.
     * @throws Exception If failed.
     */
    @Test
    public void testCauseObject(int nodes, final int keysCnt, final long timeout, final TransactionIsolation isolation, final boolean oneOp) throws Exception {
        final Ignite ignite = grid(new Random().nextInt(nodes));

        final IgniteCache<Integer, Account> cache = ignite.cache(DEFAULT_CACHE_NAME);
        final List<Integer> keys = new ArrayList<>(keysCnt);

        for (int i = 0; i < keysCnt; i++) {
            keys.add(i);
            cache.put(i, new Account(i, i * 100));
        }

        final List<Integer> keysReversed = new ArrayList<>(keys);
        Collections.reverse(keysReversed);

        final AtomicBoolean reverse = new AtomicBoolean();
        final AtomicReference<Exception> ex = new AtomicReference<>();
        final CyclicBarrier barrier = new CyclicBarrier(2);

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(new CAX() {
            @Override public void applyx() throws IgniteCheckedException {
                try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC, isolation,
                    timeout, keys.size())) {

                    List<Integer> keys0 = getAndFlip(reverse) ? keys : keysReversed;

                    for (int i = 0; i < keys0.size(); i++) {
                        Integer key = keys0.get(i);

                        if (oneOp)
                            cache.getAndPut(key, new Account(key, (key + 1) * 100));
                        else
                            cache.put(key, new Account(cache.get(key).id, (key + 1) * 100));

                        if (i == 0)
                            barrier.await(timeout >> 1, TimeUnit.MILLISECONDS);
                    }

                    tx.commit();
                } catch (Exception e) {
                    ex.compareAndSet(null, e);
                }
            }
        }, 2, "tx");

        fut.get(timeout << 1);

        Exception e = ex.get();

        assertNotNull(e);

        boolean detected = X.hasCause(e, TransactionDeadlockException.class);

        if (!detected)
            U.error(log, "Failed to detect a deadlock.", e);
        else
            log.info(X.cause(e, TransactionDeadlockException.class).getMessage());

        assertTrue(detected);

        try {
            assertEquals(TransactionTimeoutException.class, e.getCause().getClass());
            assertEquals(TransactionDeadlockException.class, e.getCause().getCause().getClass());
        }
        catch (AssertionError err) {
            U.error(log, "Unexpected exception structure.", e);

            throw err;
        }
    }

    /**
     * @param b AtomicBoolean.
     * @return Current value.
     */
    private boolean getAndFlip(AtomicBoolean b) {
        while (true) {
            boolean res = b.get();

            if (b.compareAndSet(res, !res))
                return res;
        }
    }

    /** */
    static class Account implements Serializable {
        /** Account ID. */
        private int id;

        /** Account balance. */
        private double balance;

        /**
         * @param id Account ID.
         * @param balance Balance.
         */
        Account(int id, double balance) {
            this.id = id;
            this.balance = balance;
        }

        /**
         * Change balance by specified amount.
         *
         * @param amount Amount to add to balance (may be negative).
         */
        void update(double amount) {
            balance += amount;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Account [id=" + id + ", balance=$" + balance + ']';
        }
    }
}
