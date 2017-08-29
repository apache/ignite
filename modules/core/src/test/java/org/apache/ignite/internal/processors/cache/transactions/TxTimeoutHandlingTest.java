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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Tests ability to rollback not properly closed transaction.
 */
public class TxTimeoutHandlingTest extends GridCommonAbstractTest {
    /** */
    private static final long TX_TIMEOUT = 3_000L;

    /** */
    private static final String CACHE_NAME = "test";

    /** IP finder. */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private final CountDownLatch l = new CountDownLatch(1);

    /** */
    private final Object mux = new Object();

    /** */
    private volatile boolean released;


    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        TransactionConfiguration txCfg = new TransactionConfiguration();
        txCfg.setDefaultTxTimeout(TX_TIMEOUT);

        cfg.setTransactionConfiguration(txCfg);

        CacheConfiguration ccfg = new CacheConfiguration(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** */
    public void testTxTimeoutHandling() throws Exception {
        try {
            final Ignite ignite = startGrid(0);

            IgniteInternalFuture<?> fut1 = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    // Start tx with default settings.
                    try (Transaction tx = ignite.transactions().txStart()) {
                        ignite.cache(CACHE_NAME).put(1, 1);

                        l.countDown();

                        // Wait longer than default timeout.
                        synchronized (mux) {
                            while (!released) {
                                try {
                                    mux.wait();
                                }
                                catch (InterruptedException e) {
                                    throw new IgniteException(e);
                                }
                            }
                        }

                        try {
                            tx.commit();

                            fail();
                        }
                        catch (IgniteException e) {
                            // Expect exception - tx is rolled back.
                        }
                    }
                }
            }, 1, "Locker");

            IgniteInternalFuture<?> fut2 = multithreadedAsync(new Runnable() {
                @Override public void run() {
                    U.awaitQuiet(l);

                    // Try to acquire lock.
                    // Acquisition will be successful then first transaction will be rolled back after timeout.
                    try (Transaction tx = ignite.transactions().txStart(TransactionConcurrency.PESSIMISTIC,
                        TransactionIsolation.REPEATABLE_READ, 0, 1)) {
                        ignite.cache(CACHE_NAME).put(1, 1);

                        tx.commit();
                    }
                }
            }, 1, "Waiter");

            Thread.sleep(TX_TIMEOUT + 1_000);

            fut2.get();

            startGrid(1);

            released = true;

            synchronized (mux) {
                mux.notify();
            }
        }
        finally {
            stopAllGrids();
        }
    }
}