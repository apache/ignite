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

package org.apache.ignite.internal.processors.cache.persistence;

import java.util.Random;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assert;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;

/**
 *
 */
public class IgnitePdsTxCrashRecoveryMultiJvmTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        // Simple 1 character name. For easy debug.
        cfg.setConsistentId(new String(new char[] {name.charAt(name.length() - 1)}));

        cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 20 * 60 * 1000;
    }

    /**
     * @throws Exception If failed.
     */
    public void test() throws Exception {
        int it = 10;

        for (int i = 0; i < it; i++) {
            runTest(6, 10);

            System.out.println(">>> iteration " + i);

            stopAllGrids();

            deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void runTest(final int nodes, int threads) throws Exception {
        IgniteEx ig = (IgniteEx)startGrids(nodes);

        IgniteConfiguration igCfg = getConfiguration(getTestIgniteInstanceName(nodes));

        igCfg.setClientMode(true);

        final IgniteEx igClient = startGrid(igCfg);

        ig.active(true);

        assertEquals(nodes + 1, ig.cluster().nodes().size());

        CacheConfiguration<Long, Account> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(3);

        ig.createCache(ccfg);

        final int accounts = 1_000;
        final int balance = 100_000;

        load(igClient, accounts, balance);

        GridCompoundFuture<?, ?> txFut = new GridCompoundFuture<>();

        Random r = new Random();

        final long startTime = U.currentTimeMillis();
        final long stopTime = startTime + 10_000;

        final long crashTime = startTime + r.nextInt((int)(stopTime - startTime));

        for (int i = 0; i < threads; i++) {
            IgniteInternalFuture f = runAsync(new Runnable() {
                @Override public void run() {
                    Random r = new Random();

                    long txCommited = 0;
                    long txFail = 0;

                    IgniteCache<Long, Account> c = igClient.cache(CACHE_NAME);

                    while (true) {
                        if (U.currentTimeMillis() > stopTime) {
                            System.out.println(
                                "Thread name:" + Thread.currentThread().getName() + "\n" +
                                    "Tx commited:" + txCommited + "\n" +
                                    "Tx failure:" + txFail + "\n");
                            break;
                        }

                        TransactionConcurrency tc = r.nextBoolean() ? PESSIMISTIC : OPTIMISTIC;

                        TransactionIsolation ti = null;

                        switch (r.nextInt(3)) {
                            case 0:
                                ti = TransactionIsolation.REPEATABLE_READ;

                                break;
                            case 1:
                                ti = TransactionIsolation.READ_COMMITTED;

                                break;
                            case 2:
                                ti = TransactionIsolation.SERIALIZABLE;
                        }

                        assert ti != null;

                        try (Transaction tx = igClient.transactions().txStart(tc, ti)) {
                            long id1 = r.nextInt(accounts);

                            long id2;

                            do {
                                id2 = r.nextInt(accounts);

                                if (id1 != id2)
                                    break;
                            }
                            while (true);

                            int money = r.nextInt(balance / 100);

                            Account acc1 = c.get(id1);
                            Account acc2 = c.get(id2);

                            acc1.balance -= money;
                            acc2.balance += money;

                            c.put(id1, acc1);
                            c.put(id2, acc2);

                            tx.commit();

                            txCommited++;
                        }
                        catch (Throwable h) {
                            // No-op.
                            txFail++;
                        }
                    }
                }
            });

            txFut.add(f);
        }

        txFut.markInitialized();

        final GridCompoundFuture<?, ?> stopFut = new GridCompoundFuture<>();

        runAsync(new Runnable() {
            @Override public void run() {
                while (true) {
                    if (U.currentTimeMillis() > crashTime)
                        break;

                    try {
                        Thread.sleep(30);
                    }
                    catch (InterruptedException e) {
                        U.error(log, "Thread interrupted", e);
                    }
                }

                for (int i = 0; i <= nodes; i++) {
                    final int node = i;

                    final IgniteEx ignite = grid(node);

                    IgniteInternalFuture f = runAsync(new Runnable() {
                        @Override public void run() {
                            try {
                                if (ignite instanceof IgniteProcessProxy)
                                    ((IgniteProcessProxy)ignite).kill();
                                else
                                    stopGrid(node, true);
                            }
                            catch (Exception e) {
                                U.error(log, "Fail kill node " + node, e);
                            }
                        }
                    });

                    stopFut.add(f);
                }

                stopFut.markInitialized();
            }
        }).get();

        txFut.get();

        stopFut.get();

        ig = (IgniteEx)startGrids(nodes);

        igCfg = getConfiguration("persistence.IgnitePdsTxCrashRecoveryMultiJvmTest" + nodes);

        igCfg.setClientMode(true);

        final IgniteEx client0 = startGrid(igCfg);

        ig.active(true);

        check(client0, accounts, balance);
    }

    /**
     *
     */
    private void load(IgniteEx ig, int accounts, int balance) {
        IgniteCache<Long, Account> cache = ig.cache(CACHE_NAME);

        for (long id = 0; id < accounts; id++) {
            cache.put(id, new Account(id, balance));

            if (id % 100 == 0)
                System.out.println(">>> " + id);
        }

        // TODO streamer does not work with proxy.
        /* try (IgniteDataStreamer<Long, Account> st = ig.dataStreamer(CACHE_NAME)) {
            st.allowOverwrite(true);

            for (long id = 0; id < accounts; id++)
                st.addData(id, new Account(id, balance));
        }*/
    }

    /**
     *
     */
    private void check(IgniteEx ig, int accounts, int balance) {
        long total = 0;

        IgniteCache<Long, Account> cache = ig.cache(CACHE_NAME);

        for (long i = 0; i < accounts; i++) {
            Account acc = cache.get(i);

            total += acc.balance;
        }

        Assert.assertEquals(balance * accounts, total);
    }

    /**
     *
     */
    private static class Account {
        /** */
        private long id;
        /** */
        private long balance;

        /**
         *
         */
        public Account(long id, long balance) {
            this.id = id;
            this.balance = balance;
        }

        /**
         *
         */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Account account = (Account)o;

            if (id != account.id)
                return false;
            return balance == account.balance;
        }

        /**
         *
         */
        @Override public int hashCode() {
            int result = (int)(id ^ (id >>> 32));
            result = 31 * result + (int)(balance ^ (balance >>> 32));
            return result;
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
    }
}
