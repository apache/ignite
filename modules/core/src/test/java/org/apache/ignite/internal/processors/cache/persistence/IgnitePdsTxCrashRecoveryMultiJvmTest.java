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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;
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
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgnitePdsTxCrashRecoveryMultiJvmTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    // nodes + threads
    //private final ExecutorService exec = Executors.newFixedThreadPool(4 + 8);

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
        int it = 1;

        for (int i = 0; i < it; i++) {
            System.out.println(">>> iteration " + i);

            runTest(4, 8);

            stopAllGrids();

            deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void runTest(final int nodes, int threads) throws Exception {
        final IgniteEx ig = (IgniteEx)startGrids(nodes);

        ig.active(true);

        assertEquals(nodes, ig.cluster().nodes().size());

        CacheConfiguration<Long, Account> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setRebalanceDelay(Long.MAX_VALUE);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));
        ccfg.setBackups(3);

        ig.createCache(ccfg);

        final int accounts = 50;
        final int balance = 100_000;

        load(ig, accounts, balance);

        GridCompoundFuture<?, ?> txFut = new GridCompoundFuture<>();

        Random r = new Random();

        long interval = 5_000;

        final long startTime = U.currentTimeMillis();
        final long stopTime = startTime + interval;

        final long crashTime = startTime + r.nextInt((int)(stopTime - startTime));

        final AtomicLong commited = new AtomicLong();
        final AtomicLong failed = new AtomicLong();

        for (int i = 0; i < threads; i++) {
            IgniteInternalFuture f = runAsync(new Runnable() {
                @Override public void run() {
                    Random r = new Random();

                    long txCommited = 0;
                    long txFail = 0;

                    IgniteCache<Long, Account> c = ig.cache(CACHE_NAME);

                    while (true) {
                        if (U.currentTimeMillis() > stopTime) {
                            System.out.println(
                                "Thread name:" + Thread.currentThread().getName() + "\n" +
                                    "Tx commited:" + txCommited + "\n" +
                                    "Tx failure:" + txFail + "\n");

                            commited.addAndGet(txCommited);
                            failed.addAndGet(txFail);

                            break;
                        }

                        TransactionConcurrency tc = r.nextBoolean() ? PESSIMISTIC : OPTIMISTIC;

                        TransactionIsolation ti = null;

                        switch (r.nextInt(3)) {
                            case 0:
                                ti = REPEATABLE_READ;

                                break;
                            case 1:
                                ti = READ_COMMITTED;

                                break;
                            case 2:
                                ti = SERIALIZABLE;
                        }

                        assert ti != null;

                        try (Transaction tx = ig.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
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

                            acc1.transfer = money;
                            acc2.transfer = money;

                            c.put(id1, acc1);
                            c.put(id2, acc2);

                            tx.commit();

                            txCommited++;
                        }
                        catch (Throwable h) {
                            // No-op.
                            txFail++;

                            try {
                                U.sleep(100);
                            }
                            catch (IgniteInterruptedCheckedException e) {
                                U.error(log, "Thread interrupted", e);
                            }
                        }
                    }
                }
            }, "tx-runner-" + i);

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

                System.out.println("Stop time reached " + stopTime + " nodes will be stopped.");

                for (int i = 0; i < nodes; i++) {
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

        System.out.println("Total tx commited:" + commited.get() + "\nTotal tx failure:" + failed.get() + "\n");

        IgniteEx ig0 = (IgniteEx)startGrids(nodes);

        ig0.active(true);

        check(ig0, accounts, balance);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutKeyForward() throws Exception {
        runTest2(4, 8, 10, 10_000);

        stopAllGrids();

        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
    }

    /**
     * @throws Exception If failed.
     */
    public void runTest2(final int nodes, int threads, final int batchSize, long interval) throws Exception {
        final IgniteEx ig = (IgniteEx)startGrids(nodes);

        ig.active(true);

        assertEquals(nodes, ig.cluster().nodes().size());

        CacheConfiguration<Long, Long> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setRebalanceDelay(Long.MAX_VALUE);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 16));
        ccfg.setBackups(3);

        ig.createCache(ccfg);

        final AtomicLong data = new AtomicLong();

        final Set<Long> failCommitedRange = new GridConcurrentLinkedHashSet<>();

        GridCompoundFuture<?, ?> txFut = new GridCompoundFuture<>();

        Random r = new Random();

        final long startTime = U.currentTimeMillis();
        final long stopTime = startTime + interval;

        final long crashTime = stopTime;
        //final long crashTime = startTime + r.nextInt((int)(stopTime - startTime));

        final AtomicLong commited = new AtomicLong();
        final AtomicLong failed = new AtomicLong();

        for (int i = 0; i < threads; i++) {
            IgniteInternalFuture f = runAsync(new Runnable() {
                @Override public void run() {
                    Random r = new Random();

                    long txCommited = 0;
                    long txFail = 0;

                    IgniteCache<Long, Long> c = ig.cache(CACHE_NAME);

                    while (true) {
                        if (U.currentTimeMillis() > stopTime) {
                            System.out.println(
                                "Thread name:" + Thread.currentThread().getName() + "\n" +
                                    "Tx commited:" + txCommited + "\n" +
                                    "Tx failure:" + txFail + "\n");

                            commited.addAndGet(txCommited);

                            failed.addAndGet(txFail);

                            break;
                        }

                        long cur;

                        while (true){
                            cur = data.get();

                            if (data.compareAndSet(cur, cur + batchSize))
                                break;
                        }

                        Map<Long, Long> vals = new HashMap<>();

                        for (long val = cur; val < (cur + batchSize); val++)
                            vals.put(val, val);

                        assert !vals.isEmpty();

                        try (Transaction tx = ig.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                            c.putAll(vals);

                            tx.commit();

                            txCommited++;
                        }
                        catch (Throwable h) {
                            // No-op.
                            txFail++;

                            failCommitedRange.add(cur);

                            System.out.println(h);

                            try {
                                U.sleep(100);
                            }
                            catch (IgniteInterruptedCheckedException e) {
                                U.error(log, "Thread interrupted", e);
                            }
                        }
                    }
                }
            }, "tx-runner-" + i);

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

                System.out.println("Stop time reached " + crashTime + " nodes will be stopped.");

                for (int i = 0; i < nodes; i++) {
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

        System.out.println("Total tx commited:" + commited.get() + "\nTotal tx failure:" + failed.get() + "\n");

        IgniteEx ig0 = (IgniteEx)startGrids(nodes);

        ig0.active(true);

        check(ig0, data.get(), batchSize, failCommitedRange);
    }

    /**
     *
     */
    private void load(IgniteEx ig, int accounts, int balance) {
        IgniteCache<Long, Account> cache = ig.cache(CACHE_NAME);

        try (IgniteDataStreamer<Long, Account> st = ig.dataStreamer(CACHE_NAME)) {
            st.allowOverwrite(true);

            for (long id = 0; id < accounts; id++) {
                st.addData(id, new Account(id, balance));

                if (id % 100 == 0)
                    System.out.println(">>> " + id);
            }
        }
    }

    /**
     *
     */
    private void check(IgniteEx ig, long cur, int batchSize, Set<Long> skip) {
        IgniteCache<Long, Long> cache = ig.cache(CACHE_NAME);

        for (long val = 0; val < cur; val++) {
            if (skip.contains(val)){
                val += batchSize - 1;

                continue;
            }

            Long actl = cache.get(val);

            assert actl != null : "Actual key=" + val + " cur=" + cur + " skip " + skip;
        }
    }

    /**
     *
     */
    private void check(IgniteEx ig, int accounts, int balance) {
        long total = 0;

        IgniteCache<Long, Account> cache = ig.cache(CACHE_NAME);

        for (long id = 0; id < accounts; id++) {
            Account acc = cache.get(id);

            if (acc == null)
                System.out.println("id " + id);

            total += acc.balance;
        }

        Assert.assertEquals(balance * accounts, total);
    }

    /**
     *
     */
    static class Account {
        /** */
        long id;

        /** */
        long balance;

        /** */
        long transfer;

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

        @Override public String toString() {
            return "Account[" +
                "id=" + id +
                ", balance=" + balance +
                ", transfer=" + transfer +
                ']';
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

        // deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, true));
    }
}
