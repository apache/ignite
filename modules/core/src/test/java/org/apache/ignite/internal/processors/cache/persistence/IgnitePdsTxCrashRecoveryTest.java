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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryRequest;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheTxRecoveryResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class IgnitePdsTxCrashRecoveryTest extends GridCommonAbstractTest {
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

        cfg.setCommunicationSpi(new BlockTcpCommunicationSpi());

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setCheckpointingFrequency(20_000)
        );

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionCacheRecovery1() throws Exception {
        CacheConfiguration<Long, Account> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setBackups(2);

        startGrids(3);

        IgniteEx node0 = grid(0);
        IgniteEx node1 = grid(1);
        IgniteEx node2 = grid(1);

        node0.active(true);

        Affinity<Long> affFunc = affinity(node0.createCache(ccfg));

        long key = -1L;

        while (key < 10_000) {
            key++;

            if (affFunc.isPrimary(node0.localNode(), key) &&
                affFunc.isBackup(node1.localNode(), key))
                break;
        }

        assertTrue("Fail find key.", key >= 0);

        IgniteConfiguration igCfg = getConfiguration("client");

        igCfg.setClientMode(true);
        igCfg.setConsistentId(igCfg.getIgniteInstanceName());

        final IgniteEx client = startGrid(igCfg);

        IgniteCache<Long, Account> cache = client.cache(CACHE_NAME);

        Account acc = new Account(key, 100);

        System.err.println("Acc " + acc);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key, acc);

            tx.commit();
        }

        U.sleep(2_000);

        System.out.println(">>> Account loaded.");

        BlockTcpCommunicationSpi comSpi2 = (BlockTcpCommunicationSpi)node2.configuration().getCommunicationSpi();

        comSpi2.blockMessage(new C1<GridIoMessage, Boolean>() {
            @Override public Boolean apply(GridIoMessage msg) {
                return !(msg.message() instanceof GridDhtTxPrepareResponse);
            }
        });

        BlockTcpCommunicationSpi comSpi1 = (BlockTcpCommunicationSpi)node1.configuration().getCommunicationSpi();

        final AtomicReference<Thread> stopThread = new AtomicReference<>();

        comSpi1.blockMessage(new C1<GridIoMessage, Boolean>() {
            private int cnt;

            @Override public Boolean apply(GridIoMessage msg) {
                if (msg.message() instanceof GridDhtTxPrepareResponse) {
                    Thread stop = new Thread(
                        new Runnable() {
                            @Override public void run() {
                                try {
                                    stopGrid(0, true);
                                    stopGrid(1, true);
                                    stopGrid(2, true);
                                    stopGrid(client.name(), true);
                                }
                                catch (Throwable h) {
                                    System.out.println(h.getMessage());
                                }
                            }
                        }
                    );

                    stopThread.set(stop);

                    stop.start();

                    return false;
                }
                else
                    return true;
            }
        });

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account account = cache.get(key);

            account.balance += 50;

            cache.put(key, account);

            tx.commit();
        }
        catch (IgniteException e) {
            // Skip stopping exception.
        }

        Thread th = stopThread.get();

        assertTrue(th != null);

        th.join();

        startGrids(3);

        node0 = grid(0);

        node0.active(true);

        checkCache(grid(0).<Long, Account>cache(CACHE_NAME), key, acc);
        checkCache(grid(1).<Long, Account>cache(CACHE_NAME), key, acc);
        checkCache(grid(2).<Long, Account>cache(CACHE_NAME), key, acc);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionCacheRecovery3() throws Exception {
        CacheConfiguration<Long, Account> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setBackups(2);

        startGrids(3);

        IgniteEx node0 = grid(0);
        IgniteEx node1 = grid(1);

        node0.active(true);

        Affinity<Long> affFunc = affinity(node0.createCache(ccfg));

        long key = -1L;

        while (key < 10_000) {
            key++;

            if (affFunc.isPrimary(node0.localNode(), key) &&
                affFunc.isBackup(node1.localNode(), key))
                break;
        }

        assertTrue("Fail find key.", key >= 0);

        IgniteConfiguration igCfg = getConfiguration("client");

        igCfg.setClientMode(true);
        igCfg.setConsistentId(igCfg.getIgniteInstanceName());

        final IgniteEx client = startGrid(igCfg);

        IgniteCache<Long, Account> cache = client.cache(CACHE_NAME);

        Account acc = new Account(key, 100);

        System.err.println("Acc " + acc);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key, acc);

            tx.commit();
        }

        U.sleep(2_000);

        System.out.println(">>> Account loaded.");

        BlockTcpCommunicationSpi comSpi = (BlockTcpCommunicationSpi)node0.configuration().getCommunicationSpi();

        final AtomicReference<Thread> stopThread = new AtomicReference<>();

        comSpi.blockMessage(new C1<GridIoMessage, Boolean>() {
            private int cnt;

            @Override public Boolean apply(GridIoMessage msg) {
                if (msg.message() instanceof GridDhtTxFinishRequest) {
                    Thread stop = new Thread(
                        new Runnable() {
                            @Override public void run() {
                                try {
                                    stopGrid(0, true);
                                    stopGrid(1, true);
                                    stopGrid(2, true);
                                    stopGrid(client.name(), true);
                                }
                                catch (Throwable h) {
                                    System.out.println(h.getMessage());
                                }
                            }
                        }
                    );

                    stopThread.set(stop);

                    stop.start();

                    return false;
                }
                else
                    return true;
            }
        });

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account account = cache.get(key);

            account.balance += 50;

            cache.put(key, account);

            tx.commit();
        }
        catch (IgniteException e) {
            // Skip stopping exception.
        }

        Thread th = stopThread.get();

        assertTrue(th != null);

        th.join();

        startGrids(3);

        acc.balance += 50;

        node0 = grid(0);

        node0.active(true);

        checkCache(grid(0).<Long, Account>cache(CACHE_NAME), key, acc);
        checkCache(grid(1).<Long, Account>cache(CACHE_NAME), key, acc);
        checkCache(grid(2).<Long, Account>cache(CACHE_NAME), key, acc);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionCacheRecovery4() throws Exception {
        CacheConfiguration<Long, Account> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(2);

        startGrids(3);

        IgniteEx node0 = grid(0);
        IgniteEx node1 = grid(1);
        IgniteEx node2 = grid(2);

        node0.active(true);

        Affinity<Long> affFunc = affinity(node0.createCache(ccfg));

        long key = -1L;

        while (key < 10_000) {
            key++;

            if (affFunc.isPrimary(node0.localNode(), key) &&
                affFunc.isBackup(node1.localNode(), key))
                break;
        }

        assertTrue("Fail find key.", key >= 0);

        IgniteConfiguration igCfg = getConfiguration("client");

        igCfg.setClientMode(true);
        igCfg.setConsistentId(igCfg.getIgniteInstanceName());

        final IgniteEx client = startGrid(igCfg);

        IgniteCache<Long, Account> cache = client.cache(CACHE_NAME);

        Account acc = new Account(key, 100);

        System.err.println("Acc " + acc);

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            cache.put(key, acc);

            tx.commit();
        }

        U.sleep(2_000);

        System.out.println(">>> Account loaded.");

        BlockTcpCommunicationSpi comSpi2 = (BlockTcpCommunicationSpi)node2.configuration().getCommunicationSpi();

        comSpi2.blockMessage(new C1<GridIoMessage, Boolean>() {
            @Override public Boolean apply(GridIoMessage msg) {
                return !(msg.message() instanceof GridCacheTxRecoveryRequest || msg.message() instanceof GridCacheTxRecoveryResponse);
            }
        });

        BlockTcpCommunicationSpi comSpi1 = (BlockTcpCommunicationSpi)node1.configuration().getCommunicationSpi();

        //TODO tx recovery fut
        comSpi1.blockMessage(new C1<GridIoMessage, Boolean>() {
            @Override public Boolean apply(GridIoMessage msg) {
                return !(msg.message() instanceof GridCacheTxRecoveryRequest || msg.message() instanceof GridCacheTxRecoveryResponse);
            }
        });

        BlockTcpCommunicationSpi comSpi0 = (BlockTcpCommunicationSpi)node0.configuration().getCommunicationSpi();

        final AtomicReference<Thread> stopThread = new AtomicReference<>();

        comSpi0.blockMessage(new C1<GridIoMessage, Boolean>() {
            private int cnt;

            @Override public Boolean apply(GridIoMessage msg) {
                if (msg.message() instanceof GridCacheTxRecoveryRequest || msg.message() instanceof GridCacheTxRecoveryResponse)
                    return false;

                if (msg.message() instanceof GridNearTxPrepareResponse) {
                    Thread stop = new Thread(
                        new Runnable() {
                            @Override public void run() {
                                try {
                                    stopGrid(0, true);
                                    stopGrid(1, true);
                                    stopGrid(2, true);
                                    stopGrid(client.name(), true);
                                }
                                catch (Throwable h) {
                                    System.out.println(h.getMessage());
                                }
                            }
                        }
                    );

                    stopThread.set(stop);

                    stop.start();

                    return false;
                }
                else
                    return true;
            }
        });

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account account = cache.get(key);

            account.balance += 50;

            cache.put(key, account);

            tx.commit();
        }
        catch (IgniteException e) {
            // Skip stopping exception.
        }

        Thread th = stopThread.get();

        assertTrue(th != null);

        th.join();

        startGrids(3);

        acc.balance += 50;

        node0 = grid(0);

        node0.active(true);

        checkCache(grid(0).<Long, Account>cache(CACHE_NAME), key, acc);
        checkCache(grid(1).<Long, Account>cache(CACHE_NAME), key, acc);
        checkCache(grid(2).<Long, Account>cache(CACHE_NAME), key, acc);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionCrashRecoveryOnePhaseCommitCommit() throws Exception {
        CacheConfiguration<Long, Account> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);

        startGrids(2);

        IgniteEx node0 = grid(0);
        IgniteEx node1 = grid(1);

        node0.active(true);

        Affinity<Long> affFunc = affinity(node0.createCache(ccfg));

        long key = -1L;

        while (key < 10_000) {
            key++;

            if (affFunc.isPrimary(node0.localNode(), key) &&
                affFunc.isBackup(node1.localNode(), key))
                break;
        }

        assertTrue("Fail find key.", key >= 0);

        IgniteConfiguration igCfg = getConfiguration("client");

        igCfg.setClientMode(true);
        igCfg.setConsistentId(igCfg.getIgniteInstanceName());

        final IgniteEx client = startGrid(igCfg);

        IgniteCache<Long, Account> cache = client.cache(CACHE_NAME);

        Account acc = new Account(key, 100);

        cache.put(key, acc);

        System.out.println(">>> account loaded.");

        BlockTcpCommunicationSpi comSpi = (BlockTcpCommunicationSpi)node1.configuration().getCommunicationSpi();

        final AtomicReference<Thread> stopThread = new AtomicReference<>();

        comSpi.blockMessage(new C1<GridIoMessage, Boolean>() {
            @Override public Boolean apply(GridIoMessage msg) {
                if (msg.message() instanceof GridDhtTxPrepareResponse) {
                    Thread stop = new Thread(
                        new Runnable() {
                            @Override public void run() {
                                try {
                                    stopGrid(0, true);
                                    stopGrid(1, true);
                                    stopGrid(client.name(), true);
                                }
                                catch (Throwable h) {
                                    System.out.println(h.getMessage());
                                }
                            }
                        }
                    );

                    stopThread.set(stop);

                    stop.start();

                    return false;
                }

                return true;
            }
        });

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account account = cache.get(key);

            account.balance += 10;

            cache.put(key, account);

            tx.commit();
        }
        catch (IgniteException e) {
            // Skip stopping exception.
        }

        Thread th = stopThread.get();

        assertTrue(th != null);

        th.join();

        startGrids(2);

        node0 = grid(0);

        acc.balance += 10;

        node0.active(true);

        checkCache(grid(1).<Long, Account>cache(CACHE_NAME), key, acc);
        checkCache(grid(0).<Long, Account>cache(CACHE_NAME), key, acc);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransactionCrashRecoveryOnePhaseCommitRollBack() throws Exception {
        CacheConfiguration<Long, Account> ccfg = new CacheConfiguration<>(CACHE_NAME);

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);

        startGrids(2);

        IgniteEx node0 = grid(0);
        IgniteEx node1 = grid(1);

        node0.active(true);

        Affinity<Long> affFunc = affinity(node0.createCache(ccfg));

        long key = -1L;

        while (key < 10_000) {
            key++;

            if (affFunc.isPrimary(node0.localNode(), key) &&
                affFunc.isBackup(node1.localNode(), key))
                break;
        }

        assertTrue("Fail find key.", key >= 0);

        IgniteConfiguration igCfg = getConfiguration("client");

        igCfg.setClientMode(true);
        igCfg.setConsistentId(igCfg.getIgniteInstanceName());

        final IgniteEx client = startGrid(igCfg);

        IgniteCache<Long, Account> cache = client.cache(CACHE_NAME);

        Account acc = new Account(key, 100);

        cache.put(key, acc);

        System.out.println(">>> account loaded.");

        BlockTcpCommunicationSpi comSpi = (BlockTcpCommunicationSpi)node0.configuration().getCommunicationSpi();

        final AtomicReference<Thread> stopThread = new AtomicReference<>();

        comSpi.blockMessage(new C1<GridIoMessage, Boolean>() {
            @Override public Boolean apply(GridIoMessage msg) {
                if (msg.message() instanceof GridDhtTxPrepareRequest) {
                    Thread stop = new Thread(
                        new Runnable() {
                            @Override public void run() {
                                try {
                                    stopGrid(0, true);
                                    stopGrid(1, true);
                                    stopGrid(client.name(), true);
                                }
                                catch (Throwable h) {
                                    System.out.println(h.getMessage());
                                }
                            }
                        }
                    );

                    stopThread.set(stop);

                    stop.start();

                    return false;
                }

                return true;
            }
        });

        try (Transaction tx = client.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
            Account account = cache.get(key);

            account.balance += 10;

            cache.put(key, account);

            tx.commit();
        }
        catch (IgniteException e) {
            // Skip stopping exception.
        }

        Thread th = stopThread.get();

        assertTrue(th != null);

        th.join();

        startGrids(2);

        node0 = grid(0);

        node0.active(true);

        checkCache(grid(1).<Long, Account>cache(CACHE_NAME), key, acc);
        checkCache(grid(0).<Long, Account>cache(CACHE_NAME), key, acc);
    }

    /**
     *
     */
    private void checkCache(IgniteCache<Long, Account> cache, long key, Account expVal) {
        Account acc = cache.get(key);

        assertTrue(acc != null);

        assertTrue("Exp " + expVal + " Actl " + acc, expVal.equals(acc));
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
        private Account(long id, long balance) {
            this.id = id;
            this.balance = balance;
        }

        /** {@inheritDoc} */
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

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = (int)(id ^ (id >>> 32));

            result = 31 * result + (int)(balance ^ (balance >>> 32));

            return result;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Account[" +
                "id=" + id +
                ", balance=" + balance +
                ']';
        }
    }

    /**
     *
     */
    private static class BlockTcpCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private volatile Class msgCls;

        /** */
        private volatile IgniteClosure<GridIoMessage, Boolean> lsn;

        /** */
        private ConcurrentHashMap<String, ClusterNode> classes = new ConcurrentHashMap<>();

        /** */
        @LoggerResource
        private IgniteLogger log;

        @IgniteInstanceResource
        private IgniteEx ig;

        /** {@inheritDoc} */
        @Override public void sendMessage(
            ClusterNode node,
            Message msg,
            IgniteInClosure<IgniteException> ackC
        ) throws IgniteSpiException {

            if (msg instanceof GridIoMessage && lsn != null) {
                if (!lsn.apply(((GridIoMessage)msg))) {
                    System.err.println(">>>! Block " + ((GridIoMessage)msg).message().getClass().getSimpleName() + " [" +
                        ig.context().discovery().localNode().consistentId() + " -> " + node.consistentId() + "]");

                    return;
                }

                System.err.println(">>>! Send " + ((GridIoMessage)msg).message().getClass().getSimpleName() + " [" +
                    ig.context().discovery().localNode().consistentId() + " -> " + node.consistentId() + "]");
            }

            super.sendMessage(node, msg, ackC);
        }

        /**
         *
         */
        public void blockMessage(IgniteClosure<GridIoMessage, Boolean> cls) {
            lsn = cls;
        }

        /**
         * Print collected messages.
         */
        public void print() {
            for (String s : classes.keySet())
                log.error(s);
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
