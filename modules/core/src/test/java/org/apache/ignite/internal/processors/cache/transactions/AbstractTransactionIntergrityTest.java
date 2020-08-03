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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Assert;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test transfer amount between accounts with enabled {@link StopNodeFailureHandler}.
 *
 * This test can be extended to emulate failover scenarios during transactional operations on the grid.
 */
public class AbstractTransactionIntergrityTest extends GridCommonAbstractTest {
    /** Count of accounts in one thread. */
    private static final int DFLT_ACCOUNTS_CNT = 32;

    /** Count of threads and caches. */
    private static final int DFLT_TX_THREADS_CNT = Runtime.getRuntime().availableProcessors();

    /** Count of nodes to start. */
    private static final int DFLT_NODES_CNT = 3;

    /** Count of transaction on cache. */
    private static final int DFLT_TRANSACTIONS_CNT = 10;

    /** Completed transactions map. */
    private ConcurrentLinkedHashMap[] completedTxs;

    /**
     *
     */
    protected int nodesCount() {
        return DFLT_NODES_CNT;
    }

    /**
     *
     */
    protected int accountsCount() {
        return DFLT_ACCOUNTS_CNT;
    }

    /**
     *
     */
    protected int transactionsCount() {
        return DFLT_TRANSACTIONS_CNT;
    }

    /**
     *
     */
    protected int txThreadsCount() {
        return DFLT_TX_THREADS_CNT;
    }

    /**
     * @return Flag enables secondary index on account caches.
     */
    protected boolean indexed() {
        return false;
    }

    /**
     * @return Flag enables persistence on account caches.
     */
    protected boolean persistent() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                    .setPersistenceEnabled(persistent())
                    .setMaxSize(50 * 1024 * 1024)
            )
            .setWalSegmentSize(16 * 1024 * 1024)
            .setPageSize(1024)
            .setWalMode(WALMode.LOG_ONLY));

        CacheConfiguration[] cacheConfigurations = new CacheConfiguration[txThreadsCount()];

        for (int i = 0; i < txThreadsCount(); i++) {
            CacheConfiguration ccfg = new CacheConfiguration()
                .setName(cacheName(i))
                .setAffinity(new RendezvousAffinityFunction(false, accountsCount()))
                .setBackups(1)
                .setAtomicityMode(TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setReadFromBackup(true)
                .setOnheapCacheEnabled(true);

            if (indexed())
                ccfg.setIndexedTypes(IgniteUuid.class, AccountState.class);

            cacheConfigurations[i] = ccfg;
        }

        cfg.setCacheConfiguration(cacheConfigurations);

        cfg.setFailureDetectionTimeout(30_000);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * Make test cache name by prefix.
     */
    @NotNull private String cacheName(int cachePrefixIdx) {
        return "cache" + cachePrefixIdx;
    }

    /**
     * Test transfer amount.
     *
     * @param failoverScenario Scenario.
     * @param colocatedAccounts {@code True} to use colocated on same primary node accounts.
     */
    public void doTestTransferAmount(FailoverScenario failoverScenario, boolean colocatedAccounts) throws Exception {
        failoverScenario.beforeNodesStarted();

        //given: started some nodes with client.
        startGrids(nodesCount());

        IgniteEx igniteClient = startClientGrid(getConfiguration(getTestIgniteInstanceName(nodesCount())));

        igniteClient.cluster().active(true);

        int[] initAmounts = new int[txThreadsCount()];
        completedTxs = new ConcurrentLinkedHashMap[txThreadsCount()];

        //and: fill all accounts on all caches and calculate total amount for every cache.
        for (int cachePrefixIdx = 0; cachePrefixIdx < txThreadsCount(); cachePrefixIdx++) {
            IgniteCache<Integer, AccountState> cache = igniteClient.getOrCreateCache(cacheName(cachePrefixIdx));

            AtomicInteger coinsCntr = new AtomicInteger();

            try (Transaction tx = igniteClient.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int accountId = 0; accountId < accountsCount(); accountId++) {
                    Set<Integer> initAmount = generateCoins(coinsCntr, 5);

                    cache.put(accountId, new AccountState(accountId, tx.xid(), initAmount));
                }

                tx.commit();
            }

            initAmounts[cachePrefixIdx] = coinsCntr.get();
            completedTxs[cachePrefixIdx] = new ConcurrentLinkedHashMap();
        }

        //when: start transfer amount from account to account in different threads.
        CountDownLatch firstTransactionDone = new CountDownLatch(txThreadsCount());

        ArrayList<Thread> transferThreads = new ArrayList<>();

        for (int i = 0; i < txThreadsCount(); i++) {
            transferThreads.add(new TransferAmountTxThread(firstTransactionDone,
                igniteClient, cacheName(i), i, colocatedAccounts));

            transferThreads.get(i).start();
        }

        firstTransactionDone.await(10, TimeUnit.SECONDS);

        failoverScenario.afterFirstTransaction();

        for (Thread thread : transferThreads)
            thread.join();

        failoverScenario.afterTransactionsFinished();

        consistencyCheck(initAmounts);
    }

    /**
     * Calculates total amount of coins for every thread for every node and checks that coins difference is zero (transaction integrity is saved).
     */
    private void consistencyCheck(int[] initAmount) {
        for (Ignite node : G.allGrids()) {
            for (int j = 0; j < txThreadsCount(); j++) {
                List<Integer> totalCoins = new ArrayList<>();

                String cacheName = cacheName(j);

                IgniteCache<Integer, AccountState> cache = node.getOrCreateCache(cacheName);

                AccountState[] accStates = new AccountState[accountsCount()];

                try (Transaction tx = node.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    for (int i = 0; i < accountsCount(); i++) {
                        AccountState state = cache.get(i);

                        Assert.assertNotNull("Account state has lost [node=" + node.name() + ", cache=" + cacheName + ", accNo=" + i + "]", state);

                        totalCoins.addAll(state.coins);

                        accStates[i] = state;
                    }

                    tx.commit();
                }

                Collections.sort(totalCoins);

                if (initAmount[j] != totalCoins.size()) {
                    Set<Integer> lostCoins = new HashSet<>();
                    Set<Integer> duplicateCoins = new HashSet<>();

                    for (int coin = 1; coin <= initAmount[j]; coin++)
                        if (!totalCoins.contains(coin))
                            lostCoins.add(coin);

                    for (int coinIdx = 1; coinIdx < totalCoins.size(); coinIdx++)
                        if (totalCoins.get(coinIdx).equals(totalCoins.get(coinIdx - 1)))
                            duplicateCoins.add(totalCoins.get(coinIdx));

                    log.error("Transaction integrity failed for [node=" + node.name() + ", cache=" + cacheName + "]");

                    log.error(String.format("Total amount of coins before and after transfers are not same. Lost coins: %s. Duplicate coins: %s.",
                        Objects.toString(lostCoins),
                        Objects.toString(duplicateCoins)));

                    ConcurrentLinkedHashMap<IgniteUuid, TxState> txs = completedTxs[j];

                    for (TxState tx : txs.values())
                        log.error("Tx: " + tx);

                    for (int i = 0; i < accountsCount(); i++)
                        log.error("Account state " + i + " = " + accStates[i]);

                    assertFalse("Test failed. See messages above", true);
                }
            }
        }
    }

    /**
     *
     */
    public static class AccountState {
        /** Account id. */
        private final int accId;

        /** Last performed transaction id on account state. */
        @QuerySqlField(index = true)
        private final IgniteUuid txId;

        /** Set of coins holds in account. */
        private final Set<Integer> coins;

        /**
         * @param accId Acc id.
         * @param txId Tx id.
         * @param coins Coins.
         */
        public AccountState(int accId, IgniteUuid txId, Set<Integer> coins) {
            this.txId = txId;
            this.coins = Collections.unmodifiableSet(coins);
            this.accId = accId;
        }

        /**
         * @param random Randomizer.
         * @return Set of coins need to transfer from.
         */
        public Set<Integer> coinsToTransfer(Random random) {
            int coinsNum = random.nextInt(coins.size());

            return coins.stream().limit(coinsNum).collect(Collectors.toSet());
        }

        /**
         * @param txId Transaction id.
         * @param coinsToAdd Coins to add to current account.
         * @return Account state with added coins.
         */
        public AccountState addCoins(IgniteUuid txId, Set<Integer> coinsToAdd) {
            return new AccountState(accId, txId, Sets.union(coins, coinsToAdd).immutableCopy());
        }

        /**
         * @param txId Transaction id.
         * @param coinsToRmv Coins to remove from current account.
         * @return Account state with removed coins.
         */
        public AccountState removeCoins(IgniteUuid txId, Set<Integer> coinsToRmv) {
            return new AccountState(accId, txId, Sets.difference(coins, coinsToRmv).immutableCopy());
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AccountState that = (AccountState) o;
            return Objects.equals(txId, that.txId) &&
                Objects.equals(coins, that.coins);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(txId, coins);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "AccountState{" +
                "accId=" + Objects.toString(accId) +
                ", coins=" + Objects.toString(coins) +
                '}';
        }
    }

    /**
     * @param coinsNum Coins number.
     */
    private Set<Integer> generateCoins(AtomicInteger coinsCntr, int coinsNum) {
        Set<Integer> res = new HashSet<>();

        for (int i = 0; i < coinsNum; i++)
            res.add(coinsCntr.incrementAndGet());

        return res;
    }

    /**
     * State representing transaction between two accounts.
     */
    static class TxState {
        /**
         * Account states before transaction.
         */
        AccountState before1;

        /**
         * Account states before transaction.
         */
        AccountState before2;

        /**
         * Account states after transaction.
         */
        AccountState after1;

        /**
         * Account states after transaction.
         */
        AccountState after2;

        /**
         * Transferred coins between accounts during this transaction.
         */
        Set<Integer> transferredCoins;

        /**
         * @param before1 Before 1.
         * @param before2 Before 2.
         * @param after1 After 1.
         * @param after2 After 2.
         * @param transferredCoins Transferred coins.
         */
        public TxState(AccountState before1, AccountState before2, AccountState after1, AccountState after2, Set<Integer> transferredCoins) {
            this.before1 = before1;
            this.before2 = before2;
            this.after1 = after1;
            this.after2 = after2;
            this.transferredCoins = transferredCoins;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TxState{" +
                "before1=" + before1 +
                ", before2=" + before2 +
                ", transferredCoins=" + transferredCoins +
                ", after1=" + after1 +
                ", after2=" + after2 +
                '}';
        }
    }

    /**
     *
     */
    private class TransferAmountTxThread extends Thread {
        /** */
        private CountDownLatch firstTransactionLatch;

        /** */
        private IgniteEx ignite;

        /** */
        private String cacheName;

        /** */
        private int workerIdx;

        /** */
        private Random random = new Random();

        /** */
        private final boolean colocatedAccounts;

        /**
         * @param ignite Ignite.
         */
        private TransferAmountTxThread(CountDownLatch firstTransactionLatch,
            final IgniteEx ignite,
            String cacheName,
            int workerIdx,
            boolean colocatedAccounts) {
            this.firstTransactionLatch = firstTransactionLatch;
            this.ignite = ignite;
            this.cacheName = cacheName;
            this.workerIdx = workerIdx;
            this.colocatedAccounts = colocatedAccounts;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            for (int i = 0; i < transactionsCount(); i++) {
                try {
                    updateInTransaction(ignite.cache(cacheName));
                }
                finally {
                    if (i == 0)
                        firstTransactionLatch.countDown();
                }
            }
        }

        /**
         * @throws IgniteException if fails
         */
        private void updateInTransaction(IgniteCache<Integer, AccountState> cache) throws IgniteException {
            int accIdFrom;
            int accIdTo;

            for (;;) {
                accIdFrom = random.nextInt(accountsCount());
                accIdTo = random.nextInt(accountsCount());

                if (accIdFrom == accIdTo)
                    continue;

                Affinity<Object> affinity = ignite.affinity(cacheName);

                ClusterNode primaryForAccFrom = affinity.mapKeyToNode(accIdFrom);
                assertNotNull(primaryForAccFrom);

                ClusterNode primaryForAccTo = affinity.mapKeyToNode(accIdTo);
                assertNotNull(primaryForAccTo);

                // Allows only transaction between accounts that primary on the same node if corresponding flag is enabled.
                if (colocatedAccounts && !primaryForAccFrom.id().equals(primaryForAccTo.id()))
                    continue;

                break;
            }

            AccountState acctFrom;
            AccountState acctTo;

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                acctFrom = cache.get(accIdFrom);
                assertNotNull(acctFrom);

                acctTo = cache.get(accIdTo);
                assertNotNull(acctTo);

                Set<Integer> coinsToTransfer = acctFrom.coinsToTransfer(random);

                AccountState nextFrom = acctFrom.removeCoins(tx.xid(), coinsToTransfer);
                AccountState nextTo = acctTo.addCoins(tx.xid(), coinsToTransfer);

                cache.put(accIdFrom, nextFrom);
                cache.put(accIdTo, nextTo);

                tx.commit();

                completedTxs[workerIdx].put(tx.xid(), new TxState(acctFrom, acctTo, nextFrom, nextTo, coinsToTransfer));
            }
        }
    }

    /**
     * Interface to implement custom failover scenario during transactional amount transfer.
     */
    public interface FailoverScenario {
        /**
         * Callback before nodes have started.
         */
        public default void beforeNodesStarted() throws Exception { }

        /**
         * Callback when first transaction has finished.
         */
        public default void afterFirstTransaction() throws Exception { }

        /**
         * Callback when all transactions have finished.
         */
        public default void afterTransactionsFinished() throws Exception { }
    }
}
