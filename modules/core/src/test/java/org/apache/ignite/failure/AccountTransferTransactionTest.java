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

package org.apache.ignite.failure;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.worker.WorkersControlMXBeanImpl;
import org.apache.ignite.mxbean.WorkersControlMXBean;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Test transfer amount between accounts with enabled {@link StopNodeFailureHandler}.
 */
public class AccountTransferTransactionTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);
    /** Count of accounts in one thread. */
    private static final int ACCOUNTS_CNT = 20;
    /** Count of threads and caches. */
    private static final int THREADS_CNT = 20;
    /** Count of nodes to start. */
    private static final int NODES_CNT = 3;
    /** Count of transaction on cache. */
    private static final int TRANSACTION_CNT = 10;

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);
        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setLocalHost("127.0.0.1");
        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(50 * 1024 * 1024)
                .setPersistenceEnabled(true))
        );

        CacheConfiguration[] cacheConfigurations = new CacheConfiguration[THREADS_CNT];
        for (int i = 0; i < THREADS_CNT; i++) {
            cacheConfigurations[i] = new CacheConfiguration()
                .setName(cacheName(i))
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setBackups(1)
                .setAtomicityMode(TRANSACTIONAL)
                .setCacheMode(CacheMode.PARTITIONED)
                .setWriteSynchronizationMode(FULL_SYNC)
                .setEvictionPolicy(new FifoEvictionPolicy(1000))
                .setOnheapCacheEnabled(true);
        }

        cfg.setCacheConfiguration(cacheConfigurations);

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
     * Test transfer amount.
     */
    public void testTransferAmount() throws Exception {
        //given: started some nodes with client.
        startGrids(NODES_CNT);

        IgniteEx igniteClient = startGrid(getClientConfiguration(NODES_CNT));

        igniteClient.cluster().active(true);

        Random random = new Random();

        long[] initAmount = new long[THREADS_CNT];

        //and: fill all accounts on all caches and calculate total amount for every cache.
        for (int cachePrefixIdx = 0; cachePrefixIdx < THREADS_CNT; cachePrefixIdx++) {
            IgniteCache<Object, Object> cache = igniteClient.getOrCreateCache(cacheName(cachePrefixIdx));

            try (Transaction tx = igniteClient.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                for (int accountId = 0; accountId < ACCOUNTS_CNT; accountId++) {
                    Long amount = (long)random.nextInt(1000);

                    cache.put(accountId, amount);

                    initAmount[cachePrefixIdx] += amount;
                }

                tx.commit();
            }
        }

        //when: start transfer amount from account to account in different threads.
        CountDownLatch firstTransactionDone = new CountDownLatch(THREADS_CNT);

        ArrayList<Thread> transferThreads = new ArrayList<>();

        for (int i = 0; i < THREADS_CNT; i++) {
            transferThreads.add(new TransferAmountTxThread(firstTransactionDone, igniteClient, cacheName(i)));

            transferThreads.get(i).start();
        }

        firstTransactionDone.await(10, TimeUnit.SECONDS);

        //and: terminate disco-event-worker thread on one node.
        WorkersControlMXBean bean = workersMXBean(1);

        bean.terminateWorker(
            bean.getWorkerNames().stream()
                .filter(name -> name.startsWith("disco-event-worker"))
                .findFirst()
                .orElse(null)
        );

        for (Thread thread : transferThreads) {
            thread.join();
        }

        long[] resultAmount = new long[THREADS_CNT];

        //then: calculate total amount for every thread.
        for (int j = 0; j < THREADS_CNT; j++) {
            String cacheName = cacheName(j);

            IgniteCache<Object, Object> cache = igniteClient.getOrCreateCache(cacheName);

            try (Transaction tx = igniteClient.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {

                for (int i = 0; i < ACCOUNTS_CNT; i++)
                    resultAmount[j] += getNotNullValue(cache, i);
                tx.commit();
            }

            long diffAmount = initAmount[j] - resultAmount[j];

            //and: check that result amount equal to init amount.
            assertTrue(
                String.format("Total amount before and after transfer is not same: diff=%s, cache=%s",
                    diffAmount, cacheName),
                diffAmount == 0
            );
        }
    }

    /**
     * Make test cache name by prefix.
     */
    @NotNull private String cacheName(int cachePrefixIdx) {
        return "cache" + cachePrefixIdx;
    }

    /**
     * Ignite configuration for client.
     */
    @NotNull private IgniteConfiguration getClientConfiguration(int nodesPrefix) throws Exception {
        IgniteConfiguration clientConf = getConfiguration(getTestIgniteInstanceName(nodesPrefix));

        clientConf.setClientMode(true);

        return clientConf;
    }

    /**
     * Extract not null value from cache.
     */
    private long getNotNullValue(IgniteCache<Object, Object> cache, int i) {
        Object value = cache.get(i);

        return value == null ? 0 : ((Long)value);
    }

    /**
     * Configure workers mx bean.
     */
    private WorkersControlMXBean workersMXBean(int igniteInt) throws Exception {
        ObjectName mbeanName = U.makeMBeanName(
            getTestIgniteInstanceName(igniteInt),
            "Kernal",
            WorkersControlMXBeanImpl.class.getSimpleName()
        );

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, WorkersControlMXBean.class, true);
    }

    /**
     *
     */
    private static class TransferAmountTxThread extends Thread {
        /** */
        private CountDownLatch firstTransactionLatch;
        /** */
        private Ignite ignite;
        /** */
        private String cacheName;
        /** */
        private Random random = new Random();

        /**
         * @param ignite Ignite.
         */
        private TransferAmountTxThread(CountDownLatch firstTransactionLatch, final Ignite ignite, String cacheName) {
            this.firstTransactionLatch = firstTransactionLatch;
            this.ignite = ignite;
            this.cacheName = cacheName;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            for (int i = 0; i < TRANSACTION_CNT; i++) {
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
        @SuppressWarnings("unchecked")
        private void updateInTransaction(IgniteCache cache) throws IgniteException {
            int accIdFrom = random.nextInt(ACCOUNTS_CNT);
            int accIdTo = random.nextInt(ACCOUNTS_CNT);

            if (accIdFrom == accIdTo)
                accIdTo = (int)getNextAccountId(accIdFrom);

            Long acctFrom;
            Long acctTo;

            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                acctFrom = (Long)cache.get(accIdFrom);
                acctTo = (Long)cache.get(accIdTo);

                long transactionAmount = (long)(random.nextDouble() * acctFrom);

                cache.put(accIdFrom, acctFrom - transactionAmount);
                cache.put(accIdTo, acctTo + transactionAmount);

                tx.commit();
            }
        }

        /**
         * @param curr current
         * @return random value
         */
        private long getNextAccountId(long curr) {
            long randomVal;

            do {
                randomVal = random.nextInt(ACCOUNTS_CNT);
            }
            while (curr == randomVal);

            return randomVal;
        }
    }
}
