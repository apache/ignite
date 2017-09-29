/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db;

import java.util.Random;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jsr166.LongAdder8;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Checks that transactions don't hang during checkpoint creation.
 */
public class IgnitePdsTransactionsHangTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Page cache size. */
    private static final int PAGE_CACHE_SIZE = 512;

    /** Page size. */
    private static final Integer PAGE_SIZE = 16;

    /** Cache name. */
    private static final String CACHE_NAME = "IgnitePdsTransactionsHangTest";

    /** Number of insert threads. */
    public static final int THREADS_CNT = 32;

    /** Warm up period, seconds. */
    public static final int WARM_UP_PERIOD = 20;

    /** Duration. */
    public static final int DURATION = 180;

    /** Maximum count of inserted keys. */
    public static final int MAX_KEY_COUNT = 500_000;

    /** Checkpoint frequency. */
    public static final long CHECKPOINT_FREQUENCY = 20_000;

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        deleteRecursively(U.resolveWorkDirectory(U.defaultWorkDirectory(), DFLT_STORE_DIR, false));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        discoSpi.setIpFinder(IP_FINDER);
        cfg.setDiscoverySpi(discoSpi);

        BinaryConfiguration binaryCfg = new BinaryConfiguration();
        binaryCfg.setCompactFooter(false);
        cfg.setBinaryConfiguration(binaryCfg);

        cfg.setPeerClassLoadingEnabled(true);

        TcpCommunicationSpi tcpCommSpi = new TcpCommunicationSpi();

        tcpCommSpi.setSharedMemoryPort(-1);
        cfg.setCommunicationSpi(tcpCommSpi);

        TransactionConfiguration txCfg = new TransactionConfiguration();

        txCfg.setDefaultTxIsolation(TransactionIsolation.READ_COMMITTED);

        cfg.setTransactionConfiguration(txCfg);

        cfg.setPersistentStoreConfiguration(
            new PersistentStoreConfiguration()
                .setWalHistorySize(1)
                .setCheckpointingFrequency(CHECKPOINT_FREQUENCY)
        );

        MemoryPolicyConfiguration memPlcCfg = new MemoryPolicyConfiguration();

        memPlcCfg.setName("dfltMemPlc");
        memPlcCfg.setInitialSize(PAGE_CACHE_SIZE * 1024 * 1024);
        memPlcCfg.setMaxSize(PAGE_CACHE_SIZE * 1024 * 1024);

        MemoryConfiguration memCfg = new MemoryConfiguration();

        memCfg.setMemoryPolicies(memPlcCfg);
        memCfg.setDefaultMemoryPolicyName("dfltMemPlc");

        memCfg.setPageSize(PAGE_SIZE * 1024);

        cfg.setMemoryConfiguration(memCfg);

        return cfg;
    }

    /**
     * Creates cache configuration.
     *
     * @return Cache configuration.
     * */
    private CacheConfiguration getCacheConfiguration() {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName(CACHE_NAME);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 64 * 4));
        ccfg.setReadFromBackup(true);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        return ccfg;
    }

    /**
     * Copied from customers benchmark.
     *
     * @throws Exception If failed.
     * */
    public void testTransactionsDontHang() throws Exception {
        try {
            final Ignite g = startGrids(2);

            g.active(true);

            g.getOrCreateCache(getCacheConfiguration());

            ExecutorService threadPool = Executors.newFixedThreadPool(THREADS_CNT);
            final CyclicBarrier cyclicBarrier = new CyclicBarrier(THREADS_CNT);

            final AtomicBoolean interrupt = new AtomicBoolean(false);
            final LongAdder8 operationCnt = new LongAdder8();

            final IgniteCache<Long, TestEntity> cache = g.cache(CACHE_NAME);

            for (int i = 0; i < THREADS_CNT; i++) {
                threadPool.submit(new Runnable() {
                    @Override public void run() {
                        try {
                            ThreadLocalRandom locRandom = ThreadLocalRandom.current();

                            cyclicBarrier.await();

                            while (!interrupt.get()) {
                                long randomKey = locRandom.nextLong(MAX_KEY_COUNT);

                                TestEntity entity = TestEntity.newTestEntity(locRandom);

                                try (Transaction tx = g.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                                    cache.put(randomKey, entity);

                                    tx.commit();
                                }

                                operationCnt.increment();
                            }
                        }
                        catch (Throwable e) {
                            System.out.println(e.toString());

                            throw new RuntimeException(e);
                        }
                    }
                });
            }

            long stopTime = System.currentTimeMillis() + DURATION * 1000;
            long totalOperations = 0;
            int periods = 0;
            long max = Long.MIN_VALUE, min = Long.MAX_VALUE;

            while (System.currentTimeMillis() < stopTime) {
                U.sleep(1000);

                long sum = operationCnt.sumThenReset();
                periods++;

                if (periods > WARM_UP_PERIOD) {
                    totalOperations += sum;

                    max = Math.max(max, sum);
                    min = Math.min(min, sum);

                    System.out.println("Operation count: " + sum + " min=" + min + " max=" + max + " avg=" + totalOperations / (periods - WARM_UP_PERIOD));
                }
            }

            interrupt.set(true);

            threadPool.shutdown();
            System.out.println("Test complete");

            threadPool.awaitTermination(getTestTimeout(), TimeUnit.MILLISECONDS);

            IgniteTxManager tm = internalCache(cache).context().tm();

            assertEquals("There are still active transactions", 0, tm.activeTransactions().size());
        } finally {
            stopAllGrids();
        }
    }

    /**
     * Entity for test.
     * */
    public static class TestEntity {
        /** String value. */
        private String strVal;

        /** Long value. */
        private Long longVal;

        /** Int value. */
        private int intVal;

        /**
         * @param strVal String value.
         */
        public void setStrVal(String strVal) {
            this.strVal = strVal;
        }

        /**
         * @param longVal Long value.
         */
        public void setLongVal(Long longVal) {
            this.longVal = longVal;
        }

        /**
         * @param intVal Integer value.
         */
        public void setIntVal(int intVal) {
            this.intVal = intVal;
        }

        /**
         * Creates test entity with random values.
         *
         * @param random Random seq generator.
         * @return new test entity
         * */
        private static TestEntity newTestEntity(Random random) {
            TestEntity entity = new TestEntity();

            entity.setLongVal((long) random.nextInt(1_000));
            entity.setIntVal(random.nextInt(1_000));
            entity.setStrVal("test" + random.nextInt(1_000));

            return entity;
        }
    }
}
