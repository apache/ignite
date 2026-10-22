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

package org.apache.ignite.internal;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.stream.LongStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.metric.impl.ObjectGauge;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.mxbean.TransactionMetricsMxBean;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_EXPLICIT_LOCK_HOLD_TIME_HISTOGRAM;
import static org.apache.ignite.internal.processors.cache.transactions.TransactionMetricsAdapter.METRIC_MAX_EXPLICIT_LOCK_HOLD_TIME;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.TX_METRICS;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class TransactionMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final int TRANSACTIONS = 10;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        cfg.setLocalHost("127.0.0.1");

        final CacheConfiguration cCfg = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction(false, 32))
            .setBackups(1)
            .setAtomicityMode(TRANSACTIONAL)
            .setRebalanceMode(CacheRebalanceMode.ASYNC)
            .setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(cCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        super.afterTest();
    }

    /**
     *
     */
    @Test
    public void testTxMetric() throws Exception {
        //given:
        int keysNumber = 10;

        IgniteEx ignite = startGrid(0);

        startGrid(1);

        IgniteEx client = startClientGrid(getConfiguration(getTestIgniteInstanceName(2)));

        awaitPartitionMapExchange();

        TransactionMetricsMxBean txMXBean = getMxBean(getTestIgniteInstanceName(0), "TransactionMetrics",
            TransactionMetricsMxBeanImpl.class, TransactionMetricsMxBean.class);

        MetricRegistry mreg = grid(0).context().metric().registry(TX_METRICS);

        final IgniteCache<Integer, String> cache = ignite.cache(DEFAULT_CACHE_NAME);

        //when: one transaction commit
        ignite.transactions().txStart().commit();

        //then:
        assertEquals(1, txMXBean.getTransactionsCommittedNumber());
        assertEquals(1, mreg.<IntMetric>findMetric("txCommits").value());

        //when: transaction is opening
        final Transaction tx1 = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ);

        assertEquals(0, txMXBean.getTransactionsHoldingLockNumber());

        int locKeysNum = 0;

        for (int i = 0; i < keysNumber; i++) {
            cache.put(i, "");

            if (affinity(cache).isPrimary(ignite.localNode(), i))
                locKeysNum++;
        }

        //then:
        assertEquals(locKeysNum, mreg.<LongMetric>findMetric("LockedKeysNumber").value());
        assertEquals(1, mreg.<LongMetric>findMetric("TransactionsHoldingLockNumber").value());
        assertEquals(1, mreg.<LongMetric>findMetric("OwnerTransactionsNumber").value());

        //when: transaction rollback
        tx1.rollback();

        //then:
        assertEquals(1, txMXBean.getTransactionsRolledBackNumber());
        assertEquals(1, mreg.<IntMetric>findMetric("txRollbacks").value());
        assertEquals(0, mreg.<LongMetric>findMetric("LockedKeysNumber").value());
        assertEquals(0, mreg.<LongMetric>findMetric("TransactionsHoldingLockNumber").value());
        assertEquals(0, mreg.<LongMetric>findMetric("OwnerTransactionsNumber").value());

        //when: keysNumber transactions from owner node + keysNumber transactions from client.
        CountDownLatch commitAllower = new CountDownLatch(1);
        CountDownLatch transactionStarter = new CountDownLatch(keysNumber + keysNumber);

        int txNumFromOwner = 0;

        for (int i = 0; i < keysNumber; i++) {
            new Thread(new TxThread(commitAllower, transactionStarter, ignite, i, i)).start();

            if (affinity(cache).isPrimary(ignite.localNode(), i))
                txNumFromOwner++;
        }

        int txNumFromClient = 0;

        for (int i = keysNumber; i < keysNumber * 2; i++) {
            new Thread(new TxThread(commitAllower, transactionStarter, client, i, i)).start();

            if (affinity(cache).isPrimary(ignite.localNode(), i))
                txNumFromClient++;
        }

        transactionStarter.await();

        //then:
        assertEquals(txNumFromOwner + txNumFromClient, mreg.<LongMetric>findMetric("LockedKeysNumber").value());
        assertEquals(keysNumber + txNumFromClient,
            mreg.<LongMetric>findMetric("TransactionsHoldingLockNumber").value());
        assertEquals(keysNumber, mreg.<LongMetric>findMetric("OwnerTransactionsNumber").value());

        commitAllower.countDown();
    }

    /**
     *
     */
    @Test
    public void testNearTxInfo() throws Exception {
        IgniteEx primaryNode1 = startGrid(0);
        IgniteEx primaryNode2 = startGrid(1);
        IgniteEx nearNode = startGrid(2);

        MetricRegistry mreg = grid(2).context().metric().registry(TX_METRICS);

        awaitPartitionMapExchange();

        final IgniteCache<Integer, String> primaryCache1 = primaryNode1.cache(DEFAULT_CACHE_NAME);
        final IgniteCache<Integer, String> primaryCache2 = primaryNode2.cache(DEFAULT_CACHE_NAME);

        final List<Integer> primaryKeys1 = primaryKeys(primaryCache1, TRANSACTIONS);
        final List<Integer> primaryKeys2 = primaryKeys(primaryCache2, TRANSACTIONS);

        CountDownLatch commitAllower = new CountDownLatch(1);
        CountDownLatch transactionStarter = new CountDownLatch(primaryKeys1.size());

        for (int i = 0; i < primaryKeys1.size(); i++)
            new Thread(new TxThread(
                commitAllower,
                transactionStarter,
                nearNode,
                primaryKeys1.get(i),
                primaryKeys2.get(i)
            )).start();

        transactionStarter.await();

        final Map<String, String> transactions =
            mreg.<ObjectGauge<Map<String, String>>>findMetric("AllOwnerTransactions").value();

        assertEquals(TRANSACTIONS, transactions.size());

        int match = 0;

        for (String txInfo : transactions.values()) {
            if (txInfo.contains("ACTIVE")
                && txInfo.contains("NEAR")
                && !txInfo.contains("REMOTE"))
                match++;
        }

        assertEquals(TRANSACTIONS, match);

        commitAllower.countDown();
    }

    /** */
    @Test
    public void testExplicitLockMetrics() throws Exception {
        IgniteEx srv = startGrid(0);

        IgniteEx client = startClientGrid(1);

        checkExplicitLockMetrics(client, srv, client.cache(DEFAULT_CACHE_NAME));
        checkExplicitLockMetrics(srv, client, srv.cache(DEFAULT_CACHE_NAME));
    }

    /** */
    @Test
    public void testExplicitLockMetricsNearCache() throws Exception {
        IgniteEx srv = startGrid(0);

        // A near cache can not be created on a node that has already started the cache from its static configuration.
        IgniteEx client = startClientGrid(getConfiguration(getTestIgniteInstanceName(1)).setCacheConfiguration());

        checkExplicitLockMetrics(client, srv, client.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>()));
    }

    /**
     * @param owner Node acquiring the locks.
     * @param other Node that does not acquire them.
     * @param cache Cache to lock keys of.
     */
    private void checkExplicitLockMetrics(IgniteEx owner, IgniteEx other, IgniteCache<Integer, String> cache)
        throws Exception {
        MetricRegistry mreg = owner.context().metric().registry(TX_METRICS);

        LongMetric maxHoldTime = mreg.findMetric(METRIC_MAX_EXPLICIT_LOCK_HOLD_TIME);
        HistogramMetric holdTimeHistogram = mreg.findMetric(METRIC_EXPLICIT_LOCK_HOLD_TIME_HISTOGRAM);

        Lock lock = cache.lock(1);

        lock.lock();

        try {
            assertTrue(waitForCondition(() -> maxHoldTime.value() > 200, getTestTimeout()));

            // Locks are accounted on the owner node only.
            assertEquals(0, other.context().metric().registry(TX_METRICS)
                .<LongMetric>findMetric(METRIC_MAX_EXPLICIT_LOCK_HOLD_TIME).value());

            assertEquals(0, LongStream.of(holdTimeHistogram.value()).sum());
        }
        finally {
            lock.unlock();
        }

        assertEquals(0, maxHoldTime.value());
        assertEquals(1, LongStream.of(holdTimeHistogram.value()).sum());

        Lock multiKeyLock = cache.lockAll(Arrays.asList(2, 3));

        multiKeyLock.lock();
        multiKeyLock.lock();
        multiKeyLock.unlock();
        multiKeyLock.unlock();

        assertEquals(2, LongStream.of(holdTimeHistogram.value()).sum());
    }

    /**
     *
     */
    private static class TxThread implements Runnable {
        /** */
        private CountDownLatch commitAllowLatch;

        /** */
        private CountDownLatch transactionStartLatch;

        /** */
        private Ignite ignite;

        /** */
        private int key1;

        /** */
        private int key2;

        /**
         * Create TxThread.
         */
        private TxThread(
            CountDownLatch commitAllowLatch,
            CountDownLatch transactionStartLatch,
            final Ignite ignite,
            final int key1,
            final int key2
        ) {
            this.commitAllowLatch = commitAllowLatch;
            this.transactionStartLatch = transactionStartLatch;
            this.ignite = ignite;
            this.key1 = key1;
            this.key2 = key2;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                ignite.cache(DEFAULT_CACHE_NAME).put(key1, Thread.currentThread().getName());
                ignite.cache(DEFAULT_CACHE_NAME).put(key2, Thread.currentThread().getName());

                transactionStartLatch.countDown();

                commitAllowLatch.await();

                tx.commit();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
