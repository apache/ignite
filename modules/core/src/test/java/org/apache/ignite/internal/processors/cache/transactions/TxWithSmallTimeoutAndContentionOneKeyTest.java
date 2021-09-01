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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyResultV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.PartitionKeyV2;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class TxWithSmallTimeoutAndContentionOneKeyTest extends GridCommonAbstractTest {
    /** */
    private static final int TIME_TO_EXECUTE = 30 * 1000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId("NODE_" + name.substring(name.length() - 1));

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(DataStorageConfiguration.DFLT_DATA_REGION_INITIAL_SIZE)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setBackups(3)
        );

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
     * @return Random transaction type.
     */
    protected TransactionConcurrency transactionConcurrency() {
        if (MvccFeatureChecker.forcedMvcc())
            return PESSIMISTIC;

        ThreadLocalRandom random = ThreadLocalRandom.current();

        return random.nextBoolean() ? OPTIMISTIC : PESSIMISTIC;
    }

    /**
     * @return Random transaction isolation level.
     */
    protected TransactionIsolation transactionIsolation() {
        if (MvccFeatureChecker.forcedMvcc())
            return REPEATABLE_READ;

        ThreadLocalRandom random = ThreadLocalRandom.current();

        switch (random.nextInt(3)) {
            case 0:
                return READ_COMMITTED;
            case 1:
                return REPEATABLE_READ;
            case 2:
                return SERIALIZABLE;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * @return Random timeout.
     */
    protected long randomTimeOut() {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        return random.nextLong(5, 20);
    }

    /**
     * https://issues.apache.org/jira/browse/IGNITE-9042
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        Assume.assumeFalse("https://issues.apache.org/jira/browse/IGNITE-10455", MvccFeatureChecker.forcedMvcc());

        startGrids(4);

        IgniteEx igClient = startClientGrid(getConfiguration("client").setConsistentId("Client"));

        igClient.cluster().active(true);

        AtomicBoolean stop = new AtomicBoolean(false);

        IgniteCache<Integer, Long> cache = igClient.cache(DEFAULT_CACHE_NAME);

        int threads = 1;

        int keyId = 0;

        CountDownLatch finishLatch = new CountDownLatch(threads);

        AtomicLong cnt = new AtomicLong();

        IgniteInternalFuture<Long> f = runMultiThreadedAsync(() -> {
            IgniteTransactions txMgr = igClient.transactions();

            while (!stop.get()) {
                long newVal = cnt.getAndIncrement();

                TransactionConcurrency concurrency = transactionConcurrency();

                TransactionIsolation transactionIsolation = transactionIsolation();

                try (Transaction tx = txMgr.txStart(concurrency, transactionIsolation, randomTimeOut(), 1)) {
                    cache.put(keyId, newVal);

                    tx.commit();
                }
                catch (Throwable e) {
                  // Ignore.
                }
            }

            finishLatch.countDown();

        }, threads, "tx-runner");

        runAsync(() -> {
            try {
                Thread.sleep(TIME_TO_EXECUTE);
            }
            catch (InterruptedException ignore) {
                // Ignore.
            }

            stop.set(true);
        });

        finishLatch.await();

        f.get();

        IdleVerifyResultV2 idleVerifyResult = idleVerify(igClient, DEFAULT_CACHE_NAME);

        log.info("Current counter value:" + cnt.get());

        Long val = cache.get(keyId);

        log.info("Last commited value:" + val);

        if (idleVerifyResult.hasConflicts()) {
            SB sb = new SB();

            sb.a("\n");

            buildConflicts("Hash conflicts:\n", sb, idleVerifyResult.hashConflicts());
            buildConflicts("Counters conflicts:\n", sb, idleVerifyResult.counterConflicts());

            System.out.println(sb);

            fail();
        }
    }

    /**
     * @param msg Header message.
     * @param conflicts Conflicts map.
     * @param sb String builder.
     */
    private void buildConflicts(String msg, SB sb, Map<PartitionKeyV2, List<PartitionHashRecordV2>> conflicts) {
        sb.a(msg);

        for (Map.Entry<PartitionKeyV2, List<PartitionHashRecordV2>> entry : conflicts.entrySet()) {
            sb.a(entry.getKey()).a("\n");

            for (PartitionHashRecordV2 rec : entry.getValue())
                sb.a("\t").a(rec).a("\n");
        }

    }
}
