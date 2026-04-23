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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

@RunWith(Parameterized.class)
public class TxSavepointParameterizedTest extends GridCommonAbstractTest {
    @Parameter(0)
    public boolean initKeies;

    @Parameter(1)
    public boolean useNearCache;

    @Parameter(2)
    public int backups;

    @Parameter(3)
    public boolean spKeyOnTxInitiator;

    @Parameter(4)
    public boolean replicated;

    private static Ignite ignite0;
    private static Ignite ignite1;
    private static Ignite ignite2;
    private static Ignite ignite3;

    @Parameters(name = "initKeies={0}, useNearCache={1}, backups={2}, spKeyOnTxInitiator={3}, replicated={4}")
    public static Collection<Object[]> testData() {
        return List.of(new Object[][] {
            // backups = 0
            {true,  true,  0, true, false},
            {true,  true,  0, false, false},
            {true,  false, 0, true, false},
            {true,  false, 0, false, false},
            {false, true,  0, true, false},
            {false, true,  0, false, false},
            {false, false, 0, true, false},
            {false, false, 0, false, false},

            // backups = 1
            {true,  true,  1, true, false},
            {true,  true,  1, false, false},
            {true,  false, 1, true, false},
            {true,  false, 1, false, false},
            {false, true,  1, true, false},
            {false, true,  1, false, false},
            {false, false, 1, true, false},
            {false, false, 1, false, false},

            // backups = 2
            {true,  true,  2, true, false},
            {true,  true,  2, false, false},
            {true,  false, 2, true, false},
            {true,  false, 2, false, false},
            {false, true,  2, true, false},
            {false, true,  2, false, false},
            {false, false, 2, true, false},
            {false, false, 2, false, false},

            // replicated cache.
            {true,  true,  0, true, true},
            {true,  false, 0, true, true},
            {false, true,  0, true, true},
            {false, false, 0, true, true},
        });
    }

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
        ignite3 = startGrid(3);
    }

    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    @Override protected void afterTest() throws Exception {
        ignite0.destroyCache(DEFAULT_CACHE_NAME);

        super.afterTest();
    }

    @Test
    public void testDhtEntriesAfterRollbackToSavepoint() throws Exception {
        awaitPartitionMapExchange();

        IgniteCache<Integer, Integer> cache0 = transactionalCache(ignite0);

        int keepTxAliveKey = primaryKey(cache0);
        int dhtKey = keyForPrimaryAndBackup(ignite0, ignite1);

        if (initKeies) {
            cache0.put(keepTxAliveKey, -1);
            cache0.put(dhtKey, -1);
        }

        CountDownLatch dhtKeyWrittenLatch = new CountDownLatch(1);
        CountDownLatch proceedRollbackLatch = new CountDownLatch(1);
        CountDownLatch rollbackDoneLatch = new CountDownLatch(1);
        CountDownLatch finishTxLatch = new CountDownLatch(1);
        GridCacheVersion[] nearVerRef = new GridCacheVersion[1];

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(PESSIMISTIC, REPEATABLE_READ, 30_000, 2)) {
                nearVerRef[0] = ((TransactionProxyImpl<?, ?>)tx).tx().nearXidVersion();

                cache0.put(keepTxAliveKey, 1);

                tx.savepoint("sp");

                cache0.put(dhtKey, 1);

                dhtKeyWrittenLatch.countDown();

                assertTrue(proceedRollbackLatch.await(10, TimeUnit.SECONDS));

                tx.rollbackToSavepoint("sp");

                rollbackDoneLatch.countDown();

                assertTrue(finishTxLatch.await(10, TimeUnit.SECONDS));

                tx.commit();
            }
        });

        assertTrue(dhtKeyWrittenLatch.await(10, TimeUnit.SECONDS));
        assertNotNull(nearVerRef[0]);

        proceedRollbackLatch.countDown();

        assertTrue(rollbackDoneLatch.await(10, TimeUnit.SECONDS));

        assertNoTxStateKeyOnNode(nearVerRef[0], dhtKey);
        assertNoTxStateKeyOnNode(nearVerRef[0], dhtKey);
        assertNoTxStateKeyOnNode(nearVerRef[0], dhtKey);
        assertNoTxStateKeyOnNode(nearVerRef[0], dhtKey);

        finishTxLatch.countDown();

        fut.get(10_000);

        assertNoActiveTx(nearVerRef[0]);
    }

    /**
     * Asserts that no active transaction with the given near version contains the specified key
     * in its write-set or entry map on any node.
     *
     * @param nearVer Near transaction version.
     * @param key Key that must be absent from tx state.
     * @throws Exception If waiting fails.
     */
    private void assertNoTxStateKeyOnNode(GridCacheVersion nearVer, int key) throws Exception {
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            assertTrue(GridTestUtils.waitForCondition(() -> {
                GridCacheContext<?, ?> cctx = ((IgniteKernal)ignite).internalCache(DEFAULT_CACHE_NAME).context();
                IgniteTxKey txKey = cctx.txKey(cctx.toCacheKeyObject(key));

                for (IgniteInternalTx tx : cctx.tm().activeTransactions()) {
                    if (!nearVer.equals(tx.nearXidVersion()))
                        continue;

                    if (tx.hasWriteKey(txKey) || tx.entry(txKey) != null)
                        return false;
                }

                return true;
            }, 10_000));
        }
    }

    /**
     * Asserts that no active transaction remains with the near version.
     *
     * @param nearVer Near transaction version.
     * @throws Exception If waiting fails.
     */
    private void assertNoActiveTx(GridCacheVersion nearVer) throws Exception {
        for (Ignite ignite : G.allGrids()) {
            if (ignite.configuration().isClientMode())
                continue;

            assertTrue(GridTestUtils.waitForCondition(() -> {
                GridCacheContext<?, ?> cctx = ((IgniteKernal)ignite).internalCache(DEFAULT_CACHE_NAME).context();
                for (IgniteInternalTx tx : cctx.tm().activeTransactions()) {
                    if (nearVer.equals(tx.nearXidVersion()))
                        return false;
                }

                return true;
            }, 10_000));
        }
    }

    /**
     * Creates transactional cache.
     *
     * @param ignite Node.
     * @return Transactional cache.
     */
    private IgniteCache<Integer, Integer> transactionalCache(Ignite ignite) {
        CacheConfiguration<?, ?> ccfg =
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setNearConfiguration(useNearCache ? new NearCacheConfiguration<>() : null)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setCacheMode(replicated ? CacheMode.REPLICATED : CacheMode.PARTITIONED)
                .setBackups(backups);

        return (IgniteCache<Integer, Integer>)ignite.createCache(ccfg);
    }

    /**
     * Gets key mapped to a given primary.
     *
     * @param txInitiator Transaction initiator.
     * @param primaryNode Node that should be primary.
     * @return Key mapped to given primary and backups.
     */
    private int keyForPrimaryAndBackup(Ignite txInitiator, Ignite primaryNode) {
        Ignite backupNode1 = spKeyOnTxInitiator && primaryNode != txInitiator ?
            txInitiator : G.allGrids().stream()
                          .filter(n -> n != txInitiator && n != primaryNode).findFirst().orElseThrow();
        Ignite backupNode2 = G.allGrids().stream()
            .filter(n -> n != txInitiator && n != primaryNode && n != backupNode1).findFirst().orElseThrow();

        for (int key = 0; key < 50_000; key++) {
            Collection<ClusterNode> mapping = txInitiator.affinity(DEFAULT_CACHE_NAME).mapKeyToPrimaryAndBackups(key);

            if (!replicated)
                assertEquals(backups + 1, mapping.size());

            ClusterNode primary = mapping.iterator().next();

            if (replicated && primary.id().equals(primaryNode.cluster().localNode().id()))
                return key;

            ClusterNode backup1 = mapping.stream().skip(1).findFirst().orElse(null);
            ClusterNode backup2 = mapping.stream().skip(2).findFirst().orElse(null);

            if (primary.id().equals(primaryNode.cluster().localNode().id()) &&
                (backups < 1 || backup1.id().equals(backupNode1.cluster().localNode().id())) &&
                (backups < 2 || backup2.id().equals(backupNode2.cluster().localNode().id())))
                return key;
        }

        throw new AssertionError("Failed to find key for requested primary/backup mapping.");
    }
}
