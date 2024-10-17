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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.CacheInvalidStateException;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.util.typedef.CI3;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionRollbackException;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Node left a topology when a one phase transaction committing.
 */
public class OnePhaseCommitAndNodeLeftTest extends GridCommonAbstractTest {
    /** Message appears when all owner partition was lost so a cluster do an operation cannot execute over them. */
    public static final String LOST_ALL_QWNERS_MSG = "all partition owners have left the grid, partition data has been lost";

    /** Chache backup count. */
    private int backups;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
                .setBackups(backups));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /**
     * Tests an implicit transaction on a cache without backups.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testImplicitlyTxZeroBackups() throws Exception {
        backups = 0;

        startTransactionAndFailPrimary((ignite, key, val) -> ignite.cache(DEFAULT_CACHE_NAME).put(key, val));
    }

    /**
     * Tests an implicit transaction on a cache with one backup.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testImplicitlyTxOneBackups() throws Exception {
        backups = 1;

        startTransactionAndFailPrimary((ignite, key, val) -> ignite.cache(DEFAULT_CACHE_NAME).put(key, val));
    }

    /**
     * Tests an explicit transaction on a cache without backups.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTxZeroBackups() throws Exception {
        backups = 0;

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                startTransactionAndFailPrimary((ignite, key, val) -> {
                    try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                        ignite.cache(DEFAULT_CACHE_NAME).put(key, val);

                        tx.commit();
                    }
                });

                stopAllGrids();
            }
        }
    }

    /**
     * Tests an explicit transaction on a cache with one backup.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTxOneBackups() throws Exception {
        backups = 1;

        for (TransactionConcurrency concurrency : TransactionConcurrency.values()) {
            for (TransactionIsolation isolation : TransactionIsolation.values()) {
                startTransactionAndFailPrimary((ignite, key, val) -> {
                    info("Tx pu: [concurrency=" + concurrency + ", isolation=" + isolation + ']');

                    try (Transaction tx = ignite.transactions().txStart(concurrency, isolation)) {
                        ignite.cache(DEFAULT_CACHE_NAME).put(key, val);

                        tx.commit();
                    }
                });

                stopAllGrids();
            }
        }
    }

    /**
     * Stars cluster and stops exactly primary node for a cache operation specified in parameters.
     *
     * @param cacheClosure Closure for a cache operation.
     * @throws Exception If failed.
     */
    private void startTransactionAndFailPrimary(CI3<Ignite, Integer, String> cacheClosure) throws Exception {
        Ignite ignite0 = startGrids(2);

        awaitPartitionMapExchange();

        IgniteCache cache = ignite0.cache(DEFAULT_CACHE_NAME);

        Integer key = primaryKey(ignite(1).cache(DEFAULT_CACHE_NAME));

        ClusterNode node1 = ignite0.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(key);

        assertFalse("Found key is local: " + key + " on node " + node1, node1.isLocal());

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(ignite0);

        spi.blockMessages((node, msg) -> {
            if (msg instanceof GridNearTxPrepareRequest) {
                GridNearTxPrepareRequest putReq = (GridNearTxPrepareRequest)msg;

                if (!F.isEmpty(putReq.writes())) {
                    int cacheId = putReq.writes().iterator().next().cacheId();

                    if (cacheId == CU.cacheId(DEFAULT_CACHE_NAME)) {
                        assertTrue(putReq.onePhaseCommit());

                        String nodeName = node.attribute(ATTR_IGNITE_INSTANCE_NAME);

                        return true;
                    }
                }
            }

            return false;
        });

        String testVal = "Test value";

        IgniteInternalFuture putFut = GridTestUtils.runAsync(() -> {
            try {
                cacheClosure.apply(ignite0, key, testVal);

                //We are not sure, operation completed correctly or not when backups are zero.
                //Exception could be thrown or not.
                // assertTrue(((CacheConfiguration)cache.getConfiguration(CacheConfiguration.class)).getBackups() != 0);
            }
            catch (Exception e) {
                checkException(cache, e);
            }
        });

        spi.waitForBlocked();

        assertFalse(putFut.isDone());

        ignite(1).close();

        spi.stopBlock();

        try {
            putFut.get();

            try {
                assertEquals(testVal, cache.get(key));

                assertTrue(((CacheConfiguration)cache.getConfiguration(CacheConfiguration.class)).getBackups() != 0);
            }
            catch (Exception e) {
                checkException(cache, e);
            }
        }
        catch (Exception e) {
            if (X.hasCause(e, TransactionRollbackException.class))
                info("Transaction was rolled back [err=" + e.getMessage() + "]");
        }
    }

    /**
     * Checks an exception that happened in the cache specified.
     *
     * @param cache Ignite cache.
     * @param e Checked exception.
     */
    private void checkException(IgniteCache cache, Exception e) {
        log.error("Ex", e);

        Exception ex = X.cause(e, CacheInvalidStateException.class);

        if (ex == null)
            throw new IgniteException(e);

        assertTrue(ex.getMessage().contains(LOST_ALL_QWNERS_MSG));

        assertEquals(0, ((CacheConfiguration)cache.getConfiguration(CacheConfiguration.class)).getBackups());
    }
}
