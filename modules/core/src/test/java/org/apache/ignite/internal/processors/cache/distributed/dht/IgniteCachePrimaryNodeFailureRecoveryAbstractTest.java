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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.processors.cache.ExchangeContext.IGNITE_EXCHANGE_COMPATIBILITY_VER_1;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionState.PREPARED;

/**
 *
 */
public abstract class IgniteCachePrimaryNodeFailureRecoveryAbstractTest extends IgniteCacheAbstractTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCommunicationSpi(new TestCommunicationSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        // No-op
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticPrimaryNodeFailureRecovery1() throws Exception {
        if (atomicityMode() == TRANSACTIONAL_SNAPSHOT) return;

        primaryNodeFailure(false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticPrimaryNodeFailureRecovery2() throws Exception {
        if (atomicityMode() == TRANSACTIONAL_SNAPSHOT) return;

        primaryNodeFailure(true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticPrimaryNodeFailureRollback1() throws Exception {
        if (atomicityMode() == TRANSACTIONAL_SNAPSHOT) return;

        primaryNodeFailure(false, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticPrimaryNodeFailureRollback2() throws Exception {
        if (atomicityMode() == TRANSACTIONAL_SNAPSHOT) return;

        primaryNodeFailure(true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticPrimaryNodeFailureRecovery1() throws Exception {
        primaryNodeFailure(false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticPrimaryNodeFailureRecovery2() throws Exception {
        primaryNodeFailure(true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticPrimaryNodeFailureRollback1() throws Exception {
        primaryNodeFailure(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticPrimaryNodeFailureRollback2() throws Exception {
        primaryNodeFailure(true, true, false);
    }

    /**
     * @param locBackupKey If {@code true} uses one key which is backup for originating node.
     * @param rollback If {@code true} tests rollback after primary node failure.
     * @param optimistic If {@code true} tests optimistic transaction.
     * @throws Exception If failed.
     */
    private void primaryNodeFailure(boolean locBackupKey, final boolean rollback, boolean optimistic) throws Exception {
        IgniteCache<Integer, Integer> cache0 = jcache(0);
        IgniteCache<Integer, Integer> cache2 = jcache(2);

        Affinity<Integer> aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

        Integer key0 = null;

        for (int key = 0; key < 10_000; key++) {
            if (aff.isPrimary(ignite(1).cluster().localNode(), key)) {
                if (locBackupKey == aff.isBackup(ignite(0).cluster().localNode(), key)) {
                    key0 = key;

                    break;
                }
            }
        }

        assertNotNull(key0);

        final Integer key1 = key0;
        final Integer key2 = primaryKey(cache2);

        final Collection<ClusterNode> key1Nodes = aff.mapKeyToPrimaryAndBackups(key1);
        final Collection<ClusterNode> key2Nodes = aff.mapKeyToPrimaryAndBackups(key2);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi();

        IgniteTransactions txs = ignite(0).transactions();

        try (Transaction tx = txs.txStart(optimistic ? OPTIMISTIC : PESSIMISTIC, REPEATABLE_READ)) {
            log.info("Put key1: " + key1);

            cache0.put(key1, key1);

            log.info("Put key2: " + key2);

            cache0.put(key2, key2);

            log.info("Start prepare.");

            GridNearTxLocal txEx = ((TransactionProxyImpl)tx).tx();

            commSpi.blockMessages(ignite(2).cluster().localNode().id()); // Do not allow to finish prepare for key2.

            IgniteInternalFuture<?> prepFut = txEx.prepareNearTxLocal();

            waitPrepared(ignite(1));

            log.info("Stop one primary node.");

            stopGrid(1);

            U.sleep(1000); // Wait some time to catch possible issues in tx recovery.

            commSpi.stopBlock();

            prepFut.get(10_000);

            if (rollback) {
                log.info("Rollback.");

                tx.rollback();
            }
            else {
                log.info("Commit.");

                tx.commit();
            }
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    checkKey(key1, rollback, key1Nodes, 0);
                    checkKey(key2, rollback, key2Nodes, 0);

                    return true;
                }
                catch (AssertionError e) {
                    log.info("Check failed: " + e);

                    return false;
                }
            }
        }, 5000);

        checkKey(key1, rollback, key1Nodes, 0);
        checkKey(key2, rollback, key2Nodes, 0);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticPrimaryAndOriginatingNodeFailureRecovery1() throws Exception {
        if (atomicityMode() == TRANSACTIONAL_SNAPSHOT) return;

        primaryAndOriginatingNodeFailure(false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticPrimaryAndOriginatingNodeFailureRecovery2() throws Exception {
        if (atomicityMode() == TRANSACTIONAL_SNAPSHOT) return;

        primaryAndOriginatingNodeFailure(true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticPrimaryAndOriginatingNodeFailureRollback1() throws Exception {
        if (atomicityMode() == TRANSACTIONAL_SNAPSHOT) return;

        primaryAndOriginatingNodeFailure(false, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testOptimisticPrimaryAndOriginatingNodeFailureRollback2() throws Exception {
        if (atomicityMode() == TRANSACTIONAL_SNAPSHOT) return;

        primaryAndOriginatingNodeFailure(true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticPrimaryAndOriginatingNodeFailureRecovery1() throws Exception {
        primaryAndOriginatingNodeFailure(false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticPrimaryAndOriginatingNodeFailureRecovery2() throws Exception {
        primaryAndOriginatingNodeFailure(true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticPrimaryAndOriginatingNodeFailureRollback1() throws Exception {
        primaryAndOriginatingNodeFailure(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPessimisticPrimaryAndOriginatingNodeFailureRollback2() throws Exception {
        primaryAndOriginatingNodeFailure(true, true, false);
    }

    /**
     * @param locBackupKey If {@code true} uses one key which is backup for originating node.
     * @param rollback If {@code true} tests rollback after primary node failure.
     * @param optimistic If {@code true} tests optimistic transaction.
     * @throws Exception If failed.
     */
    private void primaryAndOriginatingNodeFailure(final boolean locBackupKey,
        final boolean rollback,
        boolean optimistic)
        throws Exception {
        // TODO IGNITE-6174: when exchanges can be merged test fails because of IGNITE-6174.
        System.setProperty(IGNITE_EXCHANGE_COMPATIBILITY_VER_1, "true");

        try {
            int orig = 0;

            IgniteCache<Integer, Integer> origCache = jcache(orig);

            Affinity<Integer> aff = ignite(0).affinity(DEFAULT_CACHE_NAME);

            Integer key0 = null;

            for (int key = 0; key < 10_000; key++) {
                if (aff.isPrimary(ignite(1).cluster().localNode(), key)) {
                    if (locBackupKey == aff.isBackup(ignite(orig).cluster().localNode(), key)) {
                        key0 = key;

                        break;
                    }
                }
            }

            assertNotNull(key0);

            final Integer key1 = key0;
            final Integer key2 = primaryKey(jcache(2));

            int backups = origCache.getConfiguration(CacheConfiguration.class).getBackups();

            final Collection<ClusterNode> key1Nodes =
                (locBackupKey && backups < 2) ? Collections.emptyList() : aff.mapKeyToPrimaryAndBackups(key1);
            final Collection<ClusterNode> key2Nodes = aff.mapKeyToPrimaryAndBackups(key2);

            TestCommunicationSpi commSpi = (TestCommunicationSpi)ignite(orig).configuration().getCommunicationSpi();

            IgniteTransactions txs = ignite(orig).transactions();

            Transaction tx = txs.txStart(optimistic ? OPTIMISTIC : PESSIMISTIC, REPEATABLE_READ);

            log.info("Put key1 [key1=" + key1 + ", nodes=" + U.nodeIds(aff.mapKeyToPrimaryAndBackups(key1)) + ']');

            origCache.put(key1, key1);

            log.info("Put key2 [key2=" + key2 + ", nodes=" + U.nodeIds(aff.mapKeyToPrimaryAndBackups(key2)) + ']');

            origCache.put(key2, key2);

            log.info("Start prepare.");

            GridNearTxLocal txEx = ((TransactionProxyImpl)tx).tx();

            commSpi.blockMessages(ignite(2).cluster().localNode().id()); // Do not allow to finish prepare for key2.

            IgniteInternalFuture<?> prepFut = txEx.prepareNearTxLocal();

            waitPrepared(ignite(1));

            log.info("Stop one primary node.");

            stopGrid(1);

            U.sleep(1000); // Wait some time to catch possible issues in tx recovery.

            if (!rollback) {
                commSpi.stopBlock();

                prepFut.get(10_000);
            }

            log.info("Stop originating node.");

            stopGrid(orig);

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    try {
                        checkKey(key1, rollback, key1Nodes, 0);
                        checkKey(key2, rollback, key2Nodes, 0);

                        return true;
                    } catch (AssertionError e) {
                        log.info("Check failed: " + e);

                        return false;
                    }
                }
            }, 5000);

            checkKey(key1, rollback, key1Nodes, 0);
            checkKey(key2, rollback, key2Nodes, 0);
        }
        finally {
            System.clearProperty(IGNITE_EXCHANGE_COMPATIBILITY_VER_1);
        }
    }

    /** */
    private void checkKey(Integer key, boolean rollback, Collection<ClusterNode> keyNodes, long initUpdCntr) {
        if (rollback) {
            if (atomicityMode() != TRANSACTIONAL_SNAPSHOT) {
                for (Ignite ignite : G.allGrids()) {
                    IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

                    assertNull("Unexpected value for: " + ignite.name(), cache.localPeek(key));
                }
            }

            for (Ignite ignite : G.allGrids()) {
                IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

                assertNull("Unexpected value for: " + ignite.name(), cache.get(key));
            }

            boolean found = keyNodes.isEmpty();

            long cntr0 = -1;

            for (ClusterNode node : keyNodes) {
                try {
                    long nodeCntr = updateCoutner(grid(node), key);

                    found = true;

                    if (cntr0 == -1)
                        cntr0 = nodeCntr;

                    assertEquals(cntr0, nodeCntr);
                }
                catch (IgniteIllegalStateException ignore) {
                    // No-op.
                }
            }

            assertTrue("Failed to find key node.", found);
        }
        else if (!keyNodes.isEmpty()) {
            boolean found = false;

            long cntr0 = -1;

            for (ClusterNode node : keyNodes) {
                try {
                    Ignite ignite = grid(node);

                    found = true;

                    ignite.cache(DEFAULT_CACHE_NAME);

                    assertEquals("Unexpected value for: " + ignite.name(), key, key);

                    long nodeCntr = updateCoutner(ignite, key);

                    if (cntr0 == -1)
                        cntr0 = nodeCntr;

                    assertTrue(nodeCntr == cntr0 && nodeCntr > initUpdCntr);
                }
                catch (IgniteIllegalStateException ignore) {
                    // No-op.
                }
            }

            assertTrue("Failed to find key node.", found);

            for (Ignite ignite : G.allGrids()) {
                IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

                assertEquals("Unexpected value for: " + ignite.name(), key, cache.get(key));
            }
        }
    }

    /**
     * @param ignite Node.
     * @throws Exception If failed.
     */
    private void waitPrepared(Ignite ignite) throws Exception {
        final IgniteTxManager tm = ((IgniteKernal)ignite).context().cache().context().tm();

        boolean wait = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridDhtTxLocal locTx = null;

                for (IgniteInternalTx tx : tm.activeTransactions()) {
                    if (tx instanceof GridDhtTxLocal) {
                        assertNull("Only one tx is expected.", locTx);

                        locTx = (GridDhtTxLocal)tx;
                    }
                }

                log.info("Wait for tx, state: " + (locTx != null ? locTx.state() : null));

                return locTx != null && locTx.state() == PREPARED;
            }
        }, 5000);

        assertTrue("Failed to wait for tx.", wait);
    }

    /** */
    private static long updateCoutner(Ignite ign, Object key) {
        return dataStore(((IgniteEx)ign).cachex(DEFAULT_CACHE_NAME).context(), key)
            .map(IgniteCacheOffheapManager.CacheDataStore::updateCounter)
            .orElse(0L);
    }

    /** */
    private static Optional<IgniteCacheOffheapManager.CacheDataStore> dataStore(
        GridCacheContext<?, ?> cctx, Object key) {
        int p = cctx.affinity().partition(key);

        return StreamSupport.stream(cctx.offheap().cacheDataStores().spliterator(), false)
            .filter(ds -> ds.partId() == p)
            .findFirst();
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private UUID blockNodeId;

        /** */
        private List<T2<ClusterNode, GridIoMessage>> blockedMsgs = new ArrayList<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure)
            throws IgniteSpiException {
            if (msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridNearTxPrepareRequest) {
                    synchronized (this) {
                        if (blockNodeId != null && blockNodeId.equals(node.id())) {
                            log.info("Block message: " + msg0);

                            blockedMsgs.add(new T2<>(node, (GridIoMessage)msg));

                            return;
                        }
                    }
                }
            }

            super.sendMessage(node, msg, ackClosure);
        }

        /**
         * @param nodeId Node ID.
         */
        void blockMessages(UUID nodeId) {
            blockNodeId = nodeId;
        }

        /**
         *
         */
        void stopBlock() {
            synchronized (this) {
                blockNodeId = null;

                for (T2<ClusterNode, GridIoMessage> msg : blockedMsgs) {
                    log.info("Send blocked message: " + msg.get2().message());

                    super.sendMessage(msg.get1(), msg.get2());
                }
            }
        }
    }
}
