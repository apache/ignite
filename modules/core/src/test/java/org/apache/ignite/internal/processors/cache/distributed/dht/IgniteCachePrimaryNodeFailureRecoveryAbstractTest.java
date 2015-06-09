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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.transactions.*;

import java.util.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;
import static org.apache.ignite.transactions.TransactionState.*;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

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
    public void testOptimisticPrimaryNodeFailureRecovery1() throws Exception {
        primaryNodeFailure(false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticPrimaryNodeFailureRecovery2() throws Exception {
        primaryNodeFailure(true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticPrimaryNodeFailureRollback1() throws Exception {
        primaryNodeFailure(false, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticPrimaryNodeFailureRollback2() throws Exception {
        primaryNodeFailure(true, true, true);
    }
    /**
     * @throws Exception If failed.
     */
    public void testPessimisticPrimaryNodeFailureRecovery1() throws Exception {
        primaryNodeFailure(false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticPrimaryNodeFailureRecovery2() throws Exception {
        primaryNodeFailure(true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticPrimaryNodeFailureRollback1() throws Exception {
        primaryNodeFailure(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
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

        Affinity<Integer> aff = ignite(0).affinity(null);

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

            IgniteInternalTx txEx = ((TransactionProxyImpl)tx).tx();

            commSpi.blockMessages(ignite(2).cluster().localNode().id()); // Do not allow to finish prepare for key2.

            IgniteInternalFuture<IgniteInternalTx> prepFut = txEx.prepareAsync();

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
                    checkKey(key1, rollback ? null : key1Nodes);
                    checkKey(key2, rollback ? null : key2Nodes);

                    return true;
                }
                catch (AssertionError e) {
                    log.info("Check failed: " + e);

                    return false;
                }
            }
        }, 5000);

        checkKey(key1, rollback ? null : key1Nodes);
        checkKey(key2, rollback ? null : key2Nodes);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticPrimaryAndOriginatingNodeFailureRecovery1() throws Exception {
        primaryAndOriginatingNodeFailure(false, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticPrimaryAndOriginatingNodeFailureRecovery2() throws Exception {
        primaryAndOriginatingNodeFailure(true, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticPrimaryAndOriginatingNodeFailureRollback1() throws Exception {
        primaryAndOriginatingNodeFailure(false, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticPrimaryAndOriginatingNodeFailureRollback2() throws Exception {
        primaryAndOriginatingNodeFailure(true, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticPrimaryAndOriginatingNodeFailureRecovery1() throws Exception {
        primaryAndOriginatingNodeFailure(false, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticPrimaryAndOriginatingNodeFailureRecovery2() throws Exception {
        primaryAndOriginatingNodeFailure(true, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticPrimaryAndOriginatingNodeFailureRollback1() throws Exception {
        primaryAndOriginatingNodeFailure(false, true, false);
    }

    /**
     * @throws Exception If failed.
     */
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
        throws Exception
    {
        IgniteCache<Integer, Integer> cache0 = jcache(0);
        IgniteCache<Integer, Integer> cache2 = jcache(2);

        Affinity<Integer> aff = ignite(0).affinity(null);

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

        int backups = cache0.getConfiguration(CacheConfiguration.class).getBackups();

        final Collection<ClusterNode> key1Nodes =
            (locBackupKey && backups < 2) ? null : aff.mapKeyToPrimaryAndBackups(key1);
        final Collection<ClusterNode> key2Nodes = aff.mapKeyToPrimaryAndBackups(key2);

        TestCommunicationSpi commSpi = (TestCommunicationSpi)ignite(0).configuration().getCommunicationSpi();

        IgniteTransactions txs = ignite(0).transactions();

        Transaction tx = txs.txStart(optimistic ? OPTIMISTIC : PESSIMISTIC, REPEATABLE_READ);

        log.info("Put key1: " + key1);

        cache0.put(key1, key1);

        log.info("Put key2: " + key2);

        cache0.put(key2, key2);

        log.info("Start prepare.");

        IgniteInternalTx txEx = ((TransactionProxyImpl)tx).tx();

        commSpi.blockMessages(ignite(2).cluster().localNode().id()); // Do not allow to finish prepare for key2.

        IgniteInternalFuture<IgniteInternalTx> prepFut = txEx.prepareAsync();

        waitPrepared(ignite(1));

        log.info("Stop one primary node.");

        stopGrid(1);

        U.sleep(1000); // Wait some time to catch possible issues in tx recovery.

        if (!rollback) {
            commSpi.stopBlock();

            prepFut.get(10_000);
        }

        log.info("Stop originating node.");

        stopGrid(0);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                try {
                    checkKey(key1, rollback ? null : key1Nodes);
                    checkKey(key2, rollback ? null : key2Nodes);

                    return true;
                } catch (AssertionError e) {
                    log.info("Check failed: " + e);

                    return false;
                }
            }
        }, 5000);

        checkKey(key1, rollback ? null : key1Nodes);
        checkKey(key2, rollback ? null : key2Nodes);
    }

    /**
     * @param key Key.
     * @param keyNodes Key nodes.
     */
    private void checkKey(Integer key, Collection<ClusterNode> keyNodes) {
        if (keyNodes == null) {
            for (Ignite ignite : G.allGrids()) {
                IgniteCache<Integer, Integer> cache = ignite.cache(null);

                assertNull("Unexpected value for: " + ignite.name(), cache.localPeek(key));
            }

            for (Ignite ignite : G.allGrids()) {
                IgniteCache<Integer, Integer> cache = ignite.cache(null);

                assertNull("Unexpected value for: " + ignite.name(), cache.get(key));
            }
        }
        else {
            boolean found = false;

            for (ClusterNode node : keyNodes) {
                try {
                    Ignite ignite = grid(node);

                    found = true;

                    IgniteCache<Integer, Integer> cache = ignite.cache(null);

                    assertEquals("Unexpected value for: " + ignite.name(), key, key);
                }
                catch (IgniteIllegalStateException ignore) {
                    // No-op.
                }
            }

            assertTrue("Failed to find key node.", found);

            for (Ignite ignite : G.allGrids()) {
                IgniteCache<Integer, Integer> cache = ignite.cache(null);

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

                for (IgniteInternalTx tx : tm.txs()) {
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
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
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

            super.sendMessage(node, msg);
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
