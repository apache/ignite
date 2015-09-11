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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
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
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheEntry;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionRollbackException;

import javax.cache.CacheException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Tests one-phase commit transactions when some of the nodes fail in the middle of the transaction.
 */
@SuppressWarnings("unchecked")
public class GridCacheTxNodeFailureSelfTest extends GridCommonAbstractTest {
    /**
     * @return Grid count.
     */
    public int gridCount() {
        return 4;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration(gridName));

        cfg.setCommunicationSpi(new BanningCommunicationSpi());

        return cfg;
    }

    /**
     * @param gridName Grid name.
     * @return Cache configuration.
     */
    protected CacheConfiguration cacheConfiguration(String gridName) {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupCommitPessimistic() throws Exception {
        checkPrimaryNodeFailureBackupCommit(PESSIMISTIC, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupCommitOptimistic() throws Exception {
        checkPrimaryNodeFailureBackupCommit(OPTIMISTIC, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupCommitPessimisticOnBackup() throws Exception {
        checkPrimaryNodeFailureBackupCommit(PESSIMISTIC, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupCommitOptimisticOnBackup() throws Exception {
        checkPrimaryNodeFailureBackupCommit(OPTIMISTIC, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupRollbackPessimistic() throws Exception {
        checkPrimaryNodeFailureBackupCommit(PESSIMISTIC, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupRollbackOptimistic() throws Exception {
        checkPrimaryNodeFailureBackupCommit(OPTIMISTIC, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupRollbackPessimisticOnBackup() throws Exception {
        checkPrimaryNodeFailureBackupCommit(PESSIMISTIC, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupRollbackOptimisticOnBackup() throws Exception {
        checkPrimaryNodeFailureBackupCommit(OPTIMISTIC, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupCommitImplicit() throws Exception {
        checkPrimaryNodeFailureBackupCommit(null, false, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupCommitImplicitOnBackup() throws Exception {
        checkPrimaryNodeFailureBackupCommit(null, true, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupRollbackImplicit() throws Exception {
        checkPrimaryNodeFailureBackupCommit(null, false, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrimaryNodeFailureBackupRollbackImplicitOnBackup() throws Exception {
        checkPrimaryNodeFailureBackupCommit(null, true, false);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPrimaryNodeFailureBackupCommit(
        final TransactionConcurrency conc,
        boolean backup,
        final boolean commit
    ) throws Exception {
        startGrids(gridCount());
        awaitPartitionMapExchange();

        for (int i = 0; i < gridCount(); i++)
            info("Grid " + i + ": " + ignite(i).cluster().localNode().id());

        try {
            final Ignite ignite = ignite(0);

            final IgniteCache<Object, Object> cache = ignite.cache(null);

            final int key = generateKey(ignite, backup);

            IgniteEx backupNode = (IgniteEx)backupNode(key, null);

            assertNotNull(backupNode);

            final CountDownLatch commitLatch = new CountDownLatch(1);

            if (!commit) {
                communication(1).bannedClasses(Collections.<Class>singletonList(GridDhtTxPrepareRequest.class));
            }
            else {
                if (!backup) {
                    communication(2).bannedClasses(Collections.<Class>singletonList(GridDhtTxPrepareResponse.class));
                    communication(3).bannedClasses(Collections.<Class>singletonList(GridDhtTxPrepareResponse.class));
                }
                else
                    communication(0).bannedClasses(Collections.<Class>singletonList(GridDhtTxPrepareResponse.class));
            }

            IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    if (conc != null) {
                        try (Transaction tx = ignite.transactions().txStart(conc, REPEATABLE_READ)) {
                            cache.put(key, key);

                            Transaction asyncTx = (Transaction)tx.withAsync();

                            asyncTx.commit();

                            commitLatch.countDown();

                            try {
                                IgniteFuture<Object> fut = asyncTx.future();

                                fut.get();

                                if (!commit) {
                                    error("Transaction has been committed");

                                    fail("Transaction has been committed: " + tx);
                                }
                            }
                            catch (TransactionRollbackException e) {
                                if (commit) {
                                    error(e.getMessage(), e);

                                    fail("Failed to commit: " + e);
                                }
                                else
                                    assertTrue(X.hasCause(e, TransactionRollbackException.class));
                            }
                        }
                    }
                    else {
                        IgniteCache<Object, Object> cache0 = cache.withAsync();

                        cache0.put(key, key);

                        Thread.sleep(1000);

                        commitLatch.countDown();

                        try {
                            cache0.future().get();

                            if (!commit) {
                                error("Transaction has been committed");

                                fail("Transaction has been committed.");
                            }
                        }
                        catch (CacheException e) {
                            if (commit) {
                                error(e.getMessage(), e);

                                fail("Failed to commit: " + e);
                            }
                            else
                                assertTrue(X.hasCause(e, TransactionRollbackException.class));
                        }
                    }

                    return null;
                }
            });

            commitLatch.await();

            stopGrid(1);

            // Check that thread successfully finished.
            fut.get();

            // Check there are no hanging transactions.
            assertEquals(0, ((IgniteEx)ignite(0)).context().cache().context().tm().idMapSize());
            assertEquals(0, ((IgniteEx)ignite(2)).context().cache().context().tm().idMapSize());
            assertEquals(0, ((IgniteEx)ignite(3)).context().cache().context().tm().idMapSize());

            dataCheck((IgniteKernal)ignite(0), (IgniteKernal)backupNode, key, commit);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param orig Originating cache.
     * @param backup Backup cache.
     * @param key Key being committed and checked.
     * @param commit Commit or rollback flag.
     * @throws Exception If check failed.
     */
    private void dataCheck(IgniteKernal orig, IgniteKernal backup, int key, boolean commit) throws Exception {
        GridNearCacheEntry nearEntry = null;

        GridCacheAdapter origCache = orig.internalCache(null);

        if (origCache.isNear())
            nearEntry = (GridNearCacheEntry)origCache.peekEx(key);

        GridCacheAdapter backupCache = backup.internalCache(null);

        if (backupCache.isNear())
            backupCache = backupCache.context().near().dht();

        GridDhtCacheEntry dhtEntry = (GridDhtCacheEntry)backupCache.peekEx(key);

        if (commit) {
            assertNotNull(dhtEntry);
            assertTrue("dhtEntry=" + dhtEntry, dhtEntry.remoteMvccSnapshot().isEmpty());
            assertTrue("dhtEntry=" + dhtEntry, dhtEntry.localCandidates().isEmpty());
            assertEquals(key, backupCache.localPeek(key, null, null));

            if (nearEntry != null) {
                assertTrue("near=" + nearEntry, nearEntry.remoteMvccSnapshot().isEmpty());
                assertTrue("near=" + nearEntry, nearEntry.localCandidates().isEmpty());

                // Near peek wil be null since primary node has changed.
                assertNull("near=" + nearEntry, origCache.localPeek(key, null, null));
            }
        }
        else {
            assertTrue("near=" + nearEntry + ", hc=" + System.identityHashCode(nearEntry), nearEntry == null);
            assertTrue("Invalid backup cache entry: " + dhtEntry,
                dhtEntry == null || dhtEntry.rawGetOrUnmarshal(false) == null);
        }
    }

    /**
     * @param idx Index.
     * @return Communication SPI.
     */
    private BanningCommunicationSpi communication(int idx) {
        return (BanningCommunicationSpi)ignite(idx).configuration().getCommunicationSpi();
    }

    /**
     * @param ignite Ignite instance to generate key.
     * @return Generated key that is not primary nor backup for {@code ignite(0)} and primary for
     *      {@code ignite(1)}.
     */
    private int generateKey(Ignite ignite, boolean backup) {
        Affinity<Object> aff = ignite.affinity(null);

        for (int key = 0;;key++) {
            if (backup) {
                if (!aff.isBackup(ignite(0).cluster().localNode(), key))
                    continue;
            }
            else {
                if (aff.isPrimaryOrBackup(ignite(0).cluster().localNode(), key))
                    continue;
            }

            if (aff.isPrimary(ignite(1).cluster().localNode(), key))
                return key;
        }
    }

    /**
     *
     */
    private static class BanningCommunicationSpi extends TcpCommunicationSpi {
        /** */
        private volatile Collection<Class> bannedClasses = Collections.emptyList();

        /**
         * @param bannedClasses Banned classes.
         */
        public void bannedClasses(Collection<Class> bannedClasses) {
            this.bannedClasses = bannedClasses;
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackClosure) throws IgniteSpiException {
            GridIoMessage ioMsg = (GridIoMessage)msg;

            if (!bannedClasses.contains(ioMsg.message().getClass())) {
                super.sendMessage(node, msg, ackClosure);
            }
        }
    }
}
