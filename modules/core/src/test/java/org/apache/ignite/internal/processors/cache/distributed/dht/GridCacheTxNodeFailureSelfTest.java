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
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import javax.cache.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Tests one-phase commit transactions when some of the nodes fail in the middle of the transaction.
 */
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

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);
        ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        ccfg.setBackups(1);

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new BanningCommunicationSpi());

        return cfg;
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
            assertEquals(0, ((IgniteEx)ignite).context().cache().context().tm().idMapSize());
        }
        finally {
            stopAllGrids();
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

                U.debug(">>> Sending message: " + msg);
            }
        }
    }
}
