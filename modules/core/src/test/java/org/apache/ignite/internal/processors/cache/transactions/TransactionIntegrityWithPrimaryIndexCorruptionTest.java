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

import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test cases that check transaction data integrity after transaction commit failed.
 */
public class TransactionIntegrityWithPrimaryIndexCorruptionTest extends AbstractTransactionIntergrityTest {
    /** Corruption enabled flag. */
    private static volatile boolean corruptionEnabled;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        corruptionEnabled = false;

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 60 * 1000L;
    }

    /**
     * Throws a test {@link AssertionError} during tx commit from {@link BPlusTree} and checks after that data is consistent.
     */
    public void testPrimaryIndexCorruptionDuringCommitOnPrimaryNode1() throws Exception {
        doTestTransferAmount(new IndexCorruptionFailoverScenario(
            true,
            (hnd, tree) -> hnd instanceof BPlusTree.Search,
            failoverPredicate(true, () -> new AssertionError("Test")))
        );
    }

    /**
     * Throws a test {@link RuntimeException} during tx commit from {@link BPlusTree} and checks after that data is consistent.
     */
    public void testPrimaryIndexCorruptionDuringCommitOnPrimaryNode2() throws Exception {
        doTestTransferAmount(new IndexCorruptionFailoverScenario(
            true,
            (hnd, tree) -> hnd instanceof BPlusTree.Search,
            failoverPredicate(true, () -> new RuntimeException("Test")))
        );
    }

    /**
     * Throws a test {@link AssertionError} during tx commit from {@link BPlusTree} and checks after that data is consistent.
     */
    public void testPrimaryIndexCorruptionDuringCommitOnBackupNode() throws Exception {
        doTestTransferAmount(new IndexCorruptionFailoverScenario(
            true,
            (hnd, tree) -> hnd instanceof BPlusTree.Search,
            failoverPredicate(false, () -> new AssertionError("Test")))
        );
    }

    /**
     * Throws a test {@link IgniteCheckedException} during tx commit from {@link BPlusTree} and checks after that data is consistent.
     */
    public void testPrimaryIndexCorruptionDuringCommitOnPrimaryNode3() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-9082");

        doTestTransferAmount(new IndexCorruptionFailoverScenario(
            false,
            (hnd, tree) -> hnd instanceof BPlusTree.Search,
            failoverPredicate(true, () -> new IgniteCheckedException("Test")))
        );
    }

    /**
     * Creates failover predicate which generates error during transaction commmit.
     *
     * @param failOnPrimary If {@code true} index should be failed on transaction primary node.
     * @param errorSupplier Supplier to create various errors.
     */
    private BiFunction<IgniteEx, SearchRow, Throwable> failoverPredicate(
        boolean failOnPrimary,
        Supplier<Throwable> errorSupplier
    ) {
        return (ignite, row) -> {
            int cacheId = row.cacheId();
            int partId = row.key().partition();

            final ClusterNode locNode = ignite.localNode();
            final AffinityTopologyVersion curTopVer = ignite.context().discovery().topologyVersionEx();

            // Throw exception if current node is primary for given row.
            return ignite.cachesx(c -> c.context().cacheId() == cacheId)
                .stream()
                .filter(c -> c.context().affinity().primaryByPartition(locNode, partId, curTopVer) == failOnPrimary)
                .map(c -> errorSupplier.get())
                .findFirst()
                .orElse(null);
        };
    }

    /**
     * Index corruption failover scenario.
     */
    class IndexCorruptionFailoverScenario implements FailoverScenario {
        /** Failed node index. */
        static final int failedNodeIdx = 1;

        /** Is node stopping expected after failover. */
        private final boolean nodeStoppingExpected;

        /** Predicate that will choose an instance of {@link BPlusTree} and page operation
         * to make further failover in this tree using {@link #failoverPredicate}. */
        private final BiFunction<PageHandler, BPlusTree, Boolean> treeCorruptionPredicate;

        /** Function that may return error during row insertion into {@link BPlusTree}. */
        private final BiFunction<IgniteEx, SearchRow, Throwable> failoverPredicate;

        /**
         * @param nodeStoppingExpected Node stopping expected.
         * @param treeCorruptionPredicate Tree corruption predicate.
         * @param failoverPredicate Failover predicate.
         */
        IndexCorruptionFailoverScenario(
            boolean nodeStoppingExpected,
            BiFunction<PageHandler, BPlusTree, Boolean> treeCorruptionPredicate,
            BiFunction<IgniteEx, SearchRow, Throwable> failoverPredicate
        ) {
            this.nodeStoppingExpected = nodeStoppingExpected;
            this.treeCorruptionPredicate = treeCorruptionPredicate;
            this.failoverPredicate = failoverPredicate;
        }

        /** {@inheritDoc} */
        @Override public void beforeNodesStarted() {
            BPlusTree.pageHndWrapper = (tree, hnd) -> {
                final IgniteEx locIgnite = (IgniteEx) Ignition.localIgnite();

                if (!locIgnite.name().endsWith(String.valueOf(failedNodeIdx)))
                    return hnd;

                if (treeCorruptionPredicate.apply(hnd, tree)) {
                    log.info("Created corrupted tree handler for -> " + hnd + " " + tree);

                    PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>) hnd;

                    return new PageHandler<BPlusTree.Get, BPlusTree.Result>() {
                        @Override public BPlusTree.Result run(int cacheId, long pageId, long page, long pageAddr, PageIO io, Boolean walPlc, BPlusTree.Get arg, int lvl) throws IgniteCheckedException {
                            log.info("Invoked " + " " + cacheId + " " + arg.toString() + " for BTree (" + corruptionEnabled + ") -> " + arg.row() + " / " + arg.row().getClass());

                            if (corruptionEnabled && (arg.row() instanceof SearchRow)) {
                                SearchRow row = (SearchRow) arg.row();

                                // Store cacheId to search row explicitly, as it can be zero if there is one cache in a group.
                                Throwable res = failoverPredicate.apply(locIgnite, new SearchRow(cacheId, row.key()));

                                if (res != null) {
                                    if (res instanceof Error)
                                        throw (Error) res;
                                    else if (res instanceof RuntimeException)
                                        throw (RuntimeException) res;
                                    else if (res instanceof IgniteCheckedException)
                                        throw (IgniteCheckedException) res;
                                }
                            }

                            return delegate.run(cacheId, pageId, page, pageAddr, io, walPlc, arg, lvl);
                        }

                        @Override public boolean releaseAfterWrite(int cacheId, long pageId, long page, long pageAddr, BPlusTree.Get g, int lvl) {
                            return g.canRelease(pageId, lvl);
                        }
                    };
                }

                return hnd;
            };
        }

        /** {@inheritDoc} */
        @Override public void afterFirstTransaction() {
            // Enable BPlus tree corruption after first transactions have finished.
            corruptionEnabled = true;
        }

        /** {@inheritDoc} */
        @Override public void afterTransactionsFinished() throws Exception {
            // Disable index corruption.
            BPlusTree.pageHndWrapper = (tree, hnd) -> hnd;

            if (nodeStoppingExpected) {
                // Wait until node with corrupted index will left cluster.
                GridTestUtils.waitForCondition(() -> {
                    try {
                        grid(failedNodeIdx);
                    }
                    catch (IgniteIllegalStateException e) {
                        return true;
                    }

                    return false;
                }, getTestTimeout());

                // Failed node should be stopped.
                GridTestUtils.assertThrows(log, () -> grid(failedNodeIdx), IgniteIllegalStateException.class, "");

                // Re-start failed node.
                startGrid(failedNodeIdx);

                awaitPartitionMapExchange();
            }
        }
    }
}
