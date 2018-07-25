package org.apache.ignite.internal.processors.cache.transactions;

import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.internal.processors.cache.tree.SearchRow;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class TransactionIntegrityWithPrimaryIndexCorruptionTest extends AbstractTransactionIntergrityTest {
    private static volatile boolean corruptionEnabled;

    @Override protected void afterTest() throws Exception {
        corruptionEnabled = false;

        super.afterTest();
    }

    public void testPrimaryIndexCorruptionDuringCommitOnPrimaryNode1() throws Exception {
        doTestTransferAmount(new IndexCorruptionFailoverScenario(
            true,
            (hnd, tree) -> hnd instanceof BPlusTree.Search,
            failoverPredicate(true, () -> new AssertionError("Test")))
        );
    }

    public void testPrimaryIndexCorruptionDuringCommitOnPrimaryNode2() throws Exception {
        doTestTransferAmount(new IndexCorruptionFailoverScenario(
            true,
            (hnd, tree) -> hnd instanceof BPlusTree.Search,
            failoverPredicate(true, () -> new RuntimeException("Test")))
        );
    }

    public void testPrimaryIndexCorruptionDuringCommitOnPrimaryNode3() throws Exception {
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

    class IndexCorruptionFailoverScenario implements FailoverScenario {
        /** Failed node index. */
        static final int failedNodeIdx = 1;

        /** Node stopping expected. */
        private final boolean nodeStoppingExpected;

        /** Tree corruption predicate. */
        private final BiFunction<PageHandler, BPlusTree, Boolean> treeCorruptionPredicate;

        /** Failover predicate. */
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
        @Override public void beforeNodesStarted() throws Exception {
            BPlusTree.pageHndWrapper = (tree, hnd) -> {
                final IgniteEx locIgnite = (IgniteEx) Ignition.localIgnite();

                if (!locIgnite.name().endsWith(String.valueOf(failedNodeIdx)))
                    return hnd;

                if (treeCorruptionPredicate.apply(hnd, tree)) {
                    log.warning("Created corrupted tree handler for -> " + hnd + " " + tree);

                    PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>) hnd;

                    return new PageHandler<BPlusTree.Get, BPlusTree.Result>() {
                        @Override public BPlusTree.Result run(int cacheId, long pageId, long page, long pageAddr, PageIO io, Boolean walPlc, BPlusTree.Get arg, int lvl) throws IgniteCheckedException {
                            log.warning("Invoked " + " " + cacheId + " " + arg.toString() + " for BTree (" + corruptionEnabled + ") -> " + arg.row() + " / " + arg.row().getClass());

                            if (corruptionEnabled && (arg.row() instanceof SearchRow)) {
                                SearchRow row = (SearchRow) arg.row();

                                Throwable res = failoverPredicate.apply(locIgnite, new SearchRow(cacheId, row.key()));

                                if (res != null) {
                                    U.sleep(3000);

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
        @Override public void afterFirstTransaction() throws Exception {
            // Enable BPlus tree corruption.
            corruptionEnabled = true;
        }

        /** {@inheritDoc} */
        @Override public void afterTransactionsFinished() throws Exception {
            // Disable index corruption.
            BPlusTree.pageHndWrapper = (tree, hnd) -> hnd;

            IgniteEx crd = grid(0);

            if (nodeStoppingExpected) {
                // Wait until node with death worker will left cluster.
                GridTestUtils.waitForCondition(() -> crd.cluster().nodes().size() == nodesCount() - 1, 5000);

                // Re-start failed node.
                startGrid(failedNodeIdx);

                awaitPartitionMapExchange();
            }
        }
    }
}
