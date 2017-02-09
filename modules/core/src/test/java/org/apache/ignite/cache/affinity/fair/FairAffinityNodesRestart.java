package org.apache.ignite.cache.affinity.fair;

import java.util.List;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.P2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests that FairAffinityFunction doesn't throw exception on nodes restart,
 * with backup filter set and 0 cache backups.
 */
public class FairAffinityNodesRestart extends GridCommonAbstractTest {
    /** */
    private final static P2<ClusterNode, ClusterNode> BACKUP_FILTER = new P2<ClusterNode, ClusterNode>() {
        @Override public boolean apply(ClusterNode node, ClusterNode node2) {
            return true;
        }
    };

    /** */
    private final static P2<ClusterNode, List<ClusterNode>> AFF_BACKUP_FILTER = new P2<ClusterNode, List<ClusterNode>>() {
        @Override public boolean apply(ClusterNode node, List<ClusterNode> nodes) {
            return true;
        }
    };

    /** */
    private boolean affBackup;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration ccfg = new CacheConfiguration("fair-cache");

        FairAffinityFunction aff = new FairAffinityFunction(32);

        if (!affBackup)
            aff.setBackupFilter(BACKUP_FILTER);
        else
            aff.setAffinityBackupFilter(AFF_BACKUP_FILTER);

        ccfg.setAffinity(aff);
        ccfg.setBackups(0);
        ccfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @param idx Node index.
     * @return Future.
     */
    private IgniteInternalFuture<IgniteEx> startAsyncGrid(final int idx) {
        return GridTestUtils.runAsync(new Callable<IgniteEx>() {
            @Override public IgniteEx call() throws Exception {
                return startGrid(idx);
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupFilter() throws Exception {
        affBackup = false;

        check();
    }

    /**
     * @throws Exception If failed.
     */
    public void testAffinityBackupFilter() throws Exception {
        affBackup = true;

        check();
    }

    /**
     * @throws Exception If failed.
     */
    private void check() throws Exception {
        for (int i = 0; i < 2; i++) {
            IgniteInternalFuture<IgniteEx> fut0 = startAsyncGrid(0);
            IgniteInternalFuture<IgniteEx> fut1 = startAsyncGrid(1);
            IgniteInternalFuture<IgniteEx> fut2 = startAsyncGrid(2);

            IgniteEx ignite = fut0.get();
            fut1.get();
            fut2.get();

            IgniteCache<Integer, String> cache = ignite.cache("fair-cache");

            for (int j = 0; j < 100; j++)
                cache.put(i, String.valueOf(i));

            stopGrid(0);
            stopGrid(1);
            stopGrid(2);
        }
    }
}
