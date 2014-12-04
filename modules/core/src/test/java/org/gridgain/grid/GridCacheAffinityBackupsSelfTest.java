/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.affinity.fair.*;
import org.gridgain.grid.cache.affinity.rendezvous.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Tests affinity function with different number of backups.
 */
public class GridCacheAffinityBackupsSelfTest extends GridCommonAbstractTest {
    /** Number of backups. */
    private int backups;

    /** Affinity function. */
    private GridCacheAffinityFunction func;

    private int nodesCnt = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setCacheMode(GridCacheMode.PARTITIONED);
        ccfg.setBackups(backups);
        ccfg.setAffinity(func);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConsistentHashBackups() throws Exception {
        for (int i = 0; i < nodesCnt; i++)
            checkBackups(i, new GridCacheConsistentHashAffinityFunction());
    }

    /**
     * @throws Exception If failed.
     */
    public void testRendezvousBackups() throws Exception {
        for (int i = 0; i < nodesCnt; i++)
            checkBackups(i, new GridCacheRendezvousAffinityFunction());
    }

    /**
     * @throws Exception If failed.
     */
    public void testFairBackups() throws Exception {
        for (int i = 0; i < nodesCnt; i++)
            checkBackups(i, new GridCachePartitionFairAffinity());
    }

    /**
     * @throws Exception If failed.
     */
    private void checkBackups(int backups, GridCacheAffinityFunction func) throws Exception {
        this.backups = backups;
        this.func = func;

        startGrids(nodesCnt);

        try {
            GridCache<Object, Object> cache = grid(0).cache(null);

            Collection<UUID> members = new HashSet<>();

            for (int i = 0; i < 10000; i++) {
                Collection<ClusterNode> nodes = cache.affinity().mapKeyToPrimaryAndBackups(i);

                assertEquals(backups + 1, nodes.size());

                for (ClusterNode n : nodes)
                    members.add(n.id());
            }

            assertEquals(nodesCnt, members.size());
        }
        finally {
            stopAllGrids();
        }
    }
}
