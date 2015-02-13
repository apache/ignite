/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.affinity.consistenthash.*;
import org.apache.ignite.cache.affinity.fair.*;
import org.apache.ignite.cache.affinity.rendezvous.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheDistributionMode.*;

/**
 * Tests affinity assignment for different affinity types.
 */
public class IgniteClientAffinityAssignmentSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 256;

    /** */
    private boolean client;

    /** */
    private boolean cache;

    /** */
    private int aff;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cache) {
            CacheConfiguration ccfg = new CacheConfiguration();

            ccfg.setCacheMode(CacheMode.PARTITIONED);
            ccfg.setBackups(1);
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
            ccfg.setDistributionMode(client ? CLIENT_ONLY : PARTITIONED_ONLY);

            if (aff == 0)
                ccfg.setAffinity(new CacheConsistentHashAffinityFunction(false, PARTS));
            else if (aff == 1)
                ccfg.setAffinity(new CacheRendezvousAffinityFunction(false, PARTS));
            else
                ccfg.setAffinity(new CachePartitionFairAffinity(PARTS));

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testConsistentHashAssignment() throws Exception {
        aff = 0;

        checkAffinityFunction();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRendezvousAssignment() throws Exception {
        aff = 1;

        checkAffinityFunction();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFairAssignment() throws Exception {
        aff = 2;

        checkAffinityFunction();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinityFunction() throws Exception {
        cache = true;

        startGrids(3);

        try {
            checkAffinity();

            client = true;

            startGrid(3);

            checkAffinity();

            startGrid(4);

            checkAffinity();

            cache = false;

            startGrid(5);

            checkAffinity();

            stopGrid(5);

            checkAffinity();

            stopGrid(4);

            checkAffinity();

            stopGrid(3);

            checkAffinity();
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinity() throws Exception {
        CacheAffinity<Object> aff = grid(0).cache(null).affinity();

        for (Ignite grid : Ignition.allGrids()) {
            try {
                if (grid.cluster().localNode().id().equals(grid(0).localNode().id()))
                    continue;

                CacheAffinity<Object> checkAff = grid.cache(null).affinity();

                for (int p = 0; p < PARTS; p++)
                    assertEquals(aff.mapPartitionToPrimaryAndBackups(p), checkAff.mapPartitionToPrimaryAndBackups(p));
            }
            catch (IllegalArgumentException ignored) {
                // Skip the node without cache.
            }
        }
    }
}
