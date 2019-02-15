/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests affinity assignment for different affinity types.
 */
@RunWith(JUnit4.class)
public class IgniteClientAffinityAssignmentSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int PARTS = 256;

    /** */
    private boolean cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cache) {
            CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

            ccfg.setCacheMode(CacheMode.PARTITIONED);
            ccfg.setBackups(1);
            ccfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);

            ccfg.setNearConfiguration(null);

            ccfg.setAffinity(new RendezvousAffinityFunction(false, PARTS));

            cfg.setCacheConfiguration(ccfg);
        }
        else
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRendezvousAssignment() throws Exception {
        checkAffinityFunction();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkAffinityFunction() throws Exception {
        cache = true;

        startGridsMultiThreaded(3, true);

        long topVer = 3;

        try {
            checkAffinity(topVer++);

            cache = false;

            final Ignite ignite3 = startGrid(3);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ((IgniteKernal)ignite3).getCache(DEFAULT_CACHE_NAME);

                    return null;
                }
            }, IllegalArgumentException.class, null);

            assertNotNull(ignite3.cache(DEFAULT_CACHE_NAME)); // Start client cache.

            ((IgniteKernal)ignite3).getCache(DEFAULT_CACHE_NAME);

            checkAffinity(topVer++);

            final Ignite ignite4 = startGrid(4);

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ((IgniteKernal)ignite4).getCache(DEFAULT_CACHE_NAME);

                    return null;
                }
            }, IllegalArgumentException.class, null);

            assertNotNull(ignite4.cache(DEFAULT_CACHE_NAME)); // Start client cache.

            ((IgniteKernal)ignite4).getCache(DEFAULT_CACHE_NAME);

            checkAffinity(topVer++);

            final Ignite ignite5 = startGrid(5); // Node without cache.

            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    ((IgniteKernal)ignite5).getCache(DEFAULT_CACHE_NAME);

                    return null;
                }
            }, IllegalArgumentException.class, null);

            checkAffinity(topVer++);

            stopGrid(5);

            checkAffinity(topVer++);

            stopGrid(4);

            checkAffinity(topVer++);

            stopGrid(3);

            checkAffinity(topVer);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void checkAffinity(long topVer) throws Exception {
        awaitTopology(topVer);

        Affinity<Object> aff = grid(0).affinity(DEFAULT_CACHE_NAME);

        for (Ignite grid : Ignition.allGrids()) {
            try {
                if (grid.cluster().localNode().id().equals(grid(0).localNode().id()))
                    continue;

                Affinity<Object> checkAff = grid.affinity(DEFAULT_CACHE_NAME);

                for (int p = 0; p < PARTS; p++)
                    assertEquals(aff.mapPartitionToPrimaryAndBackups(p), checkAff.mapPartitionToPrimaryAndBackups(p));
            }
            catch (IllegalArgumentException ignored) {
                // Skip the node without cache.
            }
        }
    }

    /**
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void awaitTopology(final long topVer) throws Exception {
        for (Ignite grid : Ignition.allGrids()) {
            final GridCacheAdapter cache = ((IgniteKernal)grid).internalCache(DEFAULT_CACHE_NAME);

            if (cache == null)
                continue;

            GridTestUtils.waitForCondition(new GridAbsPredicate() {
                @Override public boolean apply() {
                    return cache.context().affinity().affinityTopologyVersion().topologyVersion() == topVer;
                }
            }, 5000);

            assertEquals(topVer, cache.context().affinity().affinityTopologyVersion().topologyVersion());
        }
    }
}
