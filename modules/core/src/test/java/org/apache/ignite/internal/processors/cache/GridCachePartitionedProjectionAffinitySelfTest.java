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

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 * Partitioned affinity test for projections.
 */
public class GridCachePartitionedProjectionAffinitySelfTest extends GridCommonAbstractTest {
    /** Backup count. */
    private static final int BACKUPS = 1;

    /** Grid count. */
    private static final int GRIDS = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(BACKUPS);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrids(GRIDS);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** @throws Exception If failed. */
    @Test
    public void testAffinity() throws Exception {
        waitTopologyUpdate();

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);

        for (int i = 0; i < 100; i++)
            assertEquals(g0.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(i).id(), g1.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(i).id());
    }

    /** @throws Exception If failed. */
    @Test
    public void testProjectionAffinity() throws Exception {
        waitTopologyUpdate();

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);

        ClusterGroup g0Pinned = g0.cluster().forNodeIds(F.asList(g0.cluster().localNode().id()));

        ClusterGroup g01Pinned =
            g1.cluster().forNodeIds(F.asList(g0.cluster().localNode().id(), g1.cluster().localNode().id()));

        for (int i = 0; i < 100; i++)
            assertEquals(g0Pinned.ignite().affinity(DEFAULT_CACHE_NAME).mapKeyToNode(i).id(),
                g01Pinned.ignite().affinity(DEFAULT_CACHE_NAME).mapKeyToNode(i).id());
    }

    /** @throws Exception If failed. */
    private void waitTopologyUpdate() throws Exception {
        GridTestUtils.waitTopologyUpdate(DEFAULT_CACHE_NAME, BACKUPS, log());
    }
}
