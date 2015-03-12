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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheDistributionMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;

/**
 * Partitioned affinity test for projections.
 */
@SuppressWarnings({"PointlessArithmeticExpression"})
public class GridCachePartitionedProjectionAffinitySelfTest extends GridCommonAbstractTest {
    /** Backup count. */
    private static final int BACKUPS = 1;

    /** Grid count. */
    private static final int GRIDS = 3;

    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(BACKUPS);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        cacheCfg.setRebalanceMode(SYNC);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

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
    public void testAffinity() throws Exception {
        waitTopologyUpdate();

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);

        for (int i = 0; i < 100; i++)
            assertEquals(g0.cluster().mapKeyToNode(null, i).id(), g1.cluster().mapKeyToNode(null, i).id());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("deprecation")
    public void testProjectionAffinity() throws Exception {
        waitTopologyUpdate();

        Ignite g0 = grid(0);
        Ignite g1 = grid(1);

        ClusterGroup g0Pinned = g0.cluster().forNodeIds(F.asList(g0.cluster().localNode().id()));

        ClusterGroup g01Pinned =
            g1.cluster().forNodeIds(F.asList(g0.cluster().localNode().id(), g1.cluster().localNode().id()));

        for (int i = 0; i < 100; i++)
            assertEquals(g0Pinned.ignite().cluster().mapKeyToNode(null, i).id(),
                g01Pinned.ignite().cluster().mapKeyToNode(null, i).id());
    }

    /** @throws Exception If failed. */
    @SuppressWarnings("BusyWait")
    private void waitTopologyUpdate() throws Exception {
        GridTestUtils.waitTopologyUpdate(null, BACKUPS, log());
    }
}
