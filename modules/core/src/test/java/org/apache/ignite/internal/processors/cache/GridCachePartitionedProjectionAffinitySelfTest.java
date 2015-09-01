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

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

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