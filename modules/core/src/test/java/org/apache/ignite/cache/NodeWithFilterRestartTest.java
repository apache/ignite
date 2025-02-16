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

package org.apache.ignite.cache;

import java.io.File;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 *
 */
public class NodeWithFilterRestartTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryVmIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean persistence;

    /** */
    private boolean testAttribute;

    /** */
    private boolean blockPme = true;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(IP_FINDER));

        if (blockPme) {
            if (getTestIgniteInstanceName(5).equals(igniteInstanceName))
                cfg.setUserAttributes(F.asMap("FILTER", "true"));

            if (getTestIgniteInstanceName(0).equals(igniteInstanceName)) {
                TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

                commSpi.blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
                    /** {@inheritDoc} */
                    @Override public boolean apply(ClusterNode node, Message msg) {
                        if (msg instanceof GridDhtPartitionsFullMessage && (node.id().getLeastSignificantBits() & 0xFFFF) == 5) {
                            GridDhtPartitionsFullMessage fullMsg = (GridDhtPartitionsFullMessage)msg;

                            if (fullMsg.exchangeId() != null && fullMsg.topologyVersion().equals(new AffinityTopologyVersion(8, 0))) {
                                info("Going to block message [node=" + node + ", msg=" + msg + ']');

                                return true;
                            }
                        }

                        return false;
                    }
                });

                cfg.setCommunicationSpi(commSpi);
            }
            else
                cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());
        }

        if (persistence) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration().
                setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));
        }

        if (testAttribute)
            cfg.setUserAttributes(F.asMap("FILTER", "true"));

        return cfg;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testSpecificRestart() throws Exception {
        try {
            startGrids(6);

            {
                CacheConfiguration cfg1 = new CacheConfiguration();
                cfg1.setName("TRANSIENT_JOURNEY_ID");
                cfg1.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
                cfg1.setBackups(1);
                cfg1.setRebalanceMode(CacheRebalanceMode.ASYNC);
                cfg1.setAffinity(new RendezvousAffinityFunction(false, 64));
                cfg1.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
                cfg1.setNodeFilter(new NodeFilter());

                CacheConfiguration cfg2 = new CacheConfiguration();
                cfg2.setName("ENTITY_CONFIG");
                cfg2.setAtomicityMode(CacheAtomicityMode.ATOMIC);
                cfg2.setCacheMode(CacheMode.REPLICATED);
                cfg2.setRebalanceMode(CacheRebalanceMode.ASYNC);
                cfg2.setBackups(0);
                cfg2.setAffinity(new RendezvousAffinityFunction(false, 256));

                grid(0).getOrCreateCaches(Arrays.asList(cfg1, cfg2));
            }

            stopGrid(5, true);

            // Start grid 5 to trigger a local join exchange (node 0 is coordinator).
            // Since the message is blocked, node 5 will not complete the exchange.
            // Fail coordinator, check if the next exchange can be completed.
            // 5 should send single message to the new coordinator 1.
            IgniteInternalFuture<IgniteEx> fut = GridTestUtils.runAsync(() -> startGrid(5));

            // Increase if does not reproduce.
            U.sleep(2000);

            stopGrid(0, true);

            fut.get();

            awaitPartitionMapExchange();
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Test
    public void testNodeRejoinsClusterAfterFilteredCacheRemoved() throws Exception {
        blockPme = false;
        persistence = true;
        testAttribute = true;

        Ignite ig = startGrids(2);

        int filteredGridIdx = G.allGrids().size();

        startFilteredGrid(filteredGridIdx);

        grid(0).cluster().state(ClusterState.ACTIVE);

        ig.cluster().baselineAutoAdjustEnabled(false);

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        int nonBaselineIdx = G.allGrids().size();

        startGrid(nonBaselineIdx);

        CacheConfiguration cacheCfg = createAndFillCache();

        File cacheMetaPath1 = grid(filteredGridIdx).context().pdsFolderResolver().fileTree().cacheConfigurationFile(cacheCfg);
        File cacheMetaPath2 = grid(nonBaselineIdx).context().pdsFolderResolver().fileTree().cacheConfigurationFile(cacheCfg);

        assertTrue(cacheMetaPath1.exists() && cacheMetaPath1.isFile());
        assertTrue(cacheMetaPath2.exists() && cacheMetaPath2.isFile());

        grid(0).destroyCache(DEFAULT_CACHE_NAME);
        awaitPartitionMapExchange();

        assertFalse(cacheMetaPath1.exists() && cacheMetaPath1.isFile());
        assertFalse(cacheMetaPath2.exists() && cacheMetaPath2.isFile());

        // Try just restart grid.stopGrid(filteredGridIdx);
        stopGrid(filteredGridIdx);
        stopGrid(nonBaselineIdx);

        startFilteredGrid(filteredGridIdx);
        startGrid(nonBaselineIdx);

        createAndFillCache();

        assertTrue(cacheMetaPath1.exists() && cacheMetaPath1.isFile());

        // Test again with the local cache proxy.
        assertEquals(100, grid(filteredGridIdx).cache(DEFAULT_CACHE_NAME).size());

        grid(0).destroyCache(DEFAULT_CACHE_NAME);
        awaitPartitionMapExchange();

        assertFalse(cacheMetaPath1.exists() && cacheMetaPath1.isFile());

        stopGrid(filteredGridIdx);
        startFilteredGrid(filteredGridIdx);
    }

    /**
     * Ensures cache with a node filter is not lost when all nodes restarted.
     */
    @Test
    public void testAllNodesRestarted() throws Exception {
        blockPme = false;
        persistence = true;
        testAttribute = true;

        startFilteredGrid(0);
        startGrid(1);
        startGrid(2);

        grid(1).cluster().state(ClusterState.ACTIVE);

        createAndFillCache();

        stopAllGrids();

        startFilteredGrid(0);

        grid(0).cluster().state(ClusterState.ACTIVE);

        assertThrows(null, () -> {
            grid(0).createCache(defaultCacheConfiguration().setName(DEFAULT_CACHE_NAME));
        }, IgniteException.class, "cache with the same name is already started");

        startGrid(1);
        startGrid(2);

        assertEquals(100, grid(0).cache(DEFAULT_CACHE_NAME).size());
        assertEquals(100, grid(1).cache(DEFAULT_CACHE_NAME).size());
    }

    /** */
    private IgniteEx startFilteredGrid(int idx) throws Exception {
        testAttribute = false;

        IgniteEx res = startGrid(idx);

        testAttribute = true;

        return res;
    }

    /** */
    private CacheConfiguration createAndFillCache() throws InterruptedException {
        final CacheConfiguration<Object, Object> cfg = defaultCacheConfiguration()
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setNodeFilter(new NodeFilter());

        grid(1).createCache(cfg);

        try (IgniteDataStreamer<Integer, Integer> ds = grid(1).dataStreamer(DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < 100; ++i)
                ds.addData(i, i);
        }

        return cfg;
    }

    /**
     *
     */
    private static final class NodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode clusterNode) {
            return "true".equals(clusterNode.attribute("FILTER"));
        }
    }
}
