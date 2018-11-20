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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.GridCacheUtils;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.EVICTED;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;

/**
 *
 */
public class CacheDataLossOnPartitionMoveTest extends GridCommonAbstractTest {
    /** */
    public static final long MB = 1024 * 1024L;

    /** */
    public static final String GRP_ATTR = "grp";

    /** */
    public static final int GRIDS_CNT = 2;

    /** */
    public static final String EVEN_GRP = "event";

    /** */
    public static final String ODD_GRP = "odd";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        cfg.setPeerClassLoadingEnabled(true);

        Map<String, Object> attrs = new HashMap<>();

        attrs.put(GRP_ATTR, grp(getTestIgniteInstanceIndex(igniteInstanceName)));

        cfg.setUserAttributes(attrs);

        DataStorageConfiguration memCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true).setInitialSize(50 * MB).setMaxSize(50 * MB))
            .setWalMode(WALMode.LOG_ONLY);

        cfg.setDataStorageConfiguration(memCfg);

        cfg.setCacheConfiguration(configuration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /**
     * @param name Name.
     */
    private CacheConfiguration configuration(String name) {
        return new CacheConfiguration(name).
            setCacheMode(CacheMode.PARTITIONED).
            setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL).
            setBackups(1).
            setRebalanceBatchSize(1).
            setAffinity(new TestAffinityFunction().setPartitions(32));
    }

    /**
     * @param idx Index.
     */
    private String grp(int idx) {
        return idx < GRIDS_CNT / 2 ? EVEN_GRP : ODD_GRP;
    }

    /**
     * @throws Exception if failed.
     */
    public void testDataLossOnPartitionMove() throws Exception {
        try {
            Ignite ignite = startGridsMultiThreaded(GRIDS_CNT / 2, false);

            ignite.cluster().active(true);

            List<Integer> toCp = movingKeysAfterJoin(ignite, DEFAULT_CACHE_NAME, 1,
                node -> ((GridTestNode)node).setAttribute(GRP_ATTR, ODD_GRP));

            int blockPartId = ignite.affinity(DEFAULT_CACHE_NAME).partition(toCp.get(0));

            awaitPartitionMapExchange();

            int c = 0;

            for (int i = 0; i < 1000; i++) {
                if (ignite.affinity(DEFAULT_CACHE_NAME).partition(i) == blockPartId) {
                    ignite.cache(DEFAULT_CACHE_NAME).put(i, i);

                    c++;
                }
            }

            assertEquals(c, ignite.cache(DEFAULT_CACHE_NAME).size());

            startGridsMultiThreaded(GRIDS_CNT / 2, GRIDS_CNT / 2);

            // Prevent rebalancing to new nodes.
            for (Ignite ig0 : G.allGrids()) {
                TestRecordingCommunicationSpi.spi(ig0).blockMessages((node, message) -> {
                    if (message instanceof GridDhtPartitionDemandMessage) {
                        assertTrue(node.order() <= GRIDS_CNT / 2);

                        GridDhtPartitionDemandMessage msg = (GridDhtPartitionDemandMessage)message;

                        return msg.groupId() == CU.cacheId(DEFAULT_CACHE_NAME);
                    }

                    return false;
                });
            }

            ignite.cluster().setBaselineTopology(GRIDS_CNT);

            for (Ignite ig0 : G.allGrids()) {
                if (ig0.cluster().localNode().order() <= GRIDS_CNT / 2)
                    continue;

                TestRecordingCommunicationSpi.spi(ig0).waitForBlocked();
            }

            assertEquals(c, ignite.cache(DEFAULT_CACHE_NAME).size());

            int i = 0;

            while(i < GRIDS_CNT / 2) {
                stopGrid(GRIDS_CNT / 2 + i);

                i++;
            }

            awaitPartitionMapExchange();

            for (Ignite ig : G.allGrids()) {
                GridDhtLocalPartition locPart = dht(ig.cache(DEFAULT_CACHE_NAME)).topology().localPartition(blockPartId);

                assertNotNull(locPart);

                assertEquals("Unexpected state", OWNING, locPart.state());
            }

            startGridsMultiThreaded(GRIDS_CNT / 2, GRIDS_CNT / 2);

            awaitPartitionMapExchange(true, true, null);

            for (Ignite ig : G.allGrids()) {
                GridDhtLocalPartition locPart = dht(ig.cache(DEFAULT_CACHE_NAME)).topology().localPartition(blockPartId);

                assertNotNull(locPart);

                switch ((String)ig.cluster().localNode().attribute(GRP_ATTR)) {
                    case EVEN_GRP:
                        assertEquals("Unexpected state", EVICTED, locPart.state());

                        break;

                    case ODD_GRP:
                        assertEquals("Unexpected state", OWNING, locPart.state());

                        break;

                    default:
                        fail();
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanPersistenceDir();
    }

    /** */
    public static class TestAffinityFunction extends RendezvousAffinityFunction {
        /** */
        public TestAffinityFunction() {
        }

        /** */
        public TestAffinityFunction(boolean exclNeighbors) {
            super(exclNeighbors);
        }

        /** */
        public TestAffinityFunction(boolean exclNeighbors, int parts) {
            super(exclNeighbors, parts);
        }

        /** */
        public TestAffinityFunction(int parts,
            @Nullable IgniteBiPredicate<ClusterNode, ClusterNode> backupFilter) {
            super(parts, backupFilter);
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            int parts = partitions();

            List<List<ClusterNode>> assignments = new ArrayList<>(parts);

            Map<UUID, Collection<ClusterNode>> neighborhoodCache = isExcludeNeighbors() ?
                GridCacheUtils.neighbors(affCtx.currentTopologySnapshot()) : null;

            List<ClusterNode> nodes = affCtx.currentTopologySnapshot();

            Map<Object, List<ClusterNode>> nodesByGrp = U.newHashMap(2);

            for (ClusterNode node : nodes) {
                Object grp = node.attribute(GRP_ATTR);

                List<ClusterNode> grpNodes = nodesByGrp.get(grp);

                if (grpNodes == null)
                    nodesByGrp.put(grp, (grpNodes = new ArrayList<>()));

                grpNodes.add(node);
            }

            boolean split = nodesByGrp.size() == 2;

            for (int i = 0; i < parts; i++) {
                List<ClusterNode> partAssignment = assignPartition(i, split ?
                        nodesByGrp.get(i % 2 == 0 ? EVEN_GRP : ODD_GRP) : nodes,
                    affCtx.backups(), neighborhoodCache);

                assignments.add(partAssignment);
            }

            return assignments;
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Integer.MAX_VALUE;
    }
}
