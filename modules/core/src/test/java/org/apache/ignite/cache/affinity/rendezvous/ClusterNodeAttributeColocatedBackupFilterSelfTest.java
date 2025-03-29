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

package org.apache.ignite.cache.affinity.rendezvous;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionBackupFilterAbstractSelfTest;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests of {@link AffinityFunction} implementations with {@link ClusterNodeAttributeColocatedBackupFilter}.
 */
public class ClusterNodeAttributeColocatedBackupFilterSelfTest extends AffinityFunctionBackupFilterAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setFailureHandler((i, f) -> true);
    }

    /** {@inheritDoc} */
    @Override protected AffinityFunction affinityFunction() {
        return affinityFunctionWithAffinityBackupFilter(SPLIT_ATTRIBUTE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected AffinityFunction affinityFunctionWithAffinityBackupFilter(String attrName) {
        RendezvousAffinityFunction aff = new RendezvousAffinityFunction(false);

        aff.setAffinityBackupFilter(new ClusterNodeAttributeColocatedBackupFilter(attrName));

        return aff;
    }

    /** {@inheritDoc} */
    @Override protected void checkPartitionsWithAffinityBackupFilter() {
        AffinityFunction aff = cacheConfiguration(grid(0).configuration(), DEFAULT_CACHE_NAME).getAffinity();

        int partCnt = aff.partitions();

        int iter = grid(0).cluster().nodes().size() / 4;

        IgniteCache<Object, Object> cache = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < partCnt; i++) {
            Collection<ClusterNode> nodes = affinity(cache).mapKeyToPrimaryAndBackups(i);

            Map<String, Integer> stat = getAttributeStatistic(nodes);

            if (stat.get(FIRST_NODE_GROUP) > 0) {
                assertEquals((Integer)Math.min(backups + 1, iter * 2), stat.get(FIRST_NODE_GROUP));
                assertEquals((Integer)0, stat.get("B"));
                assertEquals((Integer)0, stat.get("C"));
            }
            else if (stat.get("B") > 0) {
                assertEquals((Integer)0, stat.get(FIRST_NODE_GROUP));
                assertEquals((Integer)iter, stat.get("B"));
                assertEquals((Integer)0, stat.get("C"));
            }
            else if (stat.get("C") > 0) {
                assertEquals((Integer)0, stat.get(FIRST_NODE_GROUP));
                assertEquals((Integer)0, stat.get("B"));
                assertEquals((Integer)iter, stat.get("C"));
            }
            else
                fail("Unexpected partition assignment");
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkPartitions() throws Exception {
        int iter = grid(0).cluster().nodes().size() / 2;

        AffinityFunction aff = cacheConfiguration(grid(0).configuration(), DEFAULT_CACHE_NAME).getAffinity();

        Map<Integer, String> partToAttr = partToAttribute(grid(0).cache(DEFAULT_CACHE_NAME), aff.partitions());

        assertTrue(F.exist(partToAttr.values(), "A"::equals));
        assertTrue(F.exist(partToAttr.values(), "B"::equals));
        assertFalse(F.exist(partToAttr.values(), v -> !"A".equals(v) && !"B".equals(v)));
    }

    /** {@inheritDoc} */
    @Override public void testPartitionDistributionWithAffinityBackupFilter() throws Exception {
        backups = 2;

        super.testPartitionDistributionWithAffinityBackupFilter();
    }

    /** */
    @Test
    public void testBackupFilterWithBaseline() throws Exception {
        backups = 1;

        try {
            startGrid(0, "A");
            startGrid(1, "B");
            startGrid(2, "C");

            startGrid(3, "A");
            startGrid(4, "B");
            startGrid(5, "C");

            awaitPartitionMapExchange();

            AffinityFunction aff = cacheConfiguration(grid(0).configuration(), DEFAULT_CACHE_NAME).getAffinity();

            Map<Integer, String> partToAttr = partToAttribute(grid(0).cache(DEFAULT_CACHE_NAME), aff.partitions());

            grid(0).cluster().baselineAutoAdjustEnabled(false);

            // Check that we have the same distribution if some BLT nodes are offline.
            stopGrid(3);
            stopGrid(4);
            stopGrid(5);

            awaitPartitionMapExchange();

            assertEquals(partToAttr, partToAttribute(grid(0).cache(DEFAULT_CACHE_NAME), aff.partitions()));

            // Check that not BLT nodes do not affect distribution.
            startGrid(6, "D");

            awaitPartitionMapExchange();

            assertEquals(partToAttr, partToAttribute(grid(0).cache(DEFAULT_CACHE_NAME), aff.partitions()));

            // Check that distribution is recalculated after BLT change.
            long topVer = grid(0).cluster().topologyVersion();

            grid(0).cluster().setBaselineTopology(topVer);

            // Wait for rebalance and assignment change to ideal assignment.
            assertTrue(waitForCondition(() -> Objects.equals(grid(0).context().discovery().topologyVersionEx(),
                    new AffinityTopologyVersion(topVer, 2)), 5_000L));

            assertNotEquals(partToAttr, partToAttribute(grid(0).cache(DEFAULT_CACHE_NAME), aff.partitions()));
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Test
    public void testBackupFilterNullAttributeBltChange() throws Exception {
        backups = 1;

        try {
            startGrid(0, "A");
            startGrid(1, "A");

            awaitPartitionMapExchange();

            grid(0).cluster().baselineAutoAdjustEnabled(false);

            // Join of non-BLT node with the empty attribute should not trigger failure handler.
            startGrid(2, (String)null);

            assertNull(grid(0).context().failure().failureContext());
            assertNull(grid(1).context().failure().failureContext());
            assertNull(grid(2).context().failure().failureContext());

            // Include node with the empty attribute to the BLT should trigger failure handler on all nodes.
            resetBaselineTopology();

            assertNotNull(grid(0).context().failure().failureContext());
            assertNotNull(grid(1).context().failure().failureContext());
            assertNotNull(grid(2).context().failure().failureContext());
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Test
    public void testBackupFilterNullAttributeBltNodeJoin() throws Exception {
        backups = 1;

        try {
            startGrid(0, "A");
            startGrid(1, "A");

            awaitPartitionMapExchange();

            grid(0).cluster().baselineAutoAdjustEnabled(false);

            stopGrid(1);

            awaitPartitionMapExchange();

            // Join of BLT node with the empty attribute should trigger failure handler on this node.
            startGrid(1, (String)null);

            assertNull(grid(0).context().failure().failureContext());
            assertNotNull(grid(1).context().failure().failureContext());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Determine split attribute value for each partition and check that this value is the same for all nodes for
     * this partition.
     */
    private Map<Integer, String> partToAttribute(IgniteCache<Object, Object> cache, int partCnt) {
        Map<Integer, String> partToAttr = U.newHashMap(partCnt);

        for (int i = 0; i < partCnt; i++) {
            Collection<ClusterNode> nodes = affinity(cache).mapPartitionToPrimaryAndBackups(i);

            assertFalse(F.isEmpty(nodes));
            assertFalse(F.size(nodes) > backups + 1);

            String attrVal = F.first(nodes).attribute(SPLIT_ATTRIBUTE_NAME);

            partToAttr.put(i, attrVal);

            for (ClusterNode node : nodes)
                assertEquals(attrVal, node.attribute(SPLIT_ATTRIBUTE_NAME));
        }

        return partToAttr;
    }
}
