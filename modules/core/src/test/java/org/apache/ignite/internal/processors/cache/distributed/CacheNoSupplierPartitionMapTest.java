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
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.AttributeNodeFilter;
import org.junit.Test;

import static org.apache.ignite.internal.TestRecordingCommunicationSpi.blockDemandMessageForGroup;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Checks that the other nodes see the partitions that a data node owns at the end of an exchange because no node has
 * them. Only the data node writes OWNING into its partition map, so it must send the map.
 * <p>
 * The cache keeps its data only on nodes with the {@link #DATA_ATTR} attribute. The coordinator, another server and a
 * client have no data and watch the data node's partitions in their partition maps. The data node joins outside the
 * baseline and gets its partitions when the baseline changes.
 * <p>
 * A node that joins the baseline also rebalances the system cache, and the affinity change exchange after that
 * rebalance carries the node's map anyway. The test holds the system cache's rebalance on the data node, as if the
 * system cache were large.
 */
public class CacheNoSupplierPartitionMapTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE = "cache";

    /** */
    private static final int PARTS = 32;

    /** Attribute of the nodes that keep the cache's data. */
    private static final String DATA_ATTR = "data";

    /** Index of the client node. */
    private static final int CLIENT = 2;

    /** Index of the data node. */
    private static final int DATA = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        boolean data = igniteInstanceName.equals(getTestIgniteInstanceName(DATA));

        return super.getConfiguration(igniteInstanceName)
            .setUserAttributes(Collections.singletonMap(DATA_ATTR, data))
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setCacheConfiguration(new CacheConfiguration<Integer, Integer>(CACHE)
                .setBackups(0)
                .setAffinity(new RendezvousAffinityFunction(false, PARTS))
                .setNodeFilter(new AttributeNodeFilter(DATA_ATTR, true)));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * The first data node gets all partitions when it joins the baseline and owns them, since no node has them.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testOrphanPartitionsOwned() throws Exception {
        IgniteEx crd = startGrid(0);

        crd.cluster().baselineAutoAdjustEnabled(false);

        startGrid(1);
        startClientGrid(CLIENT);

        IgniteEx data = startGrid(DATA);

        TestRecordingCommunicationSpi.spi(data)
            .blockMessages(blockDemandMessageForGroup(CU.cacheId(CU.UTILITY_CACHE_NAME)));

        crd.cluster().setBaselineTopology(crd.cluster().topologyVersion());

        assertTrue("The data node didn't own its partitions", waitForCondition(() -> {
            List<GridDhtLocalPartition> parts = data.cachex(CACHE).context().topology().localPartitions();

            return parts.size() == PARTS && parts.stream().allMatch(p -> p.state() == OWNING);
        }, 10_000));

        UUID dataId = data.localNode().id();

        for (int idx = 0; idx <= CLIENT; idx++) {
            IgniteEx watcher = grid(idx);

            if (!waitForCondition(() -> notOwning(watcher, dataId).isEmpty(), 10_000)) {
                fail("The partition map of " + watcher.name() + " differs from the data node: " +
                    notOwning(watcher, dataId));
            }
        }
    }

    /**
     * @param watcher Node whose partition map is checked.
     * @param dataId ID of the data node.
     * @return Partitions that the watcher's copy of the data node's map doesn't show OWNING.
     */
    private List<String> notOwning(IgniteEx watcher, UUID dataId) {
        GridDhtPartitionMap view = watcher.cachex(CACHE).context().topology().partitionMap(false).get(dataId);

        List<String> res = new ArrayList<>();

        for (int p = 0; p < PARTS; p++) {
            GridDhtPartitionState seen = view == null ? null : view.get(p);

            if (seen != OWNING)
                res.add("p=" + p + " seen=" + seen);
        }

        return res;
    }
}
