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

package org.apache.ignite.internal.processors.affinity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests affinity history assignment diff calculation for history assignment.
 */
public class GridHistoryAffinityAssignmentTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** */
    @Test
    public void testSimple() {
        int cnt = 128;

        List<List<ClusterNode>> curr = new ArrayList<>();
        List<List<ClusterNode>> ideal = new ArrayList<>();

        for (int i = 0; i < cnt; i++) {
            List<ClusterNode> nodes = Arrays.asList(new GridTestNode(UUID.randomUUID()), new GridTestNode(UUID.randomUUID()));
            curr.add(nodes);
            ideal.add(Arrays.asList(nodes.get(1), nodes.get(0)));
        }

        AffinityTopologyVersion topVer = new AffinityTopologyVersion(1, 0);
        HistoryAffinityAssignment lateAssign =
            new HistoryAffinityAssignmentImpl(new GridAffinityAssignmentV2(topVer, curr, ideal), 1);

        assertEquals("Late", curr, lateAssign.assignment());
        assertEquals("Ideal late", ideal, lateAssign.idealAssignment());

        HistoryAffinityAssignment idealAssign = new
            HistoryAffinityAssignmentImpl(new GridAffinityAssignmentV2(topVer, ideal, ideal), 1);

        assertSame(idealAssign.assignment(), idealAssign.idealAssignment());

        assertEquals("Ideal", ideal, idealAssign.idealAssignment());
    }

    /** */
    @Test
    public void testHistoryAffinityAssignmentCalculation() throws Exception {
        try {
            IgniteEx grid0 = startGrid(0);

            AffinityAssignment a0 = affinityCache(grid0).cachedAffinity(new AffinityTopologyVersion(1, 0));

            startGrid(1);

            awaitPartitionMapExchange();

            AffinityAssignment a1 = affinityCache(grid0).cachedAffinity(new AffinityTopologyVersion(1, 0));

            assertTrue(a1 instanceof HistoryAffinityAssignment);

            AffinityAssignment a2 = affinityCache(grid0).cachedAffinity(new AffinityTopologyVersion(2, 0));
            AffinityAssignment a3 = affinityCache(grid0).cachedAffinity(new AffinityTopologyVersion(2, 1));

            // Compare head with history assignment.
            assertEquals(a0.assignment(), a1.assignment());
            assertEquals(a0.idealAssignment(), a1.idealAssignment());

            startGrid(2);

            awaitPartitionMapExchange();

            AffinityAssignment a5 = affinityCache(grid0).cachedAffinity(new AffinityTopologyVersion(2, 0));
            AffinityAssignment a6 = affinityCache(grid0).cachedAffinity(new AffinityTopologyVersion(2, 1));

            assertTrue(a5 instanceof HistoryAffinityAssignment);
            assertTrue(a6 instanceof HistoryAffinityAssignment);

            assertEquals(a2.assignment(), a5.assignment());
            assertEquals(a2.idealAssignment(), a5.idealAssignment());

            assertEquals(a3.assignment(), a6.assignment());
            assertEquals(a3.idealAssignment(), a6.idealAssignment());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @param ignite Ignite.
     */
    private GridAffinityAssignmentCache affinityCache(IgniteEx ignite) {
        GridCacheProcessor proc = ignite.context().cache();

        GridCacheContext cctx = proc.context().cacheContext(CU.cacheId(DEFAULT_CACHE_NAME));

        return GridTestUtils.getFieldValue(cctx.affinity(), "aff");
    }
}
