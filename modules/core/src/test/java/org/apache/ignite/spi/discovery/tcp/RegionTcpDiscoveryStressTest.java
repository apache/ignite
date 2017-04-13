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

package org.apache.ignite.spi.discovery.tcp;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Randomly start and stop nodes each carrying random ID
 * and assert that ring is built in proper order on each iteration.
 *
 */
public class RegionTcpDiscoveryStressTest extends GridCommonAbstractTest {
    private final Random random = new Random();
    /** Type of current test*/
    private volatile Type type = Type.RANDOM;
    /** flag for test with two available cluster region id. */
    private volatile boolean flag = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        switch (type){
            case RANDOM: return super.getConfiguration(gridName).setClusterRegionId(random.nextLong());
            case TWOREGION: {
                long id = flag?1:0;
                flag = !flag;
                return super.getConfiguration(gridName).setClusterRegionId(id);
            }
            case NONE: return super.getConfiguration(gridName);
            default: return super.getConfiguration(gridName);
        }
    }

    /**
     * Check that ring is built in proper order.
     *
     * @param nodes Ring of nodes;
     */
    private static void checkRing(Collection<TcpDiscoveryNode> nodes) {
        long lastRegionId = Long.MIN_VALUE;
        long lastId = Long.MIN_VALUE;
        for (TcpDiscoveryNode node: nodes) {
            long regionId = node.getClusterRegionId();
            assertTrue(regionId + " >= " + lastRegionId, regionId >=lastRegionId);
            long id = node.order();
            if (regionId ==lastRegionId)
                assertTrue(id >=lastId);
            lastRegionId = regionId;
            lastId = id;
        }
    }

    /**
     * Randomly start and stop nodes.
     *
     * @throws Exception If failed.
     */
    private void runGrids() throws Exception {
        final int N = 5;
        for (int i = 0; i < N; i++) {
            Ignite ignite = startGridsMultiThreaded(N*i, N);
            IgniteConfiguration cfg = GridTestUtils.getFieldValue(((IgniteKernal) (ignite)), "cfg");
            ServerImpl impl = GridTestUtils.getFieldValue(cfg.getDiscoverySpi(), TcpDiscoverySpi.class, "impl");
            ArrayList nodes = new ArrayList(impl.ring().allNodes());
            checkRing(nodes);
            assertEquals(N*(i+1)+i*(N/2), ignite.cluster().topologyVersion());

            Random rnd = new Random();
            int j = 0;
            while (j < N/2) {
                int k = rnd.nextInt(N*(i+1));
                try {
                    grid(k);
                } catch (IgniteIllegalStateException e) {
                    continue;
                }
                j++;
                stopGrid(k);
                nodes = new ArrayList(impl.ring().allNodes());
                checkRing(nodes);
            }
        }
    }

    /**
     * Tests with random cluster region id.
     *
     * @throws Exception If failed.
     */
    public void testMultiThreadedRandom() throws Exception {
        type = Type.RANDOM;
        runGrids();
        stopAllGrids();
    }

    /**
     * Tests with two available cluster region id.
     *
     * @throws Exception If failed.
     */
    public void testMultiThreadedTwoRegion() throws Exception {
        type = Type.TWOREGION;
        runGrids();
        stopAllGrids();
    }

    /**
     * Tests with cluster region id by default.
     *
     * @throws Exception If failed.
     */
    public void testMultiThreadedTwoNoneRegion() throws Exception {
        type = Type.NONE;
        runGrids();
        stopAllGrids();
    }

    /** Types of tests. */
    enum Type {
        RANDOM,
        TWOREGION,
        NONE,
    }
}
