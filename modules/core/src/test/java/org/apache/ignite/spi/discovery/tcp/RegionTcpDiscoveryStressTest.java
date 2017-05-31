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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Randomly start and stop nodes each carrying random ID
 * and assert that ring is built in proper order on each iteration.
 *
 */
@RunWith(Parameterized.class)
public class RegionTcpDiscoveryStressTest extends GridCommonAbstractTest {
    /** Random for generating cluster region ids */
    private final Random random = new Random();
    /** Type of current test*/
    private volatile Type type = Type.RANDOM;
    /** flag for test with two available cluster region id. */
    private volatile boolean flag = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);
        cfg.setNetworkTimeout(Integer.MAX_VALUE);
        cfg.setFailureDetectionTimeout(Integer.MAX_VALUE);
        cfg.setPeerClassLoadingEnabled(false);
        TcpCommunicationSpi spi = ((TcpCommunicationSpi)cfg.getCommunicationSpi());
        spi.setConnectTimeout(Integer.MAX_VALUE);
        spi.setMaxConnectTimeout(Long.MAX_VALUE);
        spi.setIdleConnectionTimeout(Integer.MAX_VALUE);
        spi.setSharedMemoryPort(-1);
        spi.setSocketWriteTimeout(Integer.MAX_VALUE);

        switch (type){
            case RANDOM: return cfg.setClusterRegionId(random.nextLong());
            case TWOREGION: {
                long id = flag?1:0;
                flag = !flag;
                return cfg.setClusterRegionId(id);
            }
            case NONE: return cfg;
            default: return cfg;
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
     * Randomly start N and stop Т/2 nodes N times.
     *
     * @throws Exception If failed.
     */
    private void runGrids() throws Exception {
        final int N = 2;
        for (int i = 0; i < N; i++) {
            System.out.println("=======================> 0");
            startGrids(N,N*i);
            System.out.println("=======================> 1");
            final IgniteConfiguration cfg = GridTestUtils.getFieldValue(((IgniteKernal) (Ignition.allGrids().get(0))), "cfg");
            final ServerImpl impl = GridTestUtils.getFieldValue(cfg.getDiscoverySpi(), TcpDiscoverySpi.class, "impl");
            System.out.println("=======================> 2");
            checkRing(impl.ring().allNodes());
            assertEquals(N*(i+1)+i*(N/2), Ignition.allGrids().get(0).cluster().topologyVersion());
            System.out.println("=======================> 3");
            final Random rnd = new Random();
            int j = 0;
            while (j < N/2) {
                final IgniteEx grid;
                try {
                    grid = grid(1+rnd.nextInt(N*(i+1)-1));
                } catch (IgniteIllegalStateException e) {
                    continue;
                }
                j++;
                System.out.println("=======================> 4_"+j);
                grid.close();
                System.out.println("=======================> 5_"+j);
                checkRing(impl.ring().allNodes());
            }
        }
    }

    @Parameterized.Parameters
    public static List<Object[]> data() {
        return Arrays.asList(new Object[10][0]);
    }

    /**
     * Tests with random cluster region id.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreadedRandom() throws Exception {
        type = Type.RANDOM;
        stopAllGrids();
        runGrids();
        //!!! Если тут заменить на startGridsMultiThreaded(4) то сломается
        stopAllGrids();
    }

    /**
     * Tests with two available cluster region id.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreadedTwoRegion() throws Exception {
        type = Type.TWOREGION;
        stopAllGrids();
        runGrids();
        //!!! Если тут заменить на startGridsMultiThreaded(4) то сломается
        stopAllGrids();
    }

    /**
     * Tests with cluster region id by default.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultiThreadedNoneRegion() throws Exception {
        type = Type.NONE;
        stopAllGrids();
        runGrids();
        //!!! Если тут заменить на startGridsMultiThreaded(4) то сломается
        stopAllGrids();
    }

    /** Types of tests. */
    enum Type {
        RANDOM,
        TWOREGION,
        NONE,
    }
}
