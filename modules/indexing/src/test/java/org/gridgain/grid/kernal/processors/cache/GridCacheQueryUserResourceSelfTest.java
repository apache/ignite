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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.discovery.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.cache.GridCacheMode.*;

/**
 * Test runs cache queries with injected user resource.
 */
public class GridCacheQueryUserResourceSelfTest extends GridCommonAbstractTest {
    /** Deploy counter key. */
    private static final String DEPLOY_CNT_KEY = "deployCnt";

    /** Undeploy counter key. */
    private static final String UNDEPLOY_CNT_KEY = "undeployCnt";

    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Node run count. */
    private static final int RUN_CNT = 2;

    /** VM ip finder for TCP discovery. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** External class loader. */
    private static final ClassLoader extClsLdr;

    /**
     * Initialize external class loader.
     */
    static {
        extClsLdr = getExternalClassLoader();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(discoverySpi());
        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Discovery SPI.
     * @throws Exception In case of error.
     */
    private DiscoverySpi discoverySpi() throws Exception {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setMaxMissedHeartbeats(Integer.MAX_VALUE);
        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /**
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration() throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 30 * 1000; // 30 seconds.
    }

    /**
     * Checks that user resource in cache query reducer is
     * deployed and undeployed correctly.
     *
     * @throws Exception If failed.
     */
    public void testCacheQueryUserResourceDeployment() throws Exception {
        // Start secondary nodes.
        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);

        try {
            for (int i = 1; i <= RUN_CNT; i++) {
                // Start primary node.
                Ignite g = startGrid();

                try {
                    runQuery(g);
                }
                finally {
                    // Stop primary node.
                    stopGrid();
                }

                final int fi = i;

                assertTrue(GridTestUtils.waitForCondition(new PA() {
                    @Override public boolean apply() {
                        for (int i = 0; i < GRID_CNT; i++) {
                            Ignite g = grid(i);

                            ClusterNodeLocalMap<String, Integer> nodeLoc = g.cluster().nodeLocalMap();

                            Integer depCnt = nodeLoc.get(DEPLOY_CNT_KEY);
                            Integer undepCnt = nodeLoc.get(UNDEPLOY_CNT_KEY);

                            if (depCnt == null || depCnt != fi)
                                return false;

                            if (undepCnt == null || undepCnt != fi)
                                return false;
                        }

                        return true;
                    }
                }, getTestTimeout()));
            }
        }
        finally {
            // Stop secondary nodes.
            for (int i = 0; i < GRID_CNT; i++)
                stopGrid(i);
        }
    }

    /**
     * Runs {@code SCAN} query with user resource injected into reducer.
     *
     * @param g Grid.
     * @throws Exception In case of error.
     */
    @SuppressWarnings("ConstantConditions")
    private void runQuery(Ignite g) throws Exception {
        GridCacheQuery<Map.Entry<Integer, Integer>> q = g.<Integer, Integer>cache(null).queries().createScanQuery(null);

        // We use external class loader here to guarantee that secondary nodes
        // won't load the reducer and user resource from each other.
        Class<?> redCls = extClsLdr.loadClass("org.gridgain.grid.tests.p2p.GridExternalCacheQueryReducerClosure");

        q.projection(g.cluster().forRemotes()).
            execute((IgniteReducer<Map.Entry<Integer, Integer>, Integer>)redCls.newInstance()).get();
    }
}
