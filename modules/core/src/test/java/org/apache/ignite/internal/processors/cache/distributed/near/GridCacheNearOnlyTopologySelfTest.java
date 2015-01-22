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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.GridCacheAtomicityMode.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheDistributionMode.*;
import static org.apache.ignite.cache.GridCachePreloadMode.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Near-only cache node startup test.
 */
public class GridCacheNearOnlyTopologySelfTest extends GridCommonAbstractTest {
    /** Shared ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Near only flag. */
    private boolean nearOnly;

    /** Use cache flag. */
    private boolean cache = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cache) {
            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setBackups(1);
            cacheCfg.setDistributionMode(nearOnly ? NEAR_ONLY : NEAR_PARTITIONED);
            cacheCfg.setPreloadMode(SYNC);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            cfg.setCacheConfiguration(cacheCfg);
        }

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        return cfg;
    }

    /** @throws Exception If failed. */
    public void testStartupFirstOneNode() throws Exception {
        checkStartupNearNode(0, 2);
    }

    /** @throws Exception If failed. */
    public void testStartupLastOneNode() throws Exception {
        checkStartupNearNode(1, 2);
    }

    /** @throws Exception If failed. */
    public void testStartupFirstTwoNodes() throws Exception {
        checkStartupNearNode(0, 3);
    }

    /** @throws Exception If failed. */
    public void testStartupInMiddleTwoNodes() throws Exception {
        checkStartupNearNode(1, 3);
    }

    /** @throws Exception If failed. */
    public void testStartupLastTwoNodes() throws Exception {
        checkStartupNearNode(2, 3);
    }

    /** @throws Exception If failed. */
    public void testKeyMapping() throws Exception {
        try {
            cache = true;

            for (int i = 0; i < 4; i++) {
                nearOnly = i == 0;

                startGrid(i);
            }

            for (int i = 0; i < 100; i++)
                assertFalse("For key: " + i, grid(0).cache(null).affinity().isPrimaryOrBackup(grid(0).localNode(), i));
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testKeyMappingOnComputeNode() throws Exception {
        try {
            cache = true;

            for (int i = 0; i < 4; i++) {
                nearOnly = i == 0;

                startGrid(i);
            }

            cache = false;

            Ignite compute = startGrid(4);

            for (int i = 0; i < 100; i++) {
                ClusterNode node = compute.cluster().mapKeyToNode(null, i);

                assertFalse("For key: " + i, node.id().equals(compute.cluster().localNode().id()));
                assertFalse("For key: " + i, node.id().equals(grid(0).localNode().id()));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    public void testNodeLeave() throws Exception {
        try {
            cache = true;

            for (int i = 0; i < 2; i++) {
                nearOnly = i == 0;

                startGrid(i);
            }

            for (int i = 0; i < 10; i++)
                grid(1).cache(null).put(i, i);

            final GridCache<Object, Object> nearOnly = grid(0).cache(null);

            // Populate near cache.
            for (int i = 0; i < 10; i++) {
                assertEquals(i, nearOnly.get(i));
                assertEquals(i, nearOnly.peek(i));
            }

            // Stop the only dht node.
            stopGrid(1);

            for (int i = 0; i < 10; i++) {
                assertNull(nearOnly.peek(i));

                final int key = i;

                GridTestUtils.assertThrows(log, new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return nearOnly.get(key);
                    }
                }, ClusterTopologyException.class, null);
            }

            // Test optimistic transaction.
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (IgniteTx tx = nearOnly.txStart(OPTIMISTIC, REPEATABLE_READ)) {
                        nearOnly.putx("key", "val");

                        tx.commit();
                    }

                    return null;
                }
            }, ClusterTopologyException.class, null);

            // Test pessimistic transaction.
            GridTestUtils.assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (IgniteTx tx = nearOnly.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        nearOnly.put("key", "val");

                        tx.commit();
                    }

                    return null;
                }
            }, ClusterTopologyException.class, null);

        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    private void checkStartupNearNode(int nearNodeIdx, int totalNodeCnt) throws Exception {
        try {
            cache = true;

            for (int i = 0; i < totalNodeCnt; i++) {
                nearOnly = nearNodeIdx == i;

                startGrid(i);
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
