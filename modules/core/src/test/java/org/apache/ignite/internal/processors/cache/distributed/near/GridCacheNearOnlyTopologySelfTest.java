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
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.apache.ignite.transactions.*;

import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheRebalanceMode.*;
import static org.apache.ignite.transactions.TransactionConcurrency.*;
import static org.apache.ignite.transactions.TransactionIsolation.*;

/**
 * Near-only cache node startup test.
 */
public class GridCacheNearOnlyTopologySelfTest extends GridCommonAbstractTest {
    /** Shared ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Near only flag. */
    private boolean cilent;

    /** Use cache flag. */
    private boolean cache = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (cilent)
            cfg.setClientMode(true);

        if (cache) {
            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setBackups(1);
            cacheCfg.setRebalanceMode(SYNC);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            cfg.setCacheConfiguration(cacheCfg);
        }

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setForceServerMode(true);
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
                cilent = i == 0;

                Ignite ignite = startGrid(i);

                if (cilent)
                    ignite.createNearCache(null, new NearCacheConfiguration());
            }

            for (int i = 0; i < 100; i++)
                assertFalse("For key: " + i, grid(0).affinity(null).isPrimaryOrBackup(grid(0).localNode(), i));
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
                cilent = i == 0;

                Ignite ignite = startGrid(i);

                if (cilent)
                    ignite.createNearCache(null, new NearCacheConfiguration());
            }

            cache = false;
            cilent = true;

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
                cilent = i == 0;

                Ignite ignite = startGrid(i);

                if (cilent)
                    ignite.createNearCache(null, new NearCacheConfiguration());
            }

            for (int i = 0; i < 10; i++)
                grid(1).cache(null).put(i, i);

            final Ignite igniteNearOnly = grid(0);
            final IgniteCache<Object, Object> nearOnly = igniteNearOnly.cache(null);

            // Populate near cache.
            for (int i = 0; i < 10; i++) {
                assertEquals(i, nearOnly.get(i));
                assertEquals(i, nearOnly.localPeek(i, CachePeekMode.ONHEAP));
            }

            // Stop the only dht node.
            stopGrid(1);

            for (int i = 0; i < 10; i++) {
                assertNull(nearOnly.localPeek(i, CachePeekMode.ONHEAP));

                final int key = i;

                GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        return nearOnly.get(key);
                    }
                }, ClusterTopologyCheckedException.class);
            }

            // Test optimistic transaction.
            GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Transaction tx = igniteNearOnly.transactions().txStart(OPTIMISTIC, REPEATABLE_READ)) {
                        nearOnly.put("key", "val");

                        tx.commit();
                    }

                    return null;
                }
            }, ClusterTopologyCheckedException.class);

            // Test pessimistic transaction.
            GridTestUtils.assertThrowsWithCause(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    try (Transaction tx = igniteNearOnly.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        nearOnly.put("key", "val");

                        tx.commit();
                    }

                    return null;
                }
            }, ClusterTopologyCheckedException.class);

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
                cilent = nearNodeIdx == i;

                Ignite ignite = startGrid(i);

                if (cilent)
                    ignite.createNearCache(null, new NearCacheConfiguration());
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
