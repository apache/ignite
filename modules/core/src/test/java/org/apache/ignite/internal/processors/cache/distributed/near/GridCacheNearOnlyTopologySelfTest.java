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

import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Near-only cache node startup test.
 */
public class GridCacheNearOnlyTopologySelfTest extends GridCommonAbstractTest {
    /** Use cache flag. */
    private boolean cache = true;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(MvccFeatureChecker.Feature.NEAR_CACHE);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (cache) {
            CacheConfiguration cacheCfg = defaultCacheConfiguration();

            cacheCfg.setCacheMode(PARTITIONED);
            cacheCfg.setBackups(1);
            cacheCfg.setRebalanceMode(SYNC);
            cacheCfg.setAtomicityMode(TRANSACTIONAL);

            cfg.setCacheConfiguration(cacheCfg);
        }

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);

        return cfg;
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartupFirstOneNode() throws Exception {
        checkStartupNearNode(0, 2);
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartupLastOneNode() throws Exception {
        checkStartupNearNode(1, 2);
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartupFirstTwoNodes() throws Exception {
        checkStartupNearNode(0, 3);
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartupInMiddleTwoNodes() throws Exception {
        checkStartupNearNode(1, 3);
    }

    /** @throws Exception If failed. */
    @Test
    public void testStartupLastTwoNodes() throws Exception {
        checkStartupNearNode(2, 3);
    }

    /** @throws Exception If failed. */
    @Test
    public void testKeyMapping() throws Exception {
        try {
            cache = true;

            for (int i = 0; i < 4; i++) {
                boolean client = i == 0;

                Ignite ignite = client ? startClientGrid(i) : startGrid(i);

                if (client)
                    ignite.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration());
            }

            for (int i = 0; i < 100; i++)
                assertFalse("For key: " + i, grid(0).affinity(DEFAULT_CACHE_NAME).isPrimaryOrBackup(grid(0).localNode(), i));
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testKeyMappingOnComputeNode() throws Exception {
        try {
            cache = true;

            for (int i = 0; i < 4; i++) {
                boolean client = i == 0;

                Ignite ignite = client ? startClientGrid(i) : startGrid(i);

                if (client)
                    ignite.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration());
            }

            cache = false;

            Ignite compute = startClientGrid(4);

            for (int i = 0; i < 100; i++) {
                ClusterNode node = compute.affinity(DEFAULT_CACHE_NAME).mapKeyToNode(i);

                assertFalse("For key: " + i, node.id().equals(compute.cluster().localNode().id()));
                assertFalse("For key: " + i, node.id().equals(grid(0).localNode().id()));
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testNodeLeave() throws Exception {
        try {
            cache = true;

            for (int i = 0; i < 2; i++) {
                boolean client = i == 0;

                Ignite ignite = client ? startClientGrid(i) : startGrid(i);

                if (client)
                    ignite.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration<>());
            }

            awaitPartitionMapExchange();

            for (int i = 0; i < 10; i++)
                grid(1).cache(DEFAULT_CACHE_NAME).put(i, i);

            final Ignite igniteNearOnly = grid(0);
            final IgniteCache<Object, Object> nearOnly = igniteNearOnly.cache(DEFAULT_CACHE_NAME);

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
                boolean client = nearNodeIdx == i;

                Ignite ignite = client ? startClientGrid(i) : startGrid(i);

                if (client)
                    ignite.createNearCache(DEFAULT_CACHE_NAME, new NearCacheConfiguration());
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
