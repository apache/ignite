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

package org.apache.ignite.internal.processors.query.h2.twostep;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.IoStatisticsHolder;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.util.PageHandler;
import org.apache.ignite.spi.metric.LongMetric;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.twostep.GridMapQueryExecutor.TREE_CORRUPTION_REG_NAME;

/**
 * Tests that the B+Tree cursor corruption metric is properly registered and incremented.
 */
public class TreeCorruptionMetricTest extends AbstractIndexingCommonTest {
    /** */
    private static final String IDX_NAME = "CORRUPT_IDX";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        BPlusTree.testHndWrapper = (tree, hnd) -> {
            if (hnd instanceof BPlusTree.Search) {
                PageHandler<Object, BPlusTree.Result> delegate = (PageHandler<Object, BPlusTree.Result>)hnd;

                return new PageHandler<>() {
                    @Override public BPlusTree.Result run(
                            int cacheId,
                            long pageId,
                            long page,
                            long pageAddr,
                            PageIO io,
                            Boolean walPlc,
                            Object arg,
                            int intArg,
                            IoStatisticsHolder statHolder
                    ) throws IgniteCheckedException {
                        BPlusTree.Result res = delegate.run(
                                cacheId, pageId, page, pageAddr, io, walPlc, arg, intArg, statHolder);

                        if (tree.name().contains(IDX_NAME))
                            throw new RuntimeException("simulated B+Tree corruption");

                        return res;
                    }
                };
            }
            else
                return hnd;
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        BPlusTree.testHndWrapper = null;

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(4))
        );

        return cfg;
    }

    /**
     * Verifies that the treeCorruption metric is registered and starts at 0.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetricRegistered() throws Exception {
        IgniteEx ignite = startGrid(0);

        LongMetric m = treeCorruptionMetric(ignite);

        assertNotNull(m);
        assertEquals(0, m.value());
    }

    /**
     * Verifies that the treeCorruption metric is incremented when a query fails
     * due to B+Tree cursor corruption.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMetricIncrementedOnCorruption() throws Exception {
        IgniteEx node0 = startGrid(0);
        IgniteEx node1 = startGrid(1);

        node0.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cache = node0.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.query(new SqlFieldsQuery(
            "CREATE TABLE corrupt_tbl (id INT PRIMARY KEY, val INT) WITH \"TEMPLATE=" + DEFAULT_CACHE_NAME + "\""));
        cache.query(new SqlFieldsQuery("CREATE INDEX " + IDX_NAME + " ON corrupt_tbl(val)"));
        cache.query(new SqlFieldsQuery("INSERT INTO corrupt_tbl VALUES (1, 100)"));

        // Metric should be 0 before the failing query.
        assertEquals(0, treeCorruptionMetric(node0).value());

        try {
            node1.cache(DEFAULT_CACHE_NAME).query(new SqlFieldsQuery("SELECT * FROM corrupt_tbl WHERE val = 100"));
        }
        catch (Exception ignored) {
            // Expected - query fails due to simulated corruption.
        }

        assertEquals(1, treeCorruptionMetric(node0).value());
    }

    /**
     * @param ignite Ignite instance.
     * @return Tree corruption counter metric.
     */
    private static LongMetric treeCorruptionMetric(IgniteEx ignite) {
        return ignite.context().metric().registry(TREE_CORRUPTION_REG_NAME).findMetric("count");
    }
}
