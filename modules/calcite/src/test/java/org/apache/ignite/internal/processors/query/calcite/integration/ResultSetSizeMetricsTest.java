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
package org.apache.ignite.internal.processors.query.calcite.integration;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.MaxValueMetric;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.running.RunningQueryManager.SQL_USER_QUERIES_REG_NAME;

/**
 * Tests for result set size histogram and max result set size metrics.
 */
public class ResultSetSizeMetricsTest extends AbstractMultiEngineIntegrationTest {
    /** */
    private static final int FILL_SIZE = 1000;

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setIndexedTypes(Integer.class, Integer.class));

        return cfg;
    }

    /** */
    @Test
    public void testResultSetSizeMetrics() throws Exception {
        IgniteEx initNode = startGrids(nodeCount());

        IgniteCache<Integer, Integer> cache = initNode.cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < FILL_SIZE; i++)
            cache.put(i, i);

        // Execute simple queries with result set sizes: 0, 1, 5, 50, 500.
        for (int limit : new int[] {0, 1, 5, 50, 500})
            sql(initNode, "SELECT _key FROM \"" + DEFAULT_CACHE_NAME + "\".Integer WHERE _key < ?", limit);

        // Execute queries with aggregation (different reducers on h2) with result set sizes: 10, 100.
        for (int limit : new int[] {10, 100})
            sql(initNode, "SELECT DISTINCT _key FROM \"" + DEFAULT_CACHE_NAME + "\".Integer WHERE _key < ?", limit);

        // Verify histogram on the initiating node.
        // Bounds: {0, 1, 10, 100, 1_000, 10_000, 100_000, 1_000_000}
        // Bucket 0: x <= 0     -> 1 (size 0)
        // Bucket 1: x <= 1     -> 1 (size 1)
        // Bucket 2: x <= 10    -> 2 (sizes 5, 10)
        // Bucket 3: x <= 100   -> 2 (sizes 50, 100)
        // Bucket 4: x <= 1000  -> 1 (size 500)
        // Buckets 5-8          -> 0
        long[] values = resultSetSizeHistogram(initNode).value();

        assertEquals(1, values[0]);
        assertEquals(1, values[1]);
        assertEquals(2, values[2]);
        assertEquals(2, values[3]);
        assertEquals(1, values[4]);

        for (int i = 5; i < values.length; i++)
            assertEquals(0, values[i]);

        // Verify max value on the initiating node.
        assertEquals(500L, resultSetSizeMax(initNode).value());

        // Verify all other server nodes have zero metrics.
        for (int i = 0; i < nodeCount(); i++) {
            IgniteEx node = grid(i);

            if (node == initNode)
                continue;

            long[] nodeVals = resultSetSizeHistogram(node).value();

            for (long v : nodeVals)
                assertEquals(0, v);

            assertEquals("Expected max value 0 on node [" + node.name() + "]",
                0L, resultSetSizeMax(node).value());
        }
    }

    /** */
    private HistogramMetricImpl resultSetSizeHistogram(IgniteEx ignite) {
        MetricRegistryImpl mreg = ignite.context().metric().registry(SQL_USER_QUERIES_REG_NAME);

        HistogramMetricImpl hist = mreg.findMetric("resultSetSizeHistogram");

        assertNotNull(hist);

        return hist;
    }

    /** */
    private MaxValueMetric resultSetSizeMax(IgniteEx ignite) {
        MetricRegistryImpl mreg = ignite.context().metric().registry(SQL_USER_QUERIES_REG_NAME);

        MaxValueMetric max = mreg.findMetric("maxResultSetSize");

        assertNotNull(max);

        return max;
    }
}
