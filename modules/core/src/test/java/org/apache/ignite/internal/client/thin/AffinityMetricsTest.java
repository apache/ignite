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

package org.apache.ignite.internal.client.thin;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientTransaction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.metric.MetricRegistry;
import org.apache.ignite.spi.metric.LongMetric;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.AFF_KEY_HITS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.AFF_KEY_MISSES;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.AFF_QRY_HITS;
import static org.apache.ignite.internal.processors.odbc.ClientListenerMetrics.AFF_QRY_MISSES;

/**
 * Test thin client affinity hits/misses metrics.
 */
public class AffinityMetricsTest extends ThinClientAbstractPartitionAwarenessTest {
    /** Grids count. */
    private static final int GRIDS_CNT = 3;

    /** */
    private static final String PART_CACHE = "partCache";

    /** */
    private static final String REPL_CACHE = "replCache";

    /** */
    private static final String[] ALL_AFF_METRICS =
        new String[] {AFF_KEY_HITS, AFF_KEY_MISSES, AFF_QRY_HITS, AFF_QRY_MISSES};

    /** */
    private final Map<String, Long> lastMetricValues = new HashMap<>();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(GRIDS_CNT);

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        initClient(getClientConfiguration(0, 1).setClusterDiscoveryEnabled(false), 0, 1);

        super.beforeTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(
            new CacheConfiguration<>(PART_CACHE).setCacheMode(PARTITIONED).setAtomicityMode(TRANSACTIONAL)
                .setStatisticsEnabled(true),
            new CacheConfiguration<>(REPL_CACHE).setCacheMode(REPLICATED).setAtomicityMode(TRANSACTIONAL)
                .setStatisticsEnabled(true)
        );
    }

    /** */
    @Test
    public void testCacheKeyAffinityMetricsPartitioned() {
        resetMetricValues();

        Integer affKey0 = primaryKey(ignite(0).cache(PART_CACHE));
        Integer affKey1 = primaryKey(ignite(1).cache(PART_CACHE));
        Integer affKey2 = primaryKey(ignite(2).cache(PART_CACHE));
        ClientCache<Integer, Integer> cache = client.cache(PART_CACHE);

        cache.put(affKey0, affKey0);

        assertEquals(1, calcMetricIncrement(ignite(0), AFF_KEY_HITS));

        cache.put(affKey1, affKey1);

        assertEquals(1, calcMetricIncrement(ignite(1), AFF_KEY_HITS));

        cache.put(affKey2, affKey2);

        assertEquals(1, calcMetricIncrement(ignite(0), AFF_KEY_MISSES) +
            calcMetricIncrement(ignite(1), AFF_KEY_MISSES));
    }

    /** */
    @Test
    public void testCacheKeyAffinityMetricsReplicated() {
        resetMetricValues();

        ClientCache<Integer, Integer> cache = client.cache(REPL_CACHE);

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        for (Ignite ignite : G.allGrids()) {
            for (String metricName : ALL_AFF_METRICS)
                assertEquals(0, calcMetricIncrement(ignite, metricName));
        }
    }

    /** */
    @Test
    public void testCacheKeyAffinityMetricsTx() {
        resetMetricValues();

        for (int i = 0; i < 100; i++) {
            try (ClientTransaction tx = client.transactions().txStart()) {
                client.cache(PART_CACHE).put(i, i);
                tx.commit();
            }
        }

        for (Ignite ignite : G.allGrids()) {
            for (String metricName : ALL_AFF_METRICS)
                assertEquals(0, calcMetricIncrement(ignite, metricName));
        }
    }

    /** */
    @Test
    public void testQueryAffinityMetricsPartitioned() {
        resetMetricValues();

        Integer affKey0 = primaryKey(ignite(0).cache(PART_CACHE));
        Integer affKey1 = primaryKey(ignite(1).cache(PART_CACHE));
        Integer affKey2 = primaryKey(ignite(2).cache(PART_CACHE));

        Affinity<Integer> aff = affinity(ignite(0).cache(PART_CACHE));
        int part0 = aff.partition(affKey0);
        int part1 = aff.partition(affKey1);
        int part2 = aff.partition(affKey2);

        ClientCache<Integer, Integer> cache = client.cache(PART_CACHE);

        cache.query(new ScanQuery<>().setPartition(part0)).getAll();

        assertEquals(1, calcMetricIncrement(ignite(0), AFF_QRY_HITS));

        cache.query(new ScanQuery<>().setPartition(part1)).getAll();

        assertEquals(1, calcMetricIncrement(ignite(1), AFF_QRY_HITS));

        cache.query(new ScanQuery<>().setPartition(part2)).getAll();

        assertEquals(1, calcMetricIncrement(ignite(0), AFF_QRY_MISSES) +
            calcMetricIncrement(ignite(1), AFF_QRY_MISSES));
    }

    /** */
    @Test
    public void testQueryAffinityMetricsReplicated() {
        resetMetricValues();

        ClientCache<Integer, Integer> cache = client.cache(REPL_CACHE);

        for (int i = 0; i < 10; i++)
            cache.query(new ScanQuery<>().setPartition(i)).getAll();

        for (Ignite ignite : G.allGrids()) {
            for (String metricName : ALL_AFF_METRICS)
                assertEquals(0, calcMetricIncrement(ignite, metricName));
        }
    }

    /** */
    private void resetMetricValues() {
        lastMetricValues.clear();

        for (Ignite ignite : G.allGrids()) {
            for (String metricName : ALL_AFF_METRICS)
                calcMetricIncrement(ignite, metricName);
        }
    }

    /** */
    private long calcMetricIncrement(Ignite ignite, String metricName) {
        MetricRegistry mreg = ((IgniteEx)ignite).context().metric().registry(GridMetricManager.CLIENT_CONNECTOR_METRICS);
        LongMetric metric = mreg.findMetric(metricName);
        long newVal = metric.value();
        Long oldVal = lastMetricValues.put(ignite.name() + '.' + metricName, newVal);

        return newVal - (oldVal == null ? 0 : oldVal);
    }
}
