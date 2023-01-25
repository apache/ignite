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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.metric.LongMetric;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.GridMetricManager.CLIENT_CONNECTOR_METRICS;

/**
 * In the test, methods are checked that relate to
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheKeyRequest}, such as:
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheClearKeyRequest}
 * * {@link ClientCache#clear(java.lang.Object)}
 * * {@link ClientCache#clearAsync(java.lang.Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheContainsKeyRequest}
 * * {@link ClientCache#containsKey(java.lang.Object)}
 * * {@link ClientCache#containsKeyAsync(java.lang.Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndPutIfAbsentRequest}
 * * {@link ClientCache#getAndPutIfAbsent(java.lang.Object, java.lang.Object)}
 * * {@link ClientCache#getAndPutIfAbsentAsync(java.lang.Object, java.lang.Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndPutRequest}
 * * {@link ClientCache#getAndPut(Object, Object)}
 * * {@link ClientCache#getAndPutAsync(Object, Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndRemoveRequest}
 * * {@link ClientCache#getAndRemove(Object)}
 * * {@link ClientCache#getAndRemoveAsync(Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetAndReplaceRequest}
 * * {@link ClientCache#getAndReplace(Object, Object)}
 * * {@link ClientCache#getAndReplaceAsync(Object, Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheGetRequest}
 * * {@link ClientCache#get(java.lang.Object)}
 * * {@link ClientCache#getAsync(java.lang.Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutIfAbsentRequest}
 * * {@link ClientCache#putIfAbsent(Object, Object)}
 * * {@link ClientCache#putIfAbsentAsync(Object, Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCachePutRequest}
 * * {@link ClientCache#put(Object, Object)}
 * * {@link ClientCache#putAsync(Object, Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveIfEqualsRequest}
 * * {@link ClientCache#remove(Object, Object)}
 * * {@link ClientCache#removeAsync(Object, Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheRemoveKeyRequest}
 * * {@link ClientCache#remove(Object)}
 * * {@link ClientCache#removeAsync(Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheReplaceIfEqualsRequest
 * * {@link ClientCache#replace(Object, Object, Object)}
 * * {@link ClientCache#replaceAsync(Object, Object, Object)}
 * <p>
 * {@link org.apache.ignite.internal.processors.platform.client.cache.ClientCacheReplaceRequest
 * * {@link ClientCache#replace(Object, Object)}
 * * {@link ClientCache#replaceAsync(Object, Object)}
 * <p>
 */
public class ThinClientAwarenessMetricsTest extends AbstractThinClientTest {
    /**
     * Number of server nodes
     */
    private static final int SERVERS_CNT = 3;

    /**
     * Number of entries
     */
    private static final int ENTRIES_CNT = 1_000;

    /**
     * Thin client instance with awareness
     */
    private static IgniteClient igniteClientWithAwareness;

    /**
     * Server ignite intance
     */
    private static Ignite server;

    /**
     * Server ignite cache intance
     */
    private static IgniteCache<Object, Object> serverCache;

    /**
     * Thin client ignite cache intance
     */
    private static ClientCache<Object, Object> awarenessClientCache;

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        server = startGrids(SERVERS_CNT);
        serverCache = server.getOrCreateCache(new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setStatisticsEnabled(true));

        igniteClientWithAwareness = Ignition.startClient(new ClientConfiguration()
            .setPartitionAwarenessEnabled(true)
            .setAddresses("127.0.0.1"));

        awarenessClientCache = igniteClientWithAwareness.cache(DEFAULT_CACHE_NAME);

    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() throws Exception {
        Ignition.allGrids().stream().forEach(e -> {
            e.cache(DEFAULT_CACHE_NAME).clearStatistics();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void afterTest() throws Exception {
        awarenessClientCache.clear();
    }

    /**
     * Check {@link ClientCache#get(Object)}
     */
    @Test
    public void testGet() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.get(i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAsync(Object)}
     */
    @Test
    public void testGetAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAsync(i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#replace(Object, Object)}
     */
    @Test
    public void testReplace() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.replace(i, i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#replaceAsync(Object, Object)}
     */
    @Test
    public void testReplaceAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.replaceAsync(i, i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#replace(Object, Object, Object)}
     */
    @Test
    public void testReplaceIfEquals() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.replace(i, i, i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#replaceAsync(Object, Object, Object)}
     */
    @Test
    public void testReplaceIfEqualsAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.replaceAsync(i, i, i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#containsKey(Object)}
     */
    @Test
    public void testContainsKey() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.containsKey(i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#containsKeyAsync(Object)}
     */
    @Test
    public void testContainsKeyAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.containsKeyAsync(i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#clear(Object)}
     */
    @Test
    public void testClear() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.clear(i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#clearAsync(java.lang.Object)}
     */
    @Test
    public void testClearAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.clearAsync(i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAndPutIfAbsent(Object, Object)}
     */
    @Test
    public void testGetAndPutIfAbsent() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAndPutIfAbsent(i, i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAndPutIfAbsentAsync(Object, Object)}
     */
    @Test
    public void testGetAndPutIfAbsentAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAndPutIfAbsentAsync(i, i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAndPut(Object, Object)}
     */
    @Test
    public void testGetAndPut() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAndPut(i, i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAndPutAsync(Object, Object)}
     */
    @Test
    public void testGetAndPutAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAndPutAsync(i, i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAndReplace(Object, Object)}
     */
    @Test
    public void testGetAndReplace() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAndReplace(i, i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAndReplaceAsync(Object, Object)}
     */
    @Test
    public void testGetAndReplaceAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAndReplaceAsync(i, i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#put(Object, Object)}
     */
    @Test
    public void testPut() throws Exception {

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.put(i, i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#putAsync(Object, Object)}
     */
    @Test
    public void testPuteAsync() throws Exception {

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.putAsync(i, i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#putIfAbsent(Object, Object)}
     */
    @Test
    public void testPutIfAbsent() throws Exception {

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.putIfAbsent(i, i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#putIfAbsentAsync(Object, Object)}
     */
    @Test
    public void testPutIfAbsentAsync() throws Exception {

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.putIfAbsentAsync(i, i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAndRemove(Object)}
     */
    @Test
    public void testGetAndRemove() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAndRemove(i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#getAndRemoveAsync(Object)}
     */
    @Test
    public void testGetAndRemoveAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.getAndRemoveAsync(i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#remove(Object)}
     */
    @Test
    public void testRemove() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.remove(i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#removeAsync(Object)}
     */
    @Test
    public void testRemoveAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.removeAsync(i).get();
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#remove(Object, Object)}
     */
    @Test
    public void testRemoveIfEquals() {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.remove(i, i);
        }

        checkMetrics();
    }

    /**
     * Check {@link ClientCache#removeAsync(Object, Object)}
     */
    @Test
    public void testRemoveIfEqualsAsync() throws Exception {
        fillData();

        for (int i = 0; i < ENTRIES_CNT; i++) {
            awarenessClientCache.removeAsync(i, i).get();
        }

        checkMetrics();
    }

    /**
     * Check ScanQuery
     */
    @Test
    public void scanQuery() {
        fillData();

        GridCacheContext<Object, Object> cacheCtx = ((IgniteEx)server).cachex(DEFAULT_CACHE_NAME).context();
        AffinityFunction aff = cacheCtx.config().getAffinity();

        for (int i = 0; i < aff.partitions(); i++) {
            awarenessClientCache.query(new ScanQuery<>().setPartition(i)).getAll();
        }

        checkMetrics();
    }

    /**
     * Check metrics for tested cache
     */

    private void checkMetrics() {
        long hit = 0;
        long miss = 0;
        for (IgniteEx ignite : F.transform(G.allGrids(), ignite -> (IgniteEx)ignite)) {

            MetricRegistry mreg = ignite.context().metric().registry(CLIENT_CONNECTOR_METRICS);

            LongMetric metricHit = mreg.<LongMetric>findMetric(ClientListenerProcessor.AWARENESS_HIT);
            LongMetric metricMiss = mreg.<LongMetric>findMetric(ClientListenerProcessor.AWARENESS_MISS);

            hit += metricHit.value();
            miss += metricMiss.value();

            metricHit.reset();
            metricMiss.reset();
        }

        Assert.assertEquals("The number of misses is expected to be 0 but we have total hits/miss " +
            hit + "/" + miss + ", total keys " + ENTRIES_CNT, 0, miss);
        Assert.assertEquals("The number of hits must equal the number of records, total hits/miss " +
            hit + "/" + miss + ", total keys " + ENTRIES_CNT, ENTRIES_CNT, hit);
    }

    /**
     * Fill data
     */
    public static void fillData() {
        for (int i = 0; i < ENTRIES_CNT; i++) {
            serverCache.put(i, i);
        }
    }
}
