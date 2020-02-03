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

package org.apache.ignite.internal.processors.cache;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * This test checks metrics cacheSize.
 * <ul>
 * <li>Check TcpDiscoveryMetricsUpdateMessage serialization.</li>
 * <li>Check {@code cache.metrics().getCacheSize()} on each node.</li>
 * <li>Check sum {@code cache.localMetrics().getCacheSize()} of all nodes.</li>
 * </ul>
 */
public class CacheMetricsCacheSizeTest extends GridCommonAbstractTest {
    /** Grid count. */
    private static final int GRID_CNT = 3;

    /** Entities cnt. */
    private static final int ENTITIES_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setMetricsLogFrequency(5000);
        cfg.setMetricsUpdateFrequency(3000);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setStatisticsEnabled(true)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    @Test
    public void testCacheSize() throws Exception {
        startClientGrid(GRID_CNT);

        IgniteCache cacheNode0 = grid(0).cache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < ENTITIES_CNT; i++)
            cacheNode0.put("key-" + i, i);

        GridCacheContext cacheContext = ((GatewayProtectedCacheProxy)cacheNode0).context();

        CacheMetrics cacheMetric = new CacheMetricsSnapshotV2(new CacheMetricsImpl(cacheContext));

        long size = cacheMetric.getCacheSize();

        HashMap<Integer, CacheMetrics> cacheMetrics = new HashMap<>();

        cacheMetrics.put(1, cacheMetric);

        TcpDiscoveryMetricsUpdateMessage msg = new TcpDiscoveryMetricsUpdateMessage(UUID.randomUUID());

        msg.setCacheMetrics(UUID.randomUUID(), cacheMetrics);

        Marshaller marshaller = grid(0).context().config().getMarshaller();

        byte[] buffer = marshaller.marshal(msg);

        Object readObject = marshaller.unmarshal(buffer, getClass().getClassLoader());

        assertTrue(readObject instanceof TcpDiscoveryMetricsUpdateMessage);

        TcpDiscoveryMetricsUpdateMessage msg2 = (TcpDiscoveryMetricsUpdateMessage)readObject;

        Map<Integer, CacheMetrics> cacheMetrics2 = msg2.cacheMetrics().values().iterator().next();

        CacheMetrics cacheMetric2 = cacheMetrics2.values().iterator().next();

        assertEquals("TcpDiscoveryMetricsUpdateMessage serialization error, cacheSize is different", size, cacheMetric2.getCacheSize());

        IgniteCache cacheNode1 = grid(1).cache(DEFAULT_CACHE_NAME);

        IgniteCache cacheNode2 = grid(2).cache(DEFAULT_CACHE_NAME);

        IgniteCache cacheNode3 = grid(3).cache(DEFAULT_CACHE_NAME);

        awaitMetricsUpdate(1);

        assertEquals(ENTITIES_CNT, cacheNode0.metrics().getCacheSize());

        long sizeNode0 = cacheNode0.localMetrics().getCacheSize();

        assertEquals(ENTITIES_CNT, cacheNode1.metrics().getCacheSize());

        long sizeNode1 = cacheNode1.localMetrics().getCacheSize();

        assertEquals(ENTITIES_CNT, cacheNode2.metrics().getCacheSize());

        long sizeNode2 = cacheNode2.localMetrics().getCacheSize();

        assertEquals(ENTITIES_CNT, sizeNode0 + sizeNode1 + sizeNode2);

        //Client metrics
        assertEquals(ENTITIES_CNT, cacheNode3.metrics().getCacheSize());
    }
}
