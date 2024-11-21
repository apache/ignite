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

package org.apache.ignite.internal.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.client.thin.TcpClientCache;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.util.lang.ConsumerX;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.stream;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 * Test metrics of DR operations.
 */
@RunWith(Parameterized.class)
public class DataReplicationMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS_PUT_CNT = 100;

    /** */
    private static final int KEYS_REMOVE_CNT = 50;

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameter(1)
    public boolean async;

    /** */
    private IgniteEx ign;

    /** */
    @Parameterized.Parameters(name = "atomicityMode={0}, async={1}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode atomicityMode : CacheAtomicityMode.values()) {
            params.add(new Object[] {atomicityMode, true});
            params.add(new Object[] {atomicityMode, false});
        }

        return params;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setMetricsUpdateFrequency(10);

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setStatisticsEnabled(true)
                .setAtomicityMode(atomicityMode)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ign = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testConflict() throws Exception {
        IgniteEx cli = startClientGrid();

        IgniteInternalCache<Integer, Integer> cachex = cli.cachex(DEFAULT_CACHE_NAME);

        GridCacheVersion confl = new GridCacheVersion(1, 0, 1, (byte)2);
        GridCacheDrInfo val = new GridCacheDrInfo(new CacheObjectImpl(1, null), confl);

        IntFunction<KeyCacheObject> keyGen = i -> new KeyCacheObjectImpl(i, null, cachex.affinity().partition(0));

        updateCache(
            keyGen,
            val,
            confl,
            async ? m -> cachex.putAllConflictAsync(m).get() : cachex::putAllConflict,
            async ? m -> cachex.removeAllConflictAsync(m).get() : cachex::removeAllConflict
        );

        checkMetrics(cli);
    }

    /** */
    @Test
    public void testConflictThin() throws Exception {
        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER))) {
            TcpClientCache<Integer, Integer> cache = (TcpClientCache<Integer, Integer>)cli.<Integer, Integer>cache(DEFAULT_CACHE_NAME);

            GridCacheVersion confl = new GridCacheVersion(1, 1, 1, (byte)2);
            T3<Integer, GridCacheVersion, Long> val = new T3<>(1, confl, 0L);

            updateCache(
                i -> i,
                val,
                confl,
                async ? m -> cache.putAllConflictAsync(m).get() : cache::putAllConflict,
                async ? m -> cache.removeAllConflictAsync(m).get() : cache::removeAllConflict
            );

            checkMetrics(ign);
        }
    }

    /** */
    private <K, V, C> void updateCache(IntFunction<K> keyGen, V val, C confl, ConsumerX<Map<K, V>> putAction,
        ConsumerX<Map<K, C>> removeAction) throws Exception {
        Map<K, V> putMap = new HashMap<>();
        Map<K, C> removeMap = new HashMap<>();

        for (int i = 0; i < KEYS_PUT_CNT; i++) {
            K key = keyGen.apply(i);

            putMap.put(key, val);

            if (i < KEYS_REMOVE_CNT)
                removeMap.put(key, confl);
        }

        putAction.accept(putMap);
        removeAction.accept(removeMap);
    }

    /** */
    private void checkMetrics(IgniteEx ignFrom) throws IgniteInterruptedCheckedException {
        MetricRegistryImpl mreg = ignFrom.context().metric().registry(cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false));

        assertTrue(stream(mreg.<HistogramMetricImpl>findMetric("PutAllConflictTime").value()).sum() > 0);
        assertTrue(stream(mreg.<HistogramMetricImpl>findMetric("RemoveAllConflictTime").value()).sum() > 0);

        MetricRegistryImpl mregDest = ign.context().metric().registry(cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false));

        assertEquals(KEYS_PUT_CNT, mregDest.<LongMetric>findMetric("CachePuts").value());
        assertEquals(KEYS_REMOVE_CNT, mregDest.<LongMetric>findMetric("CacheRemovals").value());
    }
}
