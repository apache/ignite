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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObjectImpl;
import org.apache.ignite.internal.processors.cache.dr.GridCacheDrInfo;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 * {@link IgniteInternalCache#putAllConflict(Map)} and {@link IgniteInternalCache#removeAllConflict(Map)} cache
 * operations test.
 * @see IgniteInternalCache#putAllConflict(Map)
 * @see IgniteInternalCache#removeAllConflict(Map)
 */
public class CacheMetricsConflictOperationsTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS_CNT = 100;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);
        cfg.setMetricsUpdateFrequency(10);

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setStatisticsEnabled(true)
        );

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            /** {@inheritDoc} */
            @Override public String name() {
                return "ConflictResolverProvider";
            }

            /** {@inheritDoc} */
            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (cls != CacheConflictResolutionManager.class)
                    return null;

                return (T)new AlwaysNewResolutionManager<>();
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testCacheConflictResolver() throws Exception {
        try (IgniteEx ign = startGrid(0); IgniteEx cli = startClientGrid()) {
            ign.cluster().state(ACTIVE);

            ign.cache(DEFAULT_CACHE_NAME).enableStatistics(true);
            cli.cache(DEFAULT_CACHE_NAME).enableStatistics(true);

            IgniteInternalCache<Integer, Integer> cachex = cli.cachex(DEFAULT_CACHE_NAME);

            putAndRemoveDataWithConflict(cachex, KEYS_CNT, false);
            checkMetrics(cli);

            putAndRemoveDataWithConflict(cachex, KEYS_CNT, true);
            checkMetrics(cli);
        }
    }

    private void putAndRemoveDataWithConflict(IgniteInternalCache<Integer, Integer> cachex, int cnt, boolean async) throws IgniteCheckedException {
        Map<KeyCacheObject, GridCacheDrInfo> drMapPuts = new HashMap<>();
        Map<KeyCacheObject, GridCacheVersion> drMapRemoves = new HashMap<>();

        KeyCacheObject key;

        CacheObject val = new CacheObjectImpl(1, null);
        val.prepareMarshal(cachex.context().cacheObjectContext());

        GridCacheVersion conflict = new GridCacheVersion(1, 0, 1, (byte)2);

        for (int i = 0; i < cnt; ++i) {
            key = new KeyCacheObjectImpl(i, null, cachex.affinity().partition(0));

            drMapPuts.put(key, new GridCacheDrInfo(val, conflict));
            drMapRemoves.put(key, conflict);
        }

        if (async) {
            cachex.putAllConflictAsync(drMapPuts).get();
            cachex.removeAllConflictAsync(drMapRemoves).get();
        }
        else {
            cachex.putAllConflict(drMapPuts);
            cachex.removeAllConflict(drMapRemoves);
        }
    }

    /** */
    private void checkMetrics(IgniteEx ign) {
        MetricRegistryImpl mreg = ign.context().metric().registry(cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false));

        log.info("PutAllConflictTimeTotal: " + mreg.<LongMetric>findMetric("PutAllConflictTimeTotal").value());
        log.info("RemoveAllConflictTimeTotal: " + mreg.<LongMetric>findMetric("RemoveAllConflictTimeTotal").value());

//        assertTrue(mreg.<LongMetric>findMetric("PutAllConflictTimeTotal").value() > 0);
//        assertTrue(mreg.<LongMetric>findMetric("RemoveAllConflictTimeTotal").value() > 0);
//
//        assertTrue(stream(mreg.<HistogramMetricImpl>findMetric("RemoveAllConflictTimeTotal").value()).sum() > 0);
//        assertTrue(stream(mreg.<HistogramMetricImpl>findMetric("PutAllConflictTimeTotal").value()).sum() > 0);
    }

    /** */
    private static class AlwaysNewResolutionManager<K, V>
        extends GridCacheManagerAdapter<K, V> implements CacheConflictResolutionManager<K, V> {
        /** */
        private final CacheVersionConflictResolver rslv;

        /** */
        AlwaysNewResolutionManager() {
            rslv = new CacheVersionConflictResolver() {
                @Override public <K1, V1> GridCacheVersionConflictContext<K1, V1> resolve(
                    CacheObjectValueContext ctx,
                    GridCacheVersionedEntryEx<K1, V1> oldEntry,
                    GridCacheVersionedEntryEx<K1, V1> newEntry,
                    boolean atomicVerComparator
                ) {
                    GridCacheVersionConflictContext<K1, V1> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

                    res.useNew();

                    return res;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return rslv;
        }
    }
}
