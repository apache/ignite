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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.TcpClientCache;
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
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.stream;
import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/**
 * {@link IgniteInternalCache#putAllConflict(Map)} and {@link IgniteInternalCache#removeAllConflict(Map)} cache
 * operations test.
 * @see IgniteInternalCache#putAllConflict(Map)
 * @see IgniteInternalCache#removeAllConflict(Map)
 */
@RunWith(Parameterized.class)
public class CacheMetricsConflictOperationsTest extends GridCommonAbstractTest {
    /** */
    private static final int KEYS_CNT = 100;

    /** */
    private static final int CLIENT_CONNECTOR_PORT = 10800;

    /** */
    @Parameterized.Parameter
    public CacheAtomicityMode atomicityMode;

    /** */
    @Parameterized.Parameters(name = "atomicityMode={0}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (CacheAtomicityMode atomicityMode : Arrays.asList(ATOMIC, TRANSACTIONAL))
            params.add(new Object[] {atomicityMode});

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

        cfg.setClientConnectorConfiguration(
            new ClientConnectorConfiguration()
                .setPort(CLIENT_CONNECTOR_PORT)
        );

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** Checks metrics with thick client. */
    @Test
    public void testCacheMetricsConflictOperationsThick() throws Exception {
        try (IgniteEx ign = startGrid(0); IgniteEx cli = startClientGrid()) {
            ign.cluster().state(ACTIVE);

            ign.cache(DEFAULT_CACHE_NAME).enableStatistics(true);
            cli.cache(DEFAULT_CACHE_NAME).enableStatistics(true);

            IgniteInternalCache<Integer, Integer> cachex = cli.cachex(DEFAULT_CACHE_NAME).keepBinary();

            updateCacheFromThick(cachex, KEYS_CNT, false);
            checkMetrics(cli);

            updateCacheFromThick(cachex, KEYS_CNT, true);
            checkMetrics(cli);
        }
    }

    /** Checks metrics with thin client. */
    @Test
    public void testCacheMetricsConflictOperationsThin() throws Exception {
        try (IgniteEx ign = startGrid(0); IgniteClient cli = Ignition.startClient(getClientConfiguration())) {
            ign.cluster().state(ACTIVE);

            ign.cache(DEFAULT_CACHE_NAME).enableStatistics(true);

            TcpClientCache<Object, Object> cache = (TcpClientCache<Object, Object>)cli.cache(DEFAULT_CACHE_NAME).withKeepBinary();

            updateCacheFromThin(cache, KEYS_CNT, false);
            checkMetrics(ign);

            updateCacheFromThin(cache, KEYS_CNT, true);
            checkMetrics(ign);
        }
    }

    /** Performs {@link IgniteInternalCache#putAllConflict(Map)} and {@link IgniteInternalCache#removeAllConflict(Map)}
     * operation on a given cache.
     * @param cachex - {@link IgniteInternalCache} cache instance.
     * @param cnt - number of entities to put/remove
     * @param async - {@link Boolean} flag, indicating asynchonous operations proccessing.
     * */
    private void updateCacheFromThick(IgniteInternalCache<Integer, Integer> cachex, int cnt, boolean async) throws IgniteCheckedException {
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

    /** Performs {@link TcpClientCache#putAllConflict(Map)} and {@link TcpClientCache#removeAllConflict(Map)} operation on a given cache.
     * @param cache - {@link TcpClientCache} cache instance.
     * @param cnt - number of entities to put/remove
     * @param async - {@link Boolean} flag, indicating asynchonous operations proccessing.
     * */
    private void updateCacheFromThin(TcpClientCache<Object, Object> cache, int cnt, boolean async) {
        Map<Object, T3<?, GridCacheVersion, Long>> drMapPuts = new HashMap<>();
        Map<Object, GridCacheVersion> drMapRemoves = new HashMap<>();

        GridCacheVersion conflict = new GridCacheVersion(1, 1, 1, (byte)2);
        T3<?, GridCacheVersion, Long> val = new T3<>(1, conflict, 0L);

        for (int i = 0; i < cnt; ++i) {
            drMapPuts.put(i, val);
            drMapRemoves.put(i, conflict);
        }

        if (async) {
            try {
                cache.putAllConflictAsync(drMapPuts).get();
                cache.removeAllConflictAsync(drMapRemoves).get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            cache.putAllConflict(drMapPuts);
            cache.removeAllConflict(drMapRemoves);
        }
    }

    /** Performs checks for #putAllConflict() and #removeAllConflict() operations related metrics. */
    private void checkMetrics(IgniteEx ign) {
        MetricRegistryImpl mreg = ign.context().metric().registry(cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false));

        assertTrue(mreg.<LongMetric>findMetric("PutAllConflictTimeTotal").value() > 0);
        assertTrue(mreg.<LongMetric>findMetric("RemoveAllConflictTimeTotal").value() > 0);

        assertTrue(stream(mreg.<HistogramMetricImpl>findMetric("RemoveAllConflictTime").value()).sum() > 0);
        assertTrue(stream(mreg.<HistogramMetricImpl>findMetric("PutAllConflictTime").value()).sum() > 0);
    }

    /** */
    private ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
            .setAddresses("127.0.0.1:" + CLIENT_CONNECTOR_PORT);
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
