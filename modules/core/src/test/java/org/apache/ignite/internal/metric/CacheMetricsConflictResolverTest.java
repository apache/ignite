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

import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/** Tests conflict resolver metrics per cache. */
public class CacheMetricsConflictResolverTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPluginProviders(new AbstractTestPluginProvider() {
            @Override public String name() {
                return "ConflictResolverProvider";
            }

            @Override public <T> T createComponent(PluginContext ctx, Class<T> cls) {
                if (cls != CacheConflictResolutionManager.class)
                    return null;

                return (T)new DynamicResolutionManager<>();
            }
        });

        return cfg;
    }

    /** */
    @Test
    public void testCacheConflictResolver() throws Exception {
        IgniteEx ign = startGrid(0);

        IgniteCache<Object, Object> cache = ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        checkMetrics(0, 0, 0);

        TestCacheVersionConflictResolver.plc = ResolvePolicy.USE_NEW;

        cache.put(0, 0);

        checkMetrics(1, 0, 0);

        TestCacheVersionConflictResolver.plc = ResolvePolicy.USE_OLD;

        cache.put(0, 0);

        checkMetrics(1, 1, 0);

        TestCacheVersionConflictResolver.plc = ResolvePolicy.MERGE;

        cache.put(0, 0);

        checkMetrics(1, 1, 1);
    }

    /** */
    private void checkMetrics(int expAccepted, int expRejected, int expMerged) {
        MetricRegistryImpl mreg = grid(0).context().metric().registry(cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false));

        assertEquals(expAccepted, mreg.<LongMetric>findMetric("ConflictResolverAcceptedCount").value());
        assertEquals(expRejected, mreg.<LongMetric>findMetric("ConflictResolverRejectedCount").value());
        assertEquals(expMerged, mreg.<LongMetric>findMetric("ConflictResolverMergedCount").value());
    }

    /** */
    private static class TestCacheVersionConflictResolver implements CacheVersionConflictResolver {
        /** */
        private static ResolvePolicy plc;

        /** {@inheritDoc} */
        @Override public <K, V> GridCacheVersionConflictContext<K, V> resolve(
            CacheObjectValueContext ctx,
            GridCacheVersionedEntryEx<K, V> oldEntry,
            GridCacheVersionedEntryEx<K, V> newEntry,
            boolean atomicVerComparator
        ) {
            GridCacheVersionConflictContext<K, V> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

            if (plc == ResolvePolicy.USE_NEW)
                res.useNew();
            else if (plc == ResolvePolicy.USE_OLD)
                res.useOld();
            else
                res.merge(
                    newEntry.value(ctx),
                    Math.max(oldEntry.ttl(), newEntry.ttl()),
                    Math.max(oldEntry.expireTime(), newEntry.expireTime())
                );

            return res;
        }
    }

    /** */
    private static class DynamicResolutionManager<K, V> extends GridCacheManagerAdapter<K, V>
        implements CacheConflictResolutionManager<K, V> {
        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return new TestCacheVersionConflictResolver();
        }
    }

    /** Policy for conflict resolver. */
    private enum ResolvePolicy {
        /** Use old. */
        USE_OLD,

        /** Use new. */
        USE_NEW,

        /** Merge. */
        MERGE
    }
}
