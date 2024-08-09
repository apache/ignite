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

import java.util.function.Function;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheConflictResolutionManager;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.version.CacheVersionConflictResolver;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionConflictContext;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionedEntryEx;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/** Tests conflict resolver metrics per cache. */
public class CacheMetricsConflictResolverTest extends GridCommonAbstractTest {
    /** Conflict resolver manager */
    private static DynamicResolutionManager<?, ?> conflictResolverMgr;

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

                conflictResolverMgr = new DynamicResolutionManager<>();

                return (T)conflictResolverMgr;
            }
        });

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testCacheConflictResolver() throws Exception {
        IgniteEx ign = startGrid(0);

        ign.getOrCreateCache(DEFAULT_CACHE_NAME);

        IgniteInternalCache<Integer, Integer> cachex = ign.cachex(DEFAULT_CACHE_NAME);

        MetricRegistryImpl mreg = ign.context().metric().registry(metricName(CACHE_METRICS, DEFAULT_CACHE_NAME));

        Function<String, LongMetric> metric = metricName -> (LongMetric)mreg.findMetric(metricName);

        assertEquals(0, metric.apply("ConflictResolverAcceptedCount").value());
        assertEquals(0, metric.apply("ConflictResolverRejectedCount").value());
        assertEquals(0, metric.apply("ConflictResolverMergedCount").value());

        conflictResolverMgr.setConflictResolverState(State.USE_NEW);
        cachex.put(0, 0);
        assertEquals(1, metric.apply("ConflictResolverAcceptedCount").value());

        conflictResolverMgr.setConflictResolverState(State.USE_OLD);
        cachex.put(0, 0);
        assertEquals(1, metric.apply("ConflictResolverRejectedCount").value());

        conflictResolverMgr.setConflictResolverState(State.MERGE);
        cachex.put(0, 0);
        assertEquals(1, metric.apply("ConflictResolverMergedCount").value());

        stopAllGrids();
    }

    /** */
    private static class DynamicResolutionManager<K, V>
        extends GridCacheManagerAdapter<K, V> implements CacheConflictResolutionManager<K, V> {
        /** */
        private State rslvState = State.USE_NEW;

        /** */
        private final CacheVersionConflictResolver rslv;

        /** */
        DynamicResolutionManager() {
            rslv = new CacheVersionConflictResolver() {
                @Override public <K1, V1> GridCacheVersionConflictContext<K1, V1> resolve(
                    CacheObjectValueContext ctx,
                    GridCacheVersionedEntryEx<K1, V1> oldEntry,
                    GridCacheVersionedEntryEx<K1, V1> newEntry,
                    boolean atomicVerComparator
                ) {
                    GridCacheVersionConflictContext<K1, V1> res = new GridCacheVersionConflictContext<>(ctx, oldEntry, newEntry);

                    if (rslvState == State.USE_NEW)
                        res.useNew();
                    else if (rslvState == State.USE_OLD)
                        res.useOld();
                    else
                        res.merge(
                            newEntry.value(ctx),
                            Math.max(oldEntry.ttl(), newEntry.ttl()),
                            Math.max(oldEntry.expireTime(), newEntry.expireTime())
                        );

                    return res;
                }
            };
        }

        /** {@inheritDoc} */
        @Override public CacheVersionConflictResolver conflictResolver() {
            return rslv;
        }

        /**
         * Sets conflict resolver entries handling policy.
         * @param state State.
         */
        public void setConflictResolverState(State state) {
            rslvState = state;
        }
    }

    /** State for conflict resolver. */
    private enum State {
        /** Use old. */
        USE_OLD,

        /** Use new. */
        USE_NEW,

        /** Merge. */
        MERGE
    }
}
