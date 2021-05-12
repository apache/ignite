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

package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import org.apache.ignite.cache.CacheMetrics;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.CacheMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Class for testing rebalance metrics.
 */
public class RebalanceMetricsTest extends GridCommonAbstractTest {
    /** Cache configurations. */
    @Nullable private CacheConfiguration<?, ?>[] cacheConfigs;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCacheConfiguration(cacheConfigs);
    }

    /**
     * Testing the correctness of metric {@link CacheMetrics#getRebalancedKeys()}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testReceivedKeys() throws Exception {
        cacheConfigs = new CacheConfiguration[] {
            new CacheConfiguration<>(DEFAULT_CACHE_NAME + "0")
                .setGroupName(DEFAULT_CACHE_NAME)
                .setStatisticsEnabled(true),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME + "1")
                .setGroupName(DEFAULT_CACHE_NAME)
                .setStatisticsEnabled(true),
            new CacheConfiguration<>(DEFAULT_CACHE_NAME + "2")
                .setGroupName(DEFAULT_CACHE_NAME)
                .setStatisticsEnabled(false),
        };

        IgniteEx n0 = startGrid(0);

        n0.cluster().state(ClusterState.ACTIVE);
        awaitPartitionMapExchange();

        for (CacheConfiguration<?, ?> cacheCfg : cacheConfigs) {
            for (int i = 0; i < 100; i++)
                n0.cache(cacheCfg.getName()).put(i + CU.cacheId(cacheCfg.getName()), new byte[64]);
        }

        IgniteEx n1 = startGrid(1);
        awaitPartitionMapExchange();

        for (CacheConfiguration<?, ?> cacheCfg : cacheConfigs) {
            GridCacheContext<?, ?> cacheCtx0 = n0.context().cache().cache(cacheCfg.getName()).context();
            GridCacheContext<?, ?> cacheCtx1 = n1.context().cache().cache(cacheCfg.getName()).context();

            assertEquals(cacheCfg.getName(), cacheCfg.isStatisticsEnabled(), cacheCtx0.statisticsEnabled());
            assertEquals(cacheCfg.getName(), cacheCfg.isStatisticsEnabled(), cacheCtx1.statisticsEnabled());

            CacheMetricsImpl metrics0 = cacheCtx0.cache().metrics0();
            CacheMetricsImpl metrics1 = cacheCtx1.cache().metrics0();

            assertEquals(cacheCfg.getName(), cacheCfg.isStatisticsEnabled(), metrics0.isStatisticsEnabled());
            assertEquals(cacheCfg.getName(), cacheCfg.isStatisticsEnabled(), metrics1.isStatisticsEnabled());

            assertEquals(cacheCfg.getName(), 0, metrics0.getRebalancedKeys());

            assertEquals(
                cacheCfg.getName(),
                cacheCfg.isStatisticsEnabled() ? metrics1.getKeySize() : 0,
                metrics1.getRebalancedKeys()
            );
        }
    }
}
