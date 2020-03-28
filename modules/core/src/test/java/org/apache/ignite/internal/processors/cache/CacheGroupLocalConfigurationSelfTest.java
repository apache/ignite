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

import java.util.Map;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class CacheGroupLocalConfigurationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String SECOND_NODE_NAME = "secondNode";

    /** */
    private static final int NON_STANDARD_REBALANCE_VALUE = 101;

    /** */
    private static final String NON_DEFAULT_GROUP_NAME = "cacheGroup";

    /** */
    private boolean useNonDfltCacheGrp = true;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(SECOND_NODE_NAME)) {
            CacheConfiguration ccfg = new CacheConfiguration()
                .setName(DEFAULT_CACHE_NAME)
                .setRebalanceDelay(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceBatchesPrefetchCount(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceBatchSize(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceOrder(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceThrottle(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceTimeout(NON_STANDARD_REBALANCE_VALUE);

            if (useNonDfltCacheGrp)
                ccfg.setGroupName(NON_DEFAULT_GROUP_NAME);

            cfg.setCacheConfiguration(ccfg);
        }
        else {
            CacheConfiguration ccfg = new CacheConfiguration()
                .setName(DEFAULT_CACHE_NAME);

            if (useNonDfltCacheGrp)
                ccfg.setGroupName(NON_DEFAULT_GROUP_NAME);

            cfg.setCacheConfiguration(ccfg);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test validates that all cache group configuration attributes from local config
     * that must not be overwritten by grid config are preserved for default cache group.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDefaultGroupLocalAttributesPreserved() throws Exception {
        useNonDfltCacheGrp = false;

        executeTest();
    }

    /**
     * Test validates that all cache group configuration attributes from local config
     * that must not be overwritten by grid config are preserved for non-default cache group.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNonDefaultGroupLocalAttributesPreserved() throws Exception {
        useNonDfltCacheGrp = true;

        executeTest();
    }

    /**
     * Executes actual test logic.
     *
     * @throws Exception If failed.
     */
    private void executeTest() throws Exception {
        startGrid(0);

        IgniteKernal ignite = (IgniteKernal) startGrid("secondNode");

        GridCacheProcessor cacheProc = ignite.context().cache();

        Map<Integer, CacheGroupContext> cacheGrps = U.field(cacheProc, "cacheGrps");

        CacheConfiguration cacheGroupCfg = findGroupConfig(cacheGrps,
            useNonDfltCacheGrp ? NON_DEFAULT_GROUP_NAME : DEFAULT_CACHE_NAME);

        assertNotNull("Default cache group must be presented", cacheGroupCfg);

        assertEquals("Rebalance delay", cacheGroupCfg.getRebalanceDelay(), NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance batches prefetch count",
            cacheGroupCfg.getRebalanceBatchesPrefetchCount(),
            NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance batch size", cacheGroupCfg.getRebalanceBatchSize(), NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance order", cacheGroupCfg.getRebalanceOrder(), NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance throttle", cacheGroupCfg.getRebalanceThrottle(), NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance timeout", cacheGroupCfg.getRebalanceTimeout(), NON_STANDARD_REBALANCE_VALUE);
    }

    /**
     * @param cacheGrps All configured cache groups.
     * @param groupName Name of group to find.
     * @return Cache configuration.
     */
    private CacheConfiguration findGroupConfig(Map<Integer, CacheGroupContext> cacheGrps, @Nullable String groupName) {
        if (groupName == null)
            groupName = DEFAULT_CACHE_NAME;

        for (CacheGroupContext grpCtx : cacheGrps.values()) {
            if (groupName.equals(grpCtx.cacheOrGroupName()))
                return grpCtx.config();
        }

        return null;
    }
}
