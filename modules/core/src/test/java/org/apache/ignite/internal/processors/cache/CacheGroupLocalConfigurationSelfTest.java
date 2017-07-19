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

/**
 *
 */
public class CacheGroupLocalConfigurationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String SECOND_NODE_NAME = "secondNode";

    private static final int NON_STANDARD_REBALANCE_VALUE = 101;

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.equals(SECOND_NODE_NAME))
            cfg.setCacheConfiguration(new CacheConfiguration()
                .setName(DEFAULT_CACHE_NAME)
                .setRebalanceDelay(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceBatchesPrefetchCount(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceBatchSize(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceOrder(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceThrottle(NON_STANDARD_REBALANCE_VALUE)
                .setRebalanceTimeout(NON_STANDARD_REBALANCE_VALUE)
            );
        else
            cfg.setCacheConfiguration(new CacheConfiguration()
                .setName(DEFAULT_CACHE_NAME)
            );


        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void testLocalConfigurationAttributesPreserved() throws Exception {
        startGrid(0);

        IgniteKernal ignite = (IgniteKernal) startGrid("secondNode");

        GridCacheProcessor cacheProc = ignite.context().cache();

        Map<Integer, CacheGroupContext> cacheGrps = U.field(cacheProc, "cacheGrps");

        CacheConfiguration dfltCacheGroupCfg = findDefaultGroupConfig(cacheGrps);

        assertNotNull("Default cache group must be presented", dfltCacheGroupCfg);

        assertEquals("Rebalance delay", dfltCacheGroupCfg.getRebalanceDelay(), NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance batches prefetch count",
            dfltCacheGroupCfg.getRebalanceBatchesPrefetchCount(),
            NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance batch size", dfltCacheGroupCfg.getRebalanceBatchSize(), NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance order", dfltCacheGroupCfg.getRebalanceOrder(), NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance throttle", dfltCacheGroupCfg.getRebalanceThrottle(), NON_STANDARD_REBALANCE_VALUE);

        assertEquals("Rebalance timeout", dfltCacheGroupCfg.getRebalanceTimeout(), NON_STANDARD_REBALANCE_VALUE);
    }

    private CacheConfiguration findDefaultGroupConfig(Map<Integer, CacheGroupContext> cacheGrps) {
        for (CacheGroupContext grpCtx : cacheGrps.values()) {
            if (DEFAULT_CACHE_NAME.equals(grpCtx.cacheOrGroupName()))
                return grpCtx.config();
        }

        return null;
    }
}
