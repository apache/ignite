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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.cache.ClientCreateCacheGroupOnJoinNodeMapsTest;
import org.apache.ignite.internal.processors.cache.CacheStoreTxPutAllMultiNodeTest;
import org.apache.ignite.internal.processors.cache.GridCacheOrderedPreloadingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRabalancingDelayedPartitionMapExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalanceOrderTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingAsyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingCancelTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingSyncCheckDataTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingSyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingUnmarshallingFailedSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.CleanupRestoredCachesSlowTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

/**
 * Test suite.
 */
@RunWith(DynamicSuite.class)
public class IgniteCacheTestSuite8 {
    /**
     * @return IgniteCache test suite.
     */
    public static List<Class<?>> suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests to ignore.
     * @return Test suite.
     */
    public static List<Class<?>> suite(Collection<Class> ignoredTests) {
        List<Class<?>> suite = new ArrayList<>();

        // Cache metrics.
        suite.addAll(IgniteCacheMetricsSelfTestSuite.suite(ignoredTests));

        // Topology validator.
        suite.addAll(IgniteTopologyValidatorTestSuite.suite(ignoredTests));

        // Eviction.
        suite.addAll(IgniteCacheEvictionSelfTestSuite.suite(ignoredTests));

        // Iterators.
        suite.addAll(IgniteCacheIteratorsSelfTestSuite.suite(ignoredTests));

        // Rebalancing.
        GridTestUtils.addTestIfNeeded(suite, GridCacheOrderedPreloadingSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalanceOrderTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingSyncSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingSyncCheckDataTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingUnmarshallingFailedSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingAsyncSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRabalancingDelayedPartitionMapExchangeSelfTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingCancelTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, CacheStoreTxPutAllMultiNodeTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, CleanupRestoredCachesSlowTest.class, ignoredTests);
        GridTestUtils.addTestIfNeeded(suite, ClientCreateCacheGroupOnJoinNodeMapsTest.class, ignoredTests);

        return suite;
    }
}
