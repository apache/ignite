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

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.GridCacheOrderedPreloadingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRabalancingDelayedPartitionMapExchangeSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingAsyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingCancelTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingSyncCheckDataTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingSyncSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingUnmarshallingFailedSelfTest;
import org.apache.ignite.testframework.junits.GridAbstractTest;

/**
 * Test suite.
 */
public class IgniteCacheTestSuite8 extends TestSuite {
    /**
     * @return IgniteCache test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(GridAbstractTest.PERSISTENCE_IN_TESTS_IS_ALLOWED_PROPERTY, "false");

        TestSuite suite = new TestSuite("IgniteCache Test Suite part 8");

        // Cache metrics.
        suite.addTest(IgniteCacheMetricsSelfTestSuite.suite());

        // Topology validator.
        suite.addTest(IgniteTopologyValidatorTestSuite.suite());

        // Eviction.
        suite.addTest(IgniteCacheEvictionSelfTestSuite.suite());

        // Iterators.
        suite.addTest(IgniteCacheIteratorsSelfTestSuite.suite());

        // Rebalancing.
        suite.addTestSuite(GridCacheOrderedPreloadingSelfTest.class);
        suite.addTestSuite(GridCacheRebalancingSyncSelfTest.class);
        suite.addTestSuite(GridCacheRebalancingSyncCheckDataTest.class);
        suite.addTestSuite(GridCacheRebalancingUnmarshallingFailedSelfTest.class);
        suite.addTestSuite(GridCacheRebalancingAsyncSelfTest.class);
        suite.addTestSuite(GridCacheRabalancingDelayedPartitionMapExchangeSelfTest.class);
        suite.addTestSuite(GridCacheRebalancingCancelTest.class);

        return suite;
    }
}
