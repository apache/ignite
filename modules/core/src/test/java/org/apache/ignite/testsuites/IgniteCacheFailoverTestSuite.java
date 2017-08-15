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

import java.util.Set;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.GridCacheIncrementTransformTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheTopologySafeGetSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicNodeJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSizeFailoverTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxFairAffinityNodeJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxNearDisabledFairAffinityPutGetRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxNearDisabledPutGetRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxNodeJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtAtomicRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtClientRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheTxNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridNearCacheTxNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteAtomicLongChangingTopologySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.AtomicPutAllChangingTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicClientInvalidPartitionHandlingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicClientRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicInvalidPartitionHandlingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicPrimaryWriteOrderRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicPrimaryWriteOrderNearRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingPartitionDistributionTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test suite.
 */
public class IgniteCacheFailoverTestSuite extends TestSuite {
    /**
     * @return Ignite Cache Failover test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite(Set<Class> ignoredTests) throws Exception {
        TestSuite suite = new TestSuite("Cache Failover Test Suite");

        suite.addTestSuite(GridCacheAtomicInvalidPartitionHandlingSelfTest.class);
        suite.addTestSuite(GridCacheAtomicClientInvalidPartitionHandlingSelfTest.class);
        suite.addTestSuite(GridCacheRebalancingPartitionDistributionTest.class);

        GridTestUtils.addTestIfNeeded(suite, GridCacheIncrementTransformTest.class, ignoredTests);

        // Failure consistency tests.
        suite.addTestSuite(GridCacheAtomicRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicClientRemoveFailureTest.class);

        suite.addTestSuite(GridCacheDhtAtomicRemoveFailureTest.class);
        suite.addTestSuite(GridCacheDhtRemoveFailureTest.class);
        suite.addTestSuite(GridCacheDhtClientRemoveFailureTest.class);
        suite.addTestSuite(GridCacheNearRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicNearRemoveFailureTest.class);
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderNearRemoveFailureTest.class);

        suite.addTestSuite(IgniteCacheAtomicNodeJoinTest.class);
        suite.addTestSuite(IgniteCacheTxNodeJoinTest.class);
        suite.addTestSuite(IgniteCacheTxFairAffinityNodeJoinTest.class);

        suite.addTestSuite(IgniteCacheTxNearDisabledPutGetRestartTest.class);
        suite.addTestSuite(IgniteCacheTxNearDisabledFairAffinityPutGetRestartTest.class);

        suite.addTestSuite(IgniteCacheSizeFailoverTest.class);

        suite.addTestSuite(IgniteCacheTopologySafeGetSelfTest.class);
        suite.addTestSuite(IgniteAtomicLongChangingTopologySelfTest.class);

        suite.addTestSuite(GridCacheTxNodeFailureSelfTest.class);
        suite.addTestSuite(GridNearCacheTxNodeFailureSelfTest.class);

        suite.addTestSuite(AtomicPutAllChangingTopologyTest.class);

        return suite;
    }
}
