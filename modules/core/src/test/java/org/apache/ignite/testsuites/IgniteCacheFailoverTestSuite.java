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

import java.util.Collection;
import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.GridCacheIncrementTransformTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheAtomicNodeJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheSizeFailoverTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxNearDisabledPutGetRestartTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCacheTxNodeJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtAtomicRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtClientRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheTxNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteAtomicLongChangingTopologySelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.AtomicPutAllChangingTopologyTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicClientInvalidPartitionHandlingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicClientRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicInvalidPartitionHandlingSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridCacheAtomicRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheAtomicNearRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCacheNearRemoveFailureTest;
import org.apache.ignite.internal.processors.cache.distributed.rebalancing.GridCacheRebalancingPartitionDistributionTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteChangingBaselineDownCacheRemoveFailoverTest;
import org.apache.ignite.internal.processors.cache.persistence.baseline.IgniteChangingBaselineUpCacheRemoveFailoverTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test suite.
 */
public class IgniteCacheFailoverTestSuite extends TestSuite {
    /**
     * @return Ignite Cache Failover test suite.
     */
    public static TestSuite suite() {
        return suite(null);
    }

    /**
     * @param ignoredTests Tests don't include in the execution.
     * @return Test suite.
     */
    public static TestSuite suite(Collection<Class> ignoredTests) {
        TestSuite suite = new TestSuite("Cache Failover Test Suite");

       GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicInvalidPartitionHandlingSelfTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicClientInvalidPartitionHandlingSelfTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, GridCacheRebalancingPartitionDistributionTest.class, ignoredTests);

        GridTestUtils.addTestIfNeeded(suite, GridCacheIncrementTransformTest.class, ignoredTests);

        // Failure consistency tests.
       GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicRemoveFailureTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicClientRemoveFailureTest.class, ignoredTests);

       GridTestUtils.addTestIfNeeded(suite, GridCacheDhtAtomicRemoveFailureTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, GridCacheDhtRemoveFailureTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, GridCacheDhtClientRemoveFailureTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, GridCacheNearRemoveFailureTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, GridCacheAtomicNearRemoveFailureTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, IgniteChangingBaselineUpCacheRemoveFailoverTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, IgniteChangingBaselineDownCacheRemoveFailoverTest.class, ignoredTests);

       GridTestUtils.addTestIfNeeded(suite, IgniteCacheAtomicNodeJoinTest.class, ignoredTests);
       GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNodeJoinTest.class, ignoredTests);

       GridTestUtils.addTestIfNeeded(suite, IgniteCacheTxNearDisabledPutGetRestartTest.class, ignoredTests);

       GridTestUtils.addTestIfNeeded(suite, IgniteCacheSizeFailoverTest.class, ignoredTests);

       GridTestUtils.addTestIfNeeded(suite, IgniteAtomicLongChangingTopologySelfTest.class, ignoredTests);

       GridTestUtils.addTestIfNeeded(suite, GridCacheTxNodeFailureSelfTest.class, ignoredTests);

       GridTestUtils.addTestIfNeeded(suite, AtomicPutAllChangingTopologyTest.class, ignoredTests);

        return suite;
    }
}
