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
import junit.framework.JUnit4TestAdapter;
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

        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicInvalidPartitionHandlingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicClientInvalidPartitionHandlingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheRebalancingPartitionDistributionTest.class));

        GridTestUtils.addTestIfNeeded(suite, GridCacheIncrementTransformTest.class, ignoredTests);

        // Failure consistency tests.
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicRemoveFailureTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicClientRemoveFailureTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheDhtAtomicRemoveFailureTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheDhtRemoveFailureTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheDhtClientRemoveFailureTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheNearRemoveFailureTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCacheAtomicNearRemoveFailureTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteChangingBaselineUpCacheRemoveFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteChangingBaselineDownCacheRemoveFailoverTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheAtomicNodeJoinTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNodeJoinTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheTxNearDisabledPutGetRestartTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteCacheSizeFailoverTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteAtomicLongChangingTopologySelfTest.class));

        suite.addTest(new JUnit4TestAdapter(GridCacheTxNodeFailureSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(AtomicPutAllChangingTopologyTest.class));

        return suite;
    }
}
