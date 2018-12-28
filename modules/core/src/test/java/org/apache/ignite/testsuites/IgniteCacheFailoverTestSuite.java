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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * Test suite.
 */
@RunWith(IgniteCacheFailoverTestSuite.DynamicSuite.class)
public class IgniteCacheFailoverTestSuite {
    /**
     * @return Ignite Cache Failover test suite.
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

        suite.add(GridCacheAtomicInvalidPartitionHandlingSelfTest.class);
        suite.add(GridCacheAtomicClientInvalidPartitionHandlingSelfTest.class);
        suite.add(GridCacheRebalancingPartitionDistributionTest.class);

        GridTestUtils./* todo rework */addTestIfNeeded(suite, GridCacheIncrementTransformTest.class, ignoredTests);

        // Failure consistency tests.
        suite.add(GridCacheAtomicRemoveFailureTest.class);
        suite.add(GridCacheAtomicClientRemoveFailureTest.class);

        suite.add(GridCacheDhtAtomicRemoveFailureTest.class);
        suite.add(GridCacheDhtRemoveFailureTest.class);
        suite.add(GridCacheDhtClientRemoveFailureTest.class);
        suite.add(GridCacheNearRemoveFailureTest.class);
        suite.add(GridCacheAtomicNearRemoveFailureTest.class);
        suite.add(IgniteChangingBaselineUpCacheRemoveFailoverTest.class);
        suite.add(IgniteChangingBaselineDownCacheRemoveFailoverTest.class);

        suite.add(IgniteCacheAtomicNodeJoinTest.class);
        suite.add(IgniteCacheTxNodeJoinTest.class);

        suite.add(IgniteCacheTxNearDisabledPutGetRestartTest.class);

        suite.add(IgniteCacheSizeFailoverTest.class);

        suite.add(IgniteAtomicLongChangingTopologySelfTest.class);

        suite.add(GridCacheTxNodeFailureSelfTest.class);

        suite.add(AtomicPutAllChangingTopologyTest.class);

        return suite;
    }

    /** */
    public static class DynamicSuite extends Suite {
        /** */
        public DynamicSuite(Class<?> cls) throws InitializationError {
            super(cls, suite().toArray(new Class<?>[] {null}));
        }
    }
}
