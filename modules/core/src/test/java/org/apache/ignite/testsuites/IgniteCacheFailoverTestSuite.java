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
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridCacheAtomicInvalidPartitionHandlingSelfTest.class,
    GridCacheAtomicClientInvalidPartitionHandlingSelfTest.class,
    GridCacheRebalancingPartitionDistributionTest.class,

    GridCacheIncrementTransformTest.class,

    // Failure consistency tests.
    GridCacheAtomicRemoveFailureTest.class,
    GridCacheAtomicClientRemoveFailureTest.class,

    GridCacheDhtAtomicRemoveFailureTest.class,
    GridCacheDhtRemoveFailureTest.class,
    GridCacheDhtClientRemoveFailureTest.class,
    GridCacheNearRemoveFailureTest.class,
    GridCacheAtomicNearRemoveFailureTest.class,
    IgniteChangingBaselineUpCacheRemoveFailoverTest.class,
    IgniteChangingBaselineDownCacheRemoveFailoverTest.class,

    IgniteCacheAtomicNodeJoinTest.class,
    IgniteCacheTxNodeJoinTest.class,

    IgniteCacheTxNearDisabledPutGetRestartTest.class,

    IgniteCacheSizeFailoverTest.class,

    IgniteAtomicLongChangingTopologySelfTest.class,

    GridCacheTxNodeFailureSelfTest.class,

    AtomicPutAllChangingTopologyTest.class,
})
public class IgniteCacheFailoverTestSuite {
}
