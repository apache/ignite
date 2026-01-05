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

import org.apache.ignite.internal.processors.cache.GridCacheConcurrentTxMultiNodeLoadTest;
import org.apache.ignite.internal.processors.cache.GridCacheIteratorPerformanceTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheStartStopLoadTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadPerformanceTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAffinityExcludeNeighborsPerformanceTest;
import org.apache.ignite.internal.processors.cache.eviction.sorted.SortedEvictionPolicyPerformanceTest;
import org.apache.ignite.internal.processors.datastreamer.IgniteDataStreamerPerformanceTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMapPerformanceTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafePartitionedMapPerformanceTest;
import org.apache.ignite.lang.GridFuncPerformanceTest;
import org.apache.ignite.lang.GridMetadataAwareAdapterLoadTest;
import org.apache.ignite.lang.utils.GridCircularBufferPerformanceTest;
import org.apache.ignite.lang.utils.GridLeanMapPerformanceTest;
import org.apache.ignite.loadtests.GridCacheMultiNodeLoadTest;
import org.apache.ignite.loadtests.cache.GridCacheWriteBehindStoreLoadTest;
import org.apache.ignite.loadtests.datastructures.GridCachePartitionedAtomicLongLoadTest;
import org.apache.ignite.loadtests.direct.multisplit.GridMultiSplitsLoadTest;
import org.apache.ignite.loadtests.direct.multisplit.GridMultiSplitsRedeployLoadTest;
import org.apache.ignite.loadtests.direct.newnodes.GridSingleSplitsNewNodesMulticastLoadTest;
import org.apache.ignite.loadtests.direct.redeploy.GridSingleSplitsRedeployLoadTest;
import org.apache.ignite.loadtests.direct.session.GridSessionLoadTest;
import org.apache.ignite.loadtests.direct.stealing.GridStealingLoadTest;
import org.apache.ignite.loadtests.hashmap.GridHashMapLoadTest;
import org.apache.ignite.marshaller.GridMarshallerPerformanceTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiLanLoadTest;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

/**
 * Tests suite for performance tests.
 * Note: Most of these are resource-consuming or non-terminating.
 */
@Suite
@SelectClasses({
    GridCacheDhtPreloadPerformanceTest.class,
    GridCacheIteratorPerformanceTest.class,
    GridCacheMultiNodeLoadTest.class,
    GridCacheConcurrentTxMultiNodeLoadTest.class,
    GridCachePartitionedAffinityExcludeNeighborsPerformanceTest.class,
    GridCachePartitionedAtomicLongLoadTest.class,
    GridCacheWriteBehindStoreLoadTest.class,
    GridCircularBufferPerformanceTest.class,
    GridFuncPerformanceTest.class,
    GridHashMapLoadTest.class,
    GridLeanMapPerformanceTest.class,
    GridMarshallerPerformanceTest.class,
    GridMetadataAwareAdapterLoadTest.class,
    GridMultiSplitsLoadTest.class,
    GridMultiSplitsRedeployLoadTest.class,
    GridSessionLoadTest.class,
    GridSingleSplitsNewNodesMulticastLoadTest.class,
    GridSingleSplitsRedeployLoadTest.class,
    GridStealingLoadTest.class,
    GridTcpCommunicationSpiLanLoadTest.class,
    GridUnsafeMapPerformanceTest.class,
    GridUnsafePartitionedMapPerformanceTest.class,
    IgniteDataStreamerPerformanceTest.class,
    SortedEvictionPolicyPerformanceTest.class,
    IgniteCacheStartStopLoadTest.class,
})
public class IgnitePerformanceTestSuite {
/*    @RunWith(Suite.class)
    @Suite.SuiteClasses({
            GridBasicPerformanceTest.class,
            GridBenchmarkCacheGetLoadTest.class,
            GridBoundedConcurrentLinkedHashSetLoadTest.class,
            GridCacheDataStructuresLoadTest.class,
            GridCacheLoadTest.class,
            GridCapacityLoadTest.class,
            GridContinuousOperationsLoadTest.class,
            GridFutureListenPerformanceTest.class,
            GridGcTimeoutTest.class,
            GridJobExecutionSingleNodeLoadTest.class,
            GridJobExecutionSingleNodeSemaphoreLoadTest.class,
            GridJobLoadTest.class,
            GridMergeSortLoadTest.class,
            GridNioBenchmarkTest.class,
            GridSingleExecutionTest.class
    })*/
}
