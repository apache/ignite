/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.cache.GridCacheConcurrentTxMultiNodeLoadTest;
import org.apache.ignite.internal.processors.cache.GridCacheIteratorPerformanceTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheDhtPreloadPerformanceTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridCachePartitionedAffinityExcludeNeighborsPerformanceTest;
import org.apache.ignite.internal.processors.cache.eviction.sorted.SortedEvictionPolicyPerformanceTest;
import org.apache.ignite.internal.processors.datastreamer.IgniteDataStreamerPerformanceTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMapPerformanceTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafePartitionedMapPerformanceTest;
import org.apache.ignite.lang.GridBasicPerformanceTest;
import org.apache.ignite.lang.GridFuncPerformanceTest;
import org.apache.ignite.lang.GridFutureListenPerformanceTest;
import org.apache.ignite.lang.GridMetadataAwareAdapterLoadTest;
import org.apache.ignite.lang.utils.GridCircularBufferPerformanceTest;
import org.apache.ignite.lang.utils.GridLeanMapPerformanceTest;
import org.apache.ignite.loadtests.GridCacheMultiNodeLoadTest;
import org.apache.ignite.loadtests.GridSingleExecutionTest;
import org.apache.ignite.loadtests.cache.GridCacheDataStructuresLoadTest;
import org.apache.ignite.loadtests.cache.GridCacheLoadTest;
import org.apache.ignite.loadtests.cache.GridCacheWriteBehindStoreLoadTest;
import org.apache.ignite.loadtests.capacity.GridCapacityLoadTest;
import org.apache.ignite.loadtests.continuous.GridContinuousOperationsLoadTest;
import org.apache.ignite.loadtests.datastructures.GridCachePartitionedAtomicLongLoadTest;
import org.apache.ignite.loadtests.direct.multisplit.GridMultiSplitsLoadTest;
import org.apache.ignite.loadtests.direct.multisplit.GridMultiSplitsRedeployLoadTest;
import org.apache.ignite.loadtests.direct.newnodes.GridSingleSplitsNewNodesMulticastLoadTest;
import org.apache.ignite.loadtests.direct.redeploy.GridSingleSplitsRedeployLoadTest;
import org.apache.ignite.loadtests.direct.session.GridSessionLoadTest;
import org.apache.ignite.loadtests.direct.stealing.GridStealingLoadTest;
import org.apache.ignite.loadtests.discovery.GridGcTimeoutTest;
import org.apache.ignite.loadtests.dsi.cacheget.GridBenchmarkCacheGetLoadTest;
import org.apache.ignite.loadtests.hashmap.GridBoundedConcurrentLinkedHashSetLoadTest;
import org.apache.ignite.loadtests.hashmap.GridHashMapLoadTest;
import org.apache.ignite.loadtests.job.GridJobExecutionSingleNodeLoadTest;
import org.apache.ignite.loadtests.job.GridJobExecutionSingleNodeSemaphoreLoadTest;
import org.apache.ignite.loadtests.job.GridJobLoadTest;
import org.apache.ignite.loadtests.mergesort.GridMergeSortLoadTest;
import org.apache.ignite.loadtests.nio.GridNioBenchmarkTest;
import org.apache.ignite.marshaller.GridMarshallerPerformanceTest;
import org.apache.ignite.spi.communication.tcp.GridTcpCommunicationSpiLanLoadTest;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Tests suite for performance tests tests.
 * Note: Most of these are resource-consuming or non-terminating.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
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

    IgnitePerformanceTestSuite.TentativeTests.class
})
public class IgnitePerformanceTestSuite {
    /**
     * Non-JUnit classes with Test in name, which should be either converted to JUnit or removed in the future
     * Main classes.
     */
    @RunWith(Suite.class)
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
    })
    @Ignore
    public static class TentativeTests {
    }
}
