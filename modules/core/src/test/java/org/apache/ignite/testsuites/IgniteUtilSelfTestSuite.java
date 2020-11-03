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

import org.apache.ignite.internal.IgniteVersionUtilsSelfTest;
import org.apache.ignite.internal.commandline.CommandHandlerParsingTest;
import org.apache.ignite.internal.pagemem.impl.PageIdUtilsSelfTest;
import org.apache.ignite.internal.processors.cache.GridCacheUtilsSelfTest;
import org.apache.ignite.internal.util.DistributedProcessCoordinatorLeftTest;
import org.apache.ignite.internal.util.GridArraysSelfTest;
import org.apache.ignite.internal.util.GridConcurrentMultiPairQueueTest;
import org.apache.ignite.internal.util.GridCountDownCallbackTest;
import org.apache.ignite.internal.util.IgniteDevOnlyLogTest;
import org.apache.ignite.internal.util.IgniteExceptionRegistrySelfTest;
import org.apache.ignite.internal.util.IgniteUtilsSelfTest;
import org.apache.ignite.internal.util.nio.GridNioDelimitedBufferSelfTest;
import org.apache.ignite.internal.util.nio.GridNioSelfTest;
import org.apache.ignite.internal.util.nio.GridNioSessionMetaKeySelfTest;
import org.apache.ignite.internal.util.nio.GridNioSslSelfTest;
import org.apache.ignite.internal.util.nio.impl.GridNioFilterChainSelfTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMapSelfTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemorySelfTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafePartitionedMapSelfTest;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeStripedLruSefTest;
import org.apache.ignite.internal.util.tostring.CircularStringBuilderSelfTest;
import org.apache.ignite.internal.util.tostring.GridToStringBuilderSelfTest;
import org.apache.ignite.internal.util.tostring.IncludeSensitiveAtomicTest;
import org.apache.ignite.internal.util.tostring.IncludeSensitiveTransactionalTest;
import org.apache.ignite.internal.util.tostring.TransactionSensitiveDataTest;
import org.apache.ignite.lang.GridByteArrayListSelfTest;
import org.apache.ignite.spi.discovery.ClusterMetricsSelfTest;
import org.apache.ignite.spi.discovery.ClusterMetricsSnapshotSerializeCompatibilityTest;
import org.apache.ignite.spi.discovery.ClusterMetricsSnapshotSerializeSelfTest;
import org.apache.ignite.spi.discovery.ClusterRebalancedMetricTest;
import org.apache.ignite.thread.GridThreadPoolExecutorServiceSelfTest;
import org.apache.ignite.thread.GridThreadTest;
import org.apache.ignite.thread.IgniteThreadPoolSizeTest;
import org.apache.ignite.thread.ThreadPoolMetricsTest;
import org.apache.ignite.util.GridConcurrentLinkedDequeMultiThreadedTest;
import org.apache.ignite.util.GridIntListSelfTest;
import org.apache.ignite.util.GridLogThrottleTest;
import org.apache.ignite.util.GridLongListSelfTest;
import org.apache.ignite.util.GridMessageCollectionTest;
import org.apache.ignite.util.GridPartitionMapSelfTest;
import org.apache.ignite.util.GridQueueSelfTest;
import org.apache.ignite.util.GridRandomSelfTest;
import org.apache.ignite.util.GridSnapshotLockSelfTest;
import org.apache.ignite.util.GridSpinReadWriteLockSelfTest;
import org.apache.ignite.util.GridStringBuilderFactorySelfTest;
import org.apache.ignite.util.GridTopologyHeapSizeSelfTest;
import org.apache.ignite.util.GridTransientTest;
import org.apache.ignite.util.mbeans.GridMBeanDisableSelfTest;
import org.apache.ignite.util.mbeans.GridMBeanExoticNamesSelfTest;
import org.apache.ignite.util.mbeans.GridMBeanSelfTest;
import org.apache.ignite.util.mbeans.WorkersControlMXBeanTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for Ignite utility classes.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    GridThreadPoolExecutorServiceSelfTest.class,
    IgniteThreadPoolSizeTest.class,
    IgniteUtilsSelfTest.class,
    IgniteVersionUtilsSelfTest.class,
    GridSpinReadWriteLockSelfTest.class,
    GridQueueSelfTest.class,
    GridStringBuilderFactorySelfTest.class,
    GridToStringBuilderSelfTest.class,
    CircularStringBuilderSelfTest.class,
    GridByteArrayListSelfTest.class,
    GridMBeanSelfTest.class,
    GridMBeanDisableSelfTest.class,
    GridMBeanExoticNamesSelfTest.class,
    GridLongListSelfTest.class,
    GridThreadTest.class,
    GridIntListSelfTest.class,
    GridArraysSelfTest.class,
    GridCacheUtilsSelfTest.class,
    IgniteExceptionRegistrySelfTest.class,
    GridMessageCollectionTest.class,
    WorkersControlMXBeanTest.class,
    GridConcurrentLinkedDequeMultiThreadedTest.class,
    GridLogThrottleTest.class,
    GridRandomSelfTest.class,
    GridSnapshotLockSelfTest.class,
    GridTopologyHeapSizeSelfTest.class,
    GridTransientTest.class,
    IgniteDevOnlyLogTest.class,
    GridConcurrentMultiPairQueueTest.class,

    // Sensitive toString.
    IncludeSensitiveAtomicTest.class,
    IncludeSensitiveTransactionalTest.class,
    TransactionSensitiveDataTest.class,

    // Metrics.
    ClusterMetricsSnapshotSerializeSelfTest.class,
    ClusterMetricsSnapshotSerializeCompatibilityTest.class,
    ClusterMetricsSelfTest.class,
    ClusterRebalancedMetricTest.class,
    ThreadPoolMetricsTest.class,

    // Unsafe.
    GridUnsafeMemorySelfTest.class,
    GridUnsafeStripedLruSefTest.class,
    GridUnsafeMapSelfTest.class,
    GridUnsafePartitionedMapSelfTest.class,

    // NIO.
    GridNioSessionMetaKeySelfTest.class,
    GridNioSelfTest.class,
    GridNioFilterChainSelfTest.class,
    GridNioSslSelfTest.class,
    GridNioDelimitedBufferSelfTest.class,

    GridPartitionMapSelfTest.class,

    //dbx
    PageIdUtilsSelfTest.class,

    // control.sh
    CommandHandlerParsingTest.class,

    GridCountDownCallbackTest.class,

    DistributedProcessCoordinatorLeftTest.class
})
public class IgniteUtilSelfTestSuite {
}
