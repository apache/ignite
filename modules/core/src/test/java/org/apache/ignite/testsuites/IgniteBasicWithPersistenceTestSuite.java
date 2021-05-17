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

import org.apache.ignite.failure.FailureHandlingConfigurationTest;
import org.apache.ignite.failure.IoomFailureHandlerTest;
import org.apache.ignite.failure.SystemWorkersBlockingTest;
import org.apache.ignite.failure.SystemWorkersTerminationTest;
import org.apache.ignite.internal.ClusterBaselineNodesMetricsSelfTest;
import org.apache.ignite.internal.GridNodeMetricsLogPdsSelfTest;
import org.apache.ignite.internal.cluster.FullyConnectedComponentSearcherTest;
import org.apache.ignite.internal.cluster.IgniteClusterIdTagTest;
import org.apache.ignite.internal.encryption.CacheGroupKeyChangeTest;
import org.apache.ignite.internal.encryption.CacheGroupReencryptionTest;
import org.apache.ignite.internal.encryption.EncryptedCacheBigEntryTest;
import org.apache.ignite.internal.encryption.EncryptedCacheCreateTest;
import org.apache.ignite.internal.encryption.EncryptedCacheDestroyTest;
import org.apache.ignite.internal.encryption.EncryptedCacheGroupCreateTest;
import org.apache.ignite.internal.encryption.EncryptedCacheNodeJoinTest;
import org.apache.ignite.internal.encryption.EncryptedCachePreconfiguredRestartTest;
import org.apache.ignite.internal.encryption.EncryptedCacheRestartTest;
import org.apache.ignite.internal.encryption.EncryptionMXBeanTest;
import org.apache.ignite.internal.encryption.MasterKeyChangeConsistencyCheckTest;
import org.apache.ignite.internal.encryption.MasterKeyChangeTest;
import org.apache.ignite.internal.processors.cache.persistence.CheckpointReadLockFailureTest;
import org.apache.ignite.internal.processors.cache.persistence.CommonPoolStarvationCheckpointTest;
import org.apache.ignite.internal.processors.cache.persistence.SingleNodePersistenceSslTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotCheckTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotRestoreSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteClusterSnapshotSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotMXBeanTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManagerSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotWithMetastorageTest;
import org.apache.ignite.internal.processors.performancestatistics.CacheStartTest;
import org.apache.ignite.internal.processors.performancestatistics.CheckpointTest;
import org.apache.ignite.internal.processors.performancestatistics.ForwardReadTest;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsMultipleStartTest;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsPropertiesTest;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsRotateFileTest;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsSelfTest;
import org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsThinClientTest;
import org.apache.ignite.internal.processors.performancestatistics.StringCacheTest;
import org.apache.ignite.internal.processors.performancestatistics.TopologyChangesTest;
import org.apache.ignite.marshaller.GridMarshallerMappingConsistencyTest;
import org.apache.ignite.util.GridInternalTaskUnusedWalSegmentsTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Basic test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IoomFailureHandlerTest.class,
    ClusterBaselineNodesMetricsSelfTest.class,
    GridMarshallerMappingConsistencyTest.class,
    SystemWorkersTerminationTest.class,
    FailureHandlingConfigurationTest.class,
    SystemWorkersBlockingTest.class,
    CheckpointReadLockFailureTest.class,
    CommonPoolStarvationCheckpointTest.class,

    GridInternalTaskUnusedWalSegmentsTest.class,

    GridNodeMetricsLogPdsSelfTest.class,

    EncryptedCacheBigEntryTest.class,
    EncryptedCacheCreateTest.class,
    EncryptedCacheDestroyTest.class,
    EncryptedCacheGroupCreateTest.class,
    EncryptedCacheNodeJoinTest.class,
    EncryptedCacheRestartTest.class,
    EncryptedCachePreconfiguredRestartTest.class,

    SingleNodePersistenceSslTest.class,

    MasterKeyChangeTest.class,
    MasterKeyChangeConsistencyCheckTest.class,

    CacheGroupKeyChangeTest.class,
    CacheGroupReencryptionTest.class,

    EncryptionMXBeanTest.class,

    IgniteSnapshotManagerSelfTest.class,
    IgniteClusterSnapshotSelfTest.class,
    IgniteClusterSnapshotCheckTest.class,
    IgniteSnapshotWithMetastorageTest.class,
    IgniteSnapshotMXBeanTest.class,
    IgniteClusterSnapshotRestoreSelfTest.class,

    IgniteClusterIdTagTest.class,
    FullyConnectedComponentSearcherTest.class,

    PerformanceStatisticsSelfTest.class,
    PerformanceStatisticsThinClientTest.class,
    PerformanceStatisticsRotateFileTest.class,
    TopologyChangesTest.class,
    IgniteClusterIdTagTest.class,
    StringCacheTest.class,
    PerformanceStatisticsPropertiesTest.class,
    PerformanceStatisticsMultipleStartTest.class,
    ForwardReadTest.class,
    CacheStartTest.class,
    CheckpointTest.class
})
public class IgniteBasicWithPersistenceTestSuite {
}
