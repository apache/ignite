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

import org.apache.ignite.ClassPathContentLoggingTest;
import org.apache.ignite.cache.RemoveAllDeadlockTest;
import org.apache.ignite.events.ClusterActivationStartedEventTest;
import org.apache.ignite.failure.ExchangeWorkerWaitingForTaskTest;
import org.apache.ignite.failure.FailureHandlerTriggeredTest;
import org.apache.ignite.failure.OomFailureHandlerTest;
import org.apache.ignite.failure.StopNodeFailureHandlerTest;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandlerTest;
import org.apache.ignite.internal.ClassSetTest;
import org.apache.ignite.internal.ConcurrentMappingFileReadWriteTest;
import org.apache.ignite.internal.ConsistentIdImplicitlyExplicitlyTest;
import org.apache.ignite.internal.DiagnosticLogForPartitionStatesTest;
import org.apache.ignite.internal.GridPeerDeploymentRetryModifiedTest;
import org.apache.ignite.internal.GridPeerDeploymentRetryTest;
import org.apache.ignite.internal.IgniteThreadGroupNodeRestartTest;
import org.apache.ignite.internal.MarshallerContextLockingSelfTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesMultipleConnectionsTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticPartitionReleaseFutureLimitTest;
import org.apache.ignite.internal.managers.communication.GridIoManagerFileTransmissionSelfTest;
import org.apache.ignite.internal.managers.discovery.IncompleteDeserializationExceptionTest;
import org.apache.ignite.internal.metric.MetricConfigurationTest;
import org.apache.ignite.internal.metric.MetricsClusterActivationTest;
import org.apache.ignite.internal.metric.PeriodicHistogramMetricImplTest;
import org.apache.ignite.internal.mxbean.IgniteStandardMXBeanTest;
import org.apache.ignite.internal.pagemem.wal.record.WALRecordSerializationTest;
import org.apache.ignite.internal.pagemem.wal.record.WALRecordTest;
import org.apache.ignite.internal.processors.DeadLockOnNodeLeftExchangeTest;
import org.apache.ignite.internal.processors.cache.CacheLocalGetSerializationTest;
import org.apache.ignite.internal.processors.cache.CacheLockCandidatesThreadTest;
import org.apache.ignite.internal.processors.cache.GridLongRunningInitNewCrdFutureDiagnosticsTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClassNameConflictTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClientRequestsMappingOnMissTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClientRequestsMappingTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheFSRestoreTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheSeparateDirectoryTest;
import org.apache.ignite.internal.processors.cache.RebalanceWithDifferentThreadPoolSizeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteRejectConnectOnNodeStopTest;
import org.apache.ignite.internal.processors.cache.persistence.defragmentation.LinkMapTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.ClockPageReplacementFlagsTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.ExponentialBackoffTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagePoolTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.ProgressSpeedCalculationTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.SegmentedLruPageListTest;
import org.apache.ignite.internal.processors.cache.transactions.AtomicOperationsInTxTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionIntegrityWithSystemWorkerDeathTest;
import org.apache.ignite.internal.processors.cluster.BaselineAutoAdjustMXBeanTest;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationInMemoryTest;
import org.apache.ignite.internal.processors.database.BPlusTreeFakeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeReplaceRemoveRaceTest;
import org.apache.ignite.internal.processors.database.BPlusTreeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeSelfTest;
import org.apache.ignite.internal.processors.database.CacheFreeListSelfTest;
import org.apache.ignite.internal.processors.database.DataRegionMetricsSelfTest;
import org.apache.ignite.internal.processors.database.IndexStorageSelfTest;
import org.apache.ignite.internal.processors.database.SwapPathConstructionSelfTest;
import org.apache.ignite.internal.processors.failure.FailureProcessorLoggingTest;
import org.apache.ignite.internal.processors.failure.FailureProcessorThreadDumpThrottlingTest;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorageClassloadingTest;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorageTest;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageHistoryCacheTest;
import org.apache.ignite.internal.processors.metastorage.persistence.DmsDataWriterWorkerTest;
import org.apache.ignite.internal.processors.metastorage.persistence.InMemoryCachedDistributedMetaStorageBridgeTest;
import org.apache.ignite.internal.util.collection.BitSetIntSetTest;
import org.apache.ignite.internal.util.collection.ImmutableIntSetTest;
import org.apache.ignite.internal.util.collection.IntHashMapTest;
import org.apache.ignite.internal.util.collection.IntRWHashMapTest;
import org.apache.ignite.marshaller.DynamicProxySerializationMultiJvmSelfTest;
import org.apache.ignite.marshaller.MarshallerContextSelfTest;
import org.apache.ignite.plugin.PluginConfigurationTest;
import org.apache.ignite.plugin.PluginNodeValidationTest;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilderTest;
import org.apache.ignite.spi.checkpoint.noop.NoopCheckpointSpiLoggingTest;
import org.apache.ignite.startup.properties.NotStringSystemPropertyTest;
import org.apache.ignite.testframework.MemorizingAppenderTest;
import org.apache.ignite.testframework.MessageOrderLogListenerTest;
import org.apache.ignite.testframework.test.ConfigVariationsExecutionTest;
import org.apache.ignite.testframework.test.ConfigVariationsTestSuiteBuilderTest;
import org.apache.ignite.testframework.test.ListeningTestLoggerTest;
import org.apache.ignite.testframework.test.ParametersTest;
import org.apache.ignite.testframework.test.VariationsIteratorTest;
import org.apache.ignite.util.AttributeNodeFilterSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Basic test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    DynamicProxySerializationMultiJvmSelfTest.class,

    MarshallerContextLockingSelfTest.class,
    MarshallerContextSelfTest.class,

    SecurityPermissionSetBuilderTest.class,

    AttributeNodeFilterSelfTest.class,

    WALRecordTest.class,
    WALRecordSerializationTest.class,

    GridPeerDeploymentRetryTest.class,
    GridPeerDeploymentRetryModifiedTest.class,

    // Basic DB data structures.
    PagePoolTest.class,
    SegmentedLruPageListTest.class,
    ClockPageReplacementFlagsTest.class,
    BPlusTreeSelfTest.class,
    BPlusTreeFakeReuseSelfTest.class,
    BPlusTreeReuseSelfTest.class,
    BPlusTreeReplaceRemoveRaceTest.class,
    IndexStorageSelfTest.class,
    CacheFreeListSelfTest.class,
    DataRegionMetricsSelfTest.class,
    MetricsClusterActivationTest.class,
    MetricConfigurationTest.class,
    SwapPathConstructionSelfTest.class,
    BitSetIntSetTest.class,
    ImmutableIntSetTest.class,
    IntHashMapTest.class,
    IntRWHashMapTest.class,

    IgniteMarshallerCacheFSRestoreTest.class,
    IgniteMarshallerCacheClassNameConflictTest.class,
    IgniteMarshallerCacheClientRequestsMappingOnMissTest.class,
    IgniteMarshallerCacheClientRequestsMappingTest.class,
    IgniteMarshallerCacheSeparateDirectoryTest.class,

    IgniteDiagnosticMessagesTest.class,
    IgniteDiagnosticMessagesMultipleConnectionsTest.class,
    IgniteDiagnosticPartitionReleaseFutureLimitTest.class,

    IgniteRejectConnectOnNodeStopTest.class,

    ClassSetTest.class,

    // Basic failure handlers.
    FailureHandlerTriggeredTest.class,
    StopNodeFailureHandlerTest.class,
    StopNodeOrHaltFailureHandlerTest.class,
    OomFailureHandlerTest.class,
    TransactionIntegrityWithSystemWorkerDeathTest.class,
    FailureProcessorLoggingTest.class,
    FailureProcessorThreadDumpThrottlingTest.class,
    ExchangeWorkerWaitingForTaskTest.class,

    AtomicOperationsInTxTest.class,

    RebalanceWithDifferentThreadPoolSizeTest.class,

    ListeningTestLoggerTest.class,
    GridLongRunningInitNewCrdFutureDiagnosticsTest.class,

    MessageOrderLogListenerTest.class,

    MemorizingAppenderTest.class,

    CacheLocalGetSerializationTest.class,

    PluginNodeValidationTest.class,
    PluginConfigurationTest.class,

    // In-memory Distributed MetaStorage.
    DistributedMetaStorageTest.class,
    DistributedMetaStorageHistoryCacheTest.class,
    DistributedMetaStorageClassloadingTest.class,
    DmsDataWriterWorkerTest.class,
    InMemoryCachedDistributedMetaStorageBridgeTest.class,
    DistributedConfigurationInMemoryTest.class,
    BaselineAutoAdjustMXBeanTest.class,

    ConsistentIdImplicitlyExplicitlyTest.class,
    DiagnosticLogForPartitionStatesTest.class,

    // Tests against configuration variations framework.
    ParametersTest.class,
    VariationsIteratorTest.class,
    NotStringSystemPropertyTest.class,
    ConfigVariationsExecutionTest.class,
    ConfigVariationsTestSuiteBuilderTest.class,

    DeadLockOnNodeLeftExchangeTest.class,

    ClassPathContentLoggingTest.class,

    IncompleteDeserializationExceptionTest.class,

    GridIoManagerFileTransmissionSelfTest.class,

    IgniteStandardMXBeanTest.class,

    ClusterActivationStartedEventTest.class,

    IgniteThreadGroupNodeRestartTest.class,

    LinkMapTest.class,

    // Other tests
    CacheLockCandidatesThreadTest.class,
    RemoveAllDeadlockTest.class,

    NoopCheckpointSpiLoggingTest.class,
    ExponentialBackoffTest.class,
    ProgressSpeedCalculationTest.class,

    ConcurrentMappingFileReadWriteTest.class,
    PeriodicHistogramMetricImplTest.class,
})
public class IgniteBasicTestSuite2 {
}
