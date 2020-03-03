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
import org.apache.ignite.GridSuppressedExceptionSelfTest;
import org.apache.ignite.failure.FailureHandlerTriggeredTest;
import org.apache.ignite.failure.OomFailureHandlerTest;
import org.apache.ignite.failure.StopNodeFailureHandlerTest;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandlerTest;
import org.apache.ignite.internal.ClassSetTest;
import org.apache.ignite.internal.ClusterGroupHostsSelfTest;
import org.apache.ignite.internal.ClusterGroupSelfTest;
import org.apache.ignite.internal.ClusterProcessorCheckGlobalStateComputeRequestTest;
import org.apache.ignite.internal.ConsistentIdImplicitlyExplicitlyTest;
import org.apache.ignite.internal.DiagnosticLogForPartitionStatesTest;
import org.apache.ignite.internal.GridFailFastNodeFailureDetectionSelfTest;
import org.apache.ignite.internal.GridLifecycleAwareSelfTest;
import org.apache.ignite.internal.GridLifecycleBeanSelfTest;
import org.apache.ignite.internal.GridMBeansTest;
import org.apache.ignite.internal.GridMbeansMiscTest;
import org.apache.ignite.internal.GridNodeMetricsLogSelfTest;
import org.apache.ignite.internal.GridPeerDeploymentRetryModifiedTest;
import org.apache.ignite.internal.GridPeerDeploymentRetryTest;
import org.apache.ignite.internal.GridProjectionForCachesSelfTest;
import org.apache.ignite.internal.GridReduceSelfTest;
import org.apache.ignite.internal.GridReleaseTypeSelfTest;
import org.apache.ignite.internal.GridSelfTest;
import org.apache.ignite.internal.GridStartStopSelfTest;
import org.apache.ignite.internal.GridStopWithCancelSelfTest;
import org.apache.ignite.internal.IgniteLocalNodeMapBeforeStartTest;
import org.apache.ignite.internal.IgniteSlowClientDetectionSelfTest;
import org.apache.ignite.internal.MarshallerContextLockingSelfTest;
import org.apache.ignite.internal.TransactionsMXBeanImplTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesMultipleConnectionsTest;
import org.apache.ignite.internal.managers.IgniteDiagnosticMessagesTest;
import org.apache.ignite.internal.managers.communication.GridIoManagerFileTransmissionSelfTest;
import org.apache.ignite.internal.managers.discovery.IncompleteDeserializationExceptionTest;
import org.apache.ignite.internal.mxbean.IgniteStandardMXBeanTest;
import org.apache.ignite.internal.pagemem.wal.record.WALRecordSerializationTest;
import org.apache.ignite.internal.pagemem.wal.record.WALRecordTest;
import org.apache.ignite.internal.processors.DeadLockOnNodeLeftExchangeTest;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentV2Test;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentV2TestNoOptimizations;
import org.apache.ignite.internal.processors.affinity.GridAffinityProcessorRendezvousSelfTest;
import org.apache.ignite.internal.processors.affinity.GridHistoryAffinityAssignmentTest;
import org.apache.ignite.internal.processors.affinity.GridHistoryAffinityAssignmentTestNoOptimization;
import org.apache.ignite.internal.processors.cache.CacheLocalGetSerializationTest;
import org.apache.ignite.internal.processors.cache.GridLocalIgniteSerializationTest;
import org.apache.ignite.internal.processors.cache.GridProjectionForCachesOnDaemonNodeSelfTest;
import org.apache.ignite.internal.processors.cache.IgniteDaemonNodeMarshallerCacheTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClassNameConflictTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheClientRequestsMappingOnMissTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheConcurrentReadWriteTest;
import org.apache.ignite.internal.processors.cache.IgniteMarshallerCacheFSRestoreTest;
import org.apache.ignite.internal.processors.cache.RebalanceWithDifferentThreadPoolSizeTest;
import org.apache.ignite.internal.processors.cache.SetTxTimeoutOnPartitionMapExchangeTest;
import org.apache.ignite.internal.processors.cache.distributed.IgniteRejectConnectOnNodeStopTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.DropCacheContextDuringEvictionTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.EvictPartitionInLogTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.PartitionsEvictionTaskFailureHandlerTest;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PagePoolTest;
import org.apache.ignite.internal.processors.cache.query.continuous.DiscoveryDataDeserializationFailureHanderTest;
import org.apache.ignite.internal.processors.cache.transactions.AtomicOperationsInTxTest;
import org.apache.ignite.internal.processors.cache.transactions.TransactionIntegrityWithSystemWorkerDeathTest;
import org.apache.ignite.internal.processors.closure.GridClosureProcessorRemoteTest;
import org.apache.ignite.internal.processors.closure.GridClosureProcessorSelfTest;
import org.apache.ignite.internal.processors.closure.GridClosureSerializationTest;
import org.apache.ignite.internal.processors.cluster.BaselineAutoAdjustMXBeanTest;
import org.apache.ignite.internal.processors.configuration.distributed.DistributedConfigurationInMemoryTest;
import org.apache.ignite.internal.processors.continuous.GridEventConsumeSelfTest;
import org.apache.ignite.internal.processors.continuous.GridMessageListenSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeFakeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeReuseSelfTest;
import org.apache.ignite.internal.processors.database.BPlusTreeSelfTest;
import org.apache.ignite.internal.processors.database.CacheFreeListSelfTest;
import org.apache.ignite.internal.processors.database.DataRegionMetricsSelfTest;
import org.apache.ignite.internal.processors.database.IndexStorageSelfTest;
import org.apache.ignite.internal.processors.database.SwapPathConstructionSelfTest;
import org.apache.ignite.internal.processors.failure.FailureProcessorLoggingTest;
import org.apache.ignite.internal.processors.failure.FailureProcessorThreadDumpThrottlingTest;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorageTest;
import org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageHistoryCacheTest;
import org.apache.ignite.internal.processors.metastorage.persistence.DmsDataWriterWorkerTest;
import org.apache.ignite.internal.processors.metastorage.persistence.InMemoryCachedDistributedMetaStorageBridgeTest;
import org.apache.ignite.internal.processors.odbc.OdbcConfigurationValidationSelfTest;
import org.apache.ignite.internal.processors.odbc.OdbcEscapeSequenceSelfTest;
import org.apache.ignite.internal.processors.odbc.SqlListenerUtilsTest;
import org.apache.ignite.internal.product.GridProductVersionSelfTest;
import org.apache.ignite.internal.util.GridCleanerTest;
import org.apache.ignite.internal.util.collection.BitSetIntSetTest;
import org.apache.ignite.internal.util.collection.ImmutableIntSetTest;
import org.apache.ignite.internal.util.collection.IntHashMapTest;
import org.apache.ignite.internal.util.collection.IntRWHashMapTest;
import org.apache.ignite.internal.util.nio.IgniteExceptionInNioWorkerSelfTest;
import org.apache.ignite.marshaller.DynamicProxySerializationMultiJvmSelfTest;
import org.apache.ignite.marshaller.MarshallerContextSelfTest;
import org.apache.ignite.messaging.GridMessagingNoPeerClassLoadingSelfTest;
import org.apache.ignite.messaging.GridMessagingSelfTest;
import org.apache.ignite.messaging.IgniteMessagingSendAsyncTest;
import org.apache.ignite.messaging.IgniteMessagingWithClientTest;
import org.apache.ignite.plugin.PluginConfigurationTest;
import org.apache.ignite.plugin.PluginNodeValidationTest;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilderTest;
import org.apache.ignite.spi.GridSpiLocalHostInjectionTest;
import org.apache.ignite.startup.properties.NotStringSystemPropertyTest;
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
    IgniteMarshallerSelfTestSuite.class,
    IgniteLangSelfTestSuite.class,
    IgniteUtilSelfTestSuite.class,

    IgniteKernalSelfTestSuite.class,
    IgniteStartUpTestSuite.class,
    IgniteExternalizableSelfTestSuite.class,
    IgniteP2PSelfTestSuite.class,
    IgniteCacheP2pUnmarshallingErrorTestSuite.class,
    IgniteStreamSelfTestSuite.class,

    IgnitePlatformsTestSuite.class,

    GridSelfTest.class,
    ClusterGroupHostsSelfTest.class,
    IgniteMessagingWithClientTest.class,
    IgniteMessagingSendAsyncTest.class,

    ClusterProcessorCheckGlobalStateComputeRequestTest.class,
    ClusterGroupSelfTest.class,
    GridMessagingSelfTest.class,
    GridMessagingNoPeerClassLoadingSelfTest.class,

    GridReleaseTypeSelfTest.class,
    GridProductVersionSelfTest.class,
    GridAffinityAssignmentV2Test.class,
    GridAffinityAssignmentV2TestNoOptimizations.class,
    GridHistoryAffinityAssignmentTest.class,
    GridHistoryAffinityAssignmentTestNoOptimization.class,
    GridAffinityProcessorRendezvousSelfTest.class,
    GridClosureProcessorSelfTest.class,
    GridClosureProcessorRemoteTest.class,
    GridClosureSerializationTest.class,
    GridStartStopSelfTest.class,
    GridProjectionForCachesSelfTest.class,
    GridProjectionForCachesOnDaemonNodeSelfTest.class,
    GridSpiLocalHostInjectionTest.class,
    GridLifecycleBeanSelfTest.class,
    GridStopWithCancelSelfTest.class,
    GridReduceSelfTest.class,
    GridEventConsumeSelfTest.class,
    GridSuppressedExceptionSelfTest.class,
    GridLifecycleAwareSelfTest.class,
    GridMessageListenSelfTest.class,
    GridFailFastNodeFailureDetectionSelfTest.class,
    IgniteSlowClientDetectionSelfTest.class,
    IgniteDaemonNodeMarshallerCacheTest.class,
    IgniteMarshallerCacheConcurrentReadWriteTest.class,
    GridNodeMetricsLogSelfTest.class,
    GridLocalIgniteSerializationTest.class,
    GridMBeansTest.class,
    GridMbeansMiscTest.class,
    TransactionsMXBeanImplTest.class,
    SetTxTimeoutOnPartitionMapExchangeTest.class,
    DiscoveryDataDeserializationFailureHanderTest.class,

    PartitionsEvictionTaskFailureHandlerTest.class,
    DropCacheContextDuringEvictionTest.class,
    EvictPartitionInLogTest.class,

    IgniteExceptionInNioWorkerSelfTest.class,
    IgniteLocalNodeMapBeforeStartTest.class,
    OdbcConfigurationValidationSelfTest.class,
    OdbcEscapeSequenceSelfTest.class,
    SqlListenerUtilsTest.class,

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
    BPlusTreeSelfTest.class,
    BPlusTreeFakeReuseSelfTest.class,
    BPlusTreeReuseSelfTest.class,
    IndexStorageSelfTest.class,
    CacheFreeListSelfTest.class,
    DataRegionMetricsSelfTest.class,
    SwapPathConstructionSelfTest.class,
    BitSetIntSetTest.class,
    ImmutableIntSetTest.class,
    IntHashMapTest.class,
    IntRWHashMapTest.class,

    IgniteMarshallerCacheFSRestoreTest.class,
    IgniteMarshallerCacheClassNameConflictTest.class,
    IgniteMarshallerCacheClientRequestsMappingOnMissTest.class,

    IgniteDiagnosticMessagesTest.class,
    IgniteDiagnosticMessagesMultipleConnectionsTest.class,

    IgniteRejectConnectOnNodeStopTest.class,

    GridCleanerTest.class,

    ClassSetTest.class,

    // Basic failure handlers.
    FailureHandlerTriggeredTest.class,
    StopNodeFailureHandlerTest.class,
    StopNodeOrHaltFailureHandlerTest.class,
    OomFailureHandlerTest.class,
    TransactionIntegrityWithSystemWorkerDeathTest.class,
    FailureProcessorLoggingTest.class,
    FailureProcessorThreadDumpThrottlingTest.class,

    AtomicOperationsInTxTest.class,

    RebalanceWithDifferentThreadPoolSizeTest.class,

    ListeningTestLoggerTest.class,

    MessageOrderLogListenerTest.class,

    CacheLocalGetSerializationTest.class,

    PluginNodeValidationTest.class,
    PluginConfigurationTest.class,

    // In-memory Distributed MetaStorage.
    DistributedMetaStorageTest.class,
    DistributedMetaStorageHistoryCacheTest.class,
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

    IgniteStandardMXBeanTest.class
})
public class IgniteBasicTestSuite {
}
