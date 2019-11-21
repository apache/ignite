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

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Basic test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
//    IgniteMarshallerSelfTestSuite.class,
//    IgniteLangSelfTestSuite.class,
//    IgniteUtilSelfTestSuite.class,
//
//    IgniteKernalSelfTestSuite.class,
//    IgniteStartUpTestSuite.class,
//    IgniteExternalizableSelfTestSuite.class,
//    IgniteP2PSelfTestSuite.class,
//    IgniteCacheP2pUnmarshallingErrorTestSuite.class,
//    IgniteStreamSelfTestSuite.class,
//
//    IgnitePlatformsTestSuite.class,

    SecurityTestSuite.class,
//
//    GridSelfTest.class,
//    ClusterGroupHostsSelfTest.class,
//    IgniteMessagingWithClientTest.class,
//    IgniteMessagingSendAsyncTest.class,
//
//    ClusterProcessorCheckGlobalStateComputeRequestTest.class,
//    ClusterGroupSelfTest.class,
//    GridMessagingSelfTest.class,
//    GridMessagingNoPeerClassLoadingSelfTest.class,
//
//    GridReleaseTypeSelfTest.class,
//    GridProductVersionSelfTest.class,
//    GridAffinityAssignmentV2Test.class,
//    GridAffinityAssignmentV2TestNoOptimizations.class,
//    GridHistoryAffinityAssignmentTest.class,
//    GridHistoryAffinityAssignmentTestNoOptimization.class,
//    GridAffinityProcessorRendezvousSelfTest.class,
//    GridAffinityProcessorMemoryLeakTest.class,
//    GridClosureProcessorSelfTest.class,
//    GridClosureProcessorRemoteTest.class,
//    GridClosureSerializationTest.class,
//    GridStartStopSelfTest.class,
//    GridProjectionForCachesSelfTest.class,
//    GridProjectionForCachesOnDaemonNodeSelfTest.class,
//    GridSpiLocalHostInjectionTest.class,
//    GridLifecycleBeanSelfTest.class,
//    GridStopWithCancelSelfTest.class,
//    GridReduceSelfTest.class,
//    GridEventConsumeSelfTest.class,
//    GridSuppressedExceptionSelfTest.class,
//    GridLifecycleAwareSelfTest.class,
//    GridMessageListenSelfTest.class,
//    GridFailFastNodeFailureDetectionSelfTest.class,
//    IgniteSlowClientDetectionSelfTest.class,
//    IgniteDaemonNodeMarshallerCacheTest.class,
//    IgniteMarshallerCacheConcurrentReadWriteTest.class,
//    GridNodeMetricsLogSelfTest.class,
//    GridLocalIgniteSerializationTest.class,
//    GridMBeansTest.class,
//    TransactionsMXBeanImplTest.class,
//    SetTxTimeoutOnPartitionMapExchangeTest.class,
//    DiscoveryDataDeserializationFailureHanderTest.class,
//
//    PartitionsEvictionTaskFailureHandlerTest.class,
//    DropCacheContextDuringEvictionTest.class,
//
//    IgniteExceptionInNioWorkerSelfTest.class,
//    IgniteLocalNodeMapBeforeStartTest.class,
//    OdbcConfigurationValidationSelfTest.class,
//    OdbcEscapeSequenceSelfTest.class,
//
//    DynamicProxySerializationMultiJvmSelfTest.class,
//
//    MarshallerContextLockingSelfTest.class,
//    MarshallerContextSelfTest.class,
//
//    SecurityPermissionSetBuilderTest.class,
//
//    AttributeNodeFilterSelfTest.class,
//
//    WALRecordTest.class,
//
//    GridPeerDeploymentRetryTest.class,
//    GridPeerDeploymentRetryModifiedTest.class,
//
//    // Basic DB data structures.
//    BPlusTreeSelfTest.class,
//    BPlusTreeFakeReuseSelfTest.class,
//    BPlusTreeReuseSelfTest.class,
//    IndexStorageSelfTest.class,
//    CacheFreeListSelfTest.class,
//    DataRegionMetricsSelfTest.class,
//    SwapPathConstructionSelfTest.class,
//    BitSetIntSetTest.class,
//    ImmutableIntSetTest.class,
//    IntHashMapTest.class,
//    IntRWHashMapTest.class,
//
//    IgniteMarshallerCacheFSRestoreTest.class,
//    IgniteMarshallerCacheClassNameConflictTest.class,
//    IgniteMarshallerCacheClientRequestsMappingOnMissTest.class,
//
//    IgniteDiagnosticMessagesTest.class,
//    IgniteDiagnosticMessagesMultipleConnectionsTest.class,
//
//    IgniteRejectConnectOnNodeStopTest.class,
//
//    GridCleanerTest.class,
//
//    ClassSetTest.class,
//
//    // Basic failure handlers.
//    FailureHandlerTriggeredTest.class,
//    StopNodeFailureHandlerTest.class,
//    StopNodeOrHaltFailureHandlerTest.class,
//    OomFailureHandlerTest.class,
//    TransactionIntegrityWithSystemWorkerDeathTest.class,
//
//    AtomicOperationsInTxTest.class,
//
//    RebalanceWithDifferentThreadPoolSizeTest.class,
//
//    ListeningTestLoggerTest.class,
//
//    MessageOrderLogListenerTest.class,
//
//    CacheLocalGetSerializationTest.class,
//
//    PluginNodeValidationTest.class,
//
//    // In-memory Distributed MetaStorage.
//    DistributedMetaStorageTest.class,
//    DistributedMetaStorageHistoryCacheTest.class,
//    DmsDataWriterWorkerTest.class,
//    InMemoryCachedDistributedMetaStorageBridgeTest.class,
//    DistributedConfigurationInMemoryTest.class,
//    BaselineAutoAdjustMXBeanTest.class,
//
//    ConsistentIdImplicitlyExplicitlyTest.class,
//    DiagnosticLogForPartitionStatesTest.class,
//
//    // Tests against configuration variations framework.
//    ParametersTest.class,
//    VariationsIteratorTest.class,
//    NotStringSystemPropertyTest.class,
//    ConfigVariationsExecutionTest.class,
//    ConfigVariationsTestSuiteBuilderTest.class,
//
//    DeadLockOnNodeLeftExchangeTest.class,
//
//    ClassPathContentLoggingTest.class,
//
//    IncompleteDeserializationExceptionTest.class,
//
//    GridIoManagerFileTransmissionSelfTest.class
})
public class IgniteBasicTestSuite {
}
