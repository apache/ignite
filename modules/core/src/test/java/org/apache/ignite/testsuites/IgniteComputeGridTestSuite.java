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

import org.apache.ignite.internal.GridJobCheckpointCleanupSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Compute grid test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    //IgniteTaskSessionSelfTestSuite.class,
    //IgniteTimeoutProcessorSelfTestSuite.class,
    //IgniteJobMetricsSelfTestSuite.class,
    //IgniteContinuousTaskSelfTestSuite.class,

    //GridTaskCancelSingleNodeSelfTest.class,
    //GridTaskFailoverSelfTest.class,
    //GridJobCollisionCancelSelfTest.class,
    //GridTaskTimeoutSelfTest.class,
    //GridCancelUnusedJobSelfTest.class,
    //GridTaskJobRejectSelfTest.class,
    //GridTaskExecutionSelfTest.class,
    //GridTaskExecutionContextSelfTest.class,
    //GridTaskExecutionWithoutPeerClassLoadingSelfTest.class,
    //GridFailoverSelfTest.class,
    //GridTaskListenerSelfTest.class,
    //GridFailoverTopologySelfTest.class,
    //GridTaskResultCacheSelfTest.class,
    //GridTaskMapAsyncSelfTest.class,
    //GridJobContextSelfTest.class,
    //GridJobMasterLeaveAwareSelfTest.class,
    //GridJobStealingSelfTest.class,
    //GridJobSubjectIdSelfTest.class,
    //GridMultithreadedJobStealingSelfTest.class,
    //GridAlwaysFailoverSpiFailSelfTest.class,
    //GridTaskInstanceExecutionSelfTest.class,
    //ClusterNodeMetricsSelfTest.class,
    //ClusterNodeMetricsUpdateTest.class,
    //GridNonHistoryMetricsSelfTest.class,
    //GridCancelledJobsMetricsSelfTest.class,
    //GridCollisionJobsContextSelfTest.class,
    //GridJobStealingZeroActiveJobsSelfTest.class,
    //GridTaskFutureImplStopGridSelfTest.class,
    //GridFailoverCustomTopologySelfTest.class,
    //GridMultipleSpisSelfTest.class,
    //GridStopWithWaitSelfTest.class,
    //GridCancelOnGridStopSelfTest.class,
    //GridDeploymentSelfTest.class,
    //GridDeploymentMultiThreadedSelfTest.class,
    //GridMultipleVersionsDeploymentSelfTest.class,
    //IgniteExplicitImplicitDeploymentSelfTest.class,
    //GridEventStorageCheckAllEventsSelfTest.class,
    //GridCommunicationManagerListenersSelfTest.class,
    //IgniteExecutorServiceTest.class,
    //GridTaskInstantiationSelfTest.class,
    //GridMultipleJobsSelfTest.class,
    //GridCheckpointManagerSelfTest.class,
    //GridCheckpointTaskSelfTest.class,
    //GridTaskNameAnnotationSelfTest.class,
    GridJobCheckpointCleanupSelfTest.class
    //GridEventStorageSelfTest.class,
    //GridEventStorageDefaultExceptionTest.class,
    //GridFailoverTaskWithPredicateSelfTest.class,
    //GridProjectionLocalJobMultipleArgumentsSelfTest.class,
    //GridAffinitySelfTest.class,
    //GridAffinityNoCacheSelfTest.class,
    //GridAffinityMappedTest.class,
    //GridAffinityP2PSelfTest.class,
    //GridEventStorageRuntimeConfigurationSelfTest.class,
    //GridMultinodeRedeployContinuousModeSelfTest.class,
    //GridMultinodeRedeploySharedModeSelfTest.class,
    //GridMultinodeRedeployPrivateModeSelfTest.class,
    //GridMultinodeRedeployIsolatedModeSelfTest.class,
    //IgniteComputeEmptyClusterGroupTest.class,
    //IgniteComputeTopologyExceptionTest.class,
    //IgniteComputeResultExceptionTest.class,
    //GridTaskFailoverAffinityRunTest.class,
    //TaskNodeRestartTest.class,
    //IgniteRoundRobinErrorAfterClientReconnectTest.class,
    //PublicThreadpoolStarvationTest.class,
    //StripedExecutorTest.class,
    //GridJobServicesAddNodeTest.class,

    //IgniteComputeCustomExecutorConfigurationSelfTest.class,
    //IgniteComputeCustomExecutorSelfTest.class,

    //IgniteComputeJobOneThreadTest.class,

    //VisorManagementEventSelfTest.class
})
public class IgniteComputeGridTestSuite {
}
