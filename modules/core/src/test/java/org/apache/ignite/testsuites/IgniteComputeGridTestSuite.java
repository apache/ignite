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

import org.apache.ignite.internal.ClusterNodeMetricsSelfTest;
import org.apache.ignite.internal.ClusterNodeMetricsUpdateTest;
import org.apache.ignite.internal.GridAffinityNoCacheSelfTest;
import org.apache.ignite.internal.GridAffinitySelfTest;
import org.apache.ignite.internal.GridAlwaysFailoverSpiFailSelfTest;
import org.apache.ignite.internal.GridCancelOnGridStopSelfTest;
import org.apache.ignite.internal.GridCancelUnusedJobSelfTest;
import org.apache.ignite.internal.GridCancelledJobsMetricsSelfTest;
import org.apache.ignite.internal.GridCollisionJobsContextSelfTest;
import org.apache.ignite.internal.GridDeploymentMultiThreadedSelfTest;
import org.apache.ignite.internal.GridDeploymentSelfTest;
import org.apache.ignite.internal.GridEventStorageCheckAllEventsSelfTest;
import org.apache.ignite.internal.GridEventStorageDefaultExceptionTest;
import org.apache.ignite.internal.GridEventStorageRuntimeConfigurationSelfTest;
import org.apache.ignite.internal.GridEventStorageSelfTest;
import org.apache.ignite.internal.GridFailoverCustomTopologySelfTest;
import org.apache.ignite.internal.GridFailoverSelfTest;
import org.apache.ignite.internal.GridFailoverTaskWithPredicateSelfTest;
import org.apache.ignite.internal.GridFailoverTopologySelfTest;
import org.apache.ignite.internal.GridJobCheckpointCleanupSelfTest;
import org.apache.ignite.internal.GridJobCollisionCancelSelfTest;
import org.apache.ignite.internal.GridJobContextSelfTest;
import org.apache.ignite.internal.GridJobMasterLeaveAwareSelfTest;
import org.apache.ignite.internal.GridJobServicesAddNodeTest;
import org.apache.ignite.internal.GridJobStealingSelfTest;
import org.apache.ignite.internal.GridJobStealingZeroActiveJobsSelfTest;
import org.apache.ignite.internal.GridJobSubjectIdSelfTest;
import org.apache.ignite.internal.GridMultipleJobsSelfTest;
import org.apache.ignite.internal.GridMultipleSpisSelfTest;
import org.apache.ignite.internal.GridMultipleVersionsDeploymentSelfTest;
import org.apache.ignite.internal.GridMultithreadedJobStealingSelfTest;
import org.apache.ignite.internal.GridNonHistoryMetricsSelfTest;
import org.apache.ignite.internal.GridProjectionLocalJobMultipleArgumentsSelfTest;
import org.apache.ignite.internal.GridStopWithWaitSelfTest;
import org.apache.ignite.internal.GridTaskCancelSingleNodeSelfTest;
import org.apache.ignite.internal.GridTaskExecutionSelfTest;
import org.apache.ignite.internal.GridTaskExecutionWithoutPeerClassLoadingSelfTest;
import org.apache.ignite.internal.GridTaskFailoverAffinityRunTest;
import org.apache.ignite.internal.GridTaskFailoverSelfTest;
import org.apache.ignite.internal.GridTaskFutureImplStopGridSelfTest;
import org.apache.ignite.internal.GridTaskInstanceExecutionSelfTest;
import org.apache.ignite.internal.GridTaskInstantiationSelfTest;
import org.apache.ignite.internal.GridTaskJobRejectSelfTest;
import org.apache.ignite.internal.GridTaskListenerSelfTest;
import org.apache.ignite.internal.GridTaskMapAsyncSelfTest;
import org.apache.ignite.internal.GridTaskNameAnnotationSelfTest;
import org.apache.ignite.internal.GridTaskResultCacheSelfTest;
import org.apache.ignite.internal.GridTaskTimeoutSelfTest;
import org.apache.ignite.internal.IgniteComputeEmptyClusterGroupTest;
import org.apache.ignite.internal.IgniteComputeJobOneThreadTest;
import org.apache.ignite.internal.IgniteComputeResultExceptionTest;
import org.apache.ignite.internal.IgniteComputeTopologyExceptionTest;
import org.apache.ignite.internal.IgniteExecutorServiceTest;
import org.apache.ignite.internal.IgniteExplicitImplicitDeploymentSelfTest;
import org.apache.ignite.internal.IgniteRoundRobinErrorAfterClientReconnectTest;
import org.apache.ignite.internal.TaskNodeRestartTest;
import org.apache.ignite.internal.VisorManagementEventSelfTest;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointManagerSelfTest;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointTaskSelfTest;
import org.apache.ignite.internal.managers.communication.GridCommunicationManagerListenersSelfTest;
import org.apache.ignite.internal.processors.compute.IgniteComputeCustomExecutorConfigurationSelfTest;
import org.apache.ignite.internal.processors.compute.IgniteComputeCustomExecutorSelfTest;
import org.apache.ignite.internal.processors.compute.PublicThreadpoolStarvationTest;
import org.apache.ignite.internal.util.StripedExecutorTest;
import org.apache.ignite.p2p.GridMultinodeRedeployContinuousModeSelfTest;
import org.apache.ignite.p2p.GridMultinodeRedeployIsolatedModeSelfTest;
import org.apache.ignite.p2p.GridMultinodeRedeployPrivateModeSelfTest;
import org.apache.ignite.p2p.GridMultinodeRedeploySharedModeSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Compute grid test suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    IgniteTaskSessionSelfTestSuite.class,
    IgniteTimeoutProcessorSelfTestSuite.class,
    IgniteJobMetricsSelfTestSuite.class,
    IgniteContinuousTaskSelfTestSuite.class,

    GridTaskCancelSingleNodeSelfTest.class,
    GridTaskFailoverSelfTest.class,
    GridJobCollisionCancelSelfTest.class,
    GridTaskTimeoutSelfTest.class,
    GridCancelUnusedJobSelfTest.class,
    GridTaskJobRejectSelfTest.class,
    GridTaskExecutionSelfTest.class,
    //GridTaskExecutionContextSelfTest.class,
    GridTaskExecutionWithoutPeerClassLoadingSelfTest.class,
    GridFailoverSelfTest.class,
    GridTaskListenerSelfTest.class,
    GridFailoverTopologySelfTest.class,
    GridTaskResultCacheSelfTest.class,
    GridTaskMapAsyncSelfTest.class,
    GridJobContextSelfTest.class,
    GridJobMasterLeaveAwareSelfTest.class,
    GridJobStealingSelfTest.class,
    GridJobSubjectIdSelfTest.class,
    GridMultithreadedJobStealingSelfTest.class,
    GridAlwaysFailoverSpiFailSelfTest.class,
    GridTaskInstanceExecutionSelfTest.class,
    ClusterNodeMetricsSelfTest.class,
    ClusterNodeMetricsUpdateTest.class,
    GridNonHistoryMetricsSelfTest.class,
    GridCancelledJobsMetricsSelfTest.class,
    GridCollisionJobsContextSelfTest.class,
    GridJobStealingZeroActiveJobsSelfTest.class,
    GridTaskFutureImplStopGridSelfTest.class,
    GridFailoverCustomTopologySelfTest.class,
    GridMultipleSpisSelfTest.class,
    GridStopWithWaitSelfTest.class,
    GridCancelOnGridStopSelfTest.class,
    GridDeploymentSelfTest.class,
    GridDeploymentMultiThreadedSelfTest.class,
    GridMultipleVersionsDeploymentSelfTest.class,
    IgniteExplicitImplicitDeploymentSelfTest.class,
    GridEventStorageCheckAllEventsSelfTest.class,
    GridCommunicationManagerListenersSelfTest.class,
    IgniteExecutorServiceTest.class,
    GridTaskInstantiationSelfTest.class,
    GridMultipleJobsSelfTest.class,
    GridCheckpointManagerSelfTest.class,
    GridCheckpointTaskSelfTest.class,
    GridTaskNameAnnotationSelfTest.class,
    GridJobCheckpointCleanupSelfTest.class,
    GridEventStorageSelfTest.class,
    GridEventStorageDefaultExceptionTest.class,
    GridFailoverTaskWithPredicateSelfTest.class,
    GridProjectionLocalJobMultipleArgumentsSelfTest.class,
    GridAffinitySelfTest.class,
    GridAffinityNoCacheSelfTest.class,
    //GridAffinityMappedTest.class,
    //GridAffinityP2PSelfTest.class,
    GridEventStorageRuntimeConfigurationSelfTest.class,
    GridMultinodeRedeployContinuousModeSelfTest.class,
    GridMultinodeRedeploySharedModeSelfTest.class,
    GridMultinodeRedeployPrivateModeSelfTest.class,
    GridMultinodeRedeployIsolatedModeSelfTest.class,
    IgniteComputeEmptyClusterGroupTest.class,
    IgniteComputeTopologyExceptionTest.class,
    IgniteComputeResultExceptionTest.class,
    GridTaskFailoverAffinityRunTest.class,
    TaskNodeRestartTest.class,
    IgniteRoundRobinErrorAfterClientReconnectTest.class,
    PublicThreadpoolStarvationTest.class,
    StripedExecutorTest.class,
    GridJobServicesAddNodeTest.class,

    IgniteComputeCustomExecutorConfigurationSelfTest.class,
    IgniteComputeCustomExecutorSelfTest.class,

    IgniteComputeJobOneThreadTest.class,

    VisorManagementEventSelfTest.class
})
public class IgniteComputeGridTestSuite {
}
