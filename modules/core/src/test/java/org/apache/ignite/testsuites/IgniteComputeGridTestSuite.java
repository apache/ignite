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

import junit.framework.TestSuite;
import org.apache.ignite.internal.ClusterNodeMetricsSelfTest;
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
import org.apache.ignite.internal.IgniteComputeTopologyExceptionTest;
import org.apache.ignite.internal.IgniteExecutorServiceTest;
import org.apache.ignite.internal.IgniteExplicitImplicitDeploymentSelfTest;
import org.apache.ignite.internal.IgniteRoundRobinErrorAfterClientReconnectTest;
import org.apache.ignite.internal.TaskNodeRestartTest;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointManagerSelfTest;
import org.apache.ignite.internal.managers.checkpoint.GridCheckpointTaskSelfTest;
import org.apache.ignite.internal.managers.communication.GridCommunicationManagerListenersSelfTest;
import org.apache.ignite.internal.processors.compute.PublicThreadpoolStarvationTest;
import org.apache.ignite.internal.util.StripedExecutorTest;
import org.apache.ignite.p2p.GridMultinodeRedeployContinuousModeSelfTest;
import org.apache.ignite.p2p.GridMultinodeRedeployIsolatedModeSelfTest;
import org.apache.ignite.p2p.GridMultinodeRedeployPrivateModeSelfTest;
import org.apache.ignite.p2p.GridMultinodeRedeploySharedModeSelfTest;

/**
 * Compute grid test suite.
 */
public class IgniteComputeGridTestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Compute Grid Test Suite");

        suite.addTest(IgniteTaskSessionSelfTestSuite.suite());
        suite.addTest(IgniteTimeoutProcessorSelfTestSuite.suite());
        suite.addTest(IgniteJobMetricsSelfTestSuite.suite());
        suite.addTest(IgniteContinuousTaskSelfTestSuite.suite());

        suite.addTestSuite(GridTaskCancelSingleNodeSelfTest.class);
        suite.addTestSuite(GridTaskFailoverSelfTest.class);
        suite.addTestSuite(GridJobCollisionCancelSelfTest.class);
        suite.addTestSuite(GridTaskTimeoutSelfTest.class);
        suite.addTestSuite(GridCancelUnusedJobSelfTest.class);
        suite.addTestSuite(GridTaskJobRejectSelfTest.class);
        suite.addTestSuite(GridTaskExecutionSelfTest.class);
        suite.addTestSuite(GridFailoverSelfTest.class);
        suite.addTestSuite(GridTaskListenerSelfTest.class);
        suite.addTestSuite(GridFailoverTopologySelfTest.class);
        suite.addTestSuite(GridTaskResultCacheSelfTest.class);
        suite.addTestSuite(GridTaskMapAsyncSelfTest.class);
        suite.addTestSuite(GridJobContextSelfTest.class);
        suite.addTestSuite(GridJobMasterLeaveAwareSelfTest.class);
        suite.addTestSuite(GridJobStealingSelfTest.class);
        suite.addTestSuite(GridJobSubjectIdSelfTest.class);
        suite.addTestSuite(GridMultithreadedJobStealingSelfTest.class);
        suite.addTestSuite(GridAlwaysFailoverSpiFailSelfTest.class);
        suite.addTestSuite(GridTaskInstanceExecutionSelfTest.class);
        suite.addTestSuite(ClusterNodeMetricsSelfTest.class);
        suite.addTestSuite(GridNonHistoryMetricsSelfTest.class);
        suite.addTestSuite(GridCancelledJobsMetricsSelfTest.class);
        suite.addTestSuite(GridCollisionJobsContextSelfTest.class);
        suite.addTestSuite(GridJobStealingZeroActiveJobsSelfTest.class);
        suite.addTestSuite(GridTaskFutureImplStopGridSelfTest.class);
        suite.addTestSuite(GridFailoverCustomTopologySelfTest.class);
        suite.addTestSuite(GridMultipleSpisSelfTest.class);
        suite.addTestSuite(GridStopWithWaitSelfTest.class);
        suite.addTestSuite(GridCancelOnGridStopSelfTest.class);
        suite.addTestSuite(GridDeploymentSelfTest.class);
        suite.addTestSuite(GridDeploymentMultiThreadedSelfTest.class);
        suite.addTestSuite(GridMultipleVersionsDeploymentSelfTest.class);
        suite.addTestSuite(IgniteExplicitImplicitDeploymentSelfTest.class);
        suite.addTestSuite(GridEventStorageCheckAllEventsSelfTest.class);
        suite.addTestSuite(GridCommunicationManagerListenersSelfTest.class);
        suite.addTestSuite(IgniteExecutorServiceTest.class);
        suite.addTestSuite(GridTaskInstantiationSelfTest.class);
        suite.addTestSuite(GridMultipleJobsSelfTest.class);
        suite.addTestSuite(GridCheckpointManagerSelfTest.class);
        suite.addTestSuite(GridCheckpointTaskSelfTest.class);
        suite.addTestSuite(ClusterNodeMetricsSelfTest.class);
        suite.addTestSuite(GridTaskNameAnnotationSelfTest.class);
        suite.addTestSuite(GridJobCheckpointCleanupSelfTest.class);
        suite.addTestSuite(GridEventStorageSelfTest.class);
        suite.addTestSuite(GridFailoverTaskWithPredicateSelfTest.class);
        suite.addTestSuite(GridProjectionLocalJobMultipleArgumentsSelfTest.class);
        suite.addTestSuite(GridAffinitySelfTest.class);
        suite.addTestSuite(GridAffinityNoCacheSelfTest.class);
        suite.addTestSuite(GridEventStorageRuntimeConfigurationSelfTest.class);
        suite.addTestSuite(GridMultinodeRedeployContinuousModeSelfTest.class);
        suite.addTestSuite(GridMultinodeRedeploySharedModeSelfTest.class);
        suite.addTestSuite(GridMultinodeRedeployPrivateModeSelfTest.class);
        suite.addTestSuite(GridMultinodeRedeployIsolatedModeSelfTest.class);
        suite.addTestSuite(IgniteComputeEmptyClusterGroupTest.class);
        suite.addTestSuite(IgniteComputeTopologyExceptionTest.class);
        suite.addTestSuite(GridTaskFailoverAffinityRunTest.class);
        suite.addTestSuite(TaskNodeRestartTest.class);
        suite.addTestSuite(IgniteRoundRobinErrorAfterClientReconnectTest.class);
        suite.addTestSuite(PublicThreadpoolStarvationTest.class);
        suite.addTestSuite(StripedExecutorTest.class);

        return suite;
    }
}
