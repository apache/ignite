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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
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

        suite.addTest(new JUnit4TestAdapter(GridTaskCancelSingleNodeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobCollisionCancelSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskTimeoutSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCancelUnusedJobSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskJobRejectSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskExecutionSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridTaskExecutionContextSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskExecutionWithoutPeerClassLoadingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFailoverSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskListenerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFailoverTopologySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskResultCacheSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskMapAsyncSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobContextSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobMasterLeaveAwareSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobStealingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobSubjectIdSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMultithreadedJobStealingSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAlwaysFailoverSpiFailSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskInstanceExecutionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClusterNodeMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(ClusterNodeMetricsUpdateTest.class));
        suite.addTest(new JUnit4TestAdapter(GridNonHistoryMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCancelledJobsMetricsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCollisionJobsContextSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobStealingZeroActiveJobsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskFutureImplStopGridSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFailoverCustomTopologySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMultipleSpisSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridStopWithWaitSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCancelOnGridStopSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridDeploymentSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridDeploymentMultiThreadedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMultipleVersionsDeploymentSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteExplicitImplicitDeploymentSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridEventStorageCheckAllEventsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCommunicationManagerListenersSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteExecutorServiceTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskInstantiationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMultipleJobsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCheckpointManagerSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridCheckpointTaskSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskNameAnnotationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobCheckpointCleanupSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridEventStorageSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridEventStorageDefaultExceptionTest.class));
        suite.addTest(new JUnit4TestAdapter(GridFailoverTaskWithPredicateSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridProjectionLocalJobMultipleArgumentsSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAffinitySelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridAffinityNoCacheSelfTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridAffinityMappedTest.class));
        //suite.addTest(new JUnit4TestAdapter(GridAffinityP2PSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridEventStorageRuntimeConfigurationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMultinodeRedeployContinuousModeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMultinodeRedeploySharedModeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMultinodeRedeployPrivateModeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(GridMultinodeRedeployIsolatedModeSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteComputeEmptyClusterGroupTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteComputeTopologyExceptionTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteComputeResultExceptionTest.class));
        suite.addTest(new JUnit4TestAdapter(GridTaskFailoverAffinityRunTest.class));
        suite.addTest(new JUnit4TestAdapter(TaskNodeRestartTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteRoundRobinErrorAfterClientReconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(PublicThreadpoolStarvationTest.class));
        suite.addTest(new JUnit4TestAdapter(StripedExecutorTest.class));
        suite.addTest(new JUnit4TestAdapter(GridJobServicesAddNodeTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteComputeCustomExecutorConfigurationSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(IgniteComputeCustomExecutorSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(IgniteComputeJobOneThreadTest.class));

        suite.addTest(new JUnit4TestAdapter(VisorManagementEventSelfTest.class));

        return suite;
    }
}
