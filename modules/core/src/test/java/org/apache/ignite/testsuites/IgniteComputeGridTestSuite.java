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

import junit.framework.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.checkpoint.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.p2p.*;

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
        suite.addTestSuite(GridExplicitImplicitDeploymentSelfTest.class);
        suite.addTestSuite(GridEventStorageCheckAllEventsSelfTest.class);
        suite.addTestSuite(GridCommunicationManagerListenersSelfTest.class);
        suite.addTestSuite(GridExecutorServiceTest.class);
        suite.addTestSuite(GridTaskInstantiationSelfTest.class);
        suite.addTestSuite(GridManagementJobSelfTest.class);
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
        suite.addTestSuite(GridEventStorageRuntimeConfigurationSelfTest.class);
        suite.addTestSuite(GridMultinodeRedeployContinuousModeSelfTest.class);
        suite.addTestSuite(GridMultinodeRedeploySharedModeSelfTest.class);
        suite.addTestSuite(GridMultinodeRedeployPrivateModeSelfTest.class);
        suite.addTestSuite(GridMultinodeRedeployIsolatedModeSelfTest.class);

        return suite;
    }
}
