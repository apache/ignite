/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.checkpoint.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.p2p.*;
import org.gridgain.grid.spi.communication.tcp.*;
import org.gridgain.testsuites.*;

/**
 * Compute grid test suite.
 */
public class GridComputeGridTestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Compute Grid Test Suite");

        suite.addTest(GridTaskSessionSelfTestSuite.suite());
        suite.addTest(GridTimeoutProcessorSelfTestSuite.suite());
        suite.addTest(GridJobMetricsSelfTestSuite.suite());
        suite.addTest(GridContinuousTaskSelfTestSuite.suite());

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
        suite.addTestSuite(ClusterMetricsSelfTest.class);
        suite.addTestSuite(GridTaskNameAnnotationSelfTest.class);
        suite.addTestSuite(GridJobCheckpointCleanupSelfTest.class);
        suite.addTestSuite(GridEventStorageSelfTest.class);
        suite.addTestSuite(GridOrderedMessageCancelSelfTest.class);
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
