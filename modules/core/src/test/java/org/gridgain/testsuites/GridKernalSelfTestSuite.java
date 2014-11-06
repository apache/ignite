/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.discovery.*;
import org.gridgain.grid.kernal.managers.events.*;
import org.gridgain.grid.kernal.managers.swapspace.*;
import org.gridgain.grid.kernal.processors.port.*;
import org.gridgain.grid.kernal.processors.service.*;
import org.gridgain.grid.util.*;

/**
 * Kernal self test suite.
 */
public class GridKernalSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Kernal Test Suite");

        suite.addTestSuite(GridSameVmStartupSelfTest.class);
        suite.addTestSuite(GridSpiExceptionSelfTest.class);
        suite.addTestSuite(GridRuntimeExceptionSelfTest.class);
        suite.addTestSuite(GridFailedInputParametersSelfTest.class);
        suite.addTestSuite(GridNodeFilterSelfTest.class);
        suite.addTestSuite(GridNodeVisorAttributesSelfTest.class);
        suite.addTestSuite(GridDiscoverySelfTest.class);
        suite.addTestSuite(GridCommunicationSelfTest.class);
        suite.addTestSuite(GridEventStorageManagerSelfTest.class);
        suite.addTestSuite(GridSwapSpaceManagerSelfTest.class);
        suite.addTestSuite(GridCommunicationSendMessageSelfTest.class);
        suite.addTestSuite(GridDeploymentManagerStopSelfTest.class);
        suite.addTestSuite(GridManagerStopSelfTest.class);
        suite.addTestSuite(GridDiscoveryManagerAttributesSelfTest.class);
        suite.addTestSuite(GridDiscoveryManagerAliveCacheSelfTest.class);
        suite.addTestSuite(GridDiscoveryManagerSelfTest.class);
        suite.addTestSuite(GridDiscoveryEventSelfTest.class);
        suite.addTestSuite(GridPortProcessorSelfTest.class);
        suite.addTestSuite(GridHomePathSelfTest.class);
        suite.addTestSuite(GridStartupWithSpecifiedWorkDirectorySelfTest.class);
        suite.addTestSuite(GridStartupWithUndefinedGridGainHomeSelfTest.class);
        suite.addTestSuite(GridVersionSelfTest.class);
        suite.addTestSuite(GridListenActorSelfTest.class);
        suite.addTestSuite(GridNodeLocalSelfTest.class);
        suite.addTestSuite(GridKernalConcurrentAccessStopSelfTest.class);
        suite.addTestSuite(GridUpdateNotifierSelfTest.class);
        suite.addTestSuite(GridLocalEventListenerSelfTest.class);

        // Managed Services.
        suite.addTestSuite(GridServiceProcessorSingleNodeSelfTest.class);
        suite.addTestSuite(GridServiceProcessorMultiNodeSelfTest.class);
        suite.addTestSuite(GridServiceProcessorMultiNodeConfigSelfTest.class);
        suite.addTestSuite(GridServiceProcessorProxySelfTest.class);
        suite.addTestSuite(GridServiceReassignmentSelfTest.class);

        return suite;
    }
}
