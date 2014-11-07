/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.affinity.*;
import org.gridgain.grid.kernal.processors.closure.*;
import org.gridgain.grid.kernal.processors.continuous.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testsuites.*;

/**
 * Basic test suite.
 */
public class GridBasicTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Basic Test Suite");

        suite.addTest(GridLangSelfTestSuite.suite());
        suite.addTest(GridUtilSelfTestSuite.suite());
        suite.addTest(GridMarshallerSelfTestSuite.suite());
        suite.addTest(GridKernalSelfTestSuite.suite());

        /*suite.addTest(GridLoadersSelfTestSuite.suite());
        suite.addTest(GridRichSelfTestSuite.suite());
        suite.addTest(GridExternalizableSelfTestSuite.suite());
        suite.addTest(GridP2PSelfTestSuite.suite());

        if (U.isLinux() || U.isMacOs())
            suite.addTest(GridIpcSharedMemorySelfTestSuite.suite());

        suite.addTestSuite(GridTopologyBuildVersionSelfTest.class);
        suite.addTestSuite(GridReleaseTypeSelfTest.class);
        suite.addTestSuite(GridProductVersionSelfTest.class);
        suite.addTestSuite(GridAffinityProcessorConsistentHashSelfTest.class);
        suite.addTestSuite(GridAffinityProcessorRendezvousSelfTest.class);
        suite.addTestSuite(GridClosureProcessorSelfTest.class);
        suite.addTestSuite(GridStartStopSelfTest.class);
        suite.addTestSuite(GridProjectionForCachesSelfTest.class);
        suite.addTestSuite(GridSpiLocalHostInjectionTest.class);
        suite.addTestSuite(GridLifecycleBeanSelfTest.class);
        suite.addTestSuite(GridStopWithCancelSelfTest.class);
        suite.addTestSuite(GridReduceSelfTest.class);
        suite.addTestSuite(GridEventConsumeSelfTest.class);
        suite.addTestSuite(GridExceptionHelpLinksSelfTest.class);
        suite.addTestSuite(GridSuppressedExceptionSelfTest.class);
        suite.addTestSuite(GridLifecycleAwareSelfTest.class);
        suite.addTestSuite(GridMessageListenSelfTest.class);

        // Streamer.
        suite.addTest(GridStreamerSelfTestSuite.suite());*/

        return suite;
    }
}
