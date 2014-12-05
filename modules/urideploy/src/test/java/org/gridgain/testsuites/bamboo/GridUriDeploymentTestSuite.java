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
import org.gridgain.grid.p2p.*;
import org.apache.ignite.spi.deployment.uri.*;
import org.apache.ignite.spi.deployment.uri.scanners.file.*;
import org.apache.ignite.spi.deployment.uri.scanners.http.*;
import org.gridgain.testsuites.*;

/**
 * Tests against {@link GridUriDeploymentSpi}.
 */
public class GridUriDeploymentTestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("GridUriDeploymentSpi Test Suite");

        suite.addTest(new TestSuite(GridUriDeploymentConfigSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentSimpleSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassloaderRegisterSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentFileProcessorSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassLoaderMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentMultiScannersSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentConfigSelfTest.class));

        suite.addTest(new TestSuite(GridFileDeploymentUndeploySelfTest.class));
        suite.addTest(new TestSuite(GridHttpDeploymentSelfTest.class));

        // GAR Ant task tests.
        suite.addTest(GridToolsSelfTestSuite.suite());

        suite.addTestSuite(GridTaskUriDeploymentDeadlockSelfTest.class);
        suite.addTest(new TestSuite(GridP2PDisabledSelfTest.class));

        return suite;
    }
}
