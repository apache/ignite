/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.spi.deployment.local.*;
import org.gridgain.grid.spi.deployment.uri.*;
import org.gridgain.grid.spi.deployment.uri.scanners.file.*;
import org.gridgain.grid.spi.deployment.uri.scanners.http.*;

/**
 * Test suit for deployment SPIs.
 */
public class GridSpiDeploymentSelfTestSuite extends TestSuite {
    /**
     * @return Deployment SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Deployment SPI Test Suite");

        // LocalDeploymentSpi tests
        suite.addTest(new TestSuite(GridLocalDeploymentSpiSelfTest.class));
        suite.addTest(new TestSuite(GridLocalDeploymentSpiStartStopSelfTest.class));

        // UriDeploymentSpi tests
        suite.addTest(new TestSuite(GridUriDeploymentConfigSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentSimpleSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassloaderRegisterSelfTest.class));

        // TODO: GG-8331
        //suite.addTest(new TestSuite(GridUriDeploymentFileProcessorSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentClassLoaderMultiThreadedSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentMultiScannersSelfTest.class));
        suite.addTest(new TestSuite(GridUriDeploymentConfigSelfTest.class));

        suite.addTest(new TestSuite(GridFileDeploymentUndeploySelfTest.class));
        suite.addTest(new TestSuite(GridHttpDeploymentSelfTest.class));

        return suite;
    }
}
