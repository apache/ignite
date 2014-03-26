/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.kernal.managers.*;
import org.gridgain.testsuites.*;

/**
 * Grid SPI test suite.
 */
public class GridSpiTestSuite extends TestSuite {
    /**
     * @return All SPI tests suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain SPIs Test Suite");

        // Failover.
        suite.addTest(GridSpiFailoverSelfTestSuite.suite());

        // Collision.
        suite.addTest(GridSpiCollisionSelfTestSuite.suite());

        // Event storage.
        suite.addTest(GridSpiEventStorageSelfTestSuite.suite());

        // Load Balancing.
        suite.addTest(GridSpiLoadBalancingSelfTestSuite.suite());

        // Swap space.
        suite.addTest(GridSpiSwapSpaceSelfTestSuite.suite());

        // Checkpoints.
        suite.addTest(GridSpiCheckpointSelfTestSuite.suite());

        // Deployment
        suite.addTest(GridSpiDeploymentSelfTestSuite.suite());

        // Discovery.
        suite.addTest(GridSpiDiscoverySelfTestSuite.suite());

        // Communication.
        suite.addTest(GridSpiCommunicationSelfTestSuite.suite());

        // Indexing.
        suite.addTest(GridSpiIndexingSelfTestSuite.suite());

        // All other tests.
        suite.addTestSuite(GridNoopManagerSelfTest.class);

        return suite;
    }
}
