/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.p2p.*;

/**
 * P2P test suite.
 */
public class GridP2PSelfTestSuite extends TestSuite {
    /**
     * @return P2P tests suite.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"ProhibitedExceptionDeclared"})
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain P2P Test Suite");

        suite.addTest(new TestSuite(GridP2PDoubleDeploymentSelfTest.class));
        suite.addTest(new TestSuite(GridP2PHotRedeploymentSelfTest.class));
        suite.addTest(new TestSuite(GridP2PClassLoadingSelfTest.class));
        suite.addTest(new TestSuite(GridP2PUndeploySelfTest.class));
        suite.addTest(new TestSuite(GridP2PRemoteClassLoadersSelfTest.class));
        suite.addTest(new TestSuite(GridP2PNodeLeftSelfTest.class));
        suite.addTest(new TestSuite(GridP2PDifferentClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridP2PSameClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridP2PJobClassLoaderSelfTest.class));
        suite.addTest(new TestSuite(GridP2PRecursionTaskSelfTest.class));
        suite.addTest(new TestSuite(GridP2PLocalDeploymentSelfTest.class));
        suite.addTest(new TestSuite(GridP2PTimeoutSelfTest.class));
        suite.addTest(new TestSuite(GridP2PMissedResourceCacheSizeSelfTest.class));
        suite.addTest(new TestSuite(GridP2PContinuousDeploymentSelfTest.class));
        suite.addTest(new TestSuite(GridDeploymentMessageCountSelfTest.class));

        return suite;
    }
}
