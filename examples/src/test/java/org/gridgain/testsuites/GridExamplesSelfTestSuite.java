/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.examples.*;
import org.gridgain.testframework.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Examples test suite.
 * <p>
 * Contains only Spring gridify examples tests.
 */
public class GridExamplesSelfTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        System.setProperty(GG_OVERRIDE_MCAST_GRP,
            GridTestUtils.getNextMulticastGroup(GridExamplesSelfTestSuite.class));

        TestSuite suite = new TestSuite("GridGain Examples Test Suite");

        suite.addTest(new TestSuite(GridCacheExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridBasicExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridContinuationExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridContinuousMapperExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridDeploymentExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridEventsExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridLifecycleExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridMessagingExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridMemcacheRestExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridMonteCarloExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridRouterExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridTaskExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridSpringBeanExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridCheckpointExamplesSelfTest.class));
        suite.addTest(new TestSuite(GridHibernateL2CacheExampleSelfTest.class));
        suite.addTest(new TestSuite(GridProjectionExampleSelfTest.class));

        // Multi-node.
        suite.addTest(new TestSuite(GridCacheExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheStoreLoadDataExampleMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridBasicExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridContinuationExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridContinuousMapperExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridDeploymentExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridEventsExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridTaskExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridMemcacheRestExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridRouterExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridMonteCarloExamplesMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridHibernateL2CacheExampleMultiNodeSelfTest.class));

        return suite;
    }
}
