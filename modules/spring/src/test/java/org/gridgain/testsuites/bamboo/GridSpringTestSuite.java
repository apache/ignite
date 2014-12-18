/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.cache.spring.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.p2p.*;
import org.gridgain.testsuites.*;

/**
 * Spring tests.
 */
public class GridSpringTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Spring Test Suite");

        suite.addTestSuite(GridSpringBeanSerializationSelfTest.class);
        suite.addTestSuite(GridFactorySelfTest.class);

        suite.addTest(GridResourceSelfTestSuite.suite());

        // Tests moved to this suite since they require Spring functionality.
        suite.addTest(new TestSuite(GridP2PUserVersionChangeSelfTest.class));

        suite.addTest(new TestSuite(GridSpringCacheManagerSelfTest.class));

        return suite;
    }
}
