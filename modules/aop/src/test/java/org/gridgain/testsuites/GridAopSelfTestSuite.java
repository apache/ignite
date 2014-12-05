/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.gridify.*;
import org.gridgain.testframework.*;
import org.test.gridify.*;

import static org.gridgain.grid.IgniteSystemProperties.*;

/**
 * AOP test suite.
 */
public class GridAopSelfTestSuite extends TestSuite {
    /**
     * @return AOP test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain AOP Test Suite");

        // Test configuration.
        suite.addTest(new TestSuite(GridBasicAopSelfTest.class));

        suite.addTest(new TestSuite(GridSpringAopSelfTest.class));
        suite.addTest(new TestSuite(GridNonSpringAopSelfTest.class));
        suite.addTest(new TestSuite(GridifySetToXXXSpringAopSelfTest.class));
        suite.addTest(new TestSuite(GridifySetToXXXNonSpringAopSelfTest.class));
        suite.addTest(new TestSuite(GridExternalNonSpringAopSelfTest.class));

        // Examples
        System.setProperty(GG_OVERRIDE_MCAST_GRP, GridTestUtils.getNextMulticastGroup(GridAopSelfTestSuite.class));

        return suite;
    }
}
