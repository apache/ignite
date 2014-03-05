// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.router.testsuites;

import junit.framework.*;
import org.gridgain.client.router.*;
import org.gridgain.testframework.*;

import java.io.*;

/**
 * Test suite for router tests.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridRouterTestSuite extends TestSuite {
    /**
     * @return Suite that contains all router tests.
     * @throws IOException If failed.
     */
    public static TestSuite suite() throws IOException {
        TestSuite suite = GridTestUtils.createLocalTestSuite("Gridgain Router Test Suite");

        suite.addTest(new TestSuite(GridRouterFactorySelfTest.class));
        suite.addTest(new TestSuite(GridTcpRouterSelfTest.class));
        suite.addTest(new TestSuite(GridHttpRouterSelfTest.class));
        suite.addTest(new TestSuite(GridTcpSslRouterSelfTest.class));
        suite.addTest(new TestSuite(GridHttpsRouterSelfTest.class));
        suite.addTest(new TestSuite(GridTcpRouterMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridHttpRouterMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridHttpsRouterMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridRouterMultiAuthSelfTest.class));

        return suite;
    }
}
