/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.logger.log4j.*;

/**
 * Log4j logging tests.
 */
public class GridLog4jTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Log4j Logging Test Suite");

        suite.addTest(new TestSuite(GridLog4jInitializedTest.class));
        suite.addTest(new TestSuite(GridLog4jNotInitializedTest.class));
        suite.addTest(new TestSuite(GridLog4jCorrectFileNameTest.class));

        return suite;
    }
}
