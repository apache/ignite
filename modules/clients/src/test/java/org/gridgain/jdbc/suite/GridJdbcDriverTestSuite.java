/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jdbc.suite;

import junit.framework.*;
import org.gridgain.jdbc.*;

/**
 * JDBC driver test suite.
 */
public class GridJdbcDriverTestSuite extends TestSuite {
    /**
     * @return JDBC Driver Test Suite.
     * @throws Exception In case of error.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain JDBC Driver Test Suite");

        suite.addTest(new TestSuite(GridJdbcConnectionSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcStatementSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcPreparedStatementSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcResultSetSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcComplexQuerySelfTest.class));
        suite.addTest(new TestSuite(GridJdbcMetadataSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcEmptyCacheSelfTest.class));
        suite.addTest(new TestSuite(GridJdbcLocalCachesSelfTest.class));

        return suite;
    }
}
