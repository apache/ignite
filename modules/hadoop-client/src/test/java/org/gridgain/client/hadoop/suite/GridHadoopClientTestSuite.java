/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client.hadoop.suite;

import junit.framework.*;
import org.gridgain.client.hadoop.*;
import org.gridgain.testsuites.bamboo.*;

/**
 * Test suite for Hadoop client.
 */
public class GridHadoopClientTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Hadoop Client Test Suite");

        suite.addTest(GridHadoopTestSuite.suite());

        suite.addTestSuite(GridHadoopClientProtocolSelfTest.class);
        suite.addTestSuite(GridHadoopClientProtocolEmbeddedSelfTest.class);

        return suite;
    }
}
