/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.messaging.*;
import org.gridgain.testframework.*;

/**
 * Test suite.
 */
public class GridRichSelfTestSuite extends TestSuite {
    /**
     * @return Kernal test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = GridTestUtils.createDistributedTestSuite("Gridgain Rich Test Suite");

        suite.addTest(new TestSuite(GridSelfTest.class));
        suite.addTest(new TestSuite(GridProjectionSelfTest.class));
        suite.addTest(new TestSuite(GridMessagingSelfTest.class));
        suite.addTest(new TestSuite(GridMessagingNoPeerClassLoadingSelfTest.class));

        return suite;
    }
}
