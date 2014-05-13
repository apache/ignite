/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.spi.swapspace.file.*;
import org.gridgain.grid.spi.swapspace.noop.*;

/**
 *
 */
public class GridSpiSwapSpaceSelfTestSuite {
    /**
     * @return Checkpoint test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Public Swap Space Test Suite");

        suite.addTest(new TestSuite(GridFileSwapCompactionSelfTest.class));
        suite.addTest(new TestSuite(GridFileSwapSpaceSpiSelfTest.class));
        suite.addTest(new TestSuite(GridNoopSwapSpaceSpiSelfTest.class));

        return suite;
    }
}
