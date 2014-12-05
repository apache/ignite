/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.apache.ignite.streamer.index.*;
import org.apache.ignite.streamer.window.*;
import org.gridgain.grid.kernal.processors.streamer.*;

/**
 * Streamer test suite.
 */
public class GridStreamerSelfTestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Streamer Test Suite.");

        // Streamer.
        suite.addTestSuite(GridStreamerWindowSelfTest.class);
        suite.addTestSuite(GridStreamerEvictionSelfTest.class);
        suite.addTestSuite(GridStreamerSelfTest.class);
        suite.addTestSuite(GridStreamerFailoverSelfTest.class);
        suite.addTestSuite(GridStreamerIndexSelfTest.class);
        suite.addTestSuite(GridStreamerLifecycleAwareSelfTest.class);

        return suite;
    }
}
