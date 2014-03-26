/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.util.ipc.shmem.*;

/**
 * Shared memory test suite.
 */
public class GridIpcSharedMemorySelfTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain IPC Shared Memory Test Suite.");

        suite.addTest(new TestSuite(GridIpcSharedMemorySpaceSelfTest.class));
        suite.addTest(new TestSuite(GridIpcSharedMemoryUtilsSelfTest.class));
        suite.addTest(new TestSuite(GridIpcSharedMemoryCrashDetectionSelfTest.class));

        return suite;
    }
}
