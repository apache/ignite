/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.testframework.*;

/**
 * Test suite for Hadoop file system over GridGain cache.
 * Contains tests which works on Linux and Mac OS platform only.
 */
public class GridGgfsLinuxAndMacOSTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = GridTestUtils.createLocalTestSuite("Gridgain GGFS Test Suite For Linux And Mac OS");

        suite.addTest(new TestSuite(GridGgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemShmemPrimarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemShmemSecondarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemShmemDualSyncSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemShmemDualAsyncSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLightIpcCacheSelfTest.class));

        suite.addTestSuite(GridGgfsHadoop20FileSystemShmemPrimarySelfTest.class);

        suite.addTestSuite(GridGgfsIpcEndpointShmemSelfTest.class);

        suite.addTest(GridGgfsEventsTestSuite.suite());

        return suite;
    }
}
