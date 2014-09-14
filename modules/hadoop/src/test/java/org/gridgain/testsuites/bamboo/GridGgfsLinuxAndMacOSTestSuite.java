/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.processors.hadoop.*;

/**
 * Test suite for Hadoop file system over GridGain cache.
 * Contains tests which works on Linux and Mac OS platform only.
 */
public class GridGgfsLinuxAndMacOSTestSuite extends TestSuite {
    /** */
    private static Class<?> loadClass(Class<?> cls) throws ClassNotFoundException, GridException {
        GridHadoopClassLoader ldr = new GridHadoopClassLoader(null);

        return ldr.loadClass(cls.getName());
    }

    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain GGFS Test Suite For Linux And Mac OS");

        suite.addTest(new TestSuite(loadClass(GridGgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemShmemExternalPrimarySelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemShmemExternalSecondarySelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemShmemExternalDualSyncSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemShmemExternalDualAsyncSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemShmemEmbeddedPrimarySelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemShmemEmbeddedSecondarySelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemShmemEmbeddedDualSyncSelfTest.class)));
        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemShmemEmbeddedDualAsyncSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoopFileSystemIpcCacheSelfTest.class)));

        suite.addTest(new TestSuite(loadClass(GridGgfsHadoop20FileSystemShmemPrimarySelfTest.class)));

        suite.addTest(GridGgfsEventsTestSuite.suite());

        return suite;
    }
}
