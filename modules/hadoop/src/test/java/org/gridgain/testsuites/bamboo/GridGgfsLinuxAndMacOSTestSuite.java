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
import org.gridgain.grid.kernal.processors.hadoop.*;

import static org.gridgain.testsuites.bamboo.GridHadoopTestSuite.*;

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
        downloadHadoop();

        GridHadoopClassLoader ldr = new GridHadoopClassLoader(null);
        
        TestSuite suite = new TestSuite("Gridgain GGFS Test Suite For Linux And Mac OS");

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsServerManagerIpcEndpointRegistrationOnLinuxAndMacSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemShmemExternalPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemShmemExternalSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemShmemExternalDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemShmemExternalDualAsyncSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemShmemEmbeddedPrimarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemShmemEmbeddedSecondarySelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemShmemEmbeddedDualSyncSelfTest.class.getName())));
        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemShmemEmbeddedDualAsyncSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoopFileSystemIpcCacheSelfTest.class.getName())));

        suite.addTest(new TestSuite(ldr.loadClass(GridGgfsHadoop20FileSystemShmemPrimarySelfTest.class.getName())));

        suite.addTest(GridGgfsEventsTestSuite.suite());

        return suite;
    }
}
