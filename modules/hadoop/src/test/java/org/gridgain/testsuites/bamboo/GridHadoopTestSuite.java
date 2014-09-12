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
import org.gridgain.grid.kernal.processors.hadoop.*;

/**
 * Test suite for Hadoop Map Reduce engine.
 */
public class GridHadoopTestSuite extends TestSuite {
    /** */
    private static Class<?> loadClass(Class<?> cls) throws ClassNotFoundException, GridException {
        GridHadoopClassLoader ldr = new GridHadoopClassLoader(null);

        return ldr.loadClassExplicitly(cls.getName());
    }

    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Hadoop MR Test Suite");

//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackExternalPrimarySelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackExternalSecondarySelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackExternalDualSyncSelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackExternalDualAsyncSelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackEmbeddedPrimarySelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackEmbeddedSecondarySelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackEmbeddedDualAsyncSelfTest.class));
//
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemSecondaryModeSelfTest.class));
//
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemClientSelfTest.class));
//
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoggerStateSelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoggerSelfTest.class));
//
//        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemHandshakeSelfTest.class));
//
//        suite.addTestSuite(GridGgfsHadoop20FileSystemLoopbackPrimarySelfTest.class);
//
//        suite.addTest(new TestSuite(GridGgfsHadoopDualSyncSelfTest.class));
//        suite.addTest(new TestSuite(GridGgfsHadoopDualAsyncSelfTest.class));
//
//        suite.addTest(GridGgfsEventsTestSuite.suiteNoarchOnly());
//
//        suite.addTest(new TestSuite(GridHadoopFileSystemsTest.class));
//
//        suite.addTest(new TestSuite(GridHadoopValidationSelfTest.class));
//
//        suite.addTest(new TestSuite(GridHadoopDefaultMapReducePlannerSelfTest.class));
//        suite.addTest(new TestSuite(GridHadoopJobTrackerSelfTest.class));
//        suite.addTest(new TestSuite(GridHadoopHashMapSelfTest.class));
//        suite.addTest(new TestSuite(GridHadoopDataStreamSelfTest.class));
//        suite.addTest(new TestSuite(GridHadoopConcurrentHashMultimapSelftest.class));
//        suite.addTestSuite(GridHadoopSkipListSelfTest.class);
//        suite.addTest(new TestSuite(GridHadoopTaskExecutionSelfTest.class));
//
//        suite.addTest(new TestSuite(GridHadoopV2JobSelfTest.class));
//
//        suite.addTest(new TestSuite(GridHadoopSerializationWrapperSelfTest.class));
//        suite.addTest(new TestSuite(GridHadoopSplitWrapperSelfTest.class));
//
//        suite.addTest(new TestSuite(GridHadoopTasksV1Test.class));
//        suite.addTest(new TestSuite(GridHadoopTasksV2Test.class));

//        System.out.println(TestCase.class.getProtectionDomain().getCodeSource().getLocation());

//        suite.addTest(new TestSuite(GridHadoopMapReduceTest.class));
        suite.addTest(new TestSuite(loadClass(GridHadoopMapReduceTest.class)));



//        TestSuite ts = new TestSuite();
//
//        Class<?> cls = loadClass(GridHadoopMapReduceTest.class);

//        Thread.currentThread().setContextClassLoader(cls.getClassLoader().getParent());
//
//        ts.addTest(createTest(cls, "testWholeMapReduceExecution"));
//
//        suite.addTest(ts);

//        suite.addTest(new TestSuite(cls));



//        suite.addTest(new TestSuite(GridHadoopMapReduceEmbeddedSelfTest.class));
//
//        //TODO: GG-8936 Fix and uncomment ExternalExecution tests
//        //suite.addTest(new TestSuite(GridHadoopExternalTaskExecutionSelfTest.class));
//        suite.addTest(new TestSuite(GridHadoopExternalCommunicationSelfTest.class));
//
//        suite.addTest(new TestSuite(GridHadoopSortingTest.class));
//        suite.addTest(new TestSuite(GridHadoopSortingExternalTest.class));
//
//        suite.addTest(new TestSuite(GridHadoopGroupingTest.class));

        return suite;
    }
}
