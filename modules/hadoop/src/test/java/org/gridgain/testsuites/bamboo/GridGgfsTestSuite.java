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
import org.gridgain.grid.kernal.processors.ggfs.split.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.*;

/**
 * Test suite for Hadoop file system over GridGain cache.
 * Contains platform independent tests only.
 */
public class GridGgfsTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain GGFS Test Suite For Platform Independent Tests");

        suite.addTest(new TestSuite(GridGgfsSizeSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsAttributesSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsFileInfoSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsMetaManagerSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsDataManagerSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsProcessorSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsProcessorValidationSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsCacheSelfTest.class));

        if (U.isWindows())
            suite.addTest(new TestSuite(GridGgfsServerManagerIpcEndpointRegistrationOnWindowsSelfTest.class));

        suite.addTest(new TestSuite(GridCacheGgfsPerBlockLruEvictionPolicySelfTest.class));

        suite.addTest(new TestSuite(GridGgfsStreamsSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsModesSelfTest.class));
        suite.addTest(new TestSuite(GridIpcServerEndpointDeserializerSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsMetricsSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackExternalPrimarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackExternalSecondarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackExternalDualSyncSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackExternalDualAsyncSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackEmbeddedPrimarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackEmbeddedSecondarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackEmbeddedDualSyncSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoopbackEmbeddedDualAsyncSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemSecondaryModeSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemClientSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoggerStateSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemLoggerSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsHadoopFileSystemHandshakeSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsPrimarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsPrimaryOffheapTieredSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsPrimaryOffheapValuesSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsDualSyncSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsDualAsyncSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsModeResolverSelfTest.class));

        suite.addTest(GridGgfsEventsTestSuite.suiteNoarchOnly());

        suite.addTestSuite(GridGgfsHadoop20FileSystemLoopbackPrimarySelfTest.class);

        suite.addTestSuite(GridGgfsFragmentizerSelfTest.class);
        suite.addTestSuite(GridGgfsFragmentizerTopologySelfTest.class);
        suite.addTestSuite(GridGgfsFileMapSelfTest.class);

        suite.addTestSuite(GridGgfsByteDelimiterRecordResolverSelfTest.class);
        suite.addTestSuite(GridGgfsStringDelimiterRecordResolverSelfTest.class);
        suite.addTestSuite(GridGgfsFixedLengthRecordResolverSelfTest.class);
        suite.addTestSuite(GridGgfsNewLineDelimiterRecordResolverSelfTest.class);

        suite.addTestSuite(GridGgfsTaskSelfTest.class);

        suite.addTestSuite(GridGgfsGroupDataBlockKeyMapperHashSelfTest.class);

        return suite;
    }
}
