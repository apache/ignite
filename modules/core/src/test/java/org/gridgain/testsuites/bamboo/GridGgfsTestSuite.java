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

//        suite.addTest(new TestSuite(GridGgfsSizeSelfTest.class)); TODO Enable after GG-9035
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

        suite.addTest(new TestSuite(GridGgfsPrimarySelfTest.class));
        suite.addTest(new TestSuite(GridGgfsPrimaryOffheapTieredSelfTest.class));
        suite.addTest(new TestSuite(GridGgfsPrimaryOffheapValuesSelfTest.class));

        suite.addTest(new TestSuite(GridGgfsModeResolverSelfTest.class));

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
