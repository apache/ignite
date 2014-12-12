/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.query.h2.*;
import org.gridgain.grid.kernal.processors.query.h2.opt.*;

/**
 * H2 indexing SPI tests.
 */
public class GridH2IndexingSpiTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("H2 Indexing SPI Test Suite");

        // H2 Optimized table test.
        suite.addTest(new TestSuite(GridH2TableSelfTest.class));

        // H2 Indexing in-memory.
        suite.addTest(new TestSuite(GridH2IndexingInMemSelfTest.class));

        // H2 Off-heap memory.
        suite.addTest(new TestSuite(GridH2IndexingOffheapSelfTest.class));

        // Index rebuilding.
        suite.addTest(new TestSuite(GridH2IndexRebuildTest.class));

        // Geo.
        suite.addTestSuite(GridH2IndexingGeoSelfTest.class);

        // Tests moved to this suite since they require GridH2IndexingSpi.
        suite.addTestSuite(GridCacheOffHeapAndSwapSelfTest.class);
        suite.addTestSuite(GridIndexingWithNoopSwapSelfTest.class);
        suite.addTestSuite(GridCachePartitionedHitsAndMissesSelfTest.class);
        suite.addTestSuite(GridCacheSwapSelfTest.class);
        suite.addTestSuite(GridCacheOffHeapSelfTest.class);

        return suite;
    }
}
