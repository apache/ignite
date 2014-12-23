/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.cache.*;

/**
 * Test suite that contains all tests for {@link GridCacheWriteBehindStore}.
 */
public class GridCacheWriteBehindTestSuite extends TestSuite {
    /**
     * @return GridGain Bamboo in-memory data grid test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Write-Behind Store Test Suite");

        // Write-behind tests.
        suite.addTest(new TestSuite(GridCacheWriteBehindStoreSelfTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStoreMultithreadedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStoreLocalTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStoreReplicatedTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStorePartitionedTest.class));
        suite.addTest(new TestSuite(GridCacheWriteBehindStorePartitionedMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedWritesTest.class));

        return suite;
    }
}
