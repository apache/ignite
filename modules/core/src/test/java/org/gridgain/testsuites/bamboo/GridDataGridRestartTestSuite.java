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

/**
 * In-Memory Data Grid stability test suite on changing topology.
 */
public class GridDataGridRestartTestSuite extends TestSuite {
    /**
     * @return Suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain In-Memory Data Grid Restart Test Suite");

        // Common restart tests.
        // TODO: GG-7419: Enable when fixed.
//        suite.addTestSuite(GridCachePartitionedNodeRestartTest.class);
//        suite.addTestSuite(GridCachePartitionedOptimisticTxNodeRestartTest.class);

        // TODO: uncomment when fix GG-1969
//        suite.addTestSuite(GridCacheReplicatedNodeRestartSelfTest.class);

        // The rest.
        suite.addTestSuite(GridCachePartitionedTxSalvageSelfTest.class);
        suite.addTestSuite(GridCachePutAllFailoverSelfTest.class);

        return suite;
    }
}
