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
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;

/**
 * Test suite.
 */
public class GridCacheFailoverTestSuite extends TestSuite {
    /**
     * @return Gridgain Cache Group Lock Failover test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Cache Failover Test Suite");

        // TODO GG-9141
        if (true)
            return suite;

        suite.addTestSuite(GridCacheAtomicInvalidPartitionHandlingSelfTest.class);

        // Group lock failover.
        suite.addTestSuite(GridCacheGroupLockFailoverSelfTest.class);
        suite.addTestSuite(GridCacheGroupLockFailoverOptimisticTxSelfTest.class);

        // Failure consistency tests.
        suite.addTestSuite(GridCacheDhtAtomicRemoveFailureTest.class);
        suite.addTestSuite(GridCacheDhtRemoveFailureTest.class);
        suite.addTestSuite(GridCacheNearRemoveFailureTest.class);
        // suite.addTestSuite(GridCacheAtomicNearRemoveFailureTest.class); TODO GG-9150
        suite.addTestSuite(GridCacheAtomicPrimaryWriteOrderNearRemoveFailureTest.class);

        return suite;
    }
}
