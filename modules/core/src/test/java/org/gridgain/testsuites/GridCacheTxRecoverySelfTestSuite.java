/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.*;
import org.gridgain.testframework.*;

/**
 * Tx recovery self test suite.
 */
public class GridCacheTxRecoverySelfTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = GridTestUtils.createLocalTestSuite("Gridgain Cache tx recovery test suite");

        suite.addTestSuite(GridCachePartitionedTxOriginatingNodeFailureSelfTest.class);
        suite.addTestSuite(GridCachePartitionedNearDisabledTxOriginatingNodeFailureSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxOriginatingNodeFailureSelfTest.class);

        suite.addTestSuite(GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest.class);
        suite.addTestSuite(GridCacheNearTxPessimisticOriginatingNodeFailureSelfTest.class);
        suite.addTestSuite(GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest.class);

        return suite;
    }
}
