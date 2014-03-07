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
 * Group lock test suite.
 */
public class GridCacheGroupLockSelfTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = GridTestUtils.createLocalTestSuite("Gridgain Cache Group Lock Test Suite");

        // One node.
        suite.addTest(new TestSuite(GridCacheGroupLockNearSelfTest.class));
        suite.addTest(new TestSuite(GridCacheGroupLockColocatedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheGroupLockReplicatedSelfTest.class));

        // Multiple nodes.
        suite.addTest(new TestSuite(GridCacheGroupLockMultiNodeNearSelfTest.class));
        suite.addTest(new TestSuite(GridCacheGroupLockMultiNodeColocatedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheGroupLockMultiNodeReplicatedSelfTest.class));

        return suite;
    }
}
