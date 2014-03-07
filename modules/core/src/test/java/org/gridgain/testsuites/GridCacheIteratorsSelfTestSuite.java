/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.distributed.replicated.*;
import org.gridgain.grid.kernal.processors.cache.local.*;
import org.gridgain.testframework.*;

/**
 * Cache iterators test suite.
 */
public class GridCacheIteratorsSelfTestSuite extends TestSuite {
    /**
     * @return Cache iterators test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = GridTestUtils.createLocalTestSuite("Gridgain Cache Iterators Test Suite");

        suite.addTest(new TestSuite(GridCacheLocalIteratorsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedIteratorsSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedIteratorsSelfTest.class));

        return suite;
   }
}
