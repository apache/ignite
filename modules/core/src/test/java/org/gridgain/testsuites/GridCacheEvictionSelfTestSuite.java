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
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.cache.eviction.*;
import org.gridgain.grid.kernal.processors.cache.eviction.fifo.*;
import org.gridgain.grid.kernal.processors.cache.eviction.lru.*;
import org.gridgain.grid.kernal.processors.cache.eviction.random.*;

/**
 * Test suite for cache eviction.
 */
public class GridCacheEvictionSelfTestSuite extends TestSuite {
    /**
     * @return Cache eviction test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Cache Eviction Test Suite");

        suite.addTest(new TestSuite(GridCacheFifoEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(GridCacheLruEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(GridCacheLruNearEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearOnlyLruNearEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(GridCacheRandomEvictionPolicySelfTest.class));
        suite.addTest(new TestSuite(GridCacheNearEvictionSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicNearEvictionSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEvictionFilterSelfTest.class));
        suite.addTest(new TestSuite(GridCacheConcurrentEvictionsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheConcurrentEvictionConsistencySelfTest.class));
        suite.addTest(new TestSuite(GridCacheEvictionTouchSelfTest.class));
        suite.addTest(new TestSuite(GridCacheDistributedEvictionsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEvictionLockUnlockSelfTest.class));
        suite.addTest(new TestSuite(GridCacheBatchEvictUnswapSelfTest.class));
        suite.addTest(new TestSuite(GridCachePreloadingEvictionsSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEmptyEntriesPartitionedSelfTest.class));
        suite.addTest(new TestSuite(GridCacheEmptyEntriesLocalSelfTest.class));
        suite.addTest(new TestSuite(GridCacheMemoryModeSelfTest.class));

        return suite;
    }
}
