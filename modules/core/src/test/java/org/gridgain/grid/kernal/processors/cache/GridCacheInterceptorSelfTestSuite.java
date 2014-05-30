/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import junit.framework.*;

/**
 * Cache interceptor suite.
 */
public class GridCacheInterceptorSelfTestSuite extends TestSuite {
    /**
     * @return Cache API test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain CacheInterceptor Test Suite");

        suite.addTestSuite(GridCacheInterceptorLocalSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorLocalAtomicSelfTest.class);

        suite.addTestSuite(GridCacheInterceptorAtomicSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorAtomicPrimaryWriteOrderSelfTest.class);

        suite.addTestSuite(GridCacheInterceptorAtomicReplicatedSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorAtomicReplicatedPrimaryWriteOrderSelfTest.class);

        suite.addTestSuite(GridCacheInterceptorSelfTest.class);
        suite.addTestSuite(GridCacheInterceptorReplicatedSelfTest.class);

        return suite;
    }
}
