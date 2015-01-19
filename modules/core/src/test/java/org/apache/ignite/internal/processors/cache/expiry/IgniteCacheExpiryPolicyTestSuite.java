/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.expiry;

import junit.framework.*;

/**
 *
 */
public class IgniteCacheExpiryPolicyTestSuite extends TestSuite {
    /**
     * @return Cache Expiry Policy test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Cache Expiry Policy Test Suite");

        suite.addTestSuite(IgniteCacheAtomicLocalExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicWithStoreExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicPrimaryWriteOrderWithStoreExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheAtomicReplicatedExpiryPolicyTest.class);

        suite.addTestSuite(IgniteCacheTxLocalExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxWithStoreExpiryPolicyTest.class);
        suite.addTestSuite(IgniteCacheTxReplicatedExpiryPolicyTest.class);

        return suite;
    }
}
