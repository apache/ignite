/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites.bamboo;

import junit.framework.*;
import org.gridgain.grid.cache.hibernate.*;
import org.gridgain.grid.cache.store.hibernate.*;

/**
 * Hibernate integration tests.
 */
public class GridHibernateTestSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception Thrown in case of the failure.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Hibernate Integration Test Suite");

        // Hibernate L2 cache.
        suite.addTestSuite(GridHibernateL2CacheSelfTest.class);
        suite.addTestSuite(GridHibernateL2CacheTransactionalSelfTest.class);
        suite.addTestSuite(GridHibernateL2CacheConfigurationSelfTest.class);

        suite.addTestSuite(GridCacheHibernateBlobStoreSelfTest.class);

        return suite;
    }
}
