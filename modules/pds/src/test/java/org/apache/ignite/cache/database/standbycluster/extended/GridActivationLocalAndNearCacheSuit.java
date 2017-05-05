package org.apache.ignite.cache.database.standbycluster.extended;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.local.GridCacheLocalFullApiSelfTest;

/**
 *
 */
public class GridActivationLocalAndNearCacheSuit extends GridActivationCacheAbstractTestSuit {
    static {
        addTest(GridCacheLocalAtomicFullApiSelfTest.class);
        addTest(GridCacheLocalFullApiSelfTest.class);

//        addTest(GridCacheNearOnlyFairAffinityMultiJvmFullApiSelfTest.class);
//        addTest(GridCacheNearOnlyMultiJvmFullApiSelfTest.class);
//        addTest(GridCacheNearOnlyMultiJvmP2PDisabledFullApiSelfTest.class);
    }

    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = buildSuite();

        suite.setName("Activation Stand-by Cluster After Primary Cluster Stopped Check Local and Near Cache");

        return suite;
    }
}
