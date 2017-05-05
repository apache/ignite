package org.apache.ignite.cache.database.standbycluster.extended;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedAtomicMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest;

/**
 *
 */
public class GridActivationReplicatedCacheSuit extends GridActivationCacheAbstractTestSuit {
    static {
        addTest(CacheReplicatedRendezvousAffinityExcludeNeighborsMultiNodeFullApiSelfTest.class);
        addTest(CacheReplicatedRendezvousAffinityMultiNodeFullApiSelfTest.class);
        addTest(GridCacheReplicatedAtomicFullApiSelfTest.class);
        addTest(GridCacheReplicatedFullApiSelfTest.class);
        addTest(GridCacheReplicatedMultiNodeFullApiSelfTest.class);
        addTest(GridCacheReplicatedAtomicMultiNodeFullApiSelfTest.class);
        addTest(GridCacheReplicatedMultiNodeP2PDisabledFullApiSelfTest.class);
        addTest(GridCacheReplicatedNearOnlyMultiNodeFullApiSelfTest.class);

//        tests.add(transform(GridCacheReplicatedAtomicMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedAtomicPrimaryWriteOrderMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedMultiJvmP2PDisabledFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedNearOnlyMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedOffHeapMultiJvmFullApiSelfTest.class));
//        tests.add(transform(GridCacheReplicatedOffHeapTieredMultiJvmFullApiSelfTest.class));
    }

    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = buildSuite();

        suite.setName("Activation Stand-by Cluster After Primary Cluster Stopped  Check Replicated Cache");

        return suite;
    }
}
