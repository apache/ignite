package org.apache.ignite.testsuites;

import junit.framework.TestSuite;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCollocatedAndNotTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinCustomAffinityMapper;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinNoIndexTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinPartitionedAndReplicatedTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinQueryConditionsTest;
import org.apache.ignite.internal.processors.cache.IgniteCacheDistributedJoinTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryNodeRestartDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.IgniteSqlDistributedJoinSelfTest;
import org.apache.ignite.internal.processors.query.h2.sql.H2CompareBigQueryDistributedJoinsTest;

/**
 *
 */
public class IgniteDistributedJoinTestSuite extends TestSuite {
    /**
     *
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Distributed Joins Test Suite.");

        suite.addTestSuite(H2CompareBigQueryDistributedJoinsTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCollocatedAndNotTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinCustomAffinityMapper.class);
        suite.addTestSuite(IgniteCacheDistributedJoinNoIndexTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinPartitionedAndReplicatedTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinQueryConditionsTest.class);
        suite.addTestSuite(IgniteCacheDistributedJoinTest.class);
        suite.addTestSuite(IgniteCacheQueryNodeRestartDistributedJoinSelfTest.class);
        suite.addTestSuite(IgniteCacheQueryStopOnCancelOrTimeoutDistributedJoinSelfTest.class);
        suite.addTestSuite(IgniteSqlDistributedJoinSelfTest.class);

        return suite;
    }
}
