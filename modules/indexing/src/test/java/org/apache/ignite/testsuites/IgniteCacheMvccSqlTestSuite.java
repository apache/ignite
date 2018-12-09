/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedNearDisabledPrimaryNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.dht.IgniteCachePartitionedTwoBackupsPrimaryNodeFailureRecoveryTest;
import org.apache.ignite.internal.processors.cache.distributed.replicated.GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest;
import org.apache.ignite.internal.processors.cache.index.MvccEmptyTransactionSelfTest;
import org.apache.ignite.internal.processors.cache.index.SqlTransactionsCommandsWithMvccEnabledSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccBasicContinuousQueryTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccBulkLoadTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccClientReconnectContinuousQueryTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryBackupQueueTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryClientReconnectTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryClientTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryImmutableEntryTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryMultiNodesFilteringTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryPartitionedTxOneNodeTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousQueryReplicatedTxOneNodeTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousWithTransformerClientSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousWithTransformerPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccContinuousWithTransformerReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccDmlSimpleTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccIteratorWithConcurrentJdbcTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccLocalEntriesWithConcurrentJdbcTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedBackupsTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSelectForUpdateQueryTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSqlCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSqlQueriesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSqlTxQueriesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccPartitionedSqlTxQueriesWithReducerTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedBackupsTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSelectForUpdateQueryTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSqlCoordinatorFailoverTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSqlQueriesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSqlTxQueriesTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccReplicatedSqlTxQueriesWithReducerTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccScanQueryWithConcurrentJdbcTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSizeTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSizeWithConcurrentJdbcTransactionTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSqlConfigurationValidationTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSqlContinuousQueryPartitionedSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSqlContinuousQueryReplicatedSelfTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSqlLockTimeoutTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccSqlUpdateCountersTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccStreamingInsertTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxNodeMappingTest;
import org.apache.ignite.internal.processors.cache.mvcc.CacheMvccTxRecoveryTest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccRepeatableReadBulkOpsTest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccRepeatableReadOperationsTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildWithMvccEnabledSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.AllTests;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

/**
 *
 */
@RunWith(AllTests.class)
public class IgniteCacheMvccSqlTestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("IgniteCache SQL MVCC Test Suite");

        // Simple tests.
        suite.addTest(new JUnit4TestAdapter(MvccEmptyTransactionSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccSqlConfigurationValidationTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccDmlSimpleTest.class));
        suite.addTest(new JUnit4TestAdapter(SqlTransactionsCommandsWithMvccEnabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccSizeTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccSqlUpdateCountersTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccSqlLockTimeoutTest.class));

        suite.addTest(new JUnit4TestAdapter(GridIndexRebuildWithMvccEnabledSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccTxNodeMappingTest.class));

        // SQL vs CacheAPI consistency.
        suite.addTest(new JUnit4TestAdapter(MvccRepeatableReadOperationsTest.class));
        suite.addTest(new JUnit4TestAdapter(MvccRepeatableReadBulkOpsTest.class));

        // JDBC tests.
        suite.addTest(new JUnit4TestAdapter(CacheMvccSizeWithConcurrentJdbcTransactionTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccScanQueryWithConcurrentJdbcTransactionTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccLocalEntriesWithConcurrentJdbcTransactionTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccIteratorWithConcurrentJdbcTransactionTest.class));

        // Load tests.
        suite.addTest(new JUnit4TestAdapter(CacheMvccBulkLoadTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccStreamingInsertTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccPartitionedSqlQueriesTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccReplicatedSqlQueriesTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccPartitionedSqlTxQueriesTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccReplicatedSqlTxQueriesTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccPartitionedSqlTxQueriesWithReducerTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccReplicatedSqlTxQueriesWithReducerTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccPartitionedSelectForUpdateQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccReplicatedSelectForUpdateQueryTest.class));

        // Failover tests.
        suite.addTest(new JUnit4TestAdapter(CacheMvccPartitionedBackupsTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccReplicatedBackupsTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccPartitionedSqlCoordinatorFailoverTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccReplicatedSqlCoordinatorFailoverTest.class));

        // Continuous queries.
        suite.addTest(new JUnit4TestAdapter(CacheMvccBasicContinuousQueryTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryReplicatedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccSqlContinuousQueryPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccSqlContinuousQueryReplicatedSelfTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryPartitionedTxOneNodeTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryReplicatedTxOneNodeTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryClientReconnectTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryClientTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryMultiNodesFilteringTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryBackupQueueTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousQueryImmutableEntryTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccClientReconnectContinuousQueryTest.class));

        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousWithTransformerClientSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousWithTransformerPartitionedSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(CacheMvccContinuousWithTransformerReplicatedSelfTest.class));

        // Transaction recovery.
        suite.addTest(new JUnit4TestAdapter(CacheMvccTxRecoveryTest.class));

        suite.addTest(new JUnit4TestAdapter(MvccPartitionedPrimaryNodeFailureRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(MvccPartitionedTwoBackupsPrimaryNodeFailureRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(MvccColocatedTxPessimisticOriginatingNodeFailureRecoveryTest.class));
        suite.addTest(new JUnit4TestAdapter(MvccReplicatedTxPessimisticOriginatingNodeFailureRecoveryTest.class));

        return suite;
    }

    /** */
    public static class MvccPartitionedPrimaryNodeFailureRecoveryTest
        extends IgniteCachePartitionedNearDisabledPrimaryNodeFailureRecoveryTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }
    }

    /** */
    public static class MvccPartitionedTwoBackupsPrimaryNodeFailureRecoveryTest
        extends IgniteCachePartitionedTwoBackupsPrimaryNodeFailureRecoveryTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }

        /** {@inheritDoc} */
        @Override protected NearCacheConfiguration nearConfiguration() {
            return null;
        }
    }

    /** */
    public static class MvccColocatedTxPessimisticOriginatingNodeFailureRecoveryTest
        extends GridCacheColocatedTxPessimisticOriginatingNodeFailureSelfTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }
    }

    /** */
    public static class MvccReplicatedTxPessimisticOriginatingNodeFailureRecoveryTest
        extends GridCacheReplicatedTxPessimisticOriginatingNodeFailureSelfTest {
        /** {@inheritDoc} */
        @Override protected CacheAtomicityMode atomicityMode() {
            return TRANSACTIONAL_SNAPSHOT;
        }
    }
}
