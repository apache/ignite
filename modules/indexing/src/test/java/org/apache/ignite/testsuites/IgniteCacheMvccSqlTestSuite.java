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

import junit.framework.TestSuite;
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
import org.apache.ignite.internal.processors.cache.mvcc.MvccRepeatableReadBulkOpsTest;
import org.apache.ignite.internal.processors.cache.mvcc.MvccRepeatableReadOperationsTest;
import org.apache.ignite.internal.processors.query.h2.GridIndexRebuildWithMvccEnabledSelfTest;

/**
 *
 */
public class IgniteCacheMvccSqlTestSuite extends TestSuite {
    /**
     * @return Test suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("IgniteCache SQL MVCC Test Suite");

        // Simple tests.
        suite.addTestSuite(MvccEmptyTransactionSelfTest.class);
        suite.addTestSuite(CacheMvccSqlConfigurationValidationTest.class);
        suite.addTestSuite(CacheMvccDmlSimpleTest.class);
        suite.addTestSuite(SqlTransactionsCommandsWithMvccEnabledSelfTest.class);
        suite.addTestSuite(CacheMvccSizeTest.class);
        suite.addTestSuite(CacheMvccSqlUpdateCountersTest.class);
        suite.addTestSuite(CacheMvccSqlLockTimeoutTest.class);

        suite.addTestSuite(GridIndexRebuildWithMvccEnabledSelfTest.class);

        suite.addTestSuite(CacheMvccTxNodeMappingTest.class);

        // SQL vs CacheAPI consistency.
        suite.addTestSuite(MvccRepeatableReadOperationsTest.class);
        suite.addTestSuite(MvccRepeatableReadBulkOpsTest.class);

        // JDBC tests.
        suite.addTestSuite(CacheMvccSizeWithConcurrentJdbcTransactionTest.class);
        suite.addTestSuite(CacheMvccScanQueryWithConcurrentJdbcTransactionTest.class);
        suite.addTestSuite(CacheMvccLocalEntriesWithConcurrentJdbcTransactionTest.class);
        suite.addTestSuite(CacheMvccIteratorWithConcurrentJdbcTransactionTest.class);

        // Load tests.
        suite.addTestSuite(CacheMvccBulkLoadTest.class);
        suite.addTestSuite(CacheMvccStreamingInsertTest.class);

        suite.addTestSuite(CacheMvccPartitionedSqlQueriesTest.class);
        suite.addTestSuite(CacheMvccReplicatedSqlQueriesTest.class);
        suite.addTestSuite(CacheMvccPartitionedSqlTxQueriesTest.class);
        suite.addTestSuite(CacheMvccReplicatedSqlTxQueriesTest.class);

        suite.addTestSuite(CacheMvccPartitionedSqlTxQueriesWithReducerTest.class);
        suite.addTestSuite(CacheMvccReplicatedSqlTxQueriesWithReducerTest.class);
        suite.addTestSuite(CacheMvccPartitionedSelectForUpdateQueryTest.class);
        suite.addTestSuite(CacheMvccReplicatedSelectForUpdateQueryTest.class);

        // Failover tests.
        suite.addTestSuite(CacheMvccPartitionedBackupsTest.class);
        suite.addTestSuite(CacheMvccReplicatedBackupsTest.class);

        suite.addTestSuite(CacheMvccPartitionedSqlCoordinatorFailoverTest.class);
        suite.addTestSuite(CacheMvccReplicatedSqlCoordinatorFailoverTest.class);

        // Continuous queries.
        suite.addTestSuite(CacheMvccBasicContinuousQueryTest.class);
        suite.addTestSuite(CacheMvccContinuousQueryPartitionedSelfTest.class);
        suite.addTestSuite(CacheMvccContinuousQueryReplicatedSelfTest.class);
        suite.addTestSuite(CacheMvccSqlContinuousQueryPartitionedSelfTest.class);
        suite.addTestSuite(CacheMvccSqlContinuousQueryReplicatedSelfTest.class);

        suite.addTestSuite(CacheMvccContinuousQueryPartitionedTxOneNodeTest.class);
        suite.addTestSuite(CacheMvccContinuousQueryReplicatedTxOneNodeTest.class);

        suite.addTestSuite(CacheMvccContinuousQueryClientReconnectTest.class);
        suite.addTestSuite(CacheMvccContinuousQueryClientTest.class);

        suite.addTestSuite(CacheMvccContinuousQueryMultiNodesFilteringTest.class);
        suite.addTestSuite(CacheMvccContinuousQueryBackupQueueTest.class);
        suite.addTestSuite(CacheMvccContinuousQueryImmutableEntryTest.class);
        suite.addTestSuite(CacheMvccClientReconnectContinuousQueryTest.class);

        suite.addTestSuite(CacheMvccContinuousWithTransformerClientSelfTest.class);
        suite.addTestSuite(CacheMvccContinuousWithTransformerPartitionedSelfTest.class);
        suite.addTestSuite(CacheMvccContinuousWithTransformerReplicatedSelfTest.class);

        return suite;
    }
}
