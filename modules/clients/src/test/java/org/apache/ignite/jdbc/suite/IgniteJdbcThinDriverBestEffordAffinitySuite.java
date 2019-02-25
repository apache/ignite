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

package org.apache.ignite.jdbc.suite;

import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.jdbc.thin.JdbcThinAbstractSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinAutoCloseServerCursorTest;
import org.apache.ignite.jdbc.thin.JdbcThinBatchSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadAtomicPartitionedNearSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadAtomicPartitionedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadAtomicReplicatedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadTransactionalPartitionedNearSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadTransactionalPartitionedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinBulkLoadTransactionalReplicatedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlCustomSchemaSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexDmlDdlSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinComplexQuerySelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionMvccEnabledSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionSSLTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionTimeoutSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDataSourceSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDeleteStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexAtomicPartitionedNearSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexAtomicPartitionedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexAtomicReplicatedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexTransactionalPartitionedNearSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexTransactionalPartitionedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinDynamicIndexTransactionalReplicatedSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinEmptyCacheSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinInsertStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinInsertStatementSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMergeStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMergeStatementSkipReducerOnUpdateSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMetadataPrimaryKeysSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMetadataSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinMissingLongArrayResultsTest;
import org.apache.ignite.jdbc.thin.JdbcThinNoDefaultSchemaTest;
import org.apache.ignite.jdbc.thin.JdbcThinPreparedStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinResultSetSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinSchemaCaseTest;
import org.apache.ignite.jdbc.thin.JdbcThinSelectAfterAlterTable;
import org.apache.ignite.jdbc.thin.JdbcThinStatementCancelSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStatementTimeoutSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTcpIoTest;
import org.apache.ignite.jdbc.thin.JdbcThinUpdateStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinUpdateStatementSkipReducerOnUpdateSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * JDBC Thin driver test suite to run in best efford affinity mode.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    // New thin JDBC
    JdbcThinConnectionSelfTest.class,
    JdbcThinConnectionMvccEnabledSelfTest.class,
//    JdbcThinConnectionMultipleAddressesTest.class,
    JdbcThinTcpIoTest.class,
    JdbcThinConnectionSSLTest.class,
    JdbcThinDataSourceSelfTest.class,
    JdbcThinPreparedStatementSelfTest.class,
    JdbcThinResultSetSelfTest.class,

    JdbcThinStatementSelfTest.class,
    JdbcThinComplexQuerySelfTest.class,
    JdbcThinNoDefaultSchemaTest.class,
    JdbcThinSchemaCaseTest.class,
    JdbcThinEmptyCacheSelfTest.class,
    JdbcThinMetadataSelfTest.class,
    JdbcThinMetadataPrimaryKeysSelfTest.class,
//    JdbcThinMetadataSqlMatchTest.class, Uses individual connect method from utils
//2    JdbcThinErrorsSelfTest.class, JdbcErrorsAbstractSelfTest
    JdbcThinStatementCancelSelfTest.class,
    JdbcThinStatementTimeoutSelfTest.class,
    JdbcThinConnectionTimeoutSelfTest.class,

    JdbcThinInsertStatementSelfTest.class,
    JdbcThinUpdateStatementSelfTest.class,
    JdbcThinMergeStatementSelfTest.class,
    JdbcThinDeleteStatementSelfTest.class,
    JdbcThinAutoCloseServerCursorTest.class,
    JdbcThinBatchSelfTest.class,
    JdbcThinMissingLongArrayResultsTest.class,

    // New thin JDBC driver, DDL tests
    JdbcThinDynamicIndexAtomicPartitionedNearSelfTest.class,
    JdbcThinDynamicIndexAtomicPartitionedSelfTest.class,
    JdbcThinDynamicIndexAtomicReplicatedSelfTest.class,
    JdbcThinDynamicIndexTransactionalPartitionedNearSelfTest.class,
    JdbcThinDynamicIndexTransactionalPartitionedSelfTest.class,
    JdbcThinDynamicIndexTransactionalReplicatedSelfTest.class,
//    JdbcThinMultiStatementSelfTest.class, Uses individual connect method from utils

    // New thin JDBC driver, DML tests
    JdbcThinBulkLoadAtomicPartitionedNearSelfTest.class,
    JdbcThinBulkLoadAtomicPartitionedSelfTest.class,
    JdbcThinBulkLoadAtomicReplicatedSelfTest.class,
    JdbcThinBulkLoadTransactionalPartitionedNearSelfTest.class,
    JdbcThinBulkLoadTransactionalPartitionedSelfTest.class,
    JdbcThinBulkLoadTransactionalReplicatedSelfTest.class,

    // New thin JDBC driver, full SQL tests
    JdbcThinComplexDmlDdlSelfTest.class,

    JdbcThinSelectAfterAlterTable.class,

    // Update on server
    JdbcThinInsertStatementSkipReducerOnUpdateSelfTest.class,
    JdbcThinUpdateStatementSkipReducerOnUpdateSelfTest.class,
    JdbcThinMergeStatementSkipReducerOnUpdateSelfTest.class,
    JdbcThinComplexDmlDdlSkipReducerOnUpdateSelfTest.class,
    JdbcThinComplexDmlDdlCustomSchemaSelfTest.class,
})
public class IgniteJdbcThinDriverBestEffordAffinitySuite {

    /**
     * Setup best effort affinity mode.
     */
    @BeforeClass
    public static void setupBestEffortAffinity() {
        GridTestUtils.setFieldValue(JdbcThinConnection.class, "bestEffortAffinity", true);
        GridTestUtils.setFieldValue(JdbcThinAbstractSelfTest.class, "bestEffortAffinity", true);
    }
}