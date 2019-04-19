/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.jdbc.suite;

import org.apache.ignite.jdbc.JdbcVersionMismatchSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionMvccEnabledSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsClientAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsClientNoAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsServerAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsServerNoAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsWithMvccEnabledSelfTest;
import org.apache.ignite.jdbc.thin.MvccJdbcTransactionFinishOnDeactivatedClusterSelfTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/** */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    JdbcThinConnectionMvccEnabledSelfTest.class,
    JdbcVersionMismatchSelfTest.class,

    // Transactions
    JdbcThinTransactionsWithMvccEnabledSelfTest.class,
    JdbcThinTransactionsClientAutoCommitComplexSelfTest.class,
    JdbcThinTransactionsServerAutoCommitComplexSelfTest.class,
    JdbcThinTransactionsClientNoAutoCommitComplexSelfTest.class,
    JdbcThinTransactionsServerNoAutoCommitComplexSelfTest.class,
    MvccJdbcTransactionFinishOnDeactivatedClusterSelfTest.class
})
public class IgniteJdbcDriverMvccTestSuite {
}
