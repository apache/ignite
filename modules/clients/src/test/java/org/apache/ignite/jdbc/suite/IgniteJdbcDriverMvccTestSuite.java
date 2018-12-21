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

import junit.framework.JUnit4TestAdapter;
import junit.framework.TestSuite;
import org.apache.ignite.jdbc.JdbcVersionMismatchSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionMvccEnabledSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsClientAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsClientNoAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsServerAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsServerNoAutoCommitComplexSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTransactionsWithMvccEnabledSelfTest;
import org.apache.ignite.jdbc.thin.MvccJdbcTransactionFinishOnDeactivatedClusterSelfTest;

/** */
public class IgniteJdbcDriverMvccTestSuite extends TestSuite {
    /**
     * @return JDBC Driver Test Suite.
     */
    public static TestSuite suite() {
        TestSuite suite = new TestSuite("Ignite JDBC Driver Test Suite");

        suite.addTest(new JUnit4TestAdapter(JdbcThinConnectionMvccEnabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcVersionMismatchSelfTest.class));

        // Transactions
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsWithMvccEnabledSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsClientAutoCommitComplexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsServerAutoCommitComplexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsClientNoAutoCommitComplexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(JdbcThinTransactionsServerNoAutoCommitComplexSelfTest.class));
        suite.addTest(new JUnit4TestAdapter(MvccJdbcTransactionFinishOnDeactivatedClusterSelfTest.class));

        return suite;
    }
}
