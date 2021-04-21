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

import org.apache.ignite.jdbc.thin.JdbcThinAbstractSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinConnectionSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinPartitionAwarenessReconnectionAndFailoverSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinPartitionAwarenessSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinPartitionAwarenessTransactionsSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinStatementSelfTest;
import org.apache.ignite.jdbc.thin.JdbcThinTcpIoTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * JDBC Thin driver test suite to run in partition awareness mode.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    JdbcThinConnectionSelfTest.class,
    JdbcThinTcpIoTest.class,
    JdbcThinStatementSelfTest.class,
    JdbcThinPartitionAwarenessSelfTest.class,
    JdbcThinPartitionAwarenessTransactionsSelfTest.class,
    JdbcThinPartitionAwarenessReconnectionAndFailoverSelfTest.class,
})
public class IgniteJdbcThinDriverPartitionAwarenessTestSuite {
    /**
     * Setup partition awareness mode.
     */
    @BeforeClass
    public static void setupPartitionAwareness() {
        GridTestUtils.setFieldValue(JdbcThinAbstractSelfTest.class, "partitionAwareness", true);
    }
}
