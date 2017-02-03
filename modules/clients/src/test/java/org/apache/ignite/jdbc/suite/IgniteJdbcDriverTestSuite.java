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

import junit.framework.TestSuite;
import org.apache.ignite.jdbc.JdbcComplexQuerySelfTest;
import org.apache.ignite.jdbc.JdbcConnectionSelfTest;
import org.apache.ignite.jdbc.JdbcEmptyCacheSelfTest;
import org.apache.ignite.jdbc.JdbcLocalCachesSelfTest;
import org.apache.ignite.jdbc.JdbcMetadataSelfTest;
import org.apache.ignite.jdbc.JdbcNoDefaultCacheTest;
import org.apache.ignite.jdbc.JdbcPreparedStatementSelfTest;
import org.apache.ignite.jdbc.JdbcResultSetSelfTest;
import org.apache.ignite.jdbc.JdbcStatementSelfTest;

/**
 * JDBC driver test suite.
 */
public class IgniteJdbcDriverTestSuite extends TestSuite {
    /**
     * @return JDBC Driver Test Suite.
     * @throws Exception In case of error.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Ignite JDBC Driver Test Suite");

        // Thin client based driver tests
        suite.addTest(new TestSuite(JdbcConnectionSelfTest.class));
        suite.addTest(new TestSuite(JdbcStatementSelfTest.class));
        suite.addTest(new TestSuite(JdbcPreparedStatementSelfTest.class));
        suite.addTest(new TestSuite(JdbcResultSetSelfTest.class));
        suite.addTest(new TestSuite(JdbcComplexQuerySelfTest.class));
        suite.addTest(new TestSuite(JdbcMetadataSelfTest.class));
        suite.addTest(new TestSuite(JdbcEmptyCacheSelfTest.class));
        suite.addTest(new TestSuite(JdbcLocalCachesSelfTest.class));
        suite.addTest(new TestSuite(JdbcNoDefaultCacheTest.class));

        // Ignite client node based driver tests
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcConnectionSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcStatementSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcPreparedStatementSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcResultSetSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcComplexQuerySelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcDistributedJoinsQueryTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcMetadataSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcEmptyCacheSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcLocalCachesSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcNoDefaultCacheTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcMergeStatementSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcBinaryMarshallerMergeStatementSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcInsertStatementSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcBinaryMarshallerInsertStatementSelfTest.class));
        suite.addTest(new TestSuite(org.apache.ignite.internal.jdbc2.JdbcDeleteStatementSelfTest.class));

        return suite;
    }
}
