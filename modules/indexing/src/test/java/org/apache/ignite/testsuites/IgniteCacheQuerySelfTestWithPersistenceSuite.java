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
import org.apache.ignite.internal.processors.cache.IgniteCheckClusterStateBeforeExecuteQueryTest;
import org.apache.ignite.internal.processors.cache.IgniteDynamicSqlRestoreTest;
import org.apache.ignite.internal.processors.cache.authentication.SqlUserCommandSelfTest;
import org.apache.ignite.internal.processors.cache.index.H2RowCachePageEvictionTest;
import org.apache.ignite.internal.processors.cache.index.OptimizedMarshallerIndexNameTest;
import org.apache.ignite.internal.processors.database.baseline.IgniteStableBaselineBinObjFieldsQuerySelfTest;
import org.apache.ignite.testframework.IgniteTestSuite;

/**
 * Test suite for cache queries.
 */
public class IgniteCacheQuerySelfTestWithPersistenceSuite extends TestSuite {
    /**
     * @return Test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        IgniteTestSuite suite = new IgniteTestSuite("Ignite Cache Queries Test Suite");

        suite.addTestSuite(IgniteCheckClusterStateBeforeExecuteQueryTest.class);
        suite.addTestSuite(IgniteDynamicSqlRestoreTest.class);

        suite.addTestSuite(H2RowCachePageEvictionTest.class);

        suite.addTestSuite(OptimizedMarshallerIndexNameTest.class);

        suite.addTestSuite(SqlUserCommandSelfTest.class);

        suite.addTestSuite(IgniteStableBaselineBinObjFieldsQuerySelfTest.class);


        return suite;
    }
}
