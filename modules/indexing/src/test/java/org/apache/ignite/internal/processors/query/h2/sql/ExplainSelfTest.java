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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Smoke checks for explain operations.
 */
public class ExplainSelfTest extends GridCommonAbstractTest {
    /** Ignite instance. */
    private static IgniteEx ignite;

    /** Handle to underlying cache of the TEST table. */
    private static IgniteCache<?,?> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(1);

        execute("CREATE TABLE TEST (ID LONG PRIMARY KEY, VAL LONG);");

        cache = ignite.cache("SQL_PUBLIC_TEST");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        try {
            stopAllGrids();
        }
        finally {
            super.afterTestsStopped();
        }
    }

    /**
     * Negative check that verifies EXPLAINs of update operations are not supported and cause correct exceptions.
     */
    public void testExplainUpdateOperation() {
        assertNotSupported("EXPLAIN INSERT INTO TEST VALUES (1, 2);");
        assertNotSupported("EXPLAIN UPDATE TEST SET VAL = VAL + 1;");
        assertNotSupported("EXPLAIN MERGE INTO TEST (ID, VAL) VALUES (1, 2);");
        assertNotSupported("EXPLAIN DELETE FROM TEST;");
    }

    /**
     * Check that EXPLAIN SELECT queries doesn't cause errors.
     */
    public void testExplainSelect() {
        execute("EXPLAIN SELECT * FROM TEST;");
    }

    /**
     * Executes sql query using native sql api.
     *
     * @param sql sql query.
     * @return fully fetched result of query.
     */
    private List<List<?>> execute(String sql) {
        return cache.query(new SqlFieldsQuery(sql)).getAll();
    }

    /**
     * Assert that specified explain slq query is not supported due to it explains update (update, delete, insert,
     * etc.). operation.
     *
     * @param explainSql explain query of update operation.
     */
    private void assertNotSupported(String explainSql) {
        Throwable exc = GridTestUtils.assertThrows(ignite.log(), () -> execute(explainSql), IgniteSQLException.class,
            "Explains of update queries are not supported.");

        IgniteSQLException sqlExc = ((IgniteSQLException)exc);

        assertEquals("Operation error code is not correct.",
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlExc.statusCode());
    }
}
