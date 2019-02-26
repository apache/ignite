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
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Smoke checks for explain operations.
 */
public class ExplainSelfTest extends AbstractIndexingCommonTest {
    /** Ignite instance. */
    private static IgniteEx ignite;

    /** Handle to underlying cache of the TEST table. */
    private static IgniteCache<?, ?> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ignite = startGrid(1);

        cache = ignite.getOrCreateCache("testTableCache");

        execute("CREATE TABLE PUBLIC.TEST (ID LONG PRIMARY KEY, VAL LONG) WITH \"template=replicated\";", false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        ignite = null;
        cache = null;
        super.afterTestsStopped();
    }

    /**
     * Complex negative check that verifies EXPLAINs of update operations are not supported and cause correct
     * exceptions. Used local and non local queries
     */
    @Test
    public void testComplex() {
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (1, 2);", false);
        assertNotSupported("EXPLAIN UPDATE PUBLIC.TEST SET VAL = VAL + 1;", false);
        assertNotSupported("EXPLAIN MERGE INTO PUBLIC.TEST (ID, VAL) VALUES (1, 2);", false);
        assertNotSupported("EXPLAIN DELETE FROM PUBLIC.TEST;", false);

        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (1, 2);", true);
        assertNotSupported("EXPLAIN UPDATE PUBLIC.TEST SET VAL = VAL + 1;", true);
        assertNotSupported("EXPLAIN MERGE INTO PUBLIC.TEST (ID, VAL) VALUES (1, 2);", true);
        assertNotSupported("EXPLAIN DELETE FROM PUBLIC.TEST;", true);
    }

    /*
     In tests below, values to insert affect whether or not statement will be cached.
     */

    /**
     * Check that explain updates are not supported. Perform the same sql query as non-local and then as local query.
     */
    @Test
    public void test2InsertsLocalLast() {
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (1, 2);", false);
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (1, 2);", true);
    }

    /**
     * Check that explain updates are not supported. Perform the same sql query as local and then as non-local query.
     */
    @Test
    public void test2InsertsLocalFirst() {
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (3, 4);", true);
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (3, 4);", false);
    }

    /**
     * Check that explain updates are not supported. Perform the same sql query as local 2 times in a row.
     */
    @Test
    public void test2LocalInserts() {
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (5, 6);", true);
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (5, 6);", true);
    }

    /**
     * Check that explain updates are not supported. Perform the same sql query as non-local 2 times in a row.
     */
    @Test
    public void test2NonLocalInserts() {
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (7, 8);", false);
        assertNotSupported("EXPLAIN INSERT INTO PUBLIC.TEST VALUES (7, 8);", false);
    }

    /**
     * Check that EXPLAIN SELECT queries doesn't cause errors.
     */
    @Test
    public void testExplainSelect() {
        execute("EXPLAIN SELECT * FROM PUBLIC.TEST;", false);
        execute("EXPLAIN SELECT * FROM PUBLIC.TEST;", true);
    }

    /**
     * Executes sql query using native sql api.
     *
     * @param sql sql query.
     * @param local perform local or non-local query.
     * @return fully fetched result of query.
     */
    private List<List<?>> execute(String sql, boolean local) {
        return cache.query(new SqlFieldsQuery(sql).setLocal(local)).getAll();
    }

    /**
     * Assert that specified explain slq query is not supported due to it explains update (update, delete, insert,
     * etc.). operation.
     *
     * @param explainQry explain query of update operation.
     * @param local whether or not query to be executed should be local.
     */
    private void assertNotSupported(String explainQry, boolean local) {
        Throwable exc = GridTestUtils.assertThrows(
            ignite.log(),
            () -> cache.query(new SqlFieldsQuery(explainQry).setLocal(local)).getAll(),
            IgniteSQLException.class,
            "Explains of update queries are not supported.");

        IgniteSQLException sqlExc = ((IgniteSQLException)exc);

        assertEquals("Operation error code is not correct.",
            IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlExc.statusCode());
    }
}
