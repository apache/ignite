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

package org.apache.ignite.internal.processors.query;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.DFLT_TO_STRING_INCLUDE_SENSITIVE;

/**
 * Tests for SQL MERGE.
 */
public class SqlMergeTest extends AbstractIndexingCommonTest {
    /** Node. */
    private IgniteEx srv;

    /** */
    private final LogListener logLsnr = LogListener
        .matches("The search row by explicit KEY isn't supported. The primary key is always used to search row")
        .build();

    /** */
    private ListeningTestLogger listeningTestLog;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        QueryUtils.INCLUDE_SENSITIVE = DFLT_TO_STRING_INCLUDE_SENSITIVE;

        for (String cache : srv.cacheNames())
            srv.cache(cache).destroy();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        listeningTestLog = testLog();

        listeningTestLog.registerListener(logLsnr);
    }

    /** @return Node to execute queries. */
    protected IgniteEx node() {
        return grid(0);
    }

    /**
     *
     */
    @Test
    public void test() throws Exception {
        sql("CREATE TABLE test1 (id INT, id2 INT, name VARCHAR, PRIMARY KEY (id, id2))");

        checkMergeQuery("MERGE INTO test1 (id, id2, name) VALUES (1, 2, 'Kyle')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 1",
            Arrays.asList(1, 2, "Kyle"));
        assertFalse(logLsnr.check());

        checkMergeQuery("MERGE INTO test1 (id2, id, name) VALUES (3, 2, 'Santa Claus')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 2",
            Arrays.asList(2, 3, "Santa Claus"));
        assertFalse(logLsnr.check());

        checkMergeQuery("MERGE INTO test1 (name, id, id2) VALUES ('Holy Jesus', 1, 2), ('Kartman', 3, 4)", 2L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 1",
            Arrays.asList(1, 2, "Holy Jesus"));
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 3",
            Arrays.asList(3, 4, "Kartman"));
        assertFalse(logLsnr.check());

        checkMergeQuery("MERGE INTO test1 (id, id2, name) " +
            "SELECT id, id2 * 1000, UPPER(name) FROM test1 WHERE id < 2", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 1 AND id2 = 2000",
            Arrays.asList(1, 2000, "HOLY JESUS"));
        assertFalse(logLsnr.check());

        checkMergeQuery("MERGE INTO test1 (id, id2, name) KEY(_key) VALUES (100, 1, 'Bob')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 100",
            Arrays.asList(100, 1, "Bob"));
        assertFalse(logLsnr.check());
        logLsnr.reset();

        checkMergeQuery("MERGE INTO test1 (id2, id, name) KEY(id, id2) VALUES (2, 100, 'Alice')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 100 AND id2 = 2",
            Arrays.asList(100, 2, "Alice"));
        assertFalse(logLsnr.check());
        logLsnr.reset();

        checkMergeQuery("MERGE INTO test1 (id, id2, name) KEY(id, id2) VALUES (6, 5, 'Stan')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 6",
            Arrays.asList(6, 5, "Stan"));
        assertFalse(logLsnr.check());
        logLsnr.reset();

        checkMergeQuery("MERGE INTO test1 (id, id2, name) KEY(id2, id) VALUES (10, 100, 'Satan')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 10",
            Arrays.asList(10, 100, "Satan"));
        assertFalse(logLsnr.check());
        logLsnr.reset();
    }

    /**
     *
     */
    @Test
    public void testCheckKeysWarning() throws Exception {
        sql("CREATE TABLE test2 (id INT, id2 INT, name VARCHAR, PRIMARY KEY (id, id2))");

        checkMergeQuery("MERGE INTO test2 (id2, id, name) KEY(id) VALUES (15, 32, 'Kyle')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 32",
            Arrays.asList(32, 15, "Kyle"));
        assertTrue(logLsnr.check());
        logLsnr.reset();

        checkMergeQuery("MERGE INTO test2 (name, id, id2) KEY(id2) VALUES ('Morlock', 13, 12)", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 13",
            Arrays.asList(13, 12, "Morlock"));
        assertTrue(logLsnr.check());
        logLsnr.reset();

        checkMergeQuery("MERGE INTO test2 (id, name, id2) KEY(_key, id) VALUES (10, 'Warlock', 52)", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 10",
            Arrays.asList(10, 52, "Warlock"));
        assertTrue(logLsnr.check());
        logLsnr.reset();

        LogListener sensitiveLsnr = LogListener.matches("Sherlock").build();

        listeningTestLog.registerListener(sensitiveLsnr);

        // Check sensitive log output.
        checkMergeQuery("MERGE INTO test2 (id, id2, name) KEY(name) VALUES (10, -11, 'Sherlock')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 10 and id2=-11",
            Arrays.asList(10, -11, "Sherlock"));
        assertTrue(logLsnr.check());
        assertTrue(sensitiveLsnr.check());
        logLsnr.reset();
        sensitiveLsnr.reset();

        QueryUtils.INCLUDE_SENSITIVE = false;

        checkMergeQuery("MERGE INTO test2 (id, id2, name) KEY(name) VALUES (10, -12, 'Sherlock')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 10 and id2=-12",
            Arrays.asList(10, -12, "Sherlock"));
        assertTrue(logLsnr.check());
        assertFalse(sensitiveLsnr.check());
    }

    /**
     * @param sql MERGE query.
     */
    private void checkMergeQuery(String sql, long expectedUpdateCounts) throws Exception {
        List<List<?>> resMrg = sql(sql);

        assertEquals(1, resMrg.size());
        assertEquals(1, resMrg.get(0).size());
        assertEquals(expectedUpdateCounts, resMrg.get(0).get(0));
    }

    /**
     * @param sql SELECT query to check merge result.
     * @param expectedRow Expected results of the SELECT.
     */
    private void checkSqlResults(String sql, List<?> expectedRow) throws Exception {
        List<List<?>> res = sql(sql);
        assertEquals(1, res.size());
        assertEquals(expectedRow, res.get(0));
    }

    /**
     * @param sql SQL query.
     * @return Results.
     */
    protected List<List<?>> sql(String sql) throws Exception {
        GridQueryProcessor qryProc = node().context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        return qryProc.querySqlFields(qry, true).getAll();
    }

    /**
     * Setup and return test log.
     *
     * @return Test logger.
     */
    private ListeningTestLogger testLog() {
        ListeningTestLogger testLog = new ListeningTestLogger(log);

        GridTestUtils.setFieldValue(((IgniteH2Indexing)node().context().query().getIndexing()).parser(), "log", testLog);

        return testLog;
    }
}
