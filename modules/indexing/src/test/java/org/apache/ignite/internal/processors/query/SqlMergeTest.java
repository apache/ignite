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

/**
 * Tests for SQL MERGE.
 */
public class SqlMergeTest extends AbstractIndexingCommonTest {
    /** Node. */
    private static IgniteEx srv;

    /** Node. */
    protected IgniteEx node;


    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (String cache : srv.cacheNames())
            srv.cache(cache).destroy();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        node = srv;
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

        checkMergeQuery("MERGE INTO test1 (id2, id, name) VALUES (3, 2, 'Santa Claus')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 2",
            Arrays.asList(2, 3, "Santa Claus"));

        checkMergeQuery("MERGE INTO test1 (name, id, id2) VALUES ('Holy Jesus', 1, 2), ('Kartman', 3, 4)", 2L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 1",
            Arrays.asList(1, 2, "Holy Jesus"));
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 3",
            Arrays.asList(3, 4, "Kartman"));

        checkMergeQuery("MERGE INTO test1 (id, id2, name) " +
            "SELECT id, id2 * 1000, UPPER(name) FROM test1 WHERE id < 2", 1L);
        checkSqlResults("SELECT id, id2, name FROM test1 WHERE id = 1 AND id2 = 2000",
            Arrays.asList(1, 2000, "HOLY JESUS"));
    }

    /**
     *
     */
    @Test
    public void testCheckKeysWarning() throws Exception {
        LogListener logLsnr = LogListener
            .matches("The search row by explicit KEY isn't supported. The primary key is always used to search row")
            .build();

        ListeningTestLogger listeningTestLogger = testLog();

        listeningTestLogger.registerListener(logLsnr);

        sql("CREATE TABLE test2 (id INT, id2 INT, name VARCHAR, PRIMARY KEY (id, id2))");

        checkMergeQuery("MERGE INTO test2 (id, id2, name) KEY(_key) VALUES (100, 1, 'Bob')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 100",
            Arrays.asList(100, 1, "Bob"));
        assertTrue(logLsnr.check());
        logLsnr.reset();

        checkMergeQuery("MERGE INTO test2 (id2, id, name) KEY(_key) VALUES (2, 100, 'Alice')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 100 AND id2 = 2",
            Arrays.asList(100, 2, "Alice"));
        assertTrue(logLsnr.check());
        logLsnr.reset();

        checkMergeQuery("MERGE INTO test2 (id, id2, name) KEY(id, id2) VALUES (3, 5, 'Stan')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 3",
            Arrays.asList(3, 5, "Stan"));
        assertTrue(logLsnr.check());
        logLsnr.reset();

        checkMergeQuery("MERGE INTO test2 (id, id2, name) KEY(id2, id) VALUES (1, 100, 'Satan')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 1",
            Arrays.asList(1, 100, "Satan"));
        assertTrue(logLsnr.check());
        logLsnr.reset();

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

        checkMergeQuery("MERGE INTO test2 (id, id2, name) KEY(name) VALUES (10, -11, 'Sherlock')", 1L);
        checkSqlResults("SELECT id, id2, name FROM test2 WHERE id = 10 and id2=-11",
            Arrays.asList(10, -11, "Sherlock"));
        assertTrue(logLsnr.check());
        logLsnr.reset();

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
        GridQueryProcessor qryProc = node.context().query();

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setSchema("PUBLIC");

        return qryProc.querySqlFields(qry, true).getAll();
    }

    /**
     * Setup and return test log.
     *
     * @return Test logger.
     */
    private ListeningTestLogger testLog() {
        ListeningTestLogger testLog = new ListeningTestLogger(false, log);

        GridTestUtils.setFieldValue(((IgniteH2Indexing)node.context().query().getIndexing()).parser(), "log", testLog);

        return testLog;
    }
}
