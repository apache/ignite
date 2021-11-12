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
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.QueryState;
import org.apache.ignite.internal.processors.query.RunningQuery;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.joining;

/**
 *
 */
public class RunningQueriesIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    private static IgniteEx srv;

    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 10_000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srv = grid(0);
    }

    /**
     * Execute query with a lot of JOINs to produce very long planning phase.
     * Cancel query on planning phase and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelAtPlanningPhase() throws IgniteCheckedException {
        QueryEngine engine = queryProcessor(client);
        int cnt = 9;

        for (int i = 0; i < cnt; i++)
            sql("CREATE TABLE test_tbl" + i + " (id int, val varchar)");

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "test_tbl" + i + " p" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql(sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> !engine.runningQueries().isEmpty() || fut.isDone(), TIMEOUT_IN_MS));

        Collection<? extends RunningQuery> running = engine.runningQueries();

        assertEquals("Running: " + running, 1, running.size());

        RunningQuery qry = F.first(running);

        assertSame(qry, engine.runningQuery(qry.id()));

        // Waits for planning.
        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> qry.state() == QueryState.PLANNING, TIMEOUT_IN_MS));

        qry.cancel();

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> engine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(0), IgniteSQLException.class, "The query was cancelled while planning");
    }

    /**
     * Execute query with a lot of JOINs to produce very long excution phase.
     * Cancel query on execution phase and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelAtExecutionPhase() throws IgniteCheckedException {
        QueryEngine cliEngine = queryProcessor(client);
        QueryEngine srvEngine = queryProcessor(srv);
        int cnt = 6;

        sql("CREATE TABLE person (id int, val varchar)");

        String data = IntStream.range(0, 1000).mapToObj((i) -> "(" + i + "," + i + ")").collect(joining(", "));
        String insertSql = "INSERT INTO person (id, val) VALUES " + data;

        sql(insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "person p" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql(sql));

        // The query is executing on client.
        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> {
                Collection<? extends RunningQuery> queries = cliEngine.runningQueries();

                return !queries.isEmpty() && F.first(queries).state() == QueryState.EXECUTING;
            },
            TIMEOUT_IN_MS));

        // The query is executing on sever.
        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> {
                Collection<? extends RunningQuery> queries = srvEngine.runningQueries();

                return !queries.isEmpty() && F.first(queries).state() == QueryState.EXECUTING;
            },
            TIMEOUT_IN_MS));

        Collection<? extends RunningQuery> running = cliEngine.runningQueries();

        assertEquals(1, running.size());

        RunningQuery qry = F.first(running);

        assertSame(qry, cliEngine.runningQuery(qry.id()));

        qry.cancel();

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> cliEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> srvEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }

    /**
     * Execute query with a lot of JOINs to produce very long excution phase.
     * Cancel query on execution phase on remote node (no query originator node)
     * and check query registry is empty on the all nodes of the cluster.
     */
    @Test
    public void testCancelByRemoteFragment() throws IgniteCheckedException {
        QueryEngine clientEngine = queryProcessor(client);
        QueryEngine serverEngine = queryProcessor(srv);
        int cnt = 6;

        sql("CREATE TABLE t (id int, val varchar)");

        String data = IntStream.range(0, 10000).mapToObj((i) -> "(" + i + ",'" + i + "')").collect(joining(", "));
        String insertSql = "INSERT INTO t (id, val) VALUES " + data;

        sql(insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "t t" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> sql(sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> {
                Collection<? extends RunningQuery> queries = clientEngine.runningQueries();

                return !queries.isEmpty() && F.first(queries).state() == QueryState.EXECUTING;
            },
            TIMEOUT_IN_MS));

        Assert.assertTrue(GridTestUtils.waitForCondition(() -> !serverEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Collection<? extends RunningQuery> running = serverEngine.runningQueries();
        RunningQuery qry = F.first(running);

        assertSame(qry, serverEngine.runningQuery(qry.id()));

        qry.cancel();

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> clientEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> serverEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }
}
