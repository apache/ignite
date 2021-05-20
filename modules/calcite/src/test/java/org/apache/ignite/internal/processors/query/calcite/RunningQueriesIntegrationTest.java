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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.RunningFragmentInfo;
import org.apache.ignite.internal.processors.query.RunningQueryInfo;
import org.apache.ignite.internal.processors.query.RunningStage;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.processors.query.calcite.CalciteTestUtils.executeSql;
import static org.apache.ignite.internal.processors.query.calcite.CalciteTestUtils.queryProcessor;

/**
 *
 */
public class RunningQueriesIntegrationTest extends GridCommonCalciteAbstractTest {
    /** */
    private static IgniteEx client;

    /** */
    private static IgniteEx srv;

    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 10000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        srv = startGrids(3);

        client = startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Ignite ign : G.allGrids()) {
            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);
        }

        cleanQueryPlanCache();

        super.afterTest();
    }

    /** */
    @Test
    public void testCancelAtPlanningPhase() throws IgniteCheckedException {
        QueryEngine engine = queryProcessor(client);
        int cnt = 9;

        for (int i = 0; i < cnt; i++)
            executeSql(client, "CREATE TABLE person" + i + " (id int, val varchar)");

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "person" + i + " p" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;
        ;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> executeSql(engine, sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> !engine.runningQueries().isEmpty() || fut.isDone(), TIMEOUT_IN_MS));

        List<RunningQueryInfo> running = engine.runningQueries();

        assertEquals(1, running.size());

        RunningQueryInfo run = running.get(0);
        assertEquals(RunningStage.PLANNING, run.stage());

        engine.cancelQuery(run.qryId());

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> engine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(0), IgniteSQLException.class, "The query was cancelled while planning");
    }

    /** */
    @Test
    public void testCancelAtExecutionPhase() throws IgniteCheckedException {
        QueryEngine engine = queryProcessor(client);
        int cnt = 6;

        executeSql(engine, "CREATE TABLE person (id int, val varchar)");

        String data = IntStream.range(0, 1000).mapToObj((i) -> "(" + i + "," + i + ")").collect(joining(", "));
        String insertSql = "INSERT INTO person (id, val) VALUES " + data;

        executeSql(engine, insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "person p" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> executeSql(engine, sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> {
                List<RunningQueryInfo> queries = engine.runningQueries();

                return !queries.isEmpty() && queries.get(0).stage() == RunningStage.EXECUTION;
            },
            TIMEOUT_IN_MS));

        List<RunningQueryInfo> running = engine.runningQueries();

        assertEquals(1, running.size());

        RunningQueryInfo run = running.get(0);
        engine.cancelQuery(run.qryId());

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> engine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }

    /** */
    @Test
    public void testCancelByFragment() throws IgniteCheckedException {
        QueryEngine clientEngine = queryProcessor(client);
        QueryEngine serverEngine = queryProcessor(srv);
        int cnt = 6;

        executeSql(clientEngine, "CREATE TABLE t (id int, val varchar)");

        String data = IntStream.range(0, 10000).mapToObj((i) -> "(" + i + ",'" + i + "')").collect(joining(", "));
        String insertSql = "INSERT INTO t (id, val) VALUES " + data;

        executeSql(clientEngine, insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "t t" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> executeSql(clientEngine, sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> {
                List<RunningQueryInfo> queries = clientEngine.runningQueries();

                return !queries.isEmpty() && queries.get(0).stage() == RunningStage.EXECUTION;
            },
            TIMEOUT_IN_MS));

        Assert.assertTrue(GridTestUtils.waitForCondition(() -> !serverEngine.runningFragments().isEmpty(), TIMEOUT_IN_MS));

        List<RunningFragmentInfo> fragments = serverEngine.runningFragments();
        RunningQueryInfo run = fragments.get(0);

        serverEngine.cancelQuery(run.qryId());

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> clientEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }

    /** */
    @Test
    public void testBroadcastCancelMessage() throws IgniteCheckedException {
        QueryEngine clientEngine = queryProcessor(client);
        QueryEngine serverEngine = queryProcessor(srv);
        int cnt = 6;

        executeSql(serverEngine, "CREATE TABLE t (id int, val varchar)");

        String data = IntStream.range(0, 10000).mapToObj((i) -> "(" + i + ",'" + i + "')").collect(joining(", "));
        String insertSql = "INSERT INTO t (id, val) VALUES " + data;

        executeSql(serverEngine, insertSql);

        String bigJoin = IntStream.range(0, cnt).mapToObj((i) -> "t t" + i).collect(joining(", "));
        String sql = "SELECT * FROM " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> executeSql(serverEngine, sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(() -> !serverEngine.runningFragments().isEmpty(), TIMEOUT_IN_MS));

        List<RunningFragmentInfo> fragments = serverEngine.runningFragments();
        RunningQueryInfo run = fragments.get(0);

        Assert.assertTrue(clientEngine.runningFragments().isEmpty());

        clientEngine.cancelQuery(run.qryId());

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> serverEngine.runningQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }

}
