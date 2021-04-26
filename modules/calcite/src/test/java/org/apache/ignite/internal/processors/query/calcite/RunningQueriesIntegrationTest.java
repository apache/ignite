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

import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.GridRunningQueryInfo;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.exec.RunningQueryInfo;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;
import org.junit.Test;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.cache.query.QueryCancelledException.ERR_MSG;
import static org.apache.ignite.internal.processors.query.calcite.CalciteTestUtils.executeSql;
import static org.apache.ignite.internal.processors.query.calcite.CalciteTestUtils.queryProcessor;
import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.awaitReservationsRelease;
import static org.apache.ignite.internal.processors.query.calcite.exec.RunningQueryService.RunningStage.EXECUTION;
import static org.apache.ignite.internal.processors.query.calcite.exec.RunningQueryService.RunningStage.PLANNING;

/**
 *
 */
public class RunningQueriesIntegrationTest extends GridCommonAbstractTest {
    /** */
    private static IgniteEx client;

    /** Timeout in ms for async operations. */
    private static final long TIMEOUT_IN_MS = 5000;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(3);

        client = startClientGrid("client");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        for (Ignite ign : G.allGrids()) {
            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);
        }

        cleanQueryPlanCache();
    }

    /** */
    protected void cleanQueryPlanCache() {
        for (Ignite ign : G.allGrids()) {
            CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
                ((IgniteEx)ign).context(), QueryEngine.class);

            qryProc.queryPlanCache().clear();
        }
    }

    /** */
    @Test
    public void testCancelAtPlanningPhase() throws IgniteCheckedException {
        QueryEngine engine = queryProcessor(client);
        int count = 9;

        for (int i = 0; i < count; i++)
            executeSql(client, "CREATE TABLE person" + i + " (id int, val varchar)");

        String bigJoin = IntStream.range(0, count).mapToObj((i) -> "person" + i + " p" + i).collect(joining(", "));
        String sql = "select * from " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> executeSql(client, sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> !engine.runningSqlQueries().isEmpty(), TIMEOUT_IN_MS));

        List<? extends GridRunningQueryInfo> running = engine.runningSqlQueries();

        assertEquals(1, running.size());

        RunningQueryInfo run = (RunningQueryInfo)running.get(0);
        assertEquals(PLANNING, run.stage());

        engine.cancelQuery(run.id());

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> engine.runningSqlQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(0), IgniteSQLException.class, "The query was cancelled while ");
    }

    /** */
    @Test
    public void testCancelAtExecutionPhase() throws IgniteCheckedException {
        QueryEngine engine = queryProcessor(client);
        int count = 6;

        executeSql(client, "CREATE TABLE person (id int, val varchar)");

        String data = IntStream.range(0, 1000).mapToObj((i) -> "(" + i + "," + i + ")").collect(joining(", "));
        String insertSql = "INSERT INTO person (id, val) VALUES " + data;

        executeSql(client, insertSql);

        String bigJoin = IntStream.range(0, count).mapToObj((i) -> "person p" + i).collect(joining(", "));
        String sql = "select * from " + bigJoin;

        IgniteInternalFuture<List<List<?>>> fut = GridTestUtils.runAsync(() -> executeSql(client, sql));

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> {
                List<? extends GridRunningQueryInfo> queries = engine.runningSqlQueries();

                return !queries.isEmpty() && ((RunningQueryInfo)queries.get(0)).stage() == EXECUTION;
            }
            , TIMEOUT_IN_MS));

        List<? extends GridRunningQueryInfo> running = engine.runningSqlQueries();

        assertEquals(1, running.size());

        RunningQueryInfo run = (RunningQueryInfo)running.get(0);
        engine.cancelQuery(run.id());

        Assert.assertTrue(GridTestUtils.waitForCondition(
            () -> engine.runningSqlQueries().isEmpty(), TIMEOUT_IN_MS));

        GridTestUtils.assertThrowsAnyCause(log, () -> fut.get(100), IgniteSQLException.class, "The query was cancelled while executing.");
    }

}
