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

package org.apache.ignite.internal.processors.monitoring.opencensus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

import io.opencensus.trace.SpanId;
import org.apache.ignite.client.Config;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.junit.Test;

import static java.sql.DriverManager.getConnection;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_PAGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_QRY_ID;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_BATCH_PROCESS;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CMD_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_OPEN;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_ITER_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_ITER_OPEN;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_FETCH;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_PARSE;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;
import static org.apache.ignite.spi.tracing.Scope.SQL;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_NEVER;

/**
 * Tests tracing of SQL queries execution via JDBC.
 */
public class OpenCensusSqlJdbcTracingTest extends OpenCensusSqlNativeTracingTest {
    /** JDBC URL prefix. */
    private static final String JDBC_URL_PREFIX = "jdbc:ignite:thin://";

    /** CSV file for bulk-load testing. */
    private static final String BULKLOAD_FILE = Objects.requireNonNull(resolveIgnitePath(
        "/modules/clients/src/test/resources/bulkload2.csv")).getAbsolutePath();

    /** Number of bulk-load entries. */
    private static final int BULKLOAD_ENTRIES = 2;

    /**
     * Tests tracing of local SQL SELECT query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSelectLocal() throws Exception {
        String orgTable = createTableAndPopulate(Organization.class, REPLICATED, 1);

        SpanId rootSpan = executeAndCheckRootSpan("SELECT orgVal FROM " + orgTable, TEST_SCHEMA, false, false, true);

        String qryId = getAttribute(rootSpan, SQL_QRY_ID);
        assertTrue(Long.parseLong(qryId.substring(qryId.indexOf('_') + 1)) > 0);
        UUID.fromString(qryId.substring(0, qryId.indexOf('_')));

        checkChildSpan(SQL_QRY_PARSE, rootSpan);
        checkChildSpan(SQL_CURSOR_OPEN, rootSpan);
        checkChildSpan(SQL_ITER_OPEN, rootSpan);

        SpanId iterSpan = checkChildSpan(SQL_ITER_OPEN, rootSpan);

        checkChildSpan(SQL_QRY_EXECUTE, iterSpan);

        int fetchedRows = findChildSpans(SQL_PAGE_FETCH, rootSpan).stream()
            .mapToInt(span -> Integer.parseInt(getAttribute(span, SQL_PAGE_ROWS)))
            .sum();

        assertEquals(TEST_TABLE_POPULATION, fetchedRows);

        checkChildSpan(SQL_ITER_CLOSE, rootSpan);
        assertFalse(findChildSpans(SQL_CURSOR_CLOSE, rootSpan).isEmpty());
    }

    /**
     * Test SQL bulk load query tracing.
     */
    @Test
    public void testCopy() throws Exception {
        ignite(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_NEVER).build());

        String table = "test_table";

        executeQuery(
            "CREATE TABLE " + table + "(id LONG PRIMARY KEY, first_name VARCHAR, last_name VARCHAR, age LONG)",
            DFLT_SCHEMA, false, false, null);

        ignite(0).tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS).build());

        SpanId rootSpan = executeAndCheckRootSpan(
            "COPY FROM '" + BULKLOAD_FILE + "' INTO " + table + "(id, age, first_name, last_name) FORMAT csv",
            DFLT_SCHEMA, false, false, false);

        checkChildSpan(SQL_QRY_PARSE, rootSpan);
        checkChildSpan(SQL_CMD_QRY_EXECUTE, rootSpan);
        checkSpan(SQL_BATCH_PROCESS, rootSpan, BULKLOAD_ENTRIES, null);
    }

    /** {@inheritDoc} */
    @Override public void testSelectQueryUserThreadSpanNotAffected() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);
        String orgTable = createTableAndPopulate(Organization.class, PARTITIONED, 1);

        String url = JDBC_URL_PREFIX + Config.SERVER + '/' + TEST_SCHEMA;

        try (
            Connection prsntConn = getConnection(url);
            Connection orgConn = getConnection(url);

            PreparedStatement prsntStmt = prsntConn.prepareStatement("SELECT * FROM " + prsnTable);
            PreparedStatement orgStmt = orgConn.prepareStatement("SELECT * FROM " + orgTable)
        ) {
            prsntStmt.executeQuery();
            orgStmt.executeQuery();

            try (
                ResultSet prsnResultSet = prsntStmt.getResultSet();
                ResultSet orgResultSet = orgStmt.getResultSet()
            ) {
                while (prsnResultSet.next() && orgResultSet.next()) {
                    // No-op.
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        handler().flush();

        checkDroppedSpans();

        List<SpanId> rootSpans = findRootSpans(SQL_QRY);

        assertEquals(2, rootSpans.size());

        for (SpanId rootSpan : rootSpans)
            checkBasicSelectQuerySpanTree(rootSpan, TEST_TABLE_POPULATION);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StatementWithEmptyBody")
    @Override protected void executeQuery(
        String sql,
        String schema,
        boolean skipReduceOnUpdate,
        boolean distributedJoins,
        Boolean isQry
    ) {
        String url = JDBC_URL_PREFIX + Config.SERVER + '/' + schema +
            "?skipReducerOnUpdate=" + skipReduceOnUpdate + "&distributedJoins=" + distributedJoins;

        try (
            Connection conn = getConnection(url);

            PreparedStatement stmt = conn.prepareStatement(sql)
        ) {
            stmt.setFetchSize(PAGE_SIZE);

            if (isQry == null)
                stmt.execute();
            else if (!isQry)
                stmt.executeUpdate();
            else {
                stmt.executeQuery();

                try (ResultSet resultSet = stmt.getResultSet()) {
                    while (resultSet.next()) {
                        // No-op.
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteEx reducer() {
        return ignite(0);
    }

    /** {@inheritDoc} */
    @Override protected int mapNodesCount() {
        return GRID_CNT - 1;
    }
}
