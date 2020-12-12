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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableMap;
import io.opencensus.trace.SpanId;
import io.opencensus.trace.Tracing;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.query.SqlFieldsQueryEx;
import org.apache.ignite.internal.processors.query.h2.twostep.messages.GridQueryNextPageRequest;
import org.apache.ignite.internal.processors.tracing.SpanType;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.tracing.TracingConfigurationCoordinates;
import org.apache.ignite.spi.tracing.TracingConfigurationParameters;
import org.apache.ignite.spi.tracing.TracingSpi;
import org.apache.ignite.spi.tracing.opencensus.OpenCensusTracingSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static java.lang.Integer.parseInt;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.TestRecordingCommunicationSpi.spi;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.tracing.SpanTags.CONSISTENT_ID;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NAME;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE;
import static org.apache.ignite.internal.processors.tracing.SpanTags.NODE_ID;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_CACHE_UPDATES;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_IDX_RANGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_PAGE_ROWS;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_QRY_TEXT;
import static org.apache.ignite.internal.processors.tracing.SpanTags.SQL_SCHEMA;
import static org.apache.ignite.internal.processors.tracing.SpanTags.tag;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CACHE_UPDATE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CMD_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_CANCEL;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_CURSOR_OPEN;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_DML_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_DML_QRY_EXEC_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_DML_QRY_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_FAIL_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_IDX_RANGE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_ITER_CLOSE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_ITER_OPEN;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_NEXT_PAGE_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_FETCH;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_PREPARE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_RESP;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PAGE_WAIT;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_PARTITIONS_RESERVE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_CANCEL_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_EXECUTE;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_EXEC_REQ;
import static org.apache.ignite.internal.processors.tracing.SpanType.SQL_QRY_PARSE;
import static org.apache.ignite.spi.tracing.Scope.SQL;
import static org.apache.ignite.spi.tracing.TracingConfigurationParameters.SAMPLING_RATE_ALWAYS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/**
 * Tests tracing of SQL queries based on {@link OpenCensusTracingSpi}.
 */
public class OpenCensusSqlNativeTracingTest extends AbstractTracingTest {
    /** Number of entries in test table. */
    protected static final int TEST_TABLE_POPULATION = 100;

    /** Page size for queries. */
    protected static final int PAGE_SIZE = 20;

    /** Test schema name. */
    protected static final String TEST_SCHEMA = "TEST_SCHEMA";

    /** Key counter. */
    private final AtomicInteger keyCntr = new AtomicInteger();

    /** {@inheritDoc} */
    @Override protected TracingSpi<?> getTracingSpi() {
        return new OpenCensusTracingSpi();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override public void before() throws Exception {
        super.before();

        IgniteEx cli = startClientGrid(GRID_CNT);

        cli.tracingConfiguration().set(
            new TracingConfigurationCoordinates.Builder(SQL).build(),
            new TracingConfigurationParameters.Builder()
                .withSamplingRate(SAMPLING_RATE_ALWAYS).build());
    }

    /**
     * Tests tracing of UPDATE query with skipped reduce phase.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdateWithReducerSkipped() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        SpanId rootSpan = executeAndCheckRootSpan(
            "UPDATE " + prsnTable + " SET prsnVal = (prsnVal + 1)", TEST_SCHEMA, true, false, false);

        checkChildSpan(SQL_QRY_PARSE, rootSpan);

        SpanId dmlExecSpan = checkChildSpan(SQL_DML_QRY_EXECUTE, rootSpan);

        List<SpanId> execReqSpans = checkSpan(SQL_DML_QRY_EXEC_REQ, dmlExecSpan, GRID_CNT, null);

        int fetchedRows = 0;

        int cacheUpdates = 0;

        assertEquals(GRID_CNT, execReqSpans.size());

        for (int i = 0; i < GRID_CNT; i++) {
            SpanId execReqSpan = execReqSpans.get(i);

            checkChildSpan(SQL_PARTITIONS_RESERVE, execReqSpan);
            checkSpan(SQL_QRY_PARSE, execReqSpan, 2, null);

            SpanId iterSpan = checkChildSpan(SQL_ITER_OPEN, execReqSpan);

            checkChildSpan(SQL_QRY_EXECUTE, iterSpan);

            fetchedRows += findChildSpans(SQL_PAGE_FETCH, execReqSpan).stream()
                .mapToInt(span -> getAttribute(span, SQL_PAGE_ROWS))
                .sum();

            List<SpanId> cacheUpdateSpans = findChildSpans(SQL_CACHE_UPDATE, execReqSpan);

            cacheUpdates += cacheUpdateSpans.stream()
                .mapToInt(span -> getAttribute(span, SQL_CACHE_UPDATES))
                .sum();

            checkChildSpan(SQL_ITER_CLOSE, execReqSpan);
            checkChildSpan(SQL_DML_QRY_RESP, execReqSpan);
        }

        assertEquals(TEST_TABLE_POPULATION, fetchedRows);
        assertEquals(TEST_TABLE_POPULATION, cacheUpdates);
    }

    /**
     * Tests tracing of multiple MERGE query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleMerge() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        checkDmlQuerySpans("MERGE INTO " + prsnTable + "(_key, prsnVal) SELECT _key, 0 FROM " + prsnTable,
            true, TEST_TABLE_POPULATION);
    }

    /**
     * Tests tracing of single MERGE query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleMerge() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        checkDmlQuerySpans("MERGE INTO " + prsnTable + "(_key, prsnId, prsnVal) VALUES (" + keyCntr.get() + ", 0, 0)",
            false, 1);
    }

    /**
     * Tests tracing of UPDATE query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testUpdate() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        checkDmlQuerySpans("UPDATE " + prsnTable + " SET prsnVal = (prsnVal + 1)",
            true, TEST_TABLE_POPULATION);
    }

    /**
     * Tests tracing of DELETE query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDelete() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        checkDmlQuerySpans("DELETE FROM " + prsnTable, true, TEST_TABLE_POPULATION);
    }

    /**
     * Tests tracing of multiple INSERT query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMultipleInsert() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        checkDmlQuerySpans("INSERT INTO " + prsnTable + "(_key, prsnId, prsnVal) VALUES" +
            " (" + keyCntr.incrementAndGet() + ", 0, 0)," +
            " (" + keyCntr.incrementAndGet() + ", 1, 1)", false, 2);
    }

    /**
     * Tests tracing of single INSERT query.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSingleInsert() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        checkDmlQuerySpans("INSERT INTO " + prsnTable + "(_key, prsnId, prsnVal) VALUES" +
            " (" + keyCntr.incrementAndGet() + ", 0, 0)", false, 1);
    }

    /**
     * Tests tracing of distributed join query which includes all communications between reducer and mapped nodes and
     * index range requests.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDistributedJoin() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        String orgTable = createTableAndPopulate(Organization.class, PARTITIONED, 1);

        SpanId rootSpan = executeAndCheckRootSpan(
            "SELECT * FROM " + prsnTable + " AS p JOIN " + orgTable + " AS o ON o.orgId = p.prsnId",
            TEST_SCHEMA, false, true, true);

        checkChildSpan(SQL_QRY_PARSE, rootSpan);
        checkChildSpan(SQL_CURSOR_OPEN, rootSpan);

        SpanId iterSpan = checkChildSpan(SQL_ITER_OPEN, rootSpan);

        List<SpanId> execReqSpans = checkSpan(SQL_QRY_EXEC_REQ, iterSpan, GRID_CNT, null);

        int idxRangeReqRows = 0;

        int preparedRows = 0;

        int fetchedRows = 0;

        for (int i = 0; i < GRID_CNT; i++) {
            SpanId execReqSpan = execReqSpans.get(i);

            checkChildSpan(SQL_PARTITIONS_RESERVE, execReqSpan);

            SpanId execSpan = checkChildSpan(SQL_QRY_EXECUTE, execReqSpan);

            List<SpanId> distrLookupReqSpans = findChildSpans(SQL_IDX_RANGE_REQ, execSpan);

            for (SpanId span : distrLookupReqSpans) {
                idxRangeReqRows += getAttribute(span, SQL_IDX_RANGE_ROWS);

                checkChildSpan(SQL_IDX_RANGE_RESP, span);
            }

            preparedRows += getAttribute(
                checkChildSpan(SQL_PAGE_PREPARE, execReqSpan), SQL_PAGE_ROWS);

            checkChildSpan(SQL_PAGE_RESP, execReqSpan);
        }

        SpanId pageFetchSpan = checkChildSpan(SQL_PAGE_FETCH, iterSpan);

        fetchedRows += getAttribute(pageFetchSpan, SQL_PAGE_ROWS);

        checkChildSpan(SQL_PAGE_WAIT, pageFetchSpan);

        SpanId nexPageSpan = checkChildSpan(SQL_NEXT_PAGE_REQ, pageFetchSpan);

        preparedRows += getAttribute(
            checkChildSpan(SQL_PAGE_PREPARE, nexPageSpan), SQL_PAGE_ROWS);

        checkChildSpan(SQL_PAGE_RESP, nexPageSpan);

        List<SpanId> pageFetchSpans = findChildSpans(SQL_PAGE_FETCH, rootSpan);

        for (SpanId span : pageFetchSpans) {
            fetchedRows += getAttribute(span, SQL_PAGE_ROWS);

            checkChildSpan(SQL_PAGE_WAIT, span);

            List<SpanId> nextPageSpans = findChildSpans(SQL_NEXT_PAGE_REQ, span);

            if (!nextPageSpans.isEmpty()) {
                assertEquals(1, nextPageSpans.size());

                SpanId nextPageSpan = nextPageSpans.get(0);

                preparedRows += getAttribute(
                    checkChildSpan(SQL_PAGE_PREPARE, nextPageSpan), SQL_PAGE_ROWS);

                checkChildSpan(SQL_PAGE_RESP, nextPageSpan);
            }
        }

        assertEquals(TEST_TABLE_POPULATION, fetchedRows);
        assertEquals(TEST_TABLE_POPULATION, preparedRows);
        assertEquals(TEST_TABLE_POPULATION, idxRangeReqRows);

        checkSpan(SQL_QRY_CANCEL_REQ, rootSpan, mapNodesCount(), null);

        assertFalse(findChildSpans(SQL_CURSOR_CLOSE, rootSpan).isEmpty());
    }

    /**
     * Tests tracing of SELECT query with parallelism.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSelectWithParallelism() throws Exception {
        int qryParallelism = 2;

        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, qryParallelism);

        SpanId rootSpan = executeAndCheckRootSpan("SELECT * FROM " + prsnTable,
            TEST_SCHEMA, false, false, true);

        SpanId iterOpenSpan = checkChildSpan(SQL_ITER_OPEN, rootSpan);

        List<SpanId> qryExecspans = findChildSpans(SQL_QRY_EXEC_REQ, iterOpenSpan);

        assertEquals(GRID_CNT * qryParallelism, qryExecspans.size());
    }

    /**
     * Tests tracing of the SQL query next page request failure.
     *
     * @throws Exception If failed.
     */
    @Test
    @SuppressWarnings("Convert2MethodRef")
    public void testNextPageRequestFailure() throws Exception {
        String prsnTable = createTableAndPopulate(Person.class, PARTITIONED, 1);

        IgniteEx qryInitiator = reducer();

        spi(qryInitiator).blockMessages((node, msg) -> msg instanceof GridQueryNextPageRequest);

        IgniteInternalFuture<?> iterFut = runAsync(() ->
            executeQuery("SELECT * FROM " + prsnTable, TEST_SCHEMA, false, false, true));

        spi(qryInitiator).waitForBlocked(mapNodesCount());

        qryInitiator.context().query().runningQueries(-1).iterator().next().cancel();

        spi(qryInitiator).stopBlock();

        GridTestUtils.assertThrowsWithCause(() -> iterFut.get(), IgniteCheckedException.class);

        handler().flush();

        checkDroppedSpans();

        SpanId rootSpan = checkChildSpan(SQL_QRY, null);

        SpanId cursorCancelSpan = checkChildSpan(SQL_CURSOR_CANCEL, rootSpan);

        SpanId cursorCloseSpan = checkChildSpan(SQL_CURSOR_CLOSE, cursorCancelSpan);

        SpanId iterCloseSpan = checkChildSpan(SQL_ITER_CLOSE, cursorCloseSpan);

        checkSpan(SQL_QRY_CANCEL_REQ, iterCloseSpan, mapNodesCount(), null);

        List<SpanId> pageFetchSpans = findChildSpans(SQL_PAGE_FETCH, rootSpan);

        for (SpanId pageFetchSpan : pageFetchSpans) {
            List<SpanId> nextPageReqSpans = findChildSpans(SQL_NEXT_PAGE_REQ, pageFetchSpan);

            if (!nextPageReqSpans.isEmpty()) {
                assertEquals(1, nextPageReqSpans.size());

                checkChildSpan(SQL_FAIL_RESP, nextPageReqSpans.get(0));
            }
        }
    }

    /**
     * Tests tracing of the CREATE TABLE command execution.
     */
    @Test
    public void testCreateTable() throws Exception {
        SpanId rootSpan = executeAndCheckRootSpan("CREATE TABLE test_table(id INT PRIMARY KEY, val VARCHAR)",
            DFLT_SCHEMA, false, false, null);

        checkChildSpan(SQL_QRY_PARSE, rootSpan);
        checkChildSpan(SQL_CMD_QRY_EXECUTE, rootSpan);
    }

    /**
     * Executes DML query and checks corresponding span tree.
     *
     * @param qry SQL query to execute.
     * @param fetchRequired Whether query need to fetch data before cache update.
     */
    private void checkDmlQuerySpans(String qry, boolean fetchRequired, int expCacheUpdates) throws Exception {
        SpanId rootSpan = executeAndCheckRootSpan(qry, TEST_SCHEMA, false,false,false);

        checkChildSpan(SQL_QRY_PARSE, rootSpan);

        SpanId dmlExecSpan = checkChildSpan(SQL_DML_QRY_EXECUTE, rootSpan);

        if (fetchRequired) {
            checkChildSpan(SQL_ITER_OPEN, dmlExecSpan);

            int fetchedRows = findChildSpans(SQL_PAGE_FETCH, null).stream()
                .mapToInt(span -> getAttribute(span, SQL_PAGE_ROWS))
                .sum();

            assertEquals(expCacheUpdates, fetchedRows);
        }

        int cacheUpdates = findChildSpans(SQL_CACHE_UPDATE, dmlExecSpan).stream()
            .mapToInt(span -> getAttribute(span, SQL_CACHE_UPDATES))
            .sum();

        assertEquals(expCacheUpdates, cacheUpdates);
    }

    /**
     * Checks whether parent span has a single child span with specified type.
     *
     * @param type Span type.
     * @param parentSpan Parent span id.
     * @return Id of the the child span.
     */
    protected SpanId checkChildSpan(SpanType type, SpanId parentSpan) {
        return checkSpan(type, parentSpan,1, null).get(0);
    }

    /**
     * Finds child spans with specified type and parent span.
     *
     * @param type Span type.
     * @param parentSpanId Parent span id.
     * @return Ids of the found spans.
     */
    protected List<SpanId> findChildSpans(SpanType type, SpanId parentSpanId) {
        return handler().allSpans()
            .filter(span -> parentSpanId != null ?
                parentSpanId.equals(span.getParentSpanId()) && type.spanName().equals(span.getName()) :
                type.spanName().equals(span.getName()))
            .map(span -> span.getContext().getSpanId())
            .collect(Collectors.toList());
    }

    /**
     * Obtains integer value of the attribtute from span with specified id.
     *
     * @param spanId Id of the target span.
     * @param tag Tag of the attribute.
     * @return Value of the attribute.
     */
    protected int getAttribute(SpanId spanId, String tag) {
        return parseInt(attributeValueToString(handler()
            .spanById(spanId)
            .getAttributes()
            .getAttributeMap()
            .get(tag)));
    }

    /**
     * Executes query with specified parameters.
     *
     * @param sql SQL query to execute.
     * @param schema SQL query schema.
     * @param skipReducerOnUpdate Wether reduce phase should be skipped during update query execution.
     * @param distributedJoins Whether distributed joins enabled.
     * @param isQry {@code True} if query is SELECT, {@code False} - DML queries, {@code null} - all remainings.
     */
    protected void executeQuery(
        String sql,
        String schema,
        boolean skipReducerOnUpdate,
        boolean distributedJoins,
        Boolean isQry
    ) {
        SqlFieldsQuery qry = new SqlFieldsQueryEx(sql, isQry)
            .setSkipReducerOnUpdate(skipReducerOnUpdate)
            .setDistributedJoins(distributedJoins)
            .setPageSize(PAGE_SIZE)
            .setSchema(schema);

        reducer().context().query().querySqlFields(qry, false).getAll();
    }

    /**
     * Executes specified query and checks the root span of query execution.
     *
     * @param sql SQL query to execute.
     * @param schema SQL query schema.
     * @param skipReducerOnUpdate Wether reduce phase should be skipped during update query execution.
     * @param distributedJoins Whether distributed joins enabled.
     * @param isQry {@code True} if query is SELECT, {@code False} - DML queries, {@code null} - all remainings.
     */
    protected SpanId executeAndCheckRootSpan(
        String sql,
        String schema,
        boolean skipReducerOnUpdate,
        boolean distributedJoins,
        Boolean isQry
    ) throws Exception {
        executeQuery(sql, schema, skipReducerOnUpdate, distributedJoins, isQry);

        handler().flush();

        checkDroppedSpans();

        return checkSpan(
            SQL_QRY,
            null,
            1,
            ImmutableMap.<String, String>builder()
                .put(NODE_ID, reducer().localNode().id().toString())
                .put(tag(NODE, CONSISTENT_ID), reducer().localNode().consistentId().toString())
                .put(tag(NODE, NAME), reducer().name())
                .put(SQL_QRY_TEXT, sql)
                .put(SQL_SCHEMA, schema)
                .build()
        ).get(0);
    }

    /**
     * @return Name of the table which was created.
     */
    protected String createTableAndPopulate(Class<?> cls, CacheMode mode, int qryParallelism) {
        IgniteCache<Integer, Object> cache = grid(0).createCache(
            new CacheConfiguration<Integer, Object>(cls.getSimpleName() + "_" + mode)
                .setIndexedTypes(Integer.class, cls)
                .setCacheMode(mode)
                .setQueryParallelism(qryParallelism)
                .setSqlSchema(TEST_SCHEMA)
        );

        for (int i = 0; i < TEST_TABLE_POPULATION; i++)
            cache.put(keyCntr.getAndIncrement(), cls == Organization.class ? new Organization(i, i) : new Person(i, i));

        return TEST_SCHEMA + '.' + cls.getSimpleName();
    }

    /**
     * Checks that no spans were dropped by OpencenCensus due to exporter buffer overflow.
     */
    private void checkDroppedSpans() {
        Object worker = U.field(Tracing.getExportComponent().getSpanExporter(), "worker");

        long droppedSpans = U.field(worker, "droppedSpans");

        assertEquals("Some spans were dropped by OpencenCensus due to exporter buffer overflow.",
            0, droppedSpans);
    }

    /**
     * @return Reducer node.
     */
    protected IgniteEx reducer() {
        return ignite(GRID_CNT);
    }

    /**
     * @return Number of nodes in cluster except reducer.
     */
    protected int mapNodesCount() {
        return GRID_CNT;
    }

    /** */
    public static class Person {
        /** */
        @QuerySqlField(index = true)
        private int prsnId;

        /** */
        @QuerySqlField
        private int prsnVal;

        /** */
        public Person(int prsnId, int prsnVal) {
            this.prsnId = prsnId;
            this.prsnVal = prsnVal;
        }
    }

    /** */
    public static class Organization {
        /** */
        @QuerySqlField(index = true)
        private int orgId;

        /** */
        @QuerySqlField
        private int orgVal;

        /** */
        public Organization(int orgId, int orgVal) {
            this.orgId = orgId;
            this.orgVal = orgVal;
        }
    }
}
