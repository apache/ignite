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

package org.apache.ignite.internal.processors.query;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCancelledException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.discovery.CustomMessageWrapper;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.DynamicCacheChangeBatch;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerRequest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.processors.query.h2.twostep.PartitionReservation;
import org.apache.ignite.internal.processors.query.h2.twostep.PartitionReservationManager;
import org.apache.ignite.internal.processors.query.h2.twostep.ReducePartitionMapResult;
import org.apache.ignite.internal.processors.query.h2.twostep.ReducePartitionMapper;
import org.apache.ignite.internal.processors.query.h2.twostep.msg.GridH2QueryRequest;
import org.apache.ignite.internal.processors.query.schema.message.SchemaProposeDiscoveryMessage;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.GridAbstractTest;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Arrays.stream;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.util.IgniteUtils.resolveIgnitePath;

/**
 * Test KILL QUERY requested from the same node where query was executed.
 */
@SuppressWarnings({"ThrowableNotThrown", "AssertWithSideEffects"})
@RunWith(Parameterized.class)
// We need to set this threshold bigger than partitions count to force partition pruning for the BETWEEN case.
// see org.apache.ignite.internal.processors.query.h2.affinity.PartitionExtractor.tryExtractBetween
@WithSystemProperty(key = IgniteSystemProperties.IGNITE_SQL_MAX_EXTRACTED_PARTS_FROM_BETWEEN, value = "21")
public class KillQueryTest extends GridCommonAbstractTest {
    /** Generates values for the {@link #asyncCancel} parameter. */
    @Parameterized.Parameters(name = "asyncCancel = {0}")
    public static Iterable<Object[]> valuesForAsync() {
        return Arrays.asList(new Object[][] {
            {true},
            {false}
        });
    }

    /** Whether current test execution shuould use async or non-async cancel mechanism. */
    @Parameterized.Parameter
    public boolean asyncCancel;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** A CSV file with one record. */
    private static final String BULKLOAD_20_000_LINE_CSV_FILE =
        Objects.requireNonNull(resolveIgnitePath("/modules/clients/src/test/resources/bulkload20_000.csv")).
            getAbsolutePath();

    /** Max table rows. */
    private static final int MAX_ROWS = 10000;

    /** Cancellation processing timeout. */
    public static final int TIMEOUT = 5000;

    /** Nodes count. */
    protected static final byte NODES_COUNT = 3;

    /** Timeout for checking async result. */
    public static final int CHECK_RESULT_TIMEOUT = 1_000;

    /** Number of partitions in the test chache. Keep it small to have enough rows in each partitions. */
    public static final int PARTS_CNT = 20;

    /** Connection. */
    private Connection conn;

    /** Statement. */
    private Statement stmt;

    /** Ignite. */
    protected IgniteEx ignite;

    /** Ignite instance for kill request. */
    private IgniteEx igniteForKillRequest;

    /** Node configration conter. */
    private static int cntr;

    /** Table count. */
    private static AtomicInteger tblCnt = new AtomicInteger();

    /** Barrier. Needed to test unsupported cancelation. Doesn't block threads (parties=1) by default. */
    private static volatile CyclicBarrier barrier = new CyclicBarrier(1);

    /** Allows to block messages, issued FROM the client node. */
    private static TestRecordingCommunicationSpi clientBlocker;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> cache = GridAbstractTest.defaultCacheConfiguration();

        cache.setAffinity(new RendezvousAffinityFunction(false, PARTS_CNT));

        cache.setCacheMode(PARTITIONED);
        cache.setBackups(1);
        cache.setWriteSynchronizationMode(FULL_SYNC);
        cache.setSqlFunctionClasses(TestSQLFunctions.class);
        cache.setIndexedTypes(Integer.class, Integer.class, Long.class, Long.class, String.class, Person.class);

        cfg.setCacheConfiguration(cache);

        TestRecordingCommunicationSpi commSpi = new TestRecordingCommunicationSpi();

        cfg.setCommunicationSpi(commSpi);

        if (++cntr == NODES_COUNT)
            clientBlocker = commSpi;

        cfg.setDiscoverySpi(new TcpDiscoverySpi() {
            @Override public void sendCustomEvent(DiscoverySpiCustomMessage msg) throws IgniteException {
                if (msg instanceof CustomMessageWrapper) {
                    DiscoveryCustomMessage delegate = ((CustomMessageWrapper)msg).delegate();

                    if (delegate instanceof DynamicCacheChangeBatch) {
                        try {
                            awaitTimeout();
                        }
                        catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }

                    }
                    else if (delegate instanceof SchemaProposeDiscoveryMessage) {
                        try {
                            awaitTimeout();
                        }
                        catch (Exception e) {
                            log.error(e.getMessage(), e);
                        }
                    }
                }

                super.sendCustomEvent(msg);
            }
        }.setIpFinder(IP_FINDER));

        return cfg;
    }

    /**
     * Creates and populates a new cache that is used in distributed join scenario: We have table of Persons with some
     * autogenerated PK. Join filter should be based on Person.id column which is not collocated with the pk. Result
     * size of such join (with eq condition) is {@link #MAX_ROWS} rows.
     *
     * @param cacheName name of the created cache.
     * @param shift integer to avoid collocation, put different value for the different caches.
     */
    private void createJoinCache(String cacheName, int shift) {
        CacheConfiguration<Long, Person> ccfg = GridAbstractTest.defaultCacheConfiguration();

        ccfg.setName(cacheName);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setSqlFunctionClasses(TestSQLFunctions.class);

        ccfg.setQueryEntities(Collections.singleton(
            new QueryEntity(Integer.class.getName(), Person.class.getName())
                .setTableName("PERSON")
                .setKeyFieldName("rec_id") // PK
                .addQueryField("rec_id", Integer.class.getName(), null)
                .addQueryField("id", Integer.class.getName(), null)
                .addQueryField("lastName", String.class.getName(), null)
                .setIndexes(Collections.singleton(new QueryIndex("id", true, "idx_" + cacheName)))
        ));

        grid(0).createCache(ccfg);

        try (IgniteDataStreamer<Object, Object> ds = grid(0).dataStreamer(cacheName)) {

            for (int recordId = 0; recordId < MAX_ROWS; recordId++) {
                // If two caches has the same PK, FK fields ("id") will be different.
                int intTabIdFK = (recordId + shift) % MAX_ROWS;

                ds.addData(recordId,
                    new Person(intTabIdFK,
                        "Name_" + recordId,
                        "LastName_" + recordId,
                        42));
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        cntr = 0;

        GridQueryProcessor.idxCls = MockedIndexing.class;

        startGrids(NODES_COUNT - 1);
        startClientGrid(NODES_COUNT - 1);

        // Let's set baseline topology manually. Doing so we are sure that partitions are distributed beetween our 2 srv
        // nodes, not belong only one node.
        awaitPartitionMapExchange(true, true, null);

        long curTop = grid(0).cluster().topologyVersion();

        grid(0).cluster().baselineAutoAdjustEnabled(false);

        grid(0).cluster().setBaselineTopology(curTop);

        awaitPartitionMapExchange(true, true, null);

        // Populate data.
        try (IgniteDataStreamer<Object, Object> ds = grid(0).dataStreamer(GridAbstractTest.DEFAULT_CACHE_NAME)) {
            for (int i = 0; i < MAX_ROWS; ++i) {
                ds.addData(i, i);

                ds.addData((long)i, (long)i);
            }
        }

        createJoinCache("PERS1", 1);
        createJoinCache("PERS2", 2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        GridQueryProcessor.idxCls = null;

        super.afterTestsStopped();
    }

    /**
     * @return Ignite node which will be used to execute kill query.
     */
    protected IgniteEx getKillRequestNode() {
        return grid(0);
    }

    /**
     * Called before execution of every test method in class.
     *
     * @throws Exception If failed.
     */
    @Before
    public void before() throws Exception {
        TestSQLFunctions.reset();

        newBarrier(1);

        tblCnt.incrementAndGet();

        conn = GridTestUtils.connect(grid(0), null);

        conn.setSchema('"' + GridAbstractTest.DEFAULT_CACHE_NAME + '"');

        stmt = conn.createStatement();

        ignite = grid(0);

        igniteForKillRequest = getKillRequestNode();

        MockedIndexing.resetToDefault();
    }

    /**
     * Called after execution of every test method in class.
     *
     * @throws Exception If failed.
     */
    @After
    public void after() throws Exception {
        MockedIndexing.resetToDefault();

        clientBlocker.stopBlock(false);

        if (stmt != null && !stmt.isClosed()) {
            stmt.close();

            assert stmt.isClosed();
        }

        conn.close();

        assertTrue(ignite.context().query().runningQueries(-1).isEmpty());
    }

    /**
     * Tries to cancel COPY FROM command, then checks such cancellation is unsupported.
     *
     * 1) Run COPY query, got it suspended in the middle.
     * 2) Try to cancel it, get expected exception.
     * 3) Wake up the COPY.
     * 4) Check COPY is done.
     */
    @Test
    public void testBulkLoadCancellationUnsupported() throws Exception {
        String path = Objects.requireNonNull(resolveIgnitePath("/modules/clients/src/test/resources/bulkload1.csv"))
            .getAbsolutePath();

        String createTab = "CREATE TABLE " + currentTestTableName() +
            "(id integer primary key, age integer, firstName varchar, lastname varchar)";

        String copy = "COPY FROM '" + path + "'" +
            " INTO " + currentTestTableName() +
            " (id, age, firstName, lastName)" +
            " format csv charset 'ascii'";

        // It's importaint to COPY from the client node: in this case datastreamer doesn't perform local updates so
        // it sends communication messages which we can hold.
        IgniteEx clientNode = grid(NODES_COUNT - 1);

        try (Connection clConn = GridTestUtils.connect(clientNode, null);
             final Statement client = clConn.createStatement()) {
            client.execute(createTab);

            // Suspend further copy query by holding data streamer messages.
            clientBlocker.blockMessages((dstNode, msg) -> msg instanceof DataStreamerRequest);

            IgniteInternalFuture<Boolean> copyIsDone = GridTestUtils.runAsync(() -> client.execute(copy));

            // Wait at least one streamer message, that means copy started.
            clientBlocker.waitForBlocked(1, TIMEOUT);

            // Query can be found only on the connected node.
            String globQryId = findOneRunningQuery(copy, clientNode);

            GridTestUtils.assertThrowsAnyCause(log,
                () -> igniteForKillRequest.cache(DEFAULT_CACHE_NAME).query(createKillQuery(globQryId, asyncCancel)),
                CacheException.class,
                "Query doesn't support cancellation");

            // Releases copy.
            clientBlocker.stopBlock(true);

            copyIsDone.get(TIMEOUT);

            int tabSize = clientNode.cache(DEFAULT_CACHE_NAME)
                .query(new SqlFieldsQuery("SELECT * FROM " + currentTestTableName() + " ").setSchema("PUBLIC"))
                .getAll()
                .size();

            assertEquals("COPY command inserted incorrect number of rows.", 1, tabSize);
        }
    }

    /**
     * Finds global id of the specified query on the specified node. Expecting exactly one result.
     *
     * @param query Query text to find id for.
     * @param node Node handle to the node, which initiated the query.
     */
    private String findOneRunningQuery(String query, IgniteEx node) {
        List<GridRunningQueryInfo> qryList = findQueriesOnNode(query, node);

        assertEquals("Expected only one running query: " + query + "\nBut found: " + qryList, 1, qryList.size());

        return qryList.get(0).globalQueryId();
    }

    /**
     * Finds queries that has specified text and are initially runned on the specified server node.
     *
     * @param query text of the query to find.
     * @param node server node that runs the reduce query.
     */
    private List<GridRunningQueryInfo> findQueriesOnNode(String query, IgniteEx node) {
        List<GridRunningQueryInfo> allQrs = (List<GridRunningQueryInfo>)node.context().query().runningQueries(-1);

        return allQrs.stream()
            .filter(q -> q.query().equals(query))
            .collect(Collectors.toList());
    }

    /**
     * Trying to cancel CREATE TABLE command.
     */
    @Test
    public void testCreateTableCancellationUnsupported() throws Exception {
        checkCancellationUnsupported(Collections.<String>emptyList(),
            "CREATE TABLE " + currentTestTableName() + " (id INTEGER PRIMARY KEY, name VARCHAR)",
            asyncCancel);
    }

    /**
     * Trying to cancel ALTER TABLE command.
     */
    @Test
    public void testAlterTableCancellationUnsupported() throws Exception {
        checkCancellationUnsupported(Arrays.asList("CREATE TABLE " + currentTestTableName() + " (id INTEGER PRIMARY KEY, name VARCHAR)"),
            "ALTER TABLE " + currentTestTableName() + " ADD COLUMN COL VARCHAR",
            asyncCancel);
    }

    /**
     * Trying to cancel CREATE INDEX command.
     */
    @Test
    public void testCreateIndexCancellationUnsupported() throws Exception {
        checkCancellationUnsupported(Arrays.asList("CREATE TABLE " + currentTestTableName() + " (id INTEGER PRIMARY KEY, name VARCHAR)"),
            "CREATE INDEX " + currentTestTableName() + "_IDX ON " + currentTestTableName() + "(name, id)",
            asyncCancel);
    }

    /**
     * Trying to cancel DROP INDEX command.
     */
    @Test
    public void testDropIndexCancellationUnsupported() throws Exception {
        checkCancellationUnsupported(
            Arrays.asList("CREATE TABLE " + currentTestTableName() + " (id INTEGER PRIMARY KEY, name VARCHAR)",
                "CREATE INDEX " + currentTestTableName() + "_IDX ON " + currentTestTableName() + "(name, id)"),
            "DROP INDEX " + currentTestTableName() + "_IDX",
            asyncCancel);
    }

    /**
     * Get test table name unique for per tests, but stable within one test run.
     *
     * @return Generated test table name unique for per tests, but stable within one test run.
     */
    private String currentTestTableName() {
        return "TST_TABLE_" + tblCnt.get();
    }

    /**
     * Check that trying cancellation execution of {@code sqlCmd} can't be cancelled due to it's not supported.
     *
     * @param prepareSteps Preparation SQLs before start test.
     * @param sqlCmd Command which can't be cancelled
     * @param async Execute cancellation in ASYNC mode.
     * @throws Exception In case of failure.
     */
    private void checkCancellationUnsupported(List<String> prepareSteps, String sqlCmd,
        boolean async) throws Exception {
        for (String sql : prepareSteps) {
            try {
                stmt.execute(sql);
            }
            catch (SQLException e) {
                throw new IgniteException(e);
            }
        }

        newBarrier(2);

        IgniteInternalFuture cancelRes = cancelQueryWithBarrier(sqlCmd,
            "Query doesn't support cancellation",
            async);

        stmt.execute(sqlCmd);

        cancelRes.get(TIMEOUT);
    }

    /**
     * Trying to cancel non-existing query.
     */
    @Test
    public void testKillUnknownQry() {
        UUID nodeId = ignite.localNode().id();

        GridTestUtils.assertThrows(log, () -> {
            igniteForKillRequest.cache(DEFAULT_CACHE_NAME)
                .query(createKillQuery(nodeId, Long.MAX_VALUE, asyncCancel));

            return null;
        }, CacheException.class, "Query with provided ID doesn't exist [nodeId=" + nodeId);
    }

    /**
     * Trying to cancel query on unknown node.
     */
    @Test
    public void testKillQryUnknownNode() {
        GridTestUtils.assertThrows(log, () -> {
            igniteForKillRequest.cache(DEFAULT_CACHE_NAME)
                .query(createKillQuery(UUID.randomUUID(), Long.MAX_VALUE, asyncCancel));

            return null;
        }, CacheException.class, "Failed to cancel query, node is not alive");
    }

    /**
     * Trying to kill already killed query. No exceptions expected.
     */
    @Test
    public void testKillAlreadyKilledQuery() throws Exception {
        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        FieldsQueryCursor<List<?>> cur = cache.query(new SqlFieldsQuery("select * from Integer where awaitLatchCancelled() = 0"));

        List<GridRunningQueryInfo> runningQueries = (List<GridRunningQueryInfo>)ignite.context().query().runningQueries(-1);

        GridRunningQueryInfo runQryInfo = runningQueries.get(0);

        SqlFieldsQuery killQry = createKillQuery(runQryInfo.globalQueryId(), asyncCancel);

        IgniteCache<Object, Object> reqCache = igniteForKillRequest.cache(DEFAULT_CACHE_NAME);

        IgniteInternalFuture killFut = cancel(1, asyncCancel);

        GridTestUtils.assertThrows(log,
            () -> cur.iterator().next(),
            QueryCancelledException.class,
            "The query was cancelled while executing");

        killFut.get(CHECK_RESULT_TIMEOUT);

        GridTestUtils.assertThrows(log,
            () -> reqCache.query(killQry),
            CacheException.class,
            "Query with provided ID doesn't exist");

        cur.close();

    }

    /**
     * @param nodeId Node id.
     * @param qryId Node query id.
     */
    private SqlFieldsQuery createKillQuery(UUID nodeId, long qryId, boolean async) {
        return createKillQuery(nodeId + "_" + qryId, async);
    }

    /**
     * @param globalQryId Global query id.
     */
    private SqlFieldsQuery createKillQuery(String globalQryId, boolean async) {
        return new SqlFieldsQuery("KILL QUERY" + (async ? " ASYNC" : "") + " '" + globalQryId + "'");
    }

    /**
     * Trying to cancel long running query. No exceptions expected.
     */
    @Test
    public void testCancelQuery() throws Exception {
        IgniteInternalFuture cancelRes = cancel(1, asyncCancel);

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select * from Integer where _key in " +
                "(select _key from Integer where awaitLatchCancelled() = 0) and shouldNotBeCalledInCaseOfCancellation()");

            return null;
        }, SQLException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     *
     */
    @Test
    public void testCancelBeforeIteratorObtained() throws Exception {
        FieldsQueryCursor<List<?>> cur = ignite.context().query()
            .querySqlFields(new SqlFieldsQuery("select * from \"default\".Integer").setLazy(false), false);

        Long qryId = ignite.context().query().runningQueries(-1).iterator().next().id();

        igniteForKillRequest.context().query()
            .querySqlFields(createKillQuery(ignite.context().localNodeId(), qryId, asyncCancel), false).getAll();

        if (asyncCancel)
            GridTestUtils.waitForCondition(() -> ignite.context().query().runningQueries(-1).isEmpty(), 1000);
    }

    /**
     *
     */
    @Test
    public void testCancelAfterIteratorObtained() throws Exception {
        FieldsQueryCursor<List<?>> cur = ignite.context().query()
            .querySqlFields(new SqlFieldsQuery("select * from \"default\".Integer").setLazy(false), false);

        cur.iterator();

        Long qryId = ignite.context().query().runningQueries(-1).iterator().next().id();

        igniteForKillRequest.context().query()
            .querySqlFields(createKillQuery(ignite.context().localNodeId(), qryId, asyncCancel), false).getAll();

        if (asyncCancel)
            GridTestUtils.waitForCondition(() -> ignite.context().query().runningQueries(-1).isEmpty(), 1000);
    }

    /**
     *
     */
    @Test
    public void testCancelAfterResultSetPartiallyRead() throws Exception {
        FieldsQueryCursor<List<?>> cur = ignite.context().query()
            .querySqlFields(new SqlFieldsQuery("select * from \"default\".Integer").setLazy(false), false);

        Iterator<List<?>> it = cur.iterator();

        it.next();

        Long qryId = ignite.context().query().runningQueries(-1).iterator().next().id();

        igniteForKillRequest.context().query()
            .querySqlFields(createKillQuery(ignite.context().localNodeId(), qryId, asyncCancel), false).getAll();

        if (asyncCancel)
            GridTestUtils.waitForCondition(() -> ignite.context().query().runningQueries(-1).isEmpty(), 1000);
    }

    /**
     *
     */
    @Test
    public void testCancelBeforeIteratorObtainedLazy() throws Exception {
        FieldsQueryCursor<List<?>> cur = ignite.context().query()
            .querySqlFields(new SqlFieldsQuery("select * from \"default\".Integer").setLazy(true), false);

        Long qryId = ignite.context().query().runningQueries(-1).iterator().next().id();

        igniteForKillRequest.context().query()
            .querySqlFields(createKillQuery(ignite.context().localNodeId(), qryId, asyncCancel), false).getAll();

        if (asyncCancel)
            GridTestUtils.waitForCondition(() -> ignite.context().query().runningQueries(-1).isEmpty(), 1000);
    }

    /**
     *
     */
    @Test
    public void testCancelAfterIteratorObtainedLazy() throws Exception {
        FieldsQueryCursor<List<?>> cur = ignite.context().query()
            .querySqlFields(new SqlFieldsQuery("select * from \"default\".Integer").setLazy(true), false);

        cur.iterator();

        Long qryId = ignite.context().query().runningQueries(-1).iterator().next().id();

        igniteForKillRequest.context().query()
            .querySqlFields(createKillQuery(ignite.context().localNodeId(), qryId, asyncCancel), false).getAll();

        if (asyncCancel)
            GridTestUtils.waitForCondition(() -> ignite.context().query().runningQueries(-1).isEmpty(), 1000);
    }

    /**
     *
     */
    @Test
    public void testCancelAfterResultSetPartiallyReadLazy() throws Exception {
        FieldsQueryCursor<List<?>> cur = ignite.context().query()
            .querySqlFields(new SqlFieldsQuery("select * from \"default\".Integer").setLazy(true), false);

        Iterator<List<?>> it = cur.iterator();

        it.next();

        Long qryId = ignite.context().query().runningQueries(-1).iterator().next().id();

        igniteForKillRequest.context().query()
            .querySqlFields(createKillQuery(ignite.context().localNodeId(), qryId, asyncCancel), false).getAll();

        if (asyncCancel)
            GridTestUtils.waitForCondition(() -> ignite.context().query().runningQueries(-1).isEmpty(), 1000);
    }

    /**
     * Trying to cancel long running query if partition pruning does it job. It's important to set {@link
     * IgniteSystemProperties#IGNITE_SQL_MAX_EXTRACTED_PARTS_FROM_BETWEEN} bigger than partitions count {@link
     * #PARTS_CNT}
     */
    @Test
    public void testCancelQueryPartitionPruning() throws Exception {
        IgniteInternalFuture cancelRes = cancel(1, asyncCancel);

        final int ROWS_ALLOWED_TO_PROCESS_AFTER_CANCEL = 400;

        GridTestUtils.assertThrows(log, () -> {
            stmt.executeQuery("select * from Integer where _key between 1000 and 2000 " +
                "and awaitLatchCancelled() = 0 " +
                "and shouldNotBeCalledMoreThan(" + ROWS_ALLOWED_TO_PROCESS_AFTER_CANCEL + ")");

            return null;
        }, SQLException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Check that local query can be canceled either using async or non-async method. Local query is performed using
     * cache.query() API with "local" property "true".
     */
    @Test
    public void testCancelLocalQueryNative() throws Exception {
        IgniteInternalFuture cancelRes = cancel(1, asyncCancel);

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            ignite.cache(DEFAULT_CACHE_NAME).query(
                new SqlFieldsQuery("select * from Integer where _key in " +
                    "(select _key from Integer where awaitLatchCancelled() = 0) and shouldNotBeCalledMoreThan(128)")
                    .setLocal(true)
                    .setLazy(false)
            ).getAll();

            return null;
        }, QueryCancelledException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Check that local query can be canceled either using async or non-async method. Local query is performed using
     * cache.query() API with "local" property "true".
     */
    @Test
    public void testCancelLocalLazyQueryNative() throws Exception {
        IgniteInternalFuture cancelRes = cancel(1, asyncCancel);

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            ignite.cache(DEFAULT_CACHE_NAME).query(
                new SqlFieldsQuery("select * from Integer where _key in " +
                    "(select _key from Integer where awaitLatchCancelled() = 0) and shouldNotBeCalledMoreThan(128)")
                    .setLocal(true)
                    .setLazy(true)
            ).getAll();

            return null;
        }, QueryCancelledException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Check distributed query can be canceled.
     */
    @Test
    public void testCancelDistributeJoin() throws Exception {
        IgniteInternalFuture cancelRes = cancel(1, asyncCancel);

        final int ROWS_ALLOWED_TO_PROCESS_AFTER_CANCEL = MAX_ROWS - 1;

        GridTestUtils.assertThrows(log, () -> {
            ignite.cache(DEFAULT_CACHE_NAME).query(
                new SqlFieldsQuery("SELECT p1.rec_id, p1.id, p2.rec_id " +
                    "FROM PERS1.Person p1 JOIN PERS2.Person p2 " +
                    "ON p1.id = p2.id " +
                    "AND shouldNotBeCalledMoreThan(" + ROWS_ALLOWED_TO_PROCESS_AFTER_CANCEL + ")" +
                    "AND awaitLatchCancelled() = 0")
                    .setDistributedJoins(true)
            ).getAll();

            return null;
        }, CacheException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Trying to async cancel long running multiple statements query. No exceptions expected.
     */
    @Test
    public void testKillMultipleStatementsQuery() throws Exception {
        try (Statement anotherStatement = conn.createStatement()) {
            anotherStatement.setFetchSize(1);

            String sql = "select * from Integer";

            ResultSet rs = anotherStatement.executeQuery(sql);

            assert rs.next();

            IgniteInternalFuture cancelRes = cancel(3, asyncCancel, sql, "select 100 from Integer");

            GridTestUtils.assertThrows(log, () -> {
                // Executes multiple long running query
                stmt.execute(
                    "select 100 from Integer;"
                        + "select _key from Integer where awaitLatchCancelled() = 0;");
                return null;
            }, SQLException.class, "The query was cancelled while executing");

            assert rs.next() : "The other cursor mustn't be closed";

            // Ensures that there were no exceptions within async cancellation process.
            cancelRes.get(CHECK_RESULT_TIMEOUT);
        }
    }

    /**
     * Trying to cancel long running batch query. No exceptions expected.     *
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCancelBatchQuery() throws Exception {
        try (Statement stmt2 = conn.createStatement()) {
            stmt2.setFetchSize(1);

            String sql = "SELECT * from Integer";

            ResultSet rs = stmt2.executeQuery(sql);

            Assert.assertTrue(rs.next());

            IgniteInternalFuture cancelRes = cancel(2, asyncCancel, sql);

            GridTestUtils.assertThrows(log, () -> {
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where awaitLatchCancelled() = 0");
                stmt.addBatch("update Long set _val = _val + 1 where _key < sleep_func (30)");
                stmt.addBatch("update Long set _val = _val + 1 where shouldNotBeCalledInCaseOfCancellation()");

                stmt.executeBatch();
                return null;
            }, SQLException.class, "The query was cancelled while executing");

            Assert.assertTrue("The other cursor mustn't be closed", rs.next());

            // Ensures that there were no exceptions within async cancellation process.
            cancelRes.get(CHECK_RESULT_TIMEOUT);
        }
    }

    /**
     * Check if query hangs (due to reducer spins retrying to reserve partitions but they can't be reserved), we still
     * able to cancel it. Used mocked indexing simulates 100% unability.
     */
    @Test
    public void testCancelQueryIfPartitionsCantBeReservedOnMapNodes() throws Exception {
        // Releases thread that kills query when map nodes receive query request. At least one map node received is ok.
        GridMessageListener qryStarted = (node, msg, plc) -> {
            if (msg instanceof GridH2QueryRequest)
                TestSQLFunctions.cancelLatch.countDown();
        };

        for (int i = 0; i < NODES_COUNT; i++)
            grid(i).context().io().addMessageListener(GridTopic.TOPIC_QUERY, qryStarted);

        // Suspends distributed queries on the map nodes.
        MockedIndexing.failReservations = true;

        try {
            IgniteInternalFuture cancelFut = cancel(1, asyncCancel);

            GridTestUtils.assertThrows(log, () -> {
                ignite.cache(DEFAULT_CACHE_NAME).query(
                    new SqlFieldsQuery("select * from Integer where _val <> 42")
                ).getAll();

                return null;
            }, CacheException.class, "The query was cancelled while executing.");

            cancelFut.get(CHECK_RESULT_TIMEOUT);
        }
        finally {
            for (int i = 0; i < NODES_COUNT; i++)
                grid(i).context().io().removeMessageListener(GridTopic.TOPIC_QUERY, qryStarted);
        }
    }

    /**
     * Check if query hangs due to reducer cannot get nodes for partitions, it still can be killed.
     */
    @Test
    public void testCancelQueryIfUnableToGetNodesForPartitions() throws Exception {
        // Force query to spin retrying to get nodes for partitions.
        MockedIndexing.retryNodePartMapping = true;

        String select = "select * from Integer where _val <> 42";

        IgniteInternalFuture runQueryFut = GridTestUtils.runAsync(() ->
            ignite.cache(DEFAULT_CACHE_NAME).query(
                new SqlFieldsQuery(select)
            ).getAll());

        boolean gotOneFreezedSelect = GridTestUtils.waitForCondition(
            () -> findQueriesOnNode(select, ignite).size() == 1, TIMEOUT);

        if (!gotOneFreezedSelect) {
            if (runQueryFut.isDone())
                printFuturesException("Got exception getting running the query.", runQueryFut);

            Assert.fail("Failed to wait for query to be in running queries list exactly one time " +
                "[select=" + select + ", node=" + ignite.localNode().id() + ", timeout=" + TIMEOUT + "ms].");
        }

        SqlFieldsQuery killQry = createKillQuery(findOneRunningQuery(select, ignite), asyncCancel);

        ignite.cache(DEFAULT_CACHE_NAME).query(killQry);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> runQueryFut.get(CHECK_RESULT_TIMEOUT),
            CacheException.class,
            "The query was cancelled while executing.");

    }

    /**
     * Print to log exception that have been catched on other thread and have been put to specified future.
     *
     * @param msg message to add.
     * @param fut future containing the exception.
     */
    private void printFuturesException(String msg, IgniteInternalFuture fut) {
        try {
            fut.get(TIMEOUT);
        }
        catch (Exception e) {
            log.error(msg, e);
        }
    }

    /**
     * Test if user specified partitions for query explicitly, such query is cancealble.
     *
     * We check 3 scenarious in which partitions are belong to: 1) only first node <br/> 2) only second node <br/> 3)
     * some to first, the others to second <br/>
     */
    @Test
    public void testCancelQueryWithPartitions() throws Exception {
        Affinity<Object> aff = ignite.affinity(DEFAULT_CACHE_NAME);

        int halfOfNodeParts = PARTS_CNT / 4;

        int[] firstParts = stream(aff.primaryPartitions(grid(0).localNode())).limit(halfOfNodeParts).toArray();
        int[] secondParts = stream(aff.primaryPartitions(grid(1).localNode())).limit(halfOfNodeParts).toArray();

        int[] mixedParts = IntStream.concat(
            stream(firstParts).limit(halfOfNodeParts),
            stream(secondParts).limit(halfOfNodeParts)
        ).toArray();

        checkPartitions(firstParts);
        checkPartitions(secondParts);
        checkPartitions(mixedParts);
    }

    /**
     * Test if user specified partitions for query explicitly, such query is cancealble.
     *
     * @param partitions user specified partitions, could contain partitions that are mapped on one or both nodes.
     */
    public void checkPartitions(int[] partitions) throws Exception {
        TestSQLFunctions.reset();

        IgniteInternalFuture cancelRes = cancel(1, asyncCancel);

        GridTestUtils.assertThrows(log, () -> {
            ignite.cache(DEFAULT_CACHE_NAME).query(
                new SqlFieldsQuery("select * from Integer where _key in " +
                    "(select _key from Integer where awaitLatchCancelled() = 0) and shouldNotBeCalledInCaseOfCancellation()")
                    .setPartitions(partitions)
            ).getAll();

            return null;
        }, CacheException.class, "The query was cancelled while executing.");

        // Ensures that there were no exceptions within async cancellation process.
        cancelRes.get(CHECK_RESULT_TIMEOUT);
    }

    /**
     * Wait until all map parts are finished on the specified node. Not needed when IGN-13862 is done.
     *
     * @param node node for which map request completion to wait.
     */
    private void ensureMapQueriesHasFinished(IgniteEx node) throws Exception {
        boolean noTasksInQryPool = GridTestUtils.waitForCondition(() -> queryPoolIsEmpty(node), TIMEOUT);

        Assert.assertTrue("Node " + node.localNode().id() + " has not finished its tasks in the query pool",
            noTasksInQryPool);
    }

    /**
     * @param node node which query pool to check.
     * @return {@code True} if {@link GridIoPolicy#QUERY_POOL} is empty. This means no queries are currntly executed and
     * no queries are executed at the moment; {@code false} otherwise.
     */
    private boolean queryPoolIsEmpty(IgniteEx node) {
        ThreadPoolExecutor qryPool = (ThreadPoolExecutor)node.context().getQueryExecutorService();

        return qryPool.getQueue().isEmpty() && qryPool.getActiveCount() == 0;
    }

    /**
     * Cancels current query which wait on barrier.
     *
     * @param qry Query which need to try cancel.
     * @param expErrMsg Expected error message during cancellation.
     * @param async Execute cancellation in ASYNC mode.
     * @return <code>IgniteInternalFuture</code> to check whether exception was thrown.
     */
    private IgniteInternalFuture cancelQueryWithBarrier(String qry, String expErrMsg, boolean async) {
        return GridTestUtils.runAsync(() -> {
            try {
                List<GridRunningQueryInfo> runningQueries = new ArrayList<>();

                GridTestUtils.waitForCondition(() -> {
                    List<GridRunningQueryInfo> r = (List<GridRunningQueryInfo>)ignite.context().query()
                        .runningQueries(-1);

                    runningQueries.addAll(r.stream().filter(q -> q.query().equals(qry)).collect(Collectors.toList()));

                    return !runningQueries.isEmpty();
                }, TIMEOUT);

                assertFalse(runningQueries.isEmpty());

                for (GridRunningQueryInfo runningQuery : runningQueries) {
                    GridTestUtils.assertThrowsAnyCause(log,
                        () -> igniteForKillRequest.cache(DEFAULT_CACHE_NAME).query(createKillQuery(runningQuery.globalQueryId(), async)),
                        CacheException.class, expErrMsg);
                }
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                Assert.fail("Unexpected exception");
            }
            finally {
                try {
                    awaitTimeout();
                }
                catch (Exception e) {
                    log.error("Unexpected exception.", e);

                    Assert.fail("Unexpected exception");
                }
            }
        });
    }

    /**
     * Cancels current query, actual cancel will wait <code>cancelLatch</code> to be released.
     *
     * @return <code>IgniteInternalFuture</code> to check whether exception was thrown.
     */
    private IgniteInternalFuture cancel(int expQryNum, boolean async, String... skipSqls) {
        return GridTestUtils.runAsync(() -> {
            try {
                TestSQLFunctions.cancelLatch.await();

                List<GridRunningQueryInfo> runningQueries = (List<GridRunningQueryInfo>)ignite.context().query().runningQueries(-1);

                List<IgniteInternalFuture> res = new ArrayList<>();

                for (GridRunningQueryInfo runningQuery : runningQueries) {
                    if (Stream.of(skipSqls).noneMatch((skipSql -> runningQuery.query().equals(skipSql))))
                        res.add(GridTestUtils.runAsync(() -> {
                                igniteForKillRequest.cache(DEFAULT_CACHE_NAME).query(createKillQuery(runningQuery.globalQueryId(), async));
                            }
                        ));
                }

                // This sleep is required to wait for kill queries get started.
                doSleep(500);

                if (expQryNum != runningQueries.size())
                    log.error("Found running queries are incorrect, " +
                        "expected only " + expQryNum + " queries. Found : " + runningQueries);

                assertEquals(expQryNum, runningQueries.size());

                TestSQLFunctions.reqLatch.countDown();

                for (IgniteInternalFuture fut : res)
                    fut.get(TIMEOUT);

                // Currently canceled query returns (unblocks the caller code) without waiting for map parts of the
                // query to be finished. We need to wait for them. This is a workaround for IGN-13862 because we
                // observe side effects if map parts of canceled query are still running.
                ensureMapQueriesHasFinished(grid(0));
                ensureMapQueriesHasFinished(grid(1));
            }
            catch (Exception e) {
                log.error("Unexpected exception.", e);

                Assert.fail("Unexpected exception");
            }
        });
    }

    /**
     * Fills Server Thread Pool with <code>qryCnt</code> queries. Given queries will wait for
     * <code>suspendQryLatch</code> to be released.
     *
     * @param statements Statements.
     * @param qryCnt Number of queries to execute.
     * @return <code>IgniteInternalFuture</code> in order to check whether exception was thrown or not.
     */
    private IgniteInternalFuture<Long> fillServerThreadPool(List<Statement> statements, int qryCnt) {
        AtomicInteger idx = new AtomicInteger(0);

        return GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                statements.get(idx.getAndIncrement()).executeQuery(
                    "select * from Integer where awaitQuerySuspensionLatch();");
            }
            catch (SQLException e) {
                log.error("Unexpected exception.", e);

                Assert.fail("Unexpected exception");
            }
        }, qryCnt, "ThreadName");
    }

    /**
     * Create and set new CyclicBarrier for the function.
     *
     * @param parties the number of threads that must invoke await method before the barrier is tripped
     */
    private static void newBarrier(int parties) {
        if (barrier != null)
            barrier.reset();

        barrier = new CyclicBarrier(parties);
    }

    /**
     * @throws InterruptedException In case of failure.
     * @throws TimeoutException In case of failure.
     * @throws BrokenBarrierException In case of failure.
     */
    private static void awaitTimeout() throws InterruptedException, TimeoutException, BrokenBarrierException {
        barrier.await(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    /**
     * Utility class with custom SQL functions.
     */
    public static class TestSQLFunctions {
        /** Request latch. */
        static volatile CountDownLatch reqLatch;

        /** Cancel latch. */
        static volatile CountDownLatch cancelLatch;

        /** Suspend query latch. */
        static volatile CountDownLatch suspendQryLatch;

        /** How many times function {@link #shouldNotBeCalledMoreThan} have been called so far. */
        static volatile AtomicInteger funCallCnt;

        /**
         * Recreate latches. Old latches are released.
         */
        static void reset() {
            releaseLatches(reqLatch, cancelLatch, suspendQryLatch);

            reqLatch = new CountDownLatch(1);

            cancelLatch = new CountDownLatch(1);

            suspendQryLatch = new CountDownLatch(1);

            funCallCnt = new AtomicInteger(0);
        }

        /**
         * @param latches latches to release. Our latches have initial count = 1.
         */
        private static void releaseLatches(CountDownLatch... latches) {
            for (CountDownLatch l : latches) {
                if (l != null)
                    l.countDown();
            }
        }

        /**
         * Releases cancelLatch that leeds to sending cancel Query and waits until cancel Query is fully processed.
         *
         * @return 0;
         */
        @QuerySqlFunction
        public static long awaitLatchCancelled() {
            try {
                cancelLatch.countDown();
                reqLatch.await();
            }
            catch (Exception ignored) {
                // No-op.
            }

            return 0;
        }

        /**
         * Waits latch release.
         *
         * @return 0;
         */
        @QuerySqlFunction
        public static long awaitQuerySuspensionLatch() {
            try {
                suspendQryLatch.await();
            }
            catch (Exception ignored) {
                // No-op.
            }

            return 0;
        }

        /**
         * Asserts that this function have not been called more than specified number times. Otherwise we're failing.
         * Intended to check that query is canceled but since cancel is not instant (query continues to process some
         * number of rows), it don't process all the rows in the table (in case of full scan, of course).
         *
         * @param times deadline times.
         * @return always {@link true}.
         */
        @QuerySqlFunction
        public static boolean shouldNotBeCalledMoreThan(int times) {
            if (funCallCnt.incrementAndGet() >= times)
                fail("Query is running too long since it was canceled.");

            return true;
        }

        /**
         * If called fails with corresponding message.
         *
         * @return 0;
         */
        @QuerySqlFunction
        public static long shouldNotBeCalledInCaseOfCancellation() {
            fail("Query wasn't actually cancelled.");

            return 0;
        }

        /**
         * @param v amount of milliseconds to sleep
         * @return amount of milliseconds to sleep
         */
        @QuerySqlFunction
        public static int sleep_func(int v) {
            try {
                Thread.sleep(v);
            }
            catch (InterruptedException ignored) {
                // No-op
            }
            return v;
        }
    }

    /**
     * Person.
     */
    static class Person implements Serializable {
        /** ID. */
        @QuerySqlField
        private final int id;

        /** First name. */
        @QuerySqlField
        private final String firstName;

        /** Last name. */
        @QuerySqlField
        private final String lastName;

        /** Age. */
        @QuerySqlField
        private final int age;

        /**
         * @param id ID.
         * @param firstName First name.
         * @param lastName Last name.
         * @param age Age.
         */
        Person(int id, String firstName, String lastName, int age) {
            assert !F.isEmpty(firstName);
            assert !F.isEmpty(lastName);
            assert age > 0;

            this.id = id;
            this.firstName = firstName;
            this.lastName = lastName;
            this.age = age;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Person person = (Person)o;

            if (id != person.id)
                return false;
            if (age != person.age)
                return false;
            if (!Objects.equals(firstName, person.firstName))
                return false;
            return Objects.equals(lastName, person.lastName);

        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int result = id;
            result = 31 * result + (firstName != null ? firstName.hashCode() : 0);
            result = 31 * result + (lastName != null ? lastName.hashCode() : 0);
            result = 31 * result + age;
            return result;
        }
    }

    /**
     * Mocked indexing to eventually suspend mapper or reducer code. It simulates never ending PMEs, unstable topologies
     * and etc.
     */
    static class MockedIndexing extends IgniteH2Indexing {
        /**
         * All the time this flag is set to {@code true}, no partitions can be reserved. Acts like normal indexing by
         * default.
         */
        static volatile boolean failReservations;

        /**
         * All the time this flag is set to {@code true}, reducer is not able to get nodes for given partitions because
         * mapper says "retry later".
         */
        static volatile boolean retryNodePartMapping;

        /**
         * Result of the mapping partitions to nodes, that indicates that caller should retry request later.
         */
        private static final ReducePartitionMapResult RETRY_RESULT = new ReducePartitionMapResult(null, null, null);

        /**
         * Resets to default behaviour: disable all mock added logic.
         */
        static void resetToDefault() {
            failReservations = false;

            retryNodePartMapping = false;
        }

        /**
         * Setups mock objects into this indexing, just after super initialization is done.
         */
        @Override public void start(GridKernalContext ctx, GridSpinBusyLock busyLock) throws IgniteCheckedException {
            super.start(ctx, busyLock);

            setReservationManager(new PartitionReservationManager(super.ctx) {
                /** {@inheritDoc} */
                @Override public PartitionReservation reservePartitions(@Nullable List<Integer> cacheIds,
                    AffinityTopologyVersion reqTopVer, int[] explicitParts, UUID nodeId, long reqId)
                    throws IgniteCheckedException {
                    return failReservations ? new PartitionReservation(null,
                        "[TESTS]: Failed to reserve partitions for the testing purpose!") :
                        super.reservePartitions(cacheIds, reqTopVer, explicitParts, nodeId, reqId);
                }
            });

            setMapper(new ReducePartitionMapper(ctx, ctx.log(GridReduceQueryExecutor.class)) {
                /** {@inheritDoc} */
                @Override public ReducePartitionMapResult nodesForPartitions(List<Integer> cacheIds,
                    AffinityTopologyVersion topVer, int[] parts, boolean isReplicatedOnly) {

                    return retryNodePartMapping ? RETRY_RESULT :
                        super.nodesForPartitions(cacheIds, topVer, parts, isReplicatedOnly);
                }
            });

        }

        /**
         * Injects custom {@link PartitionReservationManager} into indexing instance.
         */
        protected void setReservationManager(PartitionReservationManager mockMgr) {
            try {
                Field partReservationMgrFld = IgniteH2Indexing.class.getDeclaredField("partReservationMgr");

                partReservationMgrFld.setAccessible(true);

                partReservationMgrFld.set(this, mockMgr);
            }
            catch (Exception rethrown) {
                throw new RuntimeException(rethrown);
            }
        }

        /**
         * Injects custom {@link ReducePartitionMapper} into reducer of this indexing instance.
         */
        private void setMapper(ReducePartitionMapper mock) {
            try {
                GridReduceQueryExecutor rdcExec = this.reduceQueryExecutor();

                Field mapperFld = GridReduceQueryExecutor.class.getDeclaredField("mapper");

                mapperFld.setAccessible(true);

                mapperFld.set(rdcExec, mock);
            }
            catch (Exception rethrown) {
                throw new RuntimeException(rethrown);
            }
        }
    }
}
