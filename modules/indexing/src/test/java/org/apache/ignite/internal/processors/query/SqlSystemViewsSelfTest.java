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

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import com.google.common.collect.Sets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cache.eviction.EvictableEntry;
import org.apache.ignite.cache.eviction.EvictionFilter;
import org.apache.ignite.cache.eviction.EvictionPolicy;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.managers.discovery.ClusterMetricsImpl;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest;
import org.apache.ignite.internal.processors.cache.query.IgniteQueryErrorCode;
import org.apache.ignite.internal.util.lang.GridNodePredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.discovery.tcp.internal.TcpDiscoveryNode;
import org.apache.ignite.spi.metric.sql.SqlViewMetricExporterSpi;
import org.apache.ignite.spi.systemview.view.SqlTableView;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Assert;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests for ignite SQL system views.
 */
public class SqlSystemViewsSelfTest extends AbstractIndexingCommonTest {
    /** Metrics check attempts. */
    private static final int METRICS_CHECK_ATTEMPTS = 10;

    /** */
    private boolean isPersistenceEnabled;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        isPersistenceEnabled = false;

        GridQueryProcessor.idxCls = null;
    }

    /** @return System schema name. */
    protected String systemSchemaName() {
        return "SYS";
    }

    /**
     * @param ignite Ignite.
     * @param sql Sql.
     * @param args Args.
     */
    @SuppressWarnings("unchecked")
    private List<List<?>> execSql(Ignite ignite, String sql, Object... args) {
        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery(sql);

        if (args != null && args.length > 0)
            qry.setArgs(args);

        return cache.query(qry).getAll();
    }

    /**
     * @param sql Sql.
     * @param args Args.
     */
    private List<List<?>> execSql(String sql, Object... args) {
        return execSql(grid(), sql, args);
    }

    /**
     * @param sql Sql.
     */
    private void assertSqlError(final String sql) {
        Throwable t = GridTestUtils.assertThrowsWithCause(new Callable<Void>() {
            @Override public Void call() {
                execSql(sql);

                return null;
            }
        }, IgniteSQLException.class);

        IgniteSQLException sqlE = X.cause(t, IgniteSQLException.class);

        assert sqlE != null;

        assertEquals(IgniteQueryErrorCode.UNSUPPORTED_OPERATION, sqlE.statusCode());
    }

    /**
     * Test system views modifications.
     */
    @Test
    public void testModifications() throws Exception {
        startGrid(getConfiguration());

        assertSqlError("DROP TABLE " + systemSchemaName() + ".NODES");

        assertSqlError("TRUNCATE TABLE " + systemSchemaName() + ".NODES");

        assertSqlError("ALTER TABLE " + systemSchemaName() + ".NODES RENAME TO " + systemSchemaName() + ".N");

        assertSqlError("ALTER TABLE " + systemSchemaName() + ".NODES ADD COLUMN C VARCHAR");

        assertSqlError("ALTER TABLE " + systemSchemaName() + ".NODES DROP COLUMN NODE_ID");

        assertSqlError("ALTER TABLE " + systemSchemaName() + ".NODES RENAME COLUMN NODE_ID TO C");

        assertSqlError("CREATE INDEX IDX ON " + systemSchemaName() + ".NODES(NODE_ID)");

        assertSqlError("INSERT INTO " + systemSchemaName() + ".NODES (NODE_ID) VALUES ('-')");

        assertSqlError("UPDATE " + systemSchemaName() + ".NODES SET NODE_ID = '-'");

        assertSqlError("DELETE " + systemSchemaName() + ".NODES");
    }

    /**
     * Test schemas system view.
     * @throws Exception in case of failure.
     */
    @Test
    public void testSchemasView() throws Exception {
        IgniteEx srv = startGrid(getConfiguration()
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlSchemas("PREDIFINED_SCHEMA_1")
            )
        );

        IgniteEx client = startClientGrid(getConfiguration().setIgniteInstanceName("CLIENT")
            .setSqlConfiguration(new SqlConfiguration()
                .setSqlSchemas("PREDIFINED_SCHEMA_2")
            )
        );

        srv.createCache(cacheConfiguration("TST1"));

        String schemasSql = "SELECT * FROM " + systemSchemaName() + ".SCHEMAS";

        List<List<?>> srvNodeSchemas = execSql(schemasSql);

        List<List<?>> clientNodeSchemas = execSql(client, schemasSql);

        Set expSchemasSrv = Sets.newHashSet("PREDIFINED_SCHEMA_1", "PUBLIC", "TST1", systemSchemaName());

        Set schemasSrv = srvNodeSchemas.stream().map(f -> f.get(0)).map(String.class::cast).collect(toSet());

        Assert.assertEquals(expSchemasSrv, schemasSrv);

        Set expSchemasCli = Sets.newHashSet("PREDIFINED_SCHEMA_2", "PUBLIC", "TST1", systemSchemaName());

        Set schemasCli = clientNodeSchemas.stream().map(f -> f.get(0)).map(String.class::cast).collect(toSet());

        Assert.assertEquals(expSchemasCli, schemasCli);
    }

    /**
     * Test indexes system view.
     *
     * @throws Exception in case of failure.
     */
    @Test
    public void testIndexesView() throws Exception {
        IgniteEx srv = startGrid(getConfiguration());

        IgniteEx client = startClientGrid(getConfiguration().setIgniteInstanceName("CLIENT"));

        srv.createCache(cacheConfiguration("TST1"));

        execSql("CREATE TABLE PUBLIC.AFF_CACHE (ID1 INT, ID2 INT, MY_VAL VARCHAR, PRIMARY KEY (ID1 DESC, ID2)) WITH \"affinity_key=ID2\"");

        execSql("CREATE TABLE CACHE_SQL (ID INT PRIMARY KEY, MY_VAL VARCHAR)");

        execSql("CREATE INDEX IDX_2 ON CACHE_SQL(ID DESC) INLINE_SIZE 13");

        execSql("CREATE TABLE PUBLIC.DFLT_CACHE (ID1 INT, ID2 INT, MY_VAL VARCHAR, PRIMARY KEY (ID1 DESC, ID2))");

        execSql("CREATE INDEX IDX_1 ON PUBLIC.DFLT_CACHE(ID2 DESC, ID1, MY_VAL DESC)");

        execSql("CREATE INDEX IDX_3 ON PUBLIC.DFLT_CACHE(MY_VAL)");

        execSql("CREATE TABLE PUBLIC.DFLT_AFF_CACHE (ID1 INT, ID2 INT, MY_VAL VARCHAR, PRIMARY KEY (ID1 DESC, ID2)) WITH \"affinity_key=ID1\"");

        execSql("CREATE INDEX IDX_AFF_1 ON PUBLIC.DFLT_AFF_CACHE(ID2 DESC, ID1, MY_VAL DESC)");

        String idxSql = "SELECT " +
            "  CACHE_ID," +
            "  CACHE_NAME," +
            "  SCHEMA_NAME," +
            "  TABLE_NAME," +
            "  INDEX_NAME," +
            "  INDEX_TYPE," +
            "  COLUMNS," +
            "  IS_PK," +
            "  IS_UNIQUE," +
            "  INLINE_SIZE" +
            " FROM " + systemSchemaName() + ".INDEXES ORDER BY TABLE_NAME, INDEX_NAME";

        List<List<?>> srvNodeIndexes = execSql(srv, idxSql);

        List<List<?>> clientNodeNodeIndexes = execSql(client, idxSql);

        assertTrue(srvNodeIndexes.containsAll(clientNodeNodeIndexes));

        //ToDo: As of now we can see duplicates columns within index due to https://issues.apache.org/jira/browse/IGNITE-11125

        String[][] expectedResults = {
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "AFFINITY_KEY", "BTREE", "\"ID2\" ASC, \"ID1\" ASC", "false", "false", "10"},
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK", "BTREE", "\"ID1\" ASC, \"ID2\" ASC", "true", "true", "10"},
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK__SCAN_", "SCAN", "null", "false", "false", "0"},
            {"-825022849", "SQL_PUBLIC_AFF_CACHE", "PUBLIC", "AFF_CACHE", "_key_PK_hash", "HASH", "\"ID1\" ASC, \"ID2\" ASC, \"ID2\" ASC", "true", "true", "0"},

            {"707660652", "SQL_PUBLIC_CACHE_SQL", "PUBLIC", "CACHE_SQL", "IDX_2", "BTREE", "\"ID\" DESC, \"ID\" ASC", "false", "false", "13"},
            {"707660652", "SQL_PUBLIC_CACHE_SQL", "PUBLIC", "CACHE_SQL", "IDX_2_proxy", "BTREE", "\"ID\" DESC, \"ID\" ASC", "false", "false", "0"},
            {"707660652", "SQL_PUBLIC_CACHE_SQL", "PUBLIC", "CACHE_SQL", "_key_PK", "BTREE", "\"ID\" ASC", "true", "true", "5"},
            {"707660652", "SQL_PUBLIC_CACHE_SQL", "PUBLIC", "CACHE_SQL", "_key_PK__SCAN_", "SCAN", "null", "false", "false", "0"},
            {"707660652", "SQL_PUBLIC_CACHE_SQL", "PUBLIC", "CACHE_SQL", "_key_PK_hash", "HASH", "\"ID\" ASC", "true", "true", "0"},
            {"707660652", "SQL_PUBLIC_CACHE_SQL", "PUBLIC", "CACHE_SQL", "_key_PK_proxy", "BTREE", "\"ID\" ASC", "false", "false", "0"},

            {"1374144180", "SQL_PUBLIC_DFLT_AFF_CACHE", "PUBLIC", "DFLT_AFF_CACHE", "AFFINITY_KEY", "BTREE", "\"ID1\" ASC, \"ID2\" ASC", "false", "false", "10"},
            {"1374144180", "SQL_PUBLIC_DFLT_AFF_CACHE", "PUBLIC", "DFLT_AFF_CACHE", "IDX_AFF_1", "BTREE", "\"ID2\" DESC, \"ID1\" ASC, \"MY_VAL\" DESC", "false", "false", "10"},
            {"1374144180", "SQL_PUBLIC_DFLT_AFF_CACHE", "PUBLIC", "DFLT_AFF_CACHE", "_key_PK", "BTREE", "\"ID1\" ASC, \"ID2\" ASC", "true", "true", "10"},
            {"1374144180", "SQL_PUBLIC_DFLT_AFF_CACHE", "PUBLIC", "DFLT_AFF_CACHE", "_key_PK__SCAN_", "SCAN", "null", "false", "false", "0"},
            {"1374144180", "SQL_PUBLIC_DFLT_AFF_CACHE", "PUBLIC", "DFLT_AFF_CACHE", "_key_PK_hash", "HASH", "\"ID1\" ASC, \"ID2\" ASC, \"ID1\" ASC", "true", "true", "0"},

            {"1102275506", "SQL_PUBLIC_DFLT_CACHE", "PUBLIC", "DFLT_CACHE", "IDX_1", "BTREE", "\"ID2\" DESC, \"ID1\" ASC, \"MY_VAL\" DESC, \"ID1\" ASC, \"ID2\" ASC", "false", "false", "10"},
            {"1102275506", "SQL_PUBLIC_DFLT_CACHE", "PUBLIC", "DFLT_CACHE", "IDX_3", "BTREE", "\"MY_VAL\" ASC, \"ID1\" ASC, \"ID2\" ASC, \"ID1\" ASC, \"ID2\" ASC", "false", "false", "10"},
            {"1102275506", "SQL_PUBLIC_DFLT_CACHE", "PUBLIC", "DFLT_CACHE", "_key_PK", "BTREE", "\"ID1\" ASC, \"ID2\" ASC", "true", "true", "10"},
            {"1102275506", "SQL_PUBLIC_DFLT_CACHE", "PUBLIC", "DFLT_CACHE", "_key_PK__SCAN_", "SCAN", "null", "false", "false", "0"},
            {"1102275506", "SQL_PUBLIC_DFLT_CACHE", "PUBLIC", "DFLT_CACHE", "_key_PK_hash", "HASH", "\"ID1\" ASC, \"ID2\" ASC", "true", "true", "0"},

            {"2584860", "TST1", "TST1", "VALUECLASS", "TST1_INDEX", "BTREE", "\"KEY\" ASC, \"_KEY\" ASC", "false", "false", "10"},
            {"2584860", "TST1", "TST1", "VALUECLASS", "TST1_INDEX_proxy", "BTREE", "\"_KEY\" ASC, \"KEY\" ASC", "false", "false", "0"},
            {"2584860", "TST1", "TST1", "VALUECLASS", "_key_PK", "BTREE", "\"_KEY\" ASC", "true", "true", "5"},
            {"2584860", "TST1", "TST1", "VALUECLASS", "_key_PK__SCAN_", "SCAN", "null", "false", "false", "0"},
            {"2584860", "TST1", "TST1", "VALUECLASS", "_key_PK_hash", "HASH", "\"_KEY\" ASC", "true", "true", "0"},
            {"2584860", "TST1", "TST1", "VALUECLASS", "_key_PK_proxy", "BTREE", "\"KEY\" ASC", "false", "false", "0"}
        };

        for (int i = 0; i < srvNodeIndexes.size(); i++) {
            List<?> resRow = srvNodeIndexes.get(i);

            String[] expRow = expectedResults[i];

            assertEquals(expRow.length, resRow.size());

            for (int j = 0; j < expRow.length; j++)
                assertEquals(Integer.toString(i), expRow[j], String.valueOf(resRow.get(j)));
        }
    }

    /**
     * Tests {@link SqlTableView#isIndexRebuildInProgress()}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTableViewDuringRebuilding() throws Exception {
        isPersistenceEnabled = true;

        IgniteEx srv = startGrid(getConfiguration());

        srv.cluster().active(true);

        String cacheName1 = "CACHE_1";
        String cacheSqlName1 = "SQL_PUBLIC_" + cacheName1;

        String cacheName2 = "CACHE_2";
        String cacheSqlName2 = "SQL_PUBLIC_" + cacheName2;

        execSql("CREATE TABLE " + cacheName1 + " (ID1 INT PRIMARY KEY, MY_VAL VARCHAR)");
        execSql("CREATE INDEX IDX_1 ON " + cacheName1 + " (MY_VAL DESC)");

        execSql("CREATE TABLE " + cacheName2 + " (ID INT PRIMARY KEY, MY_VAL VARCHAR)");
        execSql("CREATE INDEX IDX_2 ON " + cacheName2 + " (ID DESC)");

        // Put data to create indexes.
        execSql("INSERT INTO " + cacheName1 + " VALUES(?, ?)", 1, "12345");
        execSql("INSERT INTO " + cacheName2 + " VALUES(?, ?)", 1, "12345");

        List<Path> idxPaths = getIndexBinPaths(cacheSqlName1);

        idxPaths.addAll(getIndexBinPaths(cacheSqlName2));

        stopAllGrids();

        idxPaths.forEach(idxPath -> assertTrue(U.delete(idxPath)));

        GridQueryProcessor.idxCls = BlockingIndexing.class;

        srv = startGrid(getConfiguration());

        srv.cluster().active(true);

        checkIndexRebuild(cacheName1, true);
        checkIndexRebuild(cacheName2, true);

        ((BlockingIndexing)srv.context().query().getIndexing()).stopBlock(cacheSqlName1);

        srv.cache(cacheSqlName1).indexReadyFuture().get(30_000);

        checkIndexRebuild(cacheName1, false);
        checkIndexRebuild(cacheName2, true);

        ((BlockingIndexing)srv.context().query().getIndexing()).stopBlock(cacheSqlName2);

        srv.cache(cacheSqlName2).indexReadyFuture().get(30_000);

        checkIndexRebuild(cacheName1, false);
        checkIndexRebuild(cacheName2, false);
    }

    /**
     * Checks index rebuilding for given cache.
     *
     * @param cacheName Cache name.
     * @param rebuild Is indexes rebuild in progress.
     */
    private void checkIndexRebuild(String cacheName, boolean rebuild) throws IgniteInterruptedCheckedException {
        String idxSql = "SELECT IS_INDEX_REBUILD_IN_PROGRESS FROM " + systemSchemaName() + ".TABLES " +
            "WHERE TABLE_NAME = ?";

        assertTrue(waitForCondition(() -> {
            List<List<?>> res = execSql(grid(), idxSql, cacheName);

            assertFalse(res.isEmpty());

            return res.stream().allMatch(row -> {
                assertEquals(1, row.size());

                Boolean isIndexRebuildInProgress = (Boolean)row.get(0);

                return isIndexRebuildInProgress == rebuild;
            });
        }, 5_000));
    }

    /**
     * @return Default cache configuration.
     */
    protected CacheConfiguration<AbstractSchemaSelfTest.KeyClass, AbstractSchemaSelfTest.ValueClass> cacheConfiguration(String cacheName) throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration().setName(cacheName);

        QueryEntity entity = new QueryEntity();

        entity.setKeyType(AbstractSchemaSelfTest.KeyClass.class.getName());
        entity.setValueType(AbstractSchemaSelfTest.ValueClass.class.getName());

        entity.setKeyFieldName("key");
        entity.addQueryField("key", entity.getKeyType(), null);

        entity.addQueryField("id", Long.class.getName(), null);
        entity.addQueryField("field1", Long.class.getName(), null);

        entity.setKeyFields(Collections.singleton("id"));

        entity.setIndexes(Collections.singletonList(
            new QueryIndex("key", true, cacheName + "_index")
        ));

        ccfg.setQueryEntities(Collections.singletonList(entity));

        return ccfg;
    }

    /**
     * Test different query modes.
     */
    @Test
    public void testQueryModes() throws Exception {
        Ignite ignite = startGrid(0);
        startGrid(1);

        UUID nodeId = ignite.cluster().localNode().id();

        IgniteCache cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        String sql = "SELECT NODE_ID FROM " + systemSchemaName() + ".NODES WHERE NODE_ORDER = 1";

        SqlFieldsQuery qry;

        qry = new SqlFieldsQuery(sql).setDistributedJoins(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));

        qry = new SqlFieldsQuery(sql).setReplicatedOnly(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));

        qry = new SqlFieldsQuery(sql).setLocal(true);

        assertEquals(nodeId, ((List<?>)cache.query(qry).getAll().get(0)).get(0));
    }

    /**
     * Test Query history system view.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testQueryHistoryMetricsModes() throws Exception {
        IgniteEx ignite = startGrid(0);

        final String SCHEMA_NAME = "TEST_SCHEMA";
        final long MAX_SLEEP = 500;
        final long MIN_SLEEP = 50;

        long tsBeforeRun = System.currentTimeMillis();

        IgniteCache cache = ignite.createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setIndexedTypes(Integer.class, String.class)
                .setSqlSchema(SCHEMA_NAME)
                .setSqlFunctionClasses(GridTestUtils.SqlTestFunctions.class)
        );

        cache.put(100, "200");

        String sql = "SELECT \"STRING\"._KEY, \"STRING\"._VAL FROM \"STRING\" WHERE _key=100 AND sleep_and_can_fail()>0";

        GridTestUtils.SqlTestFunctions.sleepMs = MIN_SLEEP;
        GridTestUtils.SqlTestFunctions.fail = false;

        cache.query(new SqlFieldsQuery(sql).setSchema(SCHEMA_NAME)).getAll();

        GridTestUtils.SqlTestFunctions.sleepMs = MAX_SLEEP;
        GridTestUtils.SqlTestFunctions.fail = false;

        cache.query(new SqlFieldsQuery(sql).setSchema(SCHEMA_NAME)).getAll();

        GridTestUtils.SqlTestFunctions.sleepMs = MIN_SLEEP;
        GridTestUtils.SqlTestFunctions.fail = true;

        GridTestUtils.assertThrows(log,
            () ->
                cache.query(new SqlFieldsQuery(sql).setSchema(SCHEMA_NAME)).getAll(),
            CacheException.class,
            "Exception calling user-defined function");

        String sqlHist = "SELECT SCHEMA_NAME, SQL, LOCAL, EXECUTIONS, FAILURES, DURATION_MIN, DURATION_MAX, LAST_START_TIME " +
            "FROM " + systemSchemaName() + ".SQL_QUERIES_HISTORY ORDER BY LAST_START_TIME";

        cache.query(new SqlFieldsQuery(sqlHist).setLocal(true)).getAll();
        cache.query(new SqlFieldsQuery(sqlHist).setLocal(true)).getAll();

        List<List<?>> res = cache.query(new SqlFieldsQuery(sqlHist).setLocal(true)).getAll();

        assertEquals(2, res.size());

        long tsAfterRun = System.currentTimeMillis();

        List<?> firstRow = res.get(0);
        List<?> secondRow = res.get(1);

        //SCHEMA_NAME
        assertEquals(SCHEMA_NAME, firstRow.get(0));
        assertEquals(SCHEMA_NAME, secondRow.get(0));

        //SQL
        assertEquals(sql, firstRow.get(1));
        assertEquals(sqlHist, secondRow.get(1));

        // LOCAL flag
        assertEquals(false, firstRow.get(2));
        assertEquals(true, secondRow.get(2));

        // EXECUTIONS
        assertEquals(3L, firstRow.get(3));
        assertEquals(2L, secondRow.get(3));

        //FAILURES
        assertEquals(1L, firstRow.get(4));
        assertEquals(0L, secondRow.get(4));

        //DURATION_MIN
        assertTrue((Long)firstRow.get(5) >= MIN_SLEEP);
        assertTrue((Long)firstRow.get(5) < (Long)firstRow.get(6));

        //DURATION_MAX
        assertTrue((Long)firstRow.get(6) >= MAX_SLEEP);

        //LAST_START_TIME
        assertFalse(((Timestamp)firstRow.get(7)).before(new Timestamp(tsBeforeRun)));
        assertFalse(((Timestamp)firstRow.get(7)).after(new Timestamp(tsAfterRun)));
    }

    /**
     * Test running queries system view.
     */
    @Test
    public void testRunningQueriesView() throws Exception {
        IgniteEx ignite = startGrid(0);

        IgniteCache cache = ignite.createCache(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME).setIndexedTypes(Integer.class, String.class)
        );

        cache.put(100,"200");

        String sql = "SELECT SQL, QUERY_ID, SCHEMA_NAME, LOCAL, START_TIME, DURATION FROM " +
            systemSchemaName() + ".SQL_QUERIES";

        FieldsQueryCursor notClosedFieldQryCursor = cache.query(new SqlFieldsQuery(sql).setLocal(true));

        List<?> cur = cache.query(new SqlFieldsQuery(sql).setLocal(true)).getAll();

        assertEquals(2, cur.size());

        List<?> res0 = (List<?>)cur.get(0);
        List<?> res1 = (List<?>)cur.get(1);

        Timestamp ts = (Timestamp)res0.get(4);

        Instant now = Instant.now();

        long diffInMillis = now.minusMillis(ts.getTime()).toEpochMilli();

        assertTrue(diffInMillis < 3000);

        assertEquals(sql, res0.get(0));

        assertEquals(sql, res1.get(0));

        assertTrue((Boolean)res0.get(3));

        String id0 = (String)res0.get(1);
        String id1 = (String)res1.get(1);

        assertNotEquals(id0, id1);

        String qryPrefix = ignite.localNode().id() + "_";

        String qryId1 = qryPrefix + "1";
        String qryId2 = qryPrefix + "2";

        assertTrue(id0.equals(qryId1) || id1.equals(qryId1));

        assertTrue(id0.equals(qryId2) || id1.equals(qryId2));

        assertEquals(2, cache.query(new SqlFieldsQuery(sql)).getAll().size());

        notClosedFieldQryCursor.close();

        assertEquals(1, cache.query(new SqlFieldsQuery(sql)).getAll().size());

        cache.put(100,"200");

        QueryCursor notClosedQryCursor = cache.query(new SqlQuery<>(String.class, "_key=100"));

        String expSqlQry = "SELECT \"default\".\"STRING\"._KEY, \"default\".\"STRING\"._VAL FROM " +
            "\"default\".\"STRING\" WHERE _key=100";

        cur = cache.query(new SqlFieldsQuery(sql)).getAll();

        assertEquals(2, cur.size());

        res0 = (List<?>)cur.get(0);
        res1 = (List<?>)cur.get(1);

        assertTrue(expSqlQry, res0.get(0).equals(expSqlQry) || res1.get(0).equals(expSqlQry));

        assertFalse((Boolean)res0.get(3));

        assertFalse((Boolean)res1.get(3));

        notClosedQryCursor.close();

        sql = "SELECT SQL, QUERY_ID FROM " + systemSchemaName() + ".SQL_QUERIES WHERE QUERY_ID='" + qryPrefix + "7'";

        assertEquals(qryPrefix + "7", ((List<?>)cache.query(new SqlFieldsQuery(sql)).getAll().get(0)).get(1));

        sql = "SELECT SQL FROM " + systemSchemaName() + ".SQL_QUERIES WHERE DURATION > 100000";

        assertTrue(cache.query(new SqlFieldsQuery(sql)).getAll().isEmpty());

        sql = "SELECT SQL FROM " + systemSchemaName() + ".SQL_QUERIES WHERE QUERY_ID='UNKNOWN'";

        assertTrue(cache.query(new SqlFieldsQuery(sql)).getAll().isEmpty());
    }

    /**
     * Test that we can't use cache tables and system views in the same query.
     */
    @Test
    public void testCacheToViewJoin() throws Exception {
        Ignite ignite = startGrid();

        ignite.createCache(new CacheConfiguration<>().setName(DEFAULT_CACHE_NAME).setQueryEntities(
            Collections.singleton(new QueryEntity(Integer.class.getName(), String.class.getName()))));

        assertSqlError("SELECT * FROM \"" + DEFAULT_CACHE_NAME + "\".String JOIN " + systemSchemaName() + ".NODES ON 1=1");
    }

    /**
     * @param rowData Row data.
     * @param colTypes Column types.
     */
    private void assertColumnTypes(List<?> rowData, Class<?>... colTypes) {
        for (int i = 0; i < colTypes.length; i++) {
            if (rowData.get(i) != null)
                assertEquals("Column " + i + " type", colTypes[i], rowData.get(i).getClass());
        }
    }

    /**
     * Test nodes system view.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNodesViews() throws Exception {
        Ignite igniteSrv = startGrid(getTestIgniteInstanceName(), getConfiguration().setMetricsUpdateFrequency(500L));

        Ignite igniteCli =
            startClientGrid(getTestIgniteInstanceName(1), getConfiguration().setMetricsUpdateFrequency(500L));

        startGrid(getTestIgniteInstanceName(2), getConfiguration().setMetricsUpdateFrequency(500L).setDaemon(true));

        UUID nodeId0 = igniteSrv.cluster().localNode().id();

        awaitPartitionMapExchange();

        List<List<?>> resAll = execSql("SELECT NODE_ID, CONSISTENT_ID, VERSION, IS_CLIENT, IS_DAEMON, " +
            "NODE_ORDER, ADDRESSES, HOSTNAMES FROM " + systemSchemaName() + ".NODES");

        assertColumnTypes(resAll.get(0), UUID.class, String.class, String.class, Boolean.class, Boolean.class,
            Long.class, String.class, String.class);

        assertEquals(3, resAll.size());

        List<List<?>> resSrv = execSql(
            "SELECT NODE_ID, NODE_ORDER FROM " +
                systemSchemaName() + ".NODES WHERE IS_CLIENT = FALSE AND IS_DAEMON = FALSE"
        );

        assertEquals(1, resSrv.size());

        assertEquals(nodeId0, resSrv.get(0).get(0));

        assertEquals(1L, resSrv.get(0).get(1));

        List<List<?>> resCli = execSql(
            "SELECT NODE_ID, NODE_ORDER FROM " + systemSchemaName() + ".NODES WHERE IS_CLIENT = TRUE");

        assertEquals(1, resCli.size());

        assertEquals(nodeId(1), resCli.get(0).get(0));

        assertEquals(2L, resCli.get(0).get(1));

        List<List<?>> resDaemon = execSql(
            "SELECT NODE_ID, NODE_ORDER FROM " + systemSchemaName() + ".NODES WHERE IS_DAEMON = TRUE");

        assertEquals(1, resDaemon.size());

        assertEquals(nodeId(2), resDaemon.get(0).get(0));

        assertEquals(3L, resDaemon.get(0).get(1));

        // Check index on ID column.
        assertEquals(0, execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODES WHERE NODE_ID = '-'").size());

        assertEquals(1, execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODES WHERE NODE_ID = ?",
            nodeId0).size());

        assertEquals(1, execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODES WHERE NODE_ID = ?",
            nodeId(2)).size());

        // Check index on ID column with disjunction.
        assertEquals(3, execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODES WHERE NODE_ID = ? " +
            "OR node_order=1 OR node_order=2 OR node_order=3", nodeId0).size());

        // Check quick-count.
        assertEquals(3L, execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".NODES").get(0).get(0));

        // Check joins
        assertEquals(nodeId0, execSql("SELECT N1.NODE_ID FROM " + systemSchemaName() + ".NODES N1 JOIN " +
            systemSchemaName() + ".NODES N2 ON N1.NODE_ORDER = N2.NODE_ORDER JOIN " +
            systemSchemaName() + ".NODES N3 ON N2.NODE_ID = N3.NODE_ID WHERE N3.NODE_ORDER = 1")
            .get(0).get(0));

        // Check sub-query
        assertEquals(nodeId0, execSql("SELECT N1.NODE_ID FROM " + systemSchemaName() + ".NODES N1 " +
            "WHERE NOT EXISTS (SELECT 1 FROM " + systemSchemaName() + ".NODES N2 WHERE N2.NODE_ID = N1.NODE_ID AND N2.NODE_ORDER <> 1)")
            .get(0).get(0));

        // Check node attributes view
        String cliAttrName = IgniteNodeAttributes.ATTR_CLIENT_MODE;

        assertColumnTypes(execSql("SELECT NODE_ID, NAME, VALUE FROM " + systemSchemaName() + ".NODE_ATTRIBUTES").get(0),
            UUID.class, String.class, String.class);

        assertEquals(1,
            execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODE_ATTRIBUTES WHERE NAME = ? AND VALUE = 'true'",
                cliAttrName).size());

        assertEquals(3,
            execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODE_ATTRIBUTES WHERE NAME = ?", cliAttrName).size());

        assertEquals(1,
            execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODE_ATTRIBUTES WHERE NODE_ID = ? AND NAME = ? AND VALUE = 'true'",
                nodeId(1), cliAttrName).size());

        assertEquals(0,
            execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODE_ATTRIBUTES WHERE NODE_ID = '-' AND NAME = ?",
                cliAttrName).size());

        assertEquals(0,
            execSql("SELECT NODE_ID FROM " + systemSchemaName() + ".NODE_ATTRIBUTES WHERE NODE_ID = ? AND NAME = '-'",
                nodeId(1)).size());

        // Check node metrics view.
        String sqlAllMetrics = "SELECT NODE_ID, LAST_UPDATE_TIME, " +
            "MAX_ACTIVE_JOBS, CUR_ACTIVE_JOBS, AVG_ACTIVE_JOBS, " +
            "MAX_WAITING_JOBS, CUR_WAITING_JOBS, AVG_WAITING_JOBS, " +
            "MAX_REJECTED_JOBS, CUR_REJECTED_JOBS, AVG_REJECTED_JOBS, TOTAL_REJECTED_JOBS, " +
            "MAX_CANCELED_JOBS, CUR_CANCELED_JOBS, AVG_CANCELED_JOBS, TOTAL_CANCELED_JOBS, " +
            "MAX_JOBS_WAIT_TIME, CUR_JOBS_WAIT_TIME, AVG_JOBS_WAIT_TIME, " +
            "MAX_JOBS_EXECUTE_TIME, CUR_JOBS_EXECUTE_TIME, AVG_JOBS_EXECUTE_TIME, TOTAL_JOBS_EXECUTE_TIME, " +
            "TOTAL_EXECUTED_JOBS, TOTAL_EXECUTED_TASKS, " +
            "TOTAL_BUSY_TIME, TOTAL_IDLE_TIME, CUR_IDLE_TIME, BUSY_TIME_PERCENTAGE, IDLE_TIME_PERCENTAGE, " +
            "TOTAL_CPU, CUR_CPU_LOAD, AVG_CPU_LOAD, CUR_GC_CPU_LOAD, " +
            "HEAP_MEMORY_INIT, HEAP_MEMORY_USED, HEAP_MEMORY_COMMITED, HEAP_MEMORY_MAX, HEAP_MEMORY_TOTAL, " +
            "NONHEAP_MEMORY_INIT, NONHEAP_MEMORY_USED, NONHEAP_MEMORY_COMMITED, NONHEAP_MEMORY_MAX, NONHEAP_MEMORY_TOTAL, " +
            "UPTIME, JVM_START_TIME, NODE_START_TIME, LAST_DATA_VERSION, " +
            "CUR_THREAD_COUNT, MAX_THREAD_COUNT, TOTAL_THREAD_COUNT, CUR_DAEMON_THREAD_COUNT, " +
            "SENT_MESSAGES_COUNT, SENT_BYTES_COUNT, RECEIVED_MESSAGES_COUNT, RECEIVED_BYTES_COUNT, " +
            "OUTBOUND_MESSAGES_QUEUE FROM " + systemSchemaName() + ".NODE_METRICS";

        List<List<?>> resMetrics = execSql(sqlAllMetrics);

        assertColumnTypes(resMetrics.get(0), UUID.class, Timestamp.class,
            Integer.class, Integer.class, Float.class, // Active jobs.
            Integer.class, Integer.class, Float.class, // Waiting jobs.
            Integer.class, Integer.class, Float.class, Integer.class, // Rejected jobs.
            Integer.class, Integer.class, Float.class, Integer.class, // Canceled jobs.
            Long.class, Long.class, Long.class, // Jobs wait time.
            Long.class, Long.class, Long.class, Long.class, // Jobs execute time.
            Integer.class, Integer.class, // Executed jobs/task.
            Long.class, Long.class, Long.class, Float.class, Float.class, // Busy/idle time.
            Integer.class, Double.class, Double.class, Double.class, // CPU.
            Long.class, Long.class, Long.class, Long.class, Long.class, // Heap memory.
            Long.class, Long.class, Long.class, Long.class, Long.class, // Nonheap memory.
            Long.class, Timestamp.class, Timestamp.class, Long.class, // Uptime.
            Integer.class, Integer.class, Long.class, Integer.class, // Threads.
            Integer.class, Long.class, Integer.class, Long.class, // Sent/received messages.
            Integer.class); // Outbound message queue.

        assertEquals(3, resAll.size());

        // Check join with nodes.
        assertEquals(3, execSql("SELECT NM.LAST_UPDATE_TIME FROM " + systemSchemaName() + ".NODES N " +
            "JOIN " + systemSchemaName() + ".NODE_METRICS NM ON N.NODE_ID = NM.NODE_ID").size());

        // Check index on NODE_ID column.
        assertEquals(1, execSql("SELECT LAST_UPDATE_TIME FROM " + systemSchemaName() + ".NODE_METRICS WHERE NODE_ID = ?",
            nodeId(1)).size());

        // Check malformed value for indexed column.
        assertEquals(0, execSql("SELECT LAST_UPDATE_TIME FROM " + systemSchemaName() + ".NODE_METRICS WHERE NODE_ID = ?",
            "-").size());

        // Check quick-count.
        assertEquals(3L, execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".NODE_METRICS").get(0).get(0));

        // Check metric values.

        // Broadcast jobs to server and client nodes to get non zero metric values.
        for (int i = 0; i < 100; i++) {
            IgniteFuture<Void> fut = igniteSrv.compute(igniteSrv.cluster().forNodeId(nodeId0, nodeId(1)))
                .broadcastAsync(
                    new IgniteRunnable() {
                        @Override public void run() {
                            Random rnd = new Random();

                            try {
                                doSleep(rnd.nextInt(100));
                            }
                            catch (Throwable ignore) {
                                // No-op.
                            }
                        }
                    });

            if (i % 10 == 0)
                fut.cancel();
        }

        doSleep(igniteSrv.configuration().getMetricsUpdateFrequency() * 3L);

        for (Ignite grid : G.allGrids()) {
            UUID nodeId = grid.cluster().localNode().id();

            // Metrics for node must be collected from another node to avoid race and get consistent metrics snapshot.
            Ignite ignite = F.eq(nodeId, nodeId0) ? igniteCli : igniteSrv;

            for (int i = 0; i < METRICS_CHECK_ATTEMPTS; i++) {
                ClusterMetrics metrics = ignite.cluster().node(nodeId).metrics();

                assertTrue(metrics instanceof ClusterMetricsSnapshot);

                resMetrics = execSql(ignite, sqlAllMetrics + " WHERE NODE_ID = ?", nodeId);

                log.info("Check metrics for node " + grid.name() + ", attempt " + (i + 1));

                if (metrics.getLastUpdateTime() == ((Timestamp)resMetrics.get(0).get(1)).getTime()) {
                    assertEquals(metrics.getMaximumActiveJobs(), resMetrics.get(0).get(2));
                    assertEquals(metrics.getCurrentActiveJobs(), resMetrics.get(0).get(3));
                    assertEquals(metrics.getAverageActiveJobs(), resMetrics.get(0).get(4));
                    assertEquals(metrics.getMaximumWaitingJobs(), resMetrics.get(0).get(5));
                    assertEquals(metrics.getCurrentWaitingJobs(), resMetrics.get(0).get(6));
                    assertEquals(metrics.getAverageWaitingJobs(), resMetrics.get(0).get(7));
                    assertEquals(metrics.getMaximumRejectedJobs(), resMetrics.get(0).get(8));
                    assertEquals(metrics.getCurrentRejectedJobs(), resMetrics.get(0).get(9));
                    assertEquals(metrics.getAverageRejectedJobs(), resMetrics.get(0).get(10));
                    assertEquals(metrics.getTotalRejectedJobs(), resMetrics.get(0).get(11));
                    assertEquals(metrics.getMaximumCancelledJobs(), resMetrics.get(0).get(12));
                    assertEquals(metrics.getCurrentCancelledJobs(), resMetrics.get(0).get(13));
                    assertEquals(metrics.getAverageCancelledJobs(), resMetrics.get(0).get(14));
                    assertEquals(metrics.getTotalCancelledJobs(), resMetrics.get(0).get(15));
                    assertEquals(metrics.getMaximumJobWaitTime(), resMetrics.get(0).get(16));
                    assertEquals(metrics.getCurrentJobWaitTime(), resMetrics.get(0).get(17));
                    assertEquals((long)metrics.getAverageJobWaitTime(), resMetrics.get(0).get(18));
                    assertEquals(metrics.getMaximumJobExecuteTime(), resMetrics.get(0).get(19));
                    assertEquals(metrics.getCurrentJobExecuteTime(), resMetrics.get(0).get(20));
                    assertEquals((long)metrics.getAverageJobExecuteTime(), resMetrics.get(0).get(21));
                    assertEquals(metrics.getTotalJobsExecutionTime(), resMetrics.get(0).get(22));
                    assertEquals(metrics.getTotalExecutedJobs(), resMetrics.get(0).get(23));
                    assertEquals(metrics.getTotalExecutedTasks(), resMetrics.get(0).get(24));
                    assertEquals(metrics.getTotalBusyTime(), resMetrics.get(0).get(25));
                    assertEquals(metrics.getTotalIdleTime(), resMetrics.get(0).get(26));
                    assertEquals(metrics.getCurrentIdleTime(), resMetrics.get(0).get(27));
                    assertEquals(metrics.getBusyTimePercentage(), resMetrics.get(0).get(28));
                    assertEquals(metrics.getIdleTimePercentage(), resMetrics.get(0).get(29));
                    assertEquals(metrics.getTotalCpus(), resMetrics.get(0).get(30));
                    assertEquals(metrics.getCurrentCpuLoad(), resMetrics.get(0).get(31));
                    assertEquals(metrics.getAverageCpuLoad(), resMetrics.get(0).get(32));
                    assertEquals(metrics.getCurrentGcCpuLoad(), resMetrics.get(0).get(33));
                    assertEquals(metrics.getHeapMemoryInitialized(), resMetrics.get(0).get(34));
                    assertEquals(metrics.getHeapMemoryUsed(), resMetrics.get(0).get(35));
                    assertEquals(metrics.getHeapMemoryCommitted(), resMetrics.get(0).get(36));
                    assertEquals(metrics.getHeapMemoryMaximum(), resMetrics.get(0).get(37));
                    assertEquals(metrics.getHeapMemoryTotal(), resMetrics.get(0).get(38));
                    assertEquals(metrics.getNonHeapMemoryInitialized(), resMetrics.get(0).get(39));
                    assertEquals(metrics.getNonHeapMemoryUsed(), resMetrics.get(0).get(40));
                    assertEquals(metrics.getNonHeapMemoryCommitted(), resMetrics.get(0).get(41));
                    assertEquals(metrics.getNonHeapMemoryMaximum(), resMetrics.get(0).get(42));
                    assertEquals(metrics.getNonHeapMemoryTotal(), resMetrics.get(0).get(43));
                    assertEquals(metrics.getUpTime(), resMetrics.get(0).get(44));
                    assertEquals(metrics.getStartTime(), ((Timestamp)resMetrics.get(0).get(45)).getTime());
                    assertEquals(metrics.getNodeStartTime(), ((Timestamp)resMetrics.get(0).get(46)).getTime());
                    assertEquals(metrics.getLastDataVersion(), resMetrics.get(0).get(47));
                    assertEquals(metrics.getCurrentThreadCount(), resMetrics.get(0).get(48));
                    assertEquals(metrics.getMaximumThreadCount(), resMetrics.get(0).get(49));
                    assertEquals(metrics.getTotalStartedThreadCount(), resMetrics.get(0).get(50));
                    assertEquals(metrics.getCurrentDaemonThreadCount(), resMetrics.get(0).get(51));
                    assertEquals(metrics.getSentMessagesCount(), resMetrics.get(0).get(52));
                    assertEquals(metrics.getSentBytesCount(), resMetrics.get(0).get(53));
                    assertEquals(metrics.getReceivedMessagesCount(), resMetrics.get(0).get(54));
                    assertEquals(metrics.getReceivedBytesCount(), resMetrics.get(0).get(55));
                    assertEquals(metrics.getOutboundMessagesQueueSize(), resMetrics.get(0).get(56));

                    break;
                }
                else {
                    log.info("Metrics was updated in background, will retry check");

                    if (i == METRICS_CHECK_ATTEMPTS - 1)
                        fail("Failed to check metrics, attempts limit reached (" + METRICS_CHECK_ATTEMPTS + ')');
                }
            }
        }
    }

    /**
     * Test baseline topology system view.
     */
    @Test
    public void testBaselineViews() throws Exception {
        cleanPersistenceDir();

        Ignite ignite = startGrid(getTestIgniteInstanceName(), getPdsConfiguration("node0"));
        startGrid(getTestIgniteInstanceName(1), getPdsConfiguration("node1"));

        ignite.cluster().active(true);

        List<List<?>> res = execSql("SELECT CONSISTENT_ID, ONLINE FROM " +
            systemSchemaName() + ".BASELINE_NODES ORDER BY CONSISTENT_ID");

        assertColumnTypes(res.get(0), String.class, Boolean.class);

        assertEquals(2, res.size());

        assertEquals("node0", res.get(0).get(0));
        assertEquals("node1", res.get(1).get(0));

        assertEquals(true, res.get(0).get(1));
        assertEquals(true, res.get(1).get(1));

        stopGrid(getTestIgniteInstanceName(1));

        res = execSql("SELECT CONSISTENT_ID FROM " + systemSchemaName() + ".BASELINE_NODES WHERE ONLINE = false");

        assertEquals(1, res.size());

        assertEquals("node1", res.get(0).get(0));

        Ignite ignite2 = startGrid(getTestIgniteInstanceName(2), getPdsConfiguration("node2"));

        assertEquals(2, execSql(ignite2, "SELECT CONSISTENT_ID FROM " + systemSchemaName() + ".BASELINE_NODES").size());

        res = execSql("SELECT CONSISTENT_ID FROM " + systemSchemaName() + ".NODES N WHERE NOT EXISTS (SELECT 1 FROM " +
            systemSchemaName() + ".BASELINE_NODES B WHERE B.CONSISTENT_ID = N.CONSISTENT_ID)");

        assertEquals(1, res.size());

        assertEquals("node2", res.get(0).get(0));
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setMetricExporterSpi(new SqlViewMetricExporterSpi());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration()
            .setCacheConfiguration(new CacheConfiguration().setName(DEFAULT_CACHE_NAME))
            .setMetricExporterSpi(new SqlViewMetricExporterSpi());

        if (isPersistenceEnabled) {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true).setMaxSize(10 * 1024 * 1024)
                )
            );
        }

        return cfg;
    }

    /**
     * Test IO statistics SQL system views for cache groups.
     *
     * @throws Exception
     */
    @Test
    public void testIoStatisticsViews() throws Exception {
        Ignite ignite = startGrid(getTestIgniteInstanceName(), getPdsConfiguration("node0"));

        ignite.cluster().active(true);

        execSql("CREATE TABLE TST(id INTEGER PRIMARY KEY, name VARCHAR, age integer)");

        for (int i = 0; i < 500; i++)
            execSql("INSERT INTO TST(id, name, age) VALUES (" + i + ",'name-" + i + "'," + i + 1 + ")");

        String sql1 = "SELECT CACHE_GROUP_ID, CACHE_GROUP_NAME, PHYSICAL_READS, LOGICAL_READS FROM " +
            systemSchemaName() + ".LOCAL_CACHE_GROUPS_IO";

        List<List<?>> res1 = execSql(sql1);

        Map<?, ?> map = res1.stream().collect(Collectors.toMap(k -> k.get(1), v -> v.get(3)));

        assertEquals(2, map.size());

        assertTrue(map.containsKey("SQL_PUBLIC_TST"));

        assertTrue((Long)map.get("SQL_PUBLIC_TST") > 0);

        assertTrue(map.containsKey(DEFAULT_CACHE_NAME));

        sql1 = "SELECT CACHE_GROUP_ID, CACHE_GROUP_NAME, PHYSICAL_READS, LOGICAL_READS FROM " +
            systemSchemaName() + ".LOCAL_CACHE_GROUPS_IO WHERE CACHE_GROUP_NAME='SQL_PUBLIC_TST'";

        assertEquals(1, execSql(sql1).size());
    }

    /**
     * Simple test for {@link SqlTableView}.
     */
    @Test
    public void testTablesView() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        GridCacheProcessor cacheProc = ignite.context().cache();

        execSql("CREATE TABLE CACHE_SQL (ID INT PRIMARY KEY, MY_VAL VARCHAR) WITH " +
            "\"cache_name=cache_sql,template=partitioned,atomicity=atomic,wrap_value=true,value_type=random_name\"");

        execSql("CREATE TABLE PUBLIC.DFLT_CACHE (ID1 INT, ID2 INT, MY_VAL VARCHAR, PRIMARY KEY (ID1, ID2)) WITH"
            + "\"affinity_key=ID2,wrap_value=false,key_type=random_name\"");

        int cacheSqlId = cacheProc.cacheDescriptor("cache_sql").cacheId();
        int ddlTabId = cacheProc.cacheDescriptor("SQL_PUBLIC_DFLT_CACHE").cacheId();

        List<List<?>> cacheSqlInfos = execSql("SELECT * FROM " + systemSchemaName() + ".TABLES WHERE " +
            "TABLE_NAME = 'CACHE_SQL'");

        List<?> expRow = asList(
            "CACHE_SQL",         // TABLE_NAME
            "PUBLIC",            // SCHEMA_NAME
            "cache_sql",         // CACHE_NAME
            cacheSqlId,          // CACHE_ID
            null,                // AFFINITY_KEY_COLUMN
            "ID",                // KEY_ALIAS
            null,                // VALUE_ALIAS
            "java.lang.Integer", // KEY_TYPE_NAME
            "random_name",       // VALUE_TYPE_NAME
            false                // IS_INDEX_REBUILD_IN_PROGRESS
        );

        assertEquals("Returned incorrect info. ", expRow, cacheSqlInfos.get(0));

        // no more rows are expected.
        assertEquals("Expected to return only one row", 1, cacheSqlInfos.size());

        List<List<?>> allInfos = execSql("SELECT * FROM " + systemSchemaName() + ".TABLES");

        List<?> allExpRows = asList(
            expRow,
            asList(
                "DFLT_CACHE",            // TABLE_NAME
                "PUBLIC",                // SCHEMA_NAME
                "SQL_PUBLIC_DFLT_CACHE", // CACHE_NAME
                ddlTabId,                // CACHE_ID
                "ID2",                   // AFFINITY_KEY_COLUMN
                null,                    // KEY_ALIAS
                "MY_VAL",                // VALUE_ALIAS
                "random_name",           // KEY_TYPE_NAME
                "java.lang.String",      // VALUE_TYPE_NAME
                false                    // IS_INDEX_REBUILD_IN_PROGRESS
            )
        );

        if (!F.eqNotOrdered(allExpRows, allInfos))
            fail("Returned incorrect rows [expected=" + allExpRows + ", actual=" + allInfos + "].");

        // Filter by cache name:
        assertEquals(
            Collections.singletonList(asList("DFLT_CACHE", "SQL_PUBLIC_DFLT_CACHE")),
            execSql("SELECT TABLE_NAME, CACHE_NAME " +
                "FROM " + systemSchemaName() + ".TABLES " +
                "WHERE CACHE_NAME LIKE 'SQL\\_PUBLIC\\_%'"));

        assertEquals(
            Collections.singletonList(asList("CACHE_SQL", "cache_sql")),
            execSql("SELECT TABLE_NAME, CACHE_NAME " +
                "FROM " + systemSchemaName() + ".TABLES " +
                "WHERE CACHE_NAME NOT LIKE 'SQL\\_PUBLIC\\_%'"));

        // Join with CACHES view.
        assertEquals(
            asList(
                asList("DFLT_CACHE", "SQL_PUBLIC_DFLT_CACHE", "SQL_PUBLIC_DFLT_CACHE"),
                asList("CACHE_SQL", "cache_sql", "cache_sql")),
            execSql("SELECT TABLE_NAME, TAB.CACHE_NAME, C.CACHE_NAME " +
                "FROM " + systemSchemaName() + ".TABLES AS TAB JOIN " + systemSchemaName() + ".CACHES AS C " +
                "ON TAB.CACHE_ID = C.CACHE_ID " +
                "ORDER BY C.CACHE_NAME")
        );
    }

    /**
     * Verify that if we drop or create table, TABLES system view reflects these changes.
     */
    @Test
    public void testTablesDropAndCreate() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        final String selectTabNameCacheName = "SELECT TABLE_NAME, CACHE_NAME FROM " + systemSchemaName() + ".TABLES ORDER BY TABLE_NAME";

        assertTrue("Initially no tables expected", execSql(selectTabNameCacheName).isEmpty());

        execSql("CREATE TABLE PUBLIC.TAB1 (ID INT PRIMARY KEY, VAL VARCHAR)");

        assertEquals(
            asList(asList("TAB1", "SQL_PUBLIC_TAB1")),
            execSql(selectTabNameCacheName));

        execSql("CREATE TABLE PUBLIC.TAB2 (ID LONG PRIMARY KEY, VAL_STR VARCHAR) WITH \"cache_name=cache2\"");
        execSql("CREATE TABLE PUBLIC.TAB3 (ID LONG PRIMARY KEY, VAL_INT INT) WITH \"cache_name=cache3\" ");

        assertEquals(
            asList(
                asList("TAB1", "SQL_PUBLIC_TAB1"),
                asList("TAB2", "cache2"),
                asList("TAB3", "cache3")
            ),
            execSql(selectTabNameCacheName));

        execSql("DROP TABLE PUBLIC.TAB2");

        assertEquals(
            asList(
                asList("TAB1", "SQL_PUBLIC_TAB1"),
                asList("TAB3", "cache3")
            ),
            execSql(selectTabNameCacheName));

        execSql("DROP TABLE PUBLIC.TAB3");

        assertEquals(
            asList(asList("TAB1", "SQL_PUBLIC_TAB1")),
            execSql(selectTabNameCacheName));

        execSql("DROP TABLE PUBLIC.TAB1");

        assertTrue("All tables should be dropped", execSql(selectTabNameCacheName).isEmpty());
    }

    /**
     * Dummy implementation of the mapper. Required to test "AFFINITY_KEY_COLUMN".
     */
    static class ConstantMapper implements AffinityKeyMapper {
        /** Serial version uid. */
        private static final long serialVersionUID = 7018626316531791556L;

        /** {@inheritDoc} */
        @Override public Object affinityKey(Object key) {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            //NO-op
        }
    }

    /**
     * Check affinity column if custom affinity mapper is specified.
     */
    @Test
    public void testTablesNullAffinityKey() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        AffinityKeyMapper fakeMapper = new ConstantMapper();

        ignite.getOrCreateCache(defaultCacheConfiguration().setName("NO_KEY_FIELDS_CACHE").setAffinityMapper(fakeMapper)
            .setQueryEntities(Collections.singleton(
                // A cache with  no key fields
                new QueryEntity(Object.class.getName(), "Object2")
                    .addQueryField("name", String.class.getName(), null)
                    .addQueryField("salary", Integer.class.getName(), null)
                    .setTableName("NO_KEY_TABLE")
            )));

        List<List<String>> expected = Collections.singletonList(asList("NO_KEY_TABLE", null));

        assertEquals(expected,
            execSql("SELECT TABLE_NAME, AFFINITY_KEY_COLUMN " +
                "FROM " + systemSchemaName() + ".TABLES " +
                "WHERE CACHE_NAME = 'NO_KEY_FIELDS_CACHE'"));

        assertEquals(expected,
            execSql("SELECT TABLE_NAME, AFFINITY_KEY_COLUMN " +
                "FROM " + systemSchemaName() + ".TABLES " +
                "WHERE AFFINITY_KEY_COLUMN IS NULL"));
    }

    /**
     * Special test for key/val name and type. Covers most used cases
     */
    @Test
    public void testTablesViewKeyVal() throws Exception {
        IgniteEx ignite = startGrid(getConfiguration());

        {
            ignite.getOrCreateCache(defaultCacheConfiguration().setName("NO_ALIAS_NON_SQL_KEY")
                .setQueryEntities(Collections.singleton(
                    // A cache with  no key fields
                    new QueryEntity(Object.class.getName(), "Object2")
                        .addQueryField("name", String.class.getName(), null)
                        .addQueryField("salary", Integer.class.getName(), null)
                        .setTableName("NO_ALIAS_NON_SQL_KEY")
                )));

            List<?> keyValAliases = execSql("SELECT KEY_ALIAS, VALUE_ALIAS FROM " + systemSchemaName() + ".TABLES " +
                "WHERE TABLE_NAME = 'NO_ALIAS_NON_SQL_KEY'").get(0);

            assertEquals(asList(null, null), keyValAliases);
        }

        {
            execSql("CREATE TABLE PUBLIC.SIMPLE_KEY_SIMPLE_VAL (ID INT PRIMARY KEY, NAME VARCHAR) WITH \"wrap_value=false\"");

            List<?> keyValAliases = execSql("SELECT KEY_ALIAS, VALUE_ALIAS FROM " + systemSchemaName() + ".TABLES " +
                "WHERE TABLE_NAME = 'SIMPLE_KEY_SIMPLE_VAL'").get(0);

            assertEquals(asList("ID", "NAME"), keyValAliases);

        }

        {
            execSql("CREATE TABLE PUBLIC.COMPLEX_KEY_COMPLEX_VAL " +
                "(ID1 INT, " +
                "ID2 INT, " +
                "VAL1 VARCHAR, " +
                "VAL2 VARCHAR, " +
                "PRIMARY KEY(ID1, ID2))");

            List<?> keyValAliases = execSql("SELECT KEY_ALIAS, VALUE_ALIAS FROM " + systemSchemaName() + ".TABLES " +
                "WHERE TABLE_NAME = 'COMPLEX_KEY_COMPLEX_VAL'").get(0);

            assertEquals(asList(null, null), keyValAliases);
        }
    }

    /**
     * Test caches system views.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testCachesViews() throws Exception {
        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setName("def").setPersistenceEnabled(true))
            .setDataRegionConfigurations(new DataRegionConfiguration().setName("dr1"),
                new DataRegionConfiguration().setName("dr2"), new DataRegionConfiguration().setName("dr3"));

        IgniteEx ignite0 = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg));

        Ignite ignite1 = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg).setIgniteInstanceName("node1"));

        ignite0.cluster().active(true);

        Ignite ignite2 = startGrid(getConfiguration().setDataStorageConfiguration(dsCfg).setIgniteInstanceName("node2"));

        Ignite ignite3 =
            startClientGrid(getConfiguration().setDataStorageConfiguration(dsCfg).setIgniteInstanceName("node3"));

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_atomic_part")
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.PARTITIONED)
            .setGroupName("cache_grp")
            .setNodeFilter(new TestNodeFilter(ignite0.cluster().localNode()))
        );

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_atomic_repl")
            .setAtomicityMode(CacheAtomicityMode.ATOMIC)
            .setCacheMode(CacheMode.REPLICATED)
            .setDataRegionName("dr1")
            .setTopologyValidator(new TestTopologyValidator())
        );

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_tx_part")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.PARTITIONED)
            .setGroupName("cache_grp")
            .setNodeFilter(new TestNodeFilter(ignite0.cluster().localNode()))
        );

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_tx_repl")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setDataRegionName("dr2")
            .setEvictionFilter(new TestEvictionFilter())
            .setEvictionPolicyFactory(new TestEvictionPolicyFactory())
            .setOnheapCacheEnabled(true)
        );

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_cust_node_filter")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setDataRegionName("dr3")
            .setEvictionFilter(new TestEvictionFilter())
            .setEvictionPolicyFactory(new TestEvictionPolicyFactory())
            .setOnheapCacheEnabled(true)
            .setNodeFilter(new CustomNodeFilter(Integer.MAX_VALUE))
        );

        ignite0.getOrCreateCache(new CacheConfiguration<>()
            .setName("cache_cust_err_node_filter")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setCacheMode(CacheMode.REPLICATED)
            .setDataRegionName("dr3")
            .setEvictionFilter(new TestEvictionFilter())
            .setEvictionPolicyFactory(new TestEvictionPolicyFactory())
            .setOnheapCacheEnabled(true)
            .setNodeFilter(new CustomNodeFilter(1))
        );

        execSql("CREATE TABLE cache_sql (ID INT PRIMARY KEY, VAL VARCHAR) WITH " +
            "\"cache_name=cache_sql,template=partitioned,atomicity=atomic\"");

        awaitPartitionMapExchange();

        List<List<?>> resAll = execSql("SELECT CACHE_GROUP_ID, CACHE_GROUP_NAME, CACHE_ID, CACHE_NAME, CACHE_TYPE," +
                "CACHE_MODE, ATOMICITY_MODE, IS_ONHEAP_CACHE_ENABLED, IS_COPY_ON_READ, IS_LOAD_PREVIOUS_VALUE, " +
                "IS_READ_FROM_BACKUP, PARTITION_LOSS_POLICY, NODE_FILTER, TOPOLOGY_VALIDATOR, IS_EAGER_TTL, " +
                "WRITE_SYNCHRONIZATION_MODE, IS_INVALIDATE, IS_EVENTS_DISABLED, IS_STATISTICS_ENABLED, " +
                "IS_MANAGEMENT_ENABLED, BACKUPS, AFFINITY, AFFINITY_MAPPER, " +
                "REBALANCE_MODE, REBALANCE_BATCH_SIZE, REBALANCE_TIMEOUT, REBALANCE_DELAY, REBALANCE_THROTTLE, " +
                "REBALANCE_BATCHES_PREFETCH_COUNT, REBALANCE_ORDER, " +
                "EVICTION_FILTER, EVICTION_POLICY_FACTORY, " +
                "IS_NEAR_CACHE_ENABLED, NEAR_CACHE_EVICTION_POLICY_FACTORY, NEAR_CACHE_START_SIZE, " +
                "DEFAULT_LOCK_TIMEOUT, INTERCEPTOR, CACHE_STORE_FACTORY, " +
                "IS_STORE_KEEP_BINARY, IS_READ_THROUGH, IS_WRITE_THROUGH, " +
                "IS_WRITE_BEHIND_ENABLED, WRITE_BEHIND_COALESCING, WRITE_BEHIND_FLUSH_SIZE, " +
                "WRITE_BEHIND_FLUSH_FREQUENCY, WRITE_BEHIND_FLUSH_THREAD_COUNT, WRITE_BEHIND_BATCH_SIZE, " +
                "MAX_CONCURRENT_ASYNC_OPERATIONS, CACHE_LOADER_FACTORY, CACHE_WRITER_FACTORY, EXPIRY_POLICY_FACTORY, " +
                "IS_SQL_ESCAPE_ALL, SQL_SCHEMA, SQL_INDEX_MAX_INLINE_SIZE, IS_SQL_ONHEAP_CACHE_ENABLED, " +
                "SQL_ONHEAP_CACHE_MAX_SIZE, QUERY_DETAIL_METRICS_SIZE, QUERY_PARALLELISM, MAX_QUERY_ITERATORS_COUNT, " +
            "DATA_REGION_NAME FROM " + systemSchemaName() + ".CACHES");

        assertColumnTypes(resAll.get(0),
            Integer.class, String.class, Integer.class, String.class, String.class,
            String.class, String.class, Boolean.class, Boolean.class, Boolean.class,
            Boolean.class, String.class, String.class, String.class, Boolean.class,
            String.class, Boolean.class, Boolean.class, Boolean.class,
            Boolean.class, Integer.class, String.class, String.class,
            String.class, Integer.class, Long.class, Long.class, Long.class, // Rebalance.
            Long.class, Integer.class,
            String.class, String.class, // Eviction.
            Boolean.class, String.class, Integer.class, // Near cache.
            Long.class, String.class, String.class,
            Boolean.class, Boolean.class, Boolean.class,
            Boolean.class, Boolean.class, Integer.class, // Write-behind.
            Long.class, Integer.class, Integer.class,
            Integer.class, String.class, String.class, String.class,
            Boolean.class, String.class, Integer.class, Boolean.class, // SQL.
            Integer.class, Integer.class, Integer.class, Integer.class,
            String.class);

        assertEquals("cache_tx_part", execSql("SELECT CACHE_NAME FROM " + systemSchemaName() + ".CACHES WHERE " +
            "CACHE_MODE = 'PARTITIONED' AND ATOMICITY_MODE = 'TRANSACTIONAL' AND CACHE_NAME like 'cache%'").get(0).get(0));

        assertEquals("cache_atomic_repl", execSql("SELECT CACHE_NAME FROM " + systemSchemaName() + ".CACHES WHERE " +
            "CACHE_MODE = 'REPLICATED' AND ATOMICITY_MODE = 'ATOMIC' AND CACHE_NAME like 'cache%'").get(0).get(0));

        assertEquals(2L, execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES WHERE CACHE_GROUP_NAME = 'cache_grp'")
            .get(0).get(0));

        assertEquals("cache_atomic_repl", execSql("SELECT CACHE_NAME FROM " + systemSchemaName() + ".CACHES " +
            "WHERE DATA_REGION_NAME = 'dr1'").get(0).get(0));

        assertEquals("cache_tx_repl", execSql("SELECT CACHE_NAME FROM " + systemSchemaName() + ".CACHES " +
            "WHERE DATA_REGION_NAME = 'dr2'").get(0).get(0));

        assertEquals("PARTITIONED", execSql("SELECT CACHE_MODE FROM " + systemSchemaName() + ".CACHES " +
            "WHERE CACHE_NAME = 'cache_atomic_part'").get(0).get(0));

        assertEquals("USER", execSql("SELECT CACHE_TYPE FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME = 'cache_sql'")
            .get(0).get(0));

        assertEquals(0L, execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME = 'no_such_cache'").get(0)
            .get(0));

        assertEquals(0L, execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME = '1'").get(0).get(0));

        assertEquals("TestNodeFilter", execSql("SELECT NODE_FILTER FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME = " +
            "'cache_atomic_part'").get(0).get(0));

        assertEquals("TestEvictionFilter", execSql("SELECT EVICTION_FILTER FROM " + systemSchemaName() + ".CACHES " +
            "WHERE CACHE_NAME = 'cache_tx_repl'").get(0).get(0));

        assertEquals("TestEvictionPolicyFactory", execSql("SELECT EVICTION_POLICY_FACTORY " +
            "FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME = 'cache_tx_repl'").get(0).get(0));

        assertEquals("TestTopologyValidator", execSql("SELECT TOPOLOGY_VALIDATOR FROM " + systemSchemaName() + ".CACHES " +
            "WHERE CACHE_NAME = 'cache_atomic_repl'").get(0).get(0));

        // Check quick count.
        assertEquals(execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES").get(0).get(0),
            execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES WHERE CACHE_ID <> CACHE_ID + 1").get(0).get(0));

        // Check that caches are the same on BLT, BLT filtered by node filter, non BLT and client nodes.
        assertEquals(7L, execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME like 'cache%'").get(0)
            .get(0));

        assertEquals(7L, execSql(ignite1, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME like 'cache%'")
            .get(0).get(0));

        assertEquals(7L, execSql(ignite2, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME like 'cache%'")
            .get(0).get(0));

        assertEquals(7L, execSql(ignite3, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES WHERE CACHE_NAME like 'cache%'")
            .get(0).get(0));

        // Check cache groups.
        resAll = execSql("SELECT CACHE_GROUP_ID, CACHE_GROUP_NAME, IS_SHARED, CACHE_COUNT, " +
            "CACHE_MODE, ATOMICITY_MODE, AFFINITY, PARTITIONS_COUNT, " +
            "NODE_FILTER, DATA_REGION_NAME, TOPOLOGY_VALIDATOR, PARTITION_LOSS_POLICY, " +
            "REBALANCE_MODE, REBALANCE_DELAY, REBALANCE_ORDER, BACKUPS " +
            "FROM " + systemSchemaName() + ".CACHE_GROUPS");

        assertColumnTypes(resAll.get(0),
            Integer.class, String.class, Boolean.class, Integer.class,
            String.class, String.class, String.class, Integer.class,
            String.class, String.class, String.class, String.class,
            String.class, Long.class, Integer.class, Integer.class);

        assertEquals(2, execSql("SELECT CACHE_COUNT FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE CACHE_GROUP_NAME = 'cache_grp'").get(0).get(0));

        assertEquals("cache_grp", execSql("SELECT CACHE_GROUP_NAME FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE IS_SHARED = true AND CACHE_GROUP_NAME like 'cache%'").get(0).get(0));

        // Check index on ID column.
        assertEquals("cache_tx_repl", execSql("SELECT CACHE_GROUP_NAME FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE CACHE_GROUP_ID = ?", ignite0.cachex("cache_tx_repl").context().groupId()).get(0).get(0));

        assertEquals(0, execSql("SELECT CACHE_GROUP_ID FROM " + systemSchemaName() + ".CACHE_GROUPS WHERE CACHE_GROUP_ID = 0").size());

        // Check join by indexed column.
        assertEquals("cache_tx_repl", execSql("SELECT CG.CACHE_GROUP_NAME FROM " + systemSchemaName() + ".CACHES C JOIN " +
            systemSchemaName() + ".CACHE_GROUPS CG ON C.CACHE_GROUP_ID = CG.CACHE_GROUP_ID WHERE C.CACHE_NAME = 'cache_tx_repl'")
            .get(0).get(0));

        // Check join by non-indexed column.
        assertEquals("cache_grp", execSql("SELECT CG.CACHE_GROUP_NAME FROM " + systemSchemaName() + ".CACHES C JOIN " +
            systemSchemaName() + ".CACHE_GROUPS CG ON C.CACHE_GROUP_NAME = CG.CACHE_GROUP_NAME WHERE C.CACHE_NAME = 'cache_tx_part'")
            .get(0).get(0));

        // Check configuration equality for cache and cache group views.
        assertEquals(5L, execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHES C JOIN " +
            systemSchemaName() + ".CACHE_GROUPS CG " +
            "ON C.CACHE_NAME = CG.CACHE_GROUP_NAME WHERE C.CACHE_NAME like 'cache%' " +
            "AND C.CACHE_MODE = CG.CACHE_MODE " +
            "AND C.ATOMICITY_MODE = CG.ATOMICITY_MODE " +
            "AND COALESCE(C.AFFINITY, '-') = COALESCE(CG.AFFINITY, '-') " +
            "AND COALESCE(C.NODE_FILTER, '-') = COALESCE(CG.NODE_FILTER, '-') " +
            "AND COALESCE(C.DATA_REGION_NAME, '-') = COALESCE(CG.DATA_REGION_NAME, '-') " +
            "AND COALESCE(C.TOPOLOGY_VALIDATOR, '-') = COALESCE(CG.TOPOLOGY_VALIDATOR, '-') " +
            "AND C.PARTITION_LOSS_POLICY = CG.PARTITION_LOSS_POLICY " +
            "AND C.REBALANCE_MODE = CG.REBALANCE_MODE " +
            "AND C.REBALANCE_DELAY = CG.REBALANCE_DELAY " +
            "AND C.REBALANCE_ORDER = CG.REBALANCE_ORDER " +
            "AND COALESCE(C.BACKUPS, -1) = COALESCE(CG.BACKUPS, -1)"
        ).get(0).get(0));

        // Check quick count.
        assertEquals(execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS").get(0).get(0),
            execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS WHERE CACHE_GROUP_ID <> CACHE_GROUP_ID + 1")
                .get(0).get(0));

        // Check that cache groups are the same on different nodes.
        assertEquals(6L, execSql("SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE CACHE_GROUP_NAME like 'cache%'").get(0).get(0));

        assertEquals(6L, execSql(ignite1, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE CACHE_GROUP_NAME like 'cache%'").get(0).get(0));

        assertEquals(6L, execSql(ignite2, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE CACHE_GROUP_NAME like 'cache%'").get(0).get(0));

        assertEquals(6L, execSql(ignite3, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE CACHE_GROUP_NAME like 'cache%'").get(0).get(0));

        assertEquals(5L, execSql(ignite0, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE NODE_FILTER is NULL").get(0).get(0));

        assertEquals(1L, execSql(ignite0, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE NODE_FILTER = 'CUSTOM_NODE_FILTER'").get(0).get(0));

        assertEquals(1L, execSql(ignite0, "SELECT COUNT(*) FROM " + systemSchemaName() + ".CACHE_GROUPS " +
            "WHERE NODE_FILTER like '%Oops... incorrect customer realization.'").get(0).get(0));
    }

    /**
     * Regression test. Verifies that duration metrics is able to be longer than 24 hours.
     */
    @Test
    public void testDurationMetricsCanBeLonger24Hours() throws Exception {
        Ignite ign = startGrid("MockedMetrics", getConfiguration().setMetricsUpdateFrequency(500));

        ClusterNode node = ign.cluster().localNode();

        assert node instanceof TcpDiscoveryNode : "Setup failed, test is incorrect.";

        // Get rid of metrics provider: current logic ignores metrics field if provider != null.
        setField(node, "metricsProvider", null);

        ClusterMetricsImpl original = getField(node, "metrics");

        setField(node, "metrics", new MockedClusterMetrics(original));;

        List<?> durationMetrics = execSql(ign,
            "SELECT " +
                "MAX_JOBS_WAIT_TIME, " +
                "CUR_JOBS_WAIT_TIME, " +
                "AVG_JOBS_WAIT_TIME, " +

                "MAX_JOBS_EXECUTE_TIME, " +
                "CUR_JOBS_EXECUTE_TIME, " +
                "AVG_JOBS_EXECUTE_TIME, " +
                "TOTAL_JOBS_EXECUTE_TIME, " +

                "TOTAL_BUSY_TIME, " +

                "TOTAL_IDLE_TIME, " +
                "CUR_IDLE_TIME, " +
                "UPTIME " +

                "FROM " + systemSchemaName() + ".NODE_METRICS").get(0);

        List<Long> elevenExpVals = LongStream
            .generate(() -> MockedClusterMetrics.LONG_DURATION_MS)
            .limit(11)
            .boxed()
            .collect(Collectors.toList());

        assertEqualsCollections(elevenExpVals, durationMetrics);
    }

    /**
     * Mock for {@link ClusterMetricsImpl} that always returns big (more than 24h) duration for all duration metrics.
     */
    public static class MockedClusterMetrics extends ClusterMetricsImpl {
        /** Some long (> 24h) duration. */
        public static final long LONG_DURATION_MS = TimeUnit.DAYS.toMillis(365);

        /**
         * Constructor.
         *
         * @param original - original cluster metrics object. Required to leave the original behaviour for not overriden
         * methods.
         */
        public MockedClusterMetrics(ClusterMetricsImpl original) throws Exception {
            super(
                getField(original, "ctx"),
                getField(original, "nodeStartTime"));
        }

        /** {@inheritDoc} */
        @Override public long getMaximumJobWaitTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public long getCurrentJobWaitTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public double getAverageJobWaitTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public long getMaximumJobExecuteTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public long getCurrentJobExecuteTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public double getAverageJobExecuteTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public long getTotalJobsExecutionTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public long getTotalBusyTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public long getTotalIdleTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public long getCurrentIdleTime() {
            return LONG_DURATION_MS;
        }

        /** {@inheritDoc} */
        @Override public long getUpTime() {
            return LONG_DURATION_MS;
        }
    }

    /**
     * Get field value using reflection.
     *
     * @param target object containing the field.
     * @param fieldName name of the field.
     */
    private static <T> T getField(Object target, String fieldName) throws Exception {
        Class clazz = target.getClass();

        Field fld = clazz.getDeclaredField(fieldName);

        fld.setAccessible(true);

        return (T) fld.get(target);
    }

    /**
     * Set field using reflection.
     *
     * @param target object containing the field.
     * @param fieldName name of the field.
     * @param val new field value.
     */
    private static void setField(Object target, String fieldName, Object val) throws Exception {
        Class clazz = target.getClass();

        Field fld = clazz.getDeclaredField(fieldName);

        fld.setAccessible(true);

        fld.set(target, val);
    }

    /**
     * Gets ignite configuration with persistence enabled.
     */
    private IgniteConfiguration getPdsConfiguration(String consistentId) throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setMaxSize(100L * 1024L * 1024L).setPersistenceEnabled(true))
        );

        cfg.setConsistentId(consistentId);

        return cfg;
    }

    /**
     *
     */
    private static class TestNodeFilter extends GridNodePredicate {
        /**
         * @param node Node.
         */
        public TestNodeFilter(ClusterNode node) {
            super(node);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestNodeFilter";
        }
    }

    /**
     *
     */
    private static class TestEvictionFilter implements EvictionFilter<Object, Object> {
        /** {@inheritDoc} */
        @Override public boolean evictAllowed(Cache.Entry<Object, Object> entry) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestEvictionFilter";
        }
    }

    /**
     *
     */
    private static class TestEvictionPolicyFactory implements Factory<EvictionPolicy<Object, Object>> {
        /** {@inheritDoc} */
        @Override public EvictionPolicy<Object, Object> create() {
            return new EvictionPolicy<Object, Object>() {
                @Override public void onEntryAccessed(boolean rmv, EvictableEntry<Object, Object> entry) {
                    // No-op.
                }
            };
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestEvictionPolicyFactory";
        }
    }

    /**
     *
     */
    private static class TestTopologyValidator implements TopologyValidator {
        /** {@inheritDoc} */
        @Override public boolean validate(Collection<ClusterNode> nodes) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestTopologyValidator";
        }
    }

    private static class CustomNodeFilter implements IgnitePredicate<ClusterNode> {
        private final int attemptsBeforeException;

        private volatile int attempts;

        public CustomNodeFilter(int attemptsBeforeException) {
            this.attemptsBeforeException = attemptsBeforeException;
        }

        @Override public boolean apply(ClusterNode node) {
            return true;
        }

        @Override public String toString() {
            if (attempts++ > attemptsBeforeException)
                throw new NullPointerException("Oops... incorrect customer realization.");

            return "CUSTOM_NODE_FILTER";
        }
    }
}
