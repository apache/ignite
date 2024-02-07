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

package org.apache.ignite.internal.processors.performancestatistics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.Config;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryType;
import org.apache.ignite.internal.processors.cache.query.IndexQueryDesc;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.gt;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.INDEX;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SCAN;
import static org.apache.ignite.internal.processors.cache.query.GridCacheQueryType.SQL_FIELDS;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.CLIENT;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.SERVER;
import static org.apache.ignite.internal.processors.performancestatistics.AbstractPerformanceStatisticsTest.ClientType.THIN_CLIENT;
import static org.apache.ignite.internal.processors.performancestatistics.PerformanceStatisticsProcessor.indexQueryText;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.junit.Assume.assumeFalse;

/** Tests query performance statistics. */
@RunWith(Parameterized.class)
public class PerformanceStatisticsQueryTest extends AbstractPerformanceStatisticsTest {
    /** Cache entry count. */
    private static final int ENTRY_COUNT = 100;

    /** Test cache 2 name. */
    private static final String CACHE_2 = "cache2";

    /** Test SQL table name. */
    private static final String SQL_TABLE = "test";

    /** Page size. */
    @Parameterized.Parameter
    public int pageSize;

    /** Client type to run queries from. */
    @Parameterized.Parameter(1)
    public ClientType clientType;

    /** @return Test parameters. */
    @Parameterized.Parameters(name = "pageSize={0}, clientType={1}")
    public static Collection<?> parameters() {
        List<Object[]> res = new ArrayList<>();

        for (Integer pageSize : new Integer[] {ENTRY_COUNT, ENTRY_COUNT / 10}) {
            for (ClientType clientType : new ClientType[] {SERVER, CLIENT, THIN_CLIENT})
                res.add(new Object[] {pageSize, clientType});
        }

        return res;
    }

    /** Server. */
    private static IgniteEx srv;

    /** Client. */
    private static IgniteEx client;

    /** Thin client. */
    private static IgniteClient thinClient;

    /** Cache. */
    private static IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();
        cleanPersistenceDir();

        srv = startGrids(2);

        thinClient = Ignition.startClient(new ClientConfiguration().setAddresses(Config.SERVER));

        client = startClientGrid("client");

        client.cluster().state(ACTIVE);

        cache = client.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(DEFAULT_CACHE_NAME)
            .setSqlSchema(DFLT_SCHEMA)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Integer.class, Integer.class)
                    .setTableName(DEFAULT_CACHE_NAME)))
        );

        IgniteCache<Object, Object> cache2 = client.getOrCreateCache(new CacheConfiguration<>()
            .setName(CACHE_2)
            .setSqlSchema(DFLT_SCHEMA)
            .setQueryEntities(Collections.singletonList(
                new QueryEntity(Long.class, Long.class)
                    .setTableName(CACHE_2)))
        );

        for (int i = 0; i < ENTRY_COUNT; i++) {
            cache.put(i, i);
            cache2.put((long)i, (long)i * 2);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        cleanPersistenceDir();

        thinClient.close();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        if (cache != null)
            cache.query(new SqlFieldsQuery("drop table if exists " + SQL_TABLE));
    }

    /** @throws Exception If failed. */
    @Test
    public void testScanQuery() throws Exception {
        ScanQuery<Object, Object> qry = new ScanQuery<>().setPageSize(pageSize);

        checkQuery(SCAN, qry, DEFAULT_CACHE_NAME, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testIndexQuery() throws Exception {
        IndexQuery<Integer, Integer> qry = new IndexQuery<>(Integer.class);

        qry.setPageSize(pageSize);
        qry.setCriteria(gt("_KEY", 0));

        String expText = indexQueryText(DEFAULT_CACHE_NAME,
            new IndexQueryDesc(qry.getCriteria(), qry.getIndexName(), qry.getValueType()));

        checkQuery(INDEX, qry, expText, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlFieldsQuery() throws Exception {
        String sql = "select * from " + DEFAULT_CACHE_NAME;

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setPageSize(pageSize);

        checkQuery(SQL_FIELDS, qry, sql, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlFieldsJoinQuery() throws Exception {
        String sql = "select * from " + DEFAULT_CACHE_NAME + " a inner join " + CACHE_2 + " b on a._key = b._key";

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setPageSize(pageSize);

        checkQuery(SQL_FIELDS, qry, sql, false);
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlFieldsQueryWithReducer() throws Exception {
        String sql = "select sum(_key) from " + DEFAULT_CACHE_NAME;

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setPageSize(pageSize);

        checkQuery(SQL_FIELDS, qry, sql, true);
    }

    /** @throws Exception If failed. */
    @Test
    public void testSqlFieldsLocalQuery() throws Exception {
        Assume.assumeTrue(clientType == SERVER);

        String sql = "select * from " + DEFAULT_CACHE_NAME;

        SqlFieldsQuery qry = new SqlFieldsQuery(sql).setPageSize(pageSize).setLocal(true);

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        srv.cache(DEFAULT_CACHE_NAME).query(qry).getAll();

        AtomicReference<String> flags = new AtomicReference<>();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void queryProperty(
                UUID nodeId,
                GridCacheQueryType type,
                UUID qryNodeId,
                long id,
                String name,
                String val
            ) {
                if ("Flags".equals(name))
                    assertTrue(flags.compareAndSet(null, val));
            }
        });

        assertEquals("local", flags.get());
    }

    /** Check query. */
    private void checkQuery(GridCacheQueryType type, Query<?> qry, String text, boolean hasReducer) throws Exception {
        client.cluster().state(INACTIVE);
        client.cluster().state(ACTIVE);

        runQueryAndCheck(type, qry, text, true, true, hasReducer);

        runQueryAndCheck(type, qry, text, true, false, hasReducer);
    }

    /** @throws Exception If failed. */
    @Test
    public void testDdlAndDmlQueries() throws Exception {
        String sql = "create table " + SQL_TABLE + " (id int, val varchar, primary key (id))";

        runQueryAndCheck(SQL_FIELDS, new SqlFieldsQuery(sql), sql, false, false, false);

        sql = "insert into " + SQL_TABLE + " (id) values (1)";

        runQueryAndCheck(SQL_FIELDS, new SqlFieldsQuery(sql), sql, false, false, false);

        sql = "update " + SQL_TABLE + " set val = 'abc'";

        runQueryAndCheck(SQL_FIELDS, new SqlFieldsQuery(sql), sql, true, false, false);
    }

    /** Runs query and checks statistics. */
    private void runQueryAndCheck(
        GridCacheQueryType expType,
        Query<?> qry,
        String expText,
        boolean hasLogicalReads,
        boolean hasPhysicalReads,
        boolean hasReducer
    ) throws Exception {
        long startTime = U.currentTimeMillis();

        cleanPerformanceStatisticsDir();

        startCollectStatistics();

        Collection<UUID> expNodeIds = new ArrayList<>();

        if (clientType == SERVER) {
            srv.cache(DEFAULT_CACHE_NAME).query(qry).getAll();

            expNodeIds.add(srv.localNode().id());
        }
        else if (clientType == CLIENT) {
            client.cache(DEFAULT_CACHE_NAME).query(qry).getAll();

            expNodeIds.add(client.localNode().id());
        }
        else if (clientType == THIN_CLIENT) {
            thinClient.cache(DEFAULT_CACHE_NAME).query(qry).getAll();

            expNodeIds.addAll(F.nodeIds(client.cluster().forServers().nodes()));
        }

        Set<UUID> readsNodes = new HashSet<>();

        if (hasLogicalReads)
            srv.cluster().forServers().nodes().forEach(node -> readsNodes.add(node.id()));

        Set<UUID> dataNodes = new HashSet<>(readsNodes);
        AtomicInteger qryCnt = new AtomicInteger();
        AtomicInteger readsCnt = new AtomicInteger();
        HashSet<Long> qryIds = new HashSet<>();
        AtomicLong mapRowCnt = new AtomicLong();
        AtomicLong rdcRowCnt = new AtomicLong();
        AtomicInteger planMapCnt = new AtomicInteger();
        AtomicInteger planRdcCnt = new AtomicInteger();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long queryStartTime,
                long duration, boolean success) {
                qryCnt.incrementAndGet();
                qryIds.add(id);

                assertTrue(expNodeIds.contains(nodeId));
                assertEquals(expType, type);
                assertEquals(expText, text);
                assertTrue(queryStartTime >= startTime);
                assertTrue(duration >= 0);
                assertTrue(success);
            }

            @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
                long logicalReads, long physicalReads) {
                readsCnt.incrementAndGet();
                qryIds.add(id);
                readsNodes.remove(nodeId);

                assertTrue(expNodeIds.contains(queryNodeId));
                assertEquals(expType, type);
                assertTrue(logicalReads > 0);
                assertTrue(hasPhysicalReads ? physicalReads > 0 : physicalReads == 0);
            }

            @Override public void queryRows(
                UUID nodeId,
                GridCacheQueryType type,
                UUID qryNodeId,
                long id,
                String action,
                long rows
            ) {
                assertEquals(expType, SQL_FIELDS);
                assertTrue(expNodeIds.contains(qryNodeId));

                if ("Fetched on mapper".equals(action)) {
                    assertTrue(dataNodes.contains(nodeId));
                    mapRowCnt.addAndGet(rows);
                }
                else if ("Fetched on reducer".equals(action)) {
                    assertTrue(expNodeIds.contains(nodeId));
                    rdcRowCnt.addAndGet(rows);
                }
            }

            @Override public void queryProperty(
                UUID nodeId,
                GridCacheQueryType type,
                UUID qryNodeId,
                long id,
                String name,
                String val
            ) {
                assertEquals(expType, SQL_FIELDS);
                assertTrue(expNodeIds.contains(qryNodeId));

                if ("Map phase plan".equals(name)) {
                    assertTrue(dataNodes.contains(nodeId));
                    planMapCnt.incrementAndGet();
                }
                else if ("Reduce phase plan".equals(name)) {
                    assertTrue(expNodeIds.contains(nodeId));
                    planRdcCnt.incrementAndGet();
                }
            }
        });

        assertEquals(1, qryCnt.get());
        assertTrue("Query reads expected on nodes: " + readsNodes, readsNodes.isEmpty());
        assertEquals(1, qryIds.size());

        // If query has logical reads, plan and rows info also expected.
        if (hasLogicalReads && expType == SQL_FIELDS) {
            assertEquals(dataNodes.size(), planMapCnt.get());
            assertTrue(mapRowCnt.get() > 0);
            if (hasReducer) {
                assertTrue(rdcRowCnt.get() > 0);
                assertEquals(1, planRdcCnt.get());
            }
            else {
                assertEquals(0, rdcRowCnt.get());
                assertEquals(0, planRdcCnt.get());
            }
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testMultipleStatementsSql() throws Exception {
        assumeFalse("Multiple statements queries are not supported by thin client.",
            clientType == THIN_CLIENT);

        long startTime = U.currentTimeMillis();

        LinkedList<String> expQrs = new LinkedList<>();

        expQrs.add("create table " + SQL_TABLE + " (id int primary key, val varchar)");
        expQrs.add("insert into " + SQL_TABLE + " (id, val) values (1, 'a')");
        expQrs.add("insert into " + SQL_TABLE + " (id, val) values (2, 'b'), (3, 'c')");

        LinkedList<String> qrsWithReads = new LinkedList<>();

        qrsWithReads.add("update " + SQL_TABLE + " set val = 'd' where id = 1");
        qrsWithReads.add("select * from " + SQL_TABLE);

        expQrs.addAll(qrsWithReads);

        startCollectStatistics();

        SqlFieldsQuery qry = new SqlFieldsQuery(F.concat(expQrs, ";"));

        IgniteEx loadNode = this.clientType == SERVER ? srv : client;

        List<FieldsQueryCursor<List<?>>> res = loadNode.context().query().querySqlFields(qry, true, false);

        assertEquals("Unexpected cursors count: " + res.size(), expQrs.size(), res.size());

        res.get(4).getAll();

        HashSet<Long> qryIds = new HashSet<>();

        stopCollectStatisticsAndRead(new TestHandler() {
            @Override public void query(UUID nodeId, GridCacheQueryType type, String text, long id, long queryStartTime,
                long duration, boolean success) {
                if (qrsWithReads.contains(text))
                    qryIds.add(id);

                assertEquals(loadNode.localNode().id(), nodeId);
                assertEquals(SQL_FIELDS, type);
                assertTrue("Unexpected query: " + text, expQrs.remove(text));
                assertTrue(queryStartTime >= startTime);
                assertTrue(duration >= 0);
                assertTrue(success);
            }

            @Override public void queryReads(UUID nodeId, GridCacheQueryType type, UUID queryNodeId, long id,
                long logicalReads, long physicalReads) {
                qryIds.add(id);

                assertEquals(SQL_FIELDS, type);
                assertEquals(loadNode.localNode().id(), queryNodeId);
                assertTrue(logicalReads > 0);
                assertEquals(0, physicalReads);
            }
        });

        assertTrue("Queries was not handled: " + expQrs, expQrs.isEmpty());
        assertEquals("Unexpected IDs: " + qryIds, qrsWithReads.size(), qryIds.size());
    }
}
