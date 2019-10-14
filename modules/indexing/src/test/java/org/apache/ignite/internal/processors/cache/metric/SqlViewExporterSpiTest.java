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

package org.apache.ignite.internal.processors.cache.metric;

import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.metric.AbstractExporterSpiTest;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.sql.SqlViewMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.spi.systemview.view.SqlSchemaView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.query.QueryUtils.SCHEMA_SYS;
import static org.apache.ignite.internal.processors.query.h2.SchemaManager.SQL_SCHEMA_VIEW;
import static org.apache.ignite.internal.util.lang.GridFunc.t;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/** */
public class SqlViewExporterSpiTest extends AbstractExporterSpiTest {
    /** */
    private static IgniteEx ignite0;

    /** */
    private static IgniteEx ignite1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        SqlViewMetricExporterSpi sqlSpi = new SqlViewMetricExporterSpi();

        if (igniteInstanceName.endsWith("1"))
            sqlSpi.setExportFilter(mgrp -> !mgrp.name().startsWith(FILTERED_PREFIX));

        cfg.setMetricExporterSpi(sqlSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        ignite0 = startGrid(0);
        ignite1 = startGrid(1);

        ignite0.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Collection<String> caches = ignite0.cacheNames();

        for (String cache : caches)
            ignite0.destroyCache(cache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testEmptyFilter() throws Exception {
        List<List<?>> res = execute(ignite0, "SELECT * FROM SYS.METRICS");

        assertNotNull(res);
        assertFalse(res.isEmpty());
    }

    /** */
    @Test
    public void testDataRegionMetrics() throws Exception {
        List<List<?>> res = execute(ignite0,
            "SELECT REPLACE(name, 'io.dataregion.default.'), value, description FROM SYS.METRICS");

        Set<String> names = new HashSet<>();

        for (List<?> row : res) {
            names.add((String)row.get(0));

            assertNotNull(row.get(1));
        }

        for (String attr : EXPECTED_ATTRIBUTES)
            assertTrue(attr + " should be exporterd via SQL view", names.contains(attr));
    }

    /** */
    @Test
    public void testFilterAndExport() throws Exception {
        createAdditionalMetrics(ignite1);

        List<List<?>> res = execute(ignite1,
            "SELECT name, value, description FROM SYS.METRICS " +
                "WHERE name LIKE 'other.prefix%' OR name LIKE '" + FILTERED_PREFIX + "%'");

        Set<IgniteBiTuple<String, String>> expVals = new HashSet<>(asList(
            t("other.prefix.test", "42"),
            t("other.prefix.test2", "43"),
            t("other.prefix2.test3", "44")
        ));

        Set<IgniteBiTuple<String, String>> vals = new HashSet<>();

        for (List<?> row : res)
            vals.add(t((String)row.get(0), (String)row.get(1)));

        assertEquals(expVals, vals);
    }

    /** */
    @Test
    public void testCachesView() throws Exception {
        Set<String> cacheNames = new HashSet<>(asList("cache-1", "cache-2"));

        for (String name : cacheNames)
            ignite0.createCache(name);

        List<List<?>> caches = execute(ignite0, "SELECT CACHE_NAME FROM SYS.CACHES");

        assertEquals(ignite0.context().cache().cacheDescriptors().size(), caches.size());

        for (List<?> row : caches)
            cacheNames.remove(row.get(0));

        assertTrue(cacheNames.toString(), cacheNames.isEmpty());
    }

    /** */
    @Test
    public void testCacheGroupsView() throws Exception {
        Set<String> grpNames = new HashSet<>(asList("grp-1", "grp-2"));

        for (String grpName : grpNames)
            ignite0.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

        List<List<?>> grps = execute(ignite0, "SELECT CACHE_GROUP_NAME FROM SYS.CACHE_GROUPS");

        assertEquals(ignite0.context().cache().cacheGroupDescriptors().size(), grps.size());

        for (List<?> row : grps)
            grpNames.remove(row.get(0));

        assertTrue(grpNames.toString(), grpNames.isEmpty());
    }

    /** */
    @Test
    public void testComputeBroadcast() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(6);

        for (int i = 0; i < 5; i++) {
            ignite0.compute().broadcastAsync(() -> {
                try {
                    barrier.await();
                    barrier.await();
                }
                catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        barrier.await();

        List<List<?>> tasks = execute(ignite0,
            "SELECT " +
            "  INTERNAL, " +
            "  AFFINITY_CACHE_NAME, " +
            "  AFFINITY_PARTITION_ID, " +
            "  TASK_CLASS_NAME, " +
            "  TASK_NAME, " +
            "  TASK_NODE_ID, " +
            "  USER_VERSION " +
            "FROM SYS.TASKS");

        assertEquals(5, tasks.size());

        List<?> t = tasks.get(0);

        assertFalse((Boolean)t.get(0));
        assertNull(t.get(1));
        assertEquals(-1, t.get(2));
        assertTrue(t.get(3).toString().startsWith(getClass().getName()));
        assertTrue(t.get(4).toString().startsWith(getClass().getName()));
        assertEquals(ignite0.localNode().id(), t.get(5));
        assertEquals("0", t.get(6));

        barrier.await();
    }

    /** */
    @Test
    public void testServices() throws Exception {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName("service");
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setService(new DummyService());

        ignite0.services().deploy(srvcCfg);

        List<List<?>> srvs = execute(ignite0,
            "SELECT " +
                "  NAME, " +
                "  SERVICE_ID, " +
                "  SERVICE_CLASS, " +
                "  TOTAL_COUNT, " +
                "  MAX_PER_NODE_COUNT, " +
                "  CACHE_NAME, " +
                "  AFFINITY_KEY, " +
                "  NODE_FILTER, " +
                "  STATICALLY_CONFIGURED, " +
                "  ORIGIN_NODE_ID " +
                "FROM SYS.SERVICES");

        assertEquals(ignite0.context().service().serviceDescriptors().size(), srvs.size());

        List<?> sysView = srvs.iterator().next();

        assertEquals(srvcCfg.getName(), sysView.get(0));
        assertEquals(DummyService.class.getName(), sysView.get(2));
        assertEquals(srvcCfg.getMaxPerNodeCount(), sysView.get(4));
    }

    /** */
    @Test
    public void testClientsConnections() throws Exception {
        String host = ignite0.configuration().getClientConnectorConfiguration().getHost();

        if (host == null)
            host = ignite0.configuration().getLocalHost();

        int port = ignite0.configuration().getClientConnectorConfiguration().getPort();

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {
            try (Connection conn = new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://" + host, new Properties())) {
                List<List<?>> conns = execute(ignite0, "SELECT * FROM SYS.CLIENT_CONNECTIONS");

                assertEquals(2, conns.size());
            }
        }
    }

    /** */
    @Test
    public void testTransactions() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite0.createCache(new CacheConfiguration<Integer, Integer>("c")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        assertTrue(execute(ignite0, "SELECT * FROM SYS.TRANSACTIONS").isEmpty());

        CountDownLatch latch1 = new CountDownLatch(10);
        CountDownLatch latch2 = new CountDownLatch(1);

        AtomicInteger cntr = new AtomicInteger();

        GridTestUtils.runMultiThreadedAsync(() -> {
            try (Transaction tx = ignite0.transactions().withLabel("test").txStart(PESSIMISTIC, REPEATABLE_READ)) {
                cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                latch1.countDown();
                latch2.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 5, "xxx");

        GridTestUtils.runMultiThreadedAsync(() -> {
            try (Transaction tx = ignite0.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                latch1.countDown();
                latch2.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }, 5, "yyy");

        latch1.await(5, TimeUnit.SECONDS);

        List<List<?>> txs = execute(ignite0, "SELECT * FROM SYS.TRANSACTIONS");

        assertEquals(10, txs.size());

        latch2.countDown();

        boolean res = waitForCondition(() -> execute(ignite0, "SELECT * FROM SYS.TRANSACTIONS").isEmpty(), 5_000);

        assertTrue(res);
    }

    /** */
    @Test
    public void testSchemas() throws Exception {
        try (IgniteEx g = startGrid(new IgniteConfiguration().setSqlSchemas("MY_SCHEMA", "ANOTHER_SCHEMA"))) {
            SystemView<SqlSchemaView> schemasSysView = g.context().systemView().view(SQL_SCHEMA_VIEW);

            Set<String> schemaFromSysView = new HashSet<>();

            schemasSysView.forEach(v -> schemaFromSysView.add(v.name()));

            HashSet<String> expSchemas = new HashSet<>(asList("MY_SCHEMA", "ANOTHER_SCHEMA", "SYS", "PUBLIC"));

            assertEquals(schemaFromSysView, expSchemas);

            List<List<?>> schemas = execute(g, "SELECT * FROM SYS.SCHEMAS");

            schemaFromSysView.clear();
            schemas.forEach(s -> schemaFromSysView.add(s.get(0).toString()));

            assertEquals(schemaFromSysView, expSchemas);
        }
    }

    /** */
    @Test
    public void testViews() throws Exception {
        Set<String> expViews = new HashSet<>(asList(
            "METRICS",
            "SERVICES",
            "CACHE_GROUPS",
            "CACHES",
            "TASKS",
            "LOCAL_SQL_QUERY_HISTORY",
            "NODES",
            "SCHEMAS",
            "NODE_METRICS",
            "BASELINE_NODES",
            "INDEXES",
            "LOCAL_CACHE_GROUPS_IO",
            "LOCAL_SQL_RUNNING_QUERIES",
            "NODE_ATTRIBUTES",
            "TABLES",
            "CLIENT_CONNECTIONS",
            "VIEWS",
            "TABLE_COLUMNS",
            "VIEW_COLUMNS",
            "TRANSACTIONS",
            "QUERY_CONTINUOUS"
        ));

        Set<String> actViews = new HashSet<>();

        List<List<?>> res = execute(ignite0, "SELECT * FROM SYS.VIEWS");

        for (List<?> row : res)
            actViews.add(row.get(0).toString());

        assertEquals(expViews, actViews);
    }

    /** */
    @Test
    public void testTable() throws Exception {
        assertTrue(execute(ignite0, "SELECT * FROM SYS.TABLES").isEmpty());

        execute(ignite0, "CREATE TABLE T1(ID LONG PRIMARY KEY, NAME VARCHAR)");

        List<List<?>> res = execute(ignite0, "SELECT * FROM SYS.TABLES");

        assertEquals(1, res.size());

        List tbl = res.get(0);

        int cacheId = cacheId("SQL_PUBLIC_T1");
        String cacheName = "SQL_PUBLIC_T1";

        assertEquals("T1", tbl.get(0)); // TABLE_NAME
        assertEquals(DFLT_SCHEMA, tbl.get(1)); // SCHEMA_NAME
        assertEquals(cacheName, tbl.get(2)); // CACHE_NAME
        assertEquals(cacheId, tbl.get(3)); // CACHE_ID
        assertNull(tbl.get(4)); // AFFINITY_KEY_COLUMN
        assertEquals("ID", tbl.get(5)); // KEY_ALIAS
        assertNull(tbl.get(6)); // VALUE_ALIAS
        assertEquals("java.lang.Long", tbl.get(7)); // KEY_TYPE_NAME
        assertNotNull(tbl.get(8)); // VALUE_TYPE_NAME

        execute(ignite0, "CREATE TABLE T2(ID LONG PRIMARY KEY, NAME VARCHAR)");

        assertEquals(2, execute(ignite0, "SELECT * FROM SYS.TABLES").size());

        execute(ignite0, "DROP TABLE T1");
        execute(ignite0, "DROP TABLE T2");

        assertTrue(execute(ignite0, "SELECT * FROM SYS.TABLES").isEmpty());
    }

    /** */
    @Test
    public void testTableColumns() throws Exception {
        assertTrue(execute(ignite0, "SELECT * FROM SYS.TABLE_COLUMNS").isEmpty());

        execute(ignite0, "CREATE TABLE T1(ID LONG PRIMARY KEY, NAME VARCHAR(40))");

        Set<?> actCols = execute(ignite0, "SELECT * FROM SYS.TABLE_COLUMNS")
            .stream()
            .map(l -> l.get(0))
            .collect(Collectors.toSet());

        assertEquals(new HashSet<>(asList("ID", "NAME", "_KEY", "_VAL")), actCols);

        execute(ignite0, "CREATE TABLE T2(ID LONG PRIMARY KEY, NAME VARCHAR(50))");

        List<List<?>> expRes = asList(
            asList("ID", "T1", "PUBLIC", false, false, "null", true, true, -1, -1, Long.class.getName()),
            asList("NAME", "T1", "PUBLIC", false, false, "null", true, false, 40, -1, String.class.getName()),
            asList("_KEY", "T1", "PUBLIC", true, false, null, false, true, -1, -1, null),
            asList("_VAL", "T1", "PUBLIC", false, false, null, true, false, -1, -1, null),
            asList("ID", "T2", "PUBLIC", false, false, "null", true, true, -1, -1, Long.class.getName()),
            asList("NAME", "T2", "PUBLIC", false, false, "null", true, false, 50, -1, String.class.getName()),
            asList("_KEY", "T2", "PUBLIC", true, false, null, false, true, -1, -1, null),
            asList("_VAL", "T2", "PUBLIC", false, false, null, true, false, -1, -1, null)
        );

        List<List<?>> res = execute(ignite0, "SELECT * FROM SYS.TABLE_COLUMNS ORDER BY TABLE_NAME, COLUMN_NAME");

        assertEquals(expRes, res);

        execute(ignite0, "DROP TABLE T1");
        execute(ignite0, "DROP TABLE T2");

        assertTrue(execute(ignite0, "SELECT * FROM SYS.TABLE_COLUMNS").isEmpty());
    }

    /** */
    @Test
    public void testViewColumns() throws Exception {
        execute(ignite0, "SELECT * FROM SYS.VIEW_COLUMNS");

        List<List<?>> expRes = asList(
            asList("CONNECTION_ID", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", true, 19L, 0, Long.class.getName()),
            asList("LOCAL_ADDRESS", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", true, (long)Integer.MAX_VALUE, 0,
                String.class.getName()),
            asList("REMOTE_ADDRESS", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", true, (long)Integer.MAX_VALUE, 0,
                String.class.getName()),
            asList("TYPE", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", true, (long)Integer.MAX_VALUE, 0,
                String.class.getName()),
            asList("USER", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", true, (long)Integer.MAX_VALUE, 0,
                String.class.getName()),
            asList("VERSION", "CLIENT_CONNECTIONS", SCHEMA_SYS, "null", true, (long)Integer.MAX_VALUE, 0,
                String.class.getName())
        );

        List<List<?>> res = execute(ignite0, "SELECT * FROM SYS.VIEW_COLUMNS WHERE VIEW_NAME = 'CLIENT_CONNECTIONS'");

        assertEquals(expRes, res);
    }

    /** */
    @Test
    public void testContinuousQuery() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite0.createCache("cache-1");

        assertTrue(execute(ignite0, "SELECT * FROM SYS.QUERY_CONTINUOUS").isEmpty());
        assertTrue(execute(ignite1, "SELECT * FROM SYS.QUERY_CONTINUOUS").isEmpty());

        try (QueryCursor qry = cache.query(new ContinuousQuery<>()
            .setInitialQuery(new ScanQuery<>())
            .setPageSize(100)
            .setTimeInterval(1000)
            .setLocalListener(evts -> {
                // No-op.
            })
            .setRemoteFilterFactory(() -> evt -> true)
        )) {
            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            checkContinuouQueryView(ignite0, true);
            checkContinuouQueryView(ignite1, false);
        }

        assertTrue(execute(ignite0, "SELECT * FROM SYS.QUERY_CONTINUOUS").isEmpty());
        assertTrue(execute(ignite1, "SELECT * FROM SYS.QUERY_CONTINUOUS").isEmpty());
    }

    /** */
    private void checkContinuouQueryView(IgniteEx g, boolean loc) {
        List<List<?>> qrys = execute(g,
            "SELECT " +
            "  CACHE_NAME, " +
            "  BUFFER_SIZE, " +
            "  INTERVAL, " +
            "  NODE_ID, " +
            "  LOCAL_LISTENER, " +
            "  REMOTE_FILTER, " +
            "  LOCAL_TRANSFORMED_LISTENER, " +
            "  REMOTE_TRANSFORMER " +
            "FROM SYS.QUERY_CONTINUOUS");

        assertEquals(1, qrys.size());

        List<?> cq = qrys.iterator().next();

        assertEquals("cache-1", cq.get(0));
        assertEquals(100, cq.get(1));
        assertEquals(1000L, cq.get(2));
        assertEquals(ignite0.localNode().id(), cq.get(3));

        if (loc)
            assertTrue(cq.get(4).toString().startsWith(getClass().getName()));
        else
            assertNull(cq.get(4));

        assertTrue(cq.get(5).toString().startsWith(getClass().getName()));
        assertNull(cq.get(6));
        assertNull(cq.get(7));
    }

    /**
     * Execute query on given node.
     *
     * @param node Node.
     * @param sql Statement.
     */
    private List<List<?>> execute(Ignite node, String sql, Object... args) {
        SqlFieldsQuery qry = new SqlFieldsQuery(sql)
            .setArgs(args)
            .setSchema("PUBLIC");

        return queryProcessor(node).querySqlFields(qry, true).getAll();
    }
}
