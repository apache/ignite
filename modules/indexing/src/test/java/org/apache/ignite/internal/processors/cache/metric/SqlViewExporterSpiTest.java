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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.Ignition;
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
import org.apache.ignite.spi.systemview.view.SqlSchemaView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA;
import static org.apache.ignite.internal.processors.query.h2.SchemaManager.SQL_SCHEMA_VIEW;
import static org.apache.ignite.internal.util.lang.GridFunc.t;

/** */
public class SqlViewExporterSpiTest extends AbstractExporterSpiTest {
    /** */
    private static IgniteEx ignite;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        SqlViewMetricExporterSpi sqlSpi = new SqlViewMetricExporterSpi();

        sqlSpi.setExportFilter(mgrp -> !mgrp.name().startsWith(FILTERED_PREFIX));

        cfg.setMetricExporterSpi(sqlSpi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        cleanPersistenceDir();

        ignite = startGrid(0);

        ignite.cluster().active(true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        Collection<String> caches = ignite.cacheNames();

        for (String cache : caches)
            ignite.destroyCache(cache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testDataRegionJmxMetrics() throws Exception {
        List<List<?>> res = execute(ignite,
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
        createAdditionalMetrics(ignite);

        List<List<?>> res = execute(ignite,
            "SELECT name, value, description FROM SYS.METRICS WHERE name LIKE 'other.prefix%'");

        Set<IgniteBiTuple<String, String>> expVals = new HashSet<>(Arrays.asList(
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
        Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

        for (String name : cacheNames)
            ignite.createCache(name);

        List<List<?>> caches = execute(ignite, "SELECT CACHE_NAME FROM SYS.CACHES");

        assertEquals(ignite.context().cache().cacheDescriptors().size(), caches.size());

        for (List<?> row : caches)
            cacheNames.remove(row.get(0));

        assertTrue(cacheNames.toString(), cacheNames.isEmpty());
    }

    /** */
    @Test
    public void testCacheGroupsView() throws Exception {
        Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

        for (String grpName : grpNames)
            ignite.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

        List<List<?>> grps = execute(ignite, "SELECT CACHE_GROUP_NAME FROM SYS.CACHE_GROUPS");

        assertEquals(ignite.context().cache().cacheGroupDescriptors().size(), grps.size());

        for (List<?> row : grps)
            grpNames.remove(row.get(0));

        assertTrue(grpNames.toString(), grpNames.isEmpty());
    }

    /** */
    @Test
    public void testComputeBroadcast() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(6);

        for (int i = 0; i < 5; i++) {
            ignite.compute().broadcastAsync(() -> {
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

        List<List<?>> tasks = execute(ignite,
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
        assertEquals(ignite.localNode().id(), t.get(5));
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

        ignite.services().deploy(srvcCfg);

        List<List<?>> srvs = execute(ignite,
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

        assertEquals(ignite.context().service().serviceDescriptors().size(), srvs.size());

        List<?> sysView = srvs.iterator().next();

        assertEquals(srvcCfg.getName(), sysView.get(0));
        assertEquals(DummyService.class.getName(), sysView.get(2));
        assertEquals(srvcCfg.getMaxPerNodeCount(), sysView.get(4));
    }

    /** */
    @Test
    public void testClientsConnections() throws Exception {
        String host = ignite.configuration().getClientConnectorConfiguration().getHost();

        if (host == null)
            host = ignite.configuration().getLocalHost();

        int port = ignite.configuration().getClientConnectorConfiguration().getPort();

        try (IgniteClient client = Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {
            try (Connection conn = new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://" + host, new Properties())) {
                List<List<?>> conns = execute(ignite, "SELECT * FROM SYS.CLIENT_CONNECTIONS");

                assertEquals(2, conns.size());
            }
        }
    }

    /** */
    @Test
    public void testSchemas() throws Exception {
        try (IgniteEx g = startGrid(new IgniteConfiguration().setSqlSchemas("MY_SCHEMA", "ANOTHER_SCHEMA"))) {
            SystemView<SqlSchemaView> schemasSysView = g.context().systemView().view(SQL_SCHEMA_VIEW);

            Set<String> schemaFromSysView = new HashSet<>();

            schemasSysView.forEach(v -> schemaFromSysView.add(v.name()));

            HashSet<String> expSchemas = new HashSet<>(Arrays.asList("MY_SCHEMA", "ANOTHER_SCHEMA", "SYS", "PUBLIC"));

            assertEquals(schemaFromSysView, expSchemas);

            List<List<?>> schemas = execute(g, "SELECT * FROM SYS.SCHEMAS");

            schemaFromSysView.clear();
            schemas.forEach(s -> schemaFromSysView.add(s.get(0).toString()));

            assertEquals(schemaFromSysView, expSchemas);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testViews() throws Exception {
        Set<String> expViews = new HashSet<>(Arrays.asList(
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
            "VIEWS"
        ));

        Set<String> actViews = new HashSet<>();

        List<List<?>> res = execute(ignite, "SELECT * FROM SYS.VIEWS");

        for (List<?> row : res)
            actViews.add(row.get(0).toString());

        assertEquals(expViews, actViews);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTable() throws Exception {
        assertTrue(execute(ignite, "SELECT * FROM SYS.TABLES").isEmpty());

        execute(ignite, "CREATE TABLE T1(ID LONG PRIMARY KEY, NAME VARCHAR)");

        List<List<?>> res = execute(ignite, "SELECT * FROM SYS.TABLES");

        assertEquals(1, res.size());

        List tbl = res.get(0);

        int cacheId = cacheId("SQL_PUBLIC_T1");
        String cacheName = "SQL_PUBLIC_T1";

        assertEquals("T1", tbl.get(0)); //TABLE_NAME
        assertEquals(DFLT_SCHEMA, tbl.get(1)); //SCHEMA_NAME
        assertEquals(cacheName, tbl.get(2)); //CACHE_NAME
        assertEquals(cacheId, tbl.get(3)); //CACHE_GROUP_ID
        assertEquals(cacheName, tbl.get(4)); //CACHE_GROUP_NAME
        assertEquals(cacheId, tbl.get(5)); //CACHE_ID
        assertNull(tbl.get(6)); //AFFINITY_KEY_COLUMN
        assertEquals("ID", tbl.get(7)); //KEY_ALIAS
        assertNull(tbl.get(8)); //VALUE_ALIAS
        assertEquals("java.lang.Long", tbl.get(9)); //KEY_TYPE_NAME
        assertNotNull(tbl.get(10)); //VALUE_TYPE_NAME

        execute(ignite, "CREATE TABLE T2(ID LONG PRIMARY KEY, NAME VARCHAR)");

        assertEquals(2, execute(ignite, "SELECT * FROM SYS.TABLES").size());

        execute(ignite, "DROP TABLE T1");
        execute(ignite, "DROP TABLE T2");

        assertTrue(execute(ignite, "SELECT * FROM SYS.TABLES").isEmpty());
    }

    //TODO: add sql.tables, sql.column, sql.indexes tests.

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
