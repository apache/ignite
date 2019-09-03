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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.ignite.internal.processors.metric.list.view.SqlSchemaView;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.sql.SqlViewExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.cache.index.AbstractSchemaSelfTest.queryProcessor;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.t;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

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

        SqlViewExporterSpi sqlSpi = new SqlViewExporterSpi();

        sqlSpi.setMetricExportFilter(mgrp -> !mgrp.name().startsWith(FILTERED_PREFIX));

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
            "SELECT REPLACE(name, 'io.dataregion.default.'), value, description FROM MONITORING.METRICS");

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
            "SELECT name, value, description FROM MONITORING.METRICS WHERE name LIKE 'other.prefix%'");

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

    @Test
    /** */
    public void testCachesList() throws Exception {
        Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

        for (String name : cacheNames)
            ignite.createCache(name);

        List<List<?>> caches = execute(ignite, "SELECT CACHE_NAME FROM MONITORING.CACHES");

        assertEquals(3, caches.size());

        for (List<?> row : caches)
            cacheNames.remove(row.get(0));

        assertTrue(cacheNames.toString(), cacheNames.isEmpty());
    }

    @Test
    /** */
    public void testCacheGroupsList() throws Exception {
        Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

        for (String grpName : grpNames)
            ignite.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

        List<List<?>> grps = execute(ignite, "SELECT CACHE_GROUP_NAME FROM MONITORING.CACHE_GROUPS");

        assertEquals(3, grps.size());

        for (List<?> row : grps)
            grpNames.remove(row.get(0));

        assertTrue(grpNames.toString(), grpNames.isEmpty());
    }

    @Test
    /** */
    public void testComputeBroadcast() throws Exception {
        for (int i = 0; i < 5; i++) {
            ignite.compute().broadcastAsync(() -> {
                try {
                    Thread.sleep(3_000L);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        List<List<?>> tasks = execute(ignite,
            "SELECT " +
            "  INTERNAL, " +
            "  AFFINITY_CACHE_NAME, " +
            "  AFFINITY_PARTITION_ID, " +
            "  TASK_CLASS_NAME, " +
            "  TASK_NAME, " +
            "  TASK_NODE_ID, " +
            "  USER_VERSION " +
            "FROM MONITORING.TASKS");

        assertEquals(5, tasks.size());

        List<?> t = tasks.get(0);

        assertFalse((Boolean)t.get(0));
        assertNull(t.get(1));
        assertEquals(-1, t.get(2));
        assertTrue(t.get(3).toString().startsWith(getClass().getName()));
        assertTrue(t.get(4).toString().startsWith(getClass().getName()));
        assertEquals(ignite.localNode().id().toString(), t.get(5));
        assertEquals("0", t.get(6));
    }

    @Test
    /** */
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
                "  AFFINITY_KEY_VALUE, " +
                "  NODE_FILTER, " +
                "  STATICALLY_CONFIGURED, " +
                "  ORIGIN_NODE_ID " +
                "FROM MONITORING.SERVICES");

        assertEquals(1, srvs.size());

        List<?> sview = srvs.iterator().next();

        assertEquals(srvcCfg.getName(), sview.get(0));
        assertEquals(DummyService.class.getName(), sview.get(2));
        assertEquals(srvcCfg.getMaxPerNodeCount(), sview.get(4));
    }

    @Test
    /** */
    public void testContinuousQuery() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.createCache("cache-1");

        QueryCursor qry = cache.query(new ContinuousQuery<>()
            .setInitialQuery(new ScanQuery<>())
            .setPageSize(100)
            .setTimeInterval(1000)
            .setLocalListener(evts -> {
                // No-op.
            })
            .setRemoteFilterFactory(() -> evt -> true)
        );

        for (int i = 0; i < 100; i++)
            cache.put(i, i);

        List<List<?>> qrys = execute(ignite, "SELECT * FROM MONITORING.QUERY_CONTINUOUS");

        assertEquals(1, qrys.size());
    }

    @Test
    /** */
    public void testClientsConnections() throws Exception {
        String host = ignite.configuration().getClientConnectorConfiguration().getHost();

        if (host == null)
            host = ignite.configuration().getLocalHost();

        int port = ignite.configuration().getClientConnectorConfiguration().getPort();

        try (IgniteClient client =
                 Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {

            try (Connection conn =
                     new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://" + host, new Properties())) {

                List<List<?>> conns = execute(ignite, "SELECT * FROM MONITORING.CLIENT_CONNECTIONS");

                assertEquals(2, conns.size());
            }
        }
    }

    @Test
    /** */
    public void testTransactions() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.createCache(new CacheConfiguration<Integer, Integer>("c")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        CountDownLatch latch = new CountDownLatch(1);

        try {
            AtomicInteger cntr = new AtomicInteger();

            GridTestUtils.runMultiThreadedAsync(() -> {
                try (Transaction tx = ignite.transactions().withLabel("test").txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, 5, "xxx");

            GridTestUtils.runMultiThreadedAsync(() -> {
                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, 5, "yyy");

            List<List<?>> txs = execute(ignite, "SELECT * FROM MONITORING.TRANSACTIONS");

            assertEquals(10, txs.size());
        }
        finally {
            latch.countDown();
        }
    }

    @Test
    /** */
    public void testNodes() throws Exception {
        List<List<?>> nodes = execute(ignite, "SELECT * FROM MONITORING.NODES");

        assertEquals(1, nodes.size());
    }

    @Test
    /** */
    public void testSchemas() throws Exception {
        try (IgniteEx g = startGrid(new IgniteConfiguration().setSqlSchemas("MY_SCHEMA", "ANOTHER_SCHEMA"))) {
            MonitoringList<String, SqlSchemaView> schemasMonList =
                g.context().metric().list(metricName("sql", "schemas"), "SQL schemas", SqlSchemaView.class);

            Set<String> schemaFromMon = new HashSet<>();

            schemasMonList.forEach(v -> schemaFromMon.add(v.name()));

            assertEquals(schemaFromMon, new HashSet<>(Arrays.asList("MY_SCHEMA", "ANOTHER_SCHEMA", "SYS", "PUBLIC")));
        }
    }

    @Test
    /** */
    public void testTables() throws Exception {
        execute(ignite, "CREATE TABLE t1(id LONG PRIMARY KEY, NAME VARCHAR) " +
            "WITH \"CACHE_NAME=c1, CACHE_GROUP=g1, VALUE_TYPE=MyType \"");
        execute(ignite, "CREATE TABLE t2(id LONG PRIMARY KEY, NAME VARCHAR)");
        execute(ignite, "CREATE TABLE t3(id LONG PRIMARY KEY, NAME VARCHAR)");

        List<List<?>> tbls = execute(ignite, "SELECT * FROM MONITORING.SQL_TABLES");

        assertEquals(3, tbls.size());
    }

    @Test
    /** */
    public void testIndexes() throws Exception {
        execute(ignite, "CREATE TABLE t1(id LONG PRIMARY KEY, NAME VARCHAR) WITH \"CACHE_NAME=c1, CACHE_GROUP=g1\"");
        execute(ignite, "CREATE INDEX name_idx ON t1(name);");

        List<List<?>> tbls = execute(ignite, "SELECT * FROM MONITORING.SQL_INDEXES");

        assertEquals(5, tbls.size());
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
