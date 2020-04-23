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

package org.apache.ignite.internal.metric;

import java.sql.Connection;
import java.text.DateFormat;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanFeatureInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanParameterInfo;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularDataSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.ProtocolVersion;
import org.apache.ignite.internal.managers.systemview.walker.CachePagesListViewWalker;
import org.apache.ignite.internal.metric.SystemViewSelfTest.TestPredicate;
import org.apache.ignite.internal.metric.SystemViewSelfTest.TestRunnable;
import org.apache.ignite.internal.metric.SystemViewSelfTest.TestTransformer;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetricImpl;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.managers.systemview.GridSystemViewManager.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.managers.systemview.GridSystemViewManager.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.managers.systemview.ScanQuerySystemView.SCAN_QRY_SYS_VIEW;
import static org.apache.ignite.internal.metric.SystemViewSelfTest.TEST_PREDICATE;
import static org.apache.ignite.internal.metric.SystemViewSelfTest.TEST_TRANSFORMER;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheGroupId;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.IGNITE_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;
import static org.apache.ignite.spi.metric.jmx.MetricRegistryMBean.searchHistogram;
import static org.apache.ignite.spi.systemview.jmx.SystemViewMBean.FILTER_OPERATION;
import static org.apache.ignite.spi.systemview.jmx.SystemViewMBean.VIEWS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/** */
public class JmxExporterSpiTest extends AbstractExporterSpiTest {
    /** */
    private static IgniteEx ignite;

    /** */
    private static final String REGISTRY_NAME = "test_registry";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(
                new DataRegionConfiguration()
                    .setPersistenceEnabled(true)));

        JmxMetricExporterSpi jmxSpi = new JmxMetricExporterSpi();

        jmxSpi.setExportFilter(mgrp -> !mgrp.name().startsWith(FILTERED_PREFIX));

        cfg.setMetricExporterSpi(jmxSpi);

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
    public void testSysJmxMetrics() throws Exception {
        DynamicMBean sysMBean = metricRegistry(ignite.name(), null, SYS_METRICS);

        Set<String> res = stream(sysMBean.getMBeanInfo().getAttributes())
            .map(MBeanFeatureInfo::getName)
            .collect(toSet());

        assertTrue(res.contains(CPU_LOAD));
        assertTrue(res.contains(GC_CPU_LOAD));
        assertTrue(res.contains(metricName("memory", "heap", "init")));
        assertTrue(res.contains(metricName("memory", "heap", "used")));
        assertTrue(res.contains(metricName("memory", "nonheap", "committed")));
        assertTrue(res.contains(metricName("memory", "nonheap", "max")));

        Optional<MBeanAttributeInfo> cpuLoad = stream(sysMBean.getMBeanInfo().getAttributes())
            .filter(a -> a.getName().equals(CPU_LOAD))
            .findFirst();

        assertTrue(cpuLoad.isPresent());
        assertEquals(CPU_LOAD_DESCRIPTION, cpuLoad.get().getDescription());

        Optional<MBeanAttributeInfo> gcCpuLoad = stream(sysMBean.getMBeanInfo().getAttributes())
            .filter(a -> a.getName().equals(GC_CPU_LOAD))
            .findFirst();

        assertTrue(gcCpuLoad.isPresent());
        assertEquals(GC_CPU_LOAD_DESCRIPTION, gcCpuLoad.get().getDescription());
    }

    /** */
    @Test
    public void testDataRegionJmxMetrics() throws Exception {
        DynamicMBean dataRegionMBean = metricRegistry(ignite.name(), "io", "dataregion.default");

        Set<String> res = stream(dataRegionMBean.getMBeanInfo().getAttributes())
            .map(MBeanFeatureInfo::getName)
            .collect(toSet());

        assertTrue(res.containsAll(EXPECTED_ATTRIBUTES));

        for (String metricName : res)
            assertNotNull(metricName, dataRegionMBean.getAttribute(metricName));
    }

    /** */
    @Test
    public void testUnregisterRemovedRegistry() throws Exception {
        String n = "cache-for-remove";

        IgniteCache c = ignite.createCache(n);

        DynamicMBean cacheBean = metricRegistry(ignite.name(), CACHE_METRICS, n);

        assertNotNull(cacheBean);

        ignite.destroyCache(n);

        assertThrowsWithCause(() -> metricRegistry(ignite.name(), CACHE_METRICS, n), IgniteException.class);
    }

    /** */
    @Test
    public void testFilterAndExport() throws Exception {
        createAdditionalMetrics(ignite);

        assertThrowsWithCause(new RunnableX() {
            @Override public void runx() throws Exception {
                metricRegistry(ignite.name(), "filtered", "metric");
            }
        }, IgniteException.class);

        DynamicMBean bean1 = metricRegistry(ignite.name(), "other", "prefix");

        assertEquals(42L, bean1.getAttribute("test"));
        assertEquals(43L, bean1.getAttribute("test2"));

        DynamicMBean bean2 = metricRegistry(ignite.name(), "other", "prefix2");

        assertEquals(44L, bean2.getAttribute("test3"));
    }

    /** */
    @Test
    public void testCachesView() throws Exception {
        Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

        for (String name : cacheNames)
            ignite.createCache(name);

        TabularDataSupport data = systemView(CACHES_VIEW);

        assertEquals(ignite.context().cache().cacheDescriptors().size(), data.size());

        for (int i = 0; i < data.size(); i++) {
            CompositeData row = data.get(new Object[] {i});

            cacheNames.remove(row.get("cacheName"));
        }

        assertTrue(cacheNames.toString(), cacheNames.isEmpty());
    }

    /** */
    @Test
    public void testCacheGroupsView() throws Exception {
        Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

        for (String grpName : grpNames)
            ignite.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

        TabularDataSupport grps = systemView(CACHE_GRPS_VIEW);

        assertEquals(ignite.context().cache().cacheGroupDescriptors().size(), grps.size());

        for (Map.Entry entry : grps.entrySet()) {
            CompositeData row = (CompositeData)entry.getValue();

            grpNames.remove(row.get("cacheGroupName"));
        }

        assertTrue(grpNames.toString(), grpNames.isEmpty());
    }

    /** */
    @Test
    public void testServices() throws Exception {
        ServiceConfiguration srvcCfg = new ServiceConfiguration();

        srvcCfg.setName("service");
        srvcCfg.setMaxPerNodeCount(1);
        srvcCfg.setService(new DummyService());

        ignite.services().deploy(srvcCfg);

        TabularDataSupport srvs = systemView(SVCS_VIEW);

        assertEquals(ignite.context().service().serviceDescriptors().size(), srvs.size());

        CompositeData sysView = srvs.get(new Object[] {0});

        assertEquals(srvcCfg.getName(), sysView.get("name"));
        assertEquals(srvcCfg.getMaxPerNodeCount(), sysView.get("maxPerNodeCount"));
        assertEquals(DummyService.class.getName(), sysView.get("serviceClass"));
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

        TabularDataSupport tasks = systemView(TASKS_VIEW);

        assertEquals(5, tasks.size());

        CompositeData t = tasks.get(new Object[] {0});

        assertFalse((Boolean)t.get("internal"));
        assertNull(t.get("affinityCacheName"));
        assertEquals(-1, t.get("affinityPartitionId"));
        assertTrue(t.get("taskClassName").toString().startsWith(getClass().getName()));
        assertTrue(t.get("taskName").toString().startsWith(getClass().getName()));
        assertEquals(ignite.localNode().id().toString(), t.get("taskNodeId"));
        assertEquals("0", t.get("userVersion"));

        barrier.await();
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
                TabularDataSupport conns = systemView(CLI_CONN_VIEW);

                Consumer<CompositeData> checkThin = c -> {
                    assertEquals("THIN", c.get("type"));
                    assertTrue(c.get("localAddress").toString().endsWith(Integer.toString(port)));
                    assertEquals(c.get("version"), ProtocolVersion.LATEST_VER.toString());
                };

                Consumer<CompositeData> checkJdbc = c -> {
                    assertEquals("JDBC", c.get("type"));
                    assertTrue(c.get("localAddress").toString().endsWith(Integer.toString(port)));
                    assertEquals(c.get("version"), JdbcConnectionContext.CURRENT_VER.asString());
                };

                CompositeData c0 = conns.get(new Object[] {0});
                CompositeData c1 = conns.get(new Object[] {1});

                if (c0.get("type").equals("JDBC")) {
                    checkJdbc.accept(c0);
                    checkThin.accept(c1);
                }
                else {
                    checkJdbc.accept(c1);
                    checkThin.accept(c0);
                }

                assertEquals(2, conns.size());
            }
        }

        boolean res = GridTestUtils.waitForCondition(() -> systemView(CLI_CONN_VIEW).isEmpty(), 5_000);

        assertTrue(res);
    }

    /** */
    @Test
    public void testContinuousQuery() throws Exception {
        try (IgniteEx remoteNode = startGrid(1)) {
            IgniteCache<Integer, Integer> cache = ignite.createCache("cache-1");

            assertEquals(0, systemView(CQ_SYS_VIEW).size());
            assertEquals(0, systemView(remoteNode, CQ_SYS_VIEW).size());

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

                checkContinuousQueryView(ignite, ignite);
                checkContinuousQueryView(ignite, remoteNode);
            }

            assertEquals(0, systemView(CQ_SYS_VIEW).size());
            assertEquals(0, systemView(remoteNode, CQ_SYS_VIEW).size());
        }
    }

    /** */
    private void checkContinuousQueryView(IgniteEx origNode, IgniteEx checkNode) {
        TabularDataSupport qrys = systemView(checkNode, CQ_SYS_VIEW);

        assertEquals(1, qrys.size());

        for (int i = 0; i < qrys.size(); i++) {
            CompositeData cq = qrys.get(new Object[] {i});

            assertEquals("cache-1", cq.get("cacheName"));
            assertEquals(100, cq.get("bufferSize"));
            assertEquals(1000L, cq.get("interval"));
            assertEquals(origNode.localNode().id().toString(), cq.get("nodeId"));

            if (origNode.localNode().id().equals(checkNode.localNode().id()))
                assertTrue(cq.get("localListener").toString().startsWith(getClass().getName()));
            else
                assertNull(cq.get("localListener"));

            assertTrue(cq.get("remoteFilter").toString().startsWith(getClass().getName()));
            assertNull(cq.get("localTransformedListener"));
            assertNull(cq.get("remoteTransformer"));
        }
    }

    /** */
    public TabularDataSupport systemView(String name) {
        return systemView(ignite, name);
    }

    /** */
    public TabularDataSupport systemView(IgniteEx g, String name) {
        try {
            DynamicMBean caches = metricRegistry(g.name(), VIEWS, name);

            MBeanAttributeInfo[] attrs = caches.getMBeanInfo().getAttributes();

            assertEquals(1, attrs.length);

            return (TabularDataSupport)caches.getAttribute(VIEWS);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public TabularDataSupport filteredSystemView(IgniteEx g, String name, Map<String, Object> filter) {
        try {
            DynamicMBean mbean = metricRegistry(g.name(), VIEWS, name);

            MBeanOperationInfo[] opers = mbean.getMBeanInfo().getOperations();

            assertEquals(1, opers.length);

            assertEquals(FILTER_OPERATION, opers[0].getName());

            MBeanParameterInfo[] paramInfo = opers[0].getSignature();

            Object params[] = new Object[paramInfo.length];
            String signature[] = new String[paramInfo.length];

            for (int i = 0; i < paramInfo.length; i++) {
                params[i] = filter.get(paramInfo[i].getName());
                signature[i] = paramInfo[i].getType();
            }

            return (TabularDataSupport)mbean.invoke(FILTER_OPERATION, params, signature);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    @Test
    public void testHistogramSearchByName() throws Exception {
        MetricRegistry mreg = new MetricRegistry("test", name -> null, name -> null, null);

        createTestHistogram(mreg);

        assertEquals(Long.valueOf(1), searchHistogram("histogram_0_50", mreg));
        assertEquals(Long.valueOf(2), searchHistogram("histogram_50_500", mreg));
        assertEquals(Long.valueOf(3), searchHistogram("histogram_500_inf", mreg));

        assertEquals(Long.valueOf(1), searchHistogram("histogram_with_underscore_0_50", mreg));
        assertEquals(Long.valueOf(2), searchHistogram("histogram_with_underscore_50_500", mreg));
        assertEquals(Long.valueOf(3), searchHistogram("histogram_with_underscore_500_inf", mreg));

        assertNull(searchHistogram("unknown", mreg));
        assertNull(searchHistogram("unknown_0", mreg));
        assertNull(searchHistogram("unknown_0_50", mreg));
        assertNull(searchHistogram("unknown_test", mreg));
        assertNull(searchHistogram("unknown_test_test", mreg));
        assertNull(searchHistogram("unknown_0_inf", mreg));

        assertNull(searchHistogram("histogram", mreg));
        assertNull(searchHistogram("histogram_0", mreg));
        assertNull(searchHistogram("histogram_0_100", mreg));
        assertNull(searchHistogram("histogram_0_inf", mreg));
        assertNull(searchHistogram("histogram_0_500", mreg));

        assertNull(searchHistogram("histogram_with_underscore", mreg));
        assertNull(searchHistogram("histogram_with_underscore_0", mreg));
        assertNull(searchHistogram("histogram_with_underscore_0_100", mreg));
        assertNull(searchHistogram("histogram_with_underscore_0_inf", mreg));
        assertNull(searchHistogram("histogram_with_underscore_0_500", mreg));
    }

    /** */
    @Test
    public void testHistogramExport() throws Exception {
        MetricRegistry mreg = ignite.context().metric().registry("histogramTest");

        createTestHistogram(mreg);

        DynamicMBean bean = metricRegistry(ignite.name(), null, "histogramTest");

        MBeanAttributeInfo[] attrs = bean.getMBeanInfo().getAttributes();

        assertEquals(6, attrs.length);

        assertEquals(1L, bean.getAttribute("histogram_0_50"));
        assertEquals(2L, bean.getAttribute("histogram_50_500"));
        assertEquals(3L, bean.getAttribute("histogram_500_inf"));

        assertEquals(1L, bean.getAttribute("histogram_with_underscore_0_50"));
        assertEquals(2L, bean.getAttribute("histogram_with_underscore_50_500"));
        assertEquals(3L, bean.getAttribute("histogram_with_underscore_500_inf"));
    }

    /** */
    @Test
    public void testJmxHistogramNamesExport() throws Exception {
        MetricRegistry reg = ignite.context().metric().registry(REGISTRY_NAME);

        String simpleName = "testhist";
        String nameWithUnderscore = "test_hist";

        reg.histogram(simpleName, new long[] {10, 100}, null);
        reg.histogram(nameWithUnderscore, new long[] {10, 100}, null);

        DynamicMBean mbn = metricRegistry(ignite.name(), null, REGISTRY_NAME);

        assertNotNull(mbn.getAttribute(simpleName + '_' + 0 + '_' + 10));
        assertEquals(0L, mbn.getAttribute(simpleName + '_' + 0 + '_' + 10));
        assertNotNull(mbn.getAttribute(simpleName + '_' + 10 + '_' + 100));
        assertEquals(0L, mbn.getAttribute(simpleName + '_' + 10 + '_' + 100));
        assertNotNull(mbn.getAttribute(nameWithUnderscore + '_' + 10 + '_' + 100));
        assertEquals(0L, mbn.getAttribute(nameWithUnderscore + '_' + 10 + '_' + 100));
        assertNotNull(mbn.getAttribute(simpleName + '_' + 100 + "_inf"));
        assertEquals(0L, mbn.getAttribute(simpleName + '_' + 100 + "_inf"));
    }

    /** */
    @Test
    public void testTransactions() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.createCache(new CacheConfiguration<Integer, Integer>("c")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        assertEquals(0, systemView(TXS_MON_LIST).size());

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

            boolean res = waitForCondition(() -> systemView(TXS_MON_LIST).size() == 5, 10_000L);

            assertTrue(res);

            CompositeData txv = systemView(TXS_MON_LIST).get(new Object[] {0});

            assertEquals(ignite.localNode().id().toString(), txv.get("localNodeId"));
            assertEquals(REPEATABLE_READ.name(), txv.get("isolation"));
            assertEquals(PESSIMISTIC.name(), txv.get("concurrency"));
            assertEquals(ACTIVE.name(), txv.get("state"));
            assertNotNull(txv.get("xid"));
            assertFalse((boolean)txv.get("system"));
            assertFalse((boolean)txv.get("implicit"));
            assertFalse((boolean)txv.get("implicitSingle"));
            assertTrue((boolean)txv.get("near"));
            assertFalse((boolean)txv.get("dht"));
            assertTrue((boolean)txv.get("colocated"));
            assertTrue((boolean)txv.get("local"));
            assertEquals("test", txv.get("label"));
            assertFalse((boolean)txv.get("onePhaseCommit"));
            assertFalse((boolean)txv.get("internal"));
            assertEquals(0L, txv.get("timeout"));
            assertTrue(((long)txv.get("startTime")) <= System.currentTimeMillis());

            //Only pessimistic transactions are supported when MVCC is enabled.
            if(Objects.equals(System.getProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS), "true"))
                return;

            GridTestUtils.runMultiThreadedAsync(() -> {
                try (Transaction tx = ignite.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                    cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                    latch.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }, 5, "xxx");

            res = waitForCondition(() -> systemView(TXS_MON_LIST).size() == 10, 10_000L);

            assertTrue(res);

            for (int i=0; i<9; i++) {
                txv = systemView(TXS_MON_LIST).get(new Object[] {i});

                if (PESSIMISTIC.name().equals(txv.get("concurrency")))
                    continue;

                assertEquals(ignite.localNode().id().toString(), txv.get("localNodeId"));
                assertEquals(SERIALIZABLE.name(), txv.get("isolation"));
                assertEquals(OPTIMISTIC.name(), txv.get("concurrency"));
                assertEquals(ACTIVE.name(), txv.get("state"));
                assertNotNull(txv.get("xid"));
                assertFalse((boolean)txv.get("system"));
                assertFalse((boolean)txv.get("implicit"));
                assertFalse((boolean)txv.get("implicitSingle"));
                assertTrue((boolean)txv.get("near"));
                assertFalse((boolean)txv.get("dht"));
                assertTrue((boolean)txv.get("colocated"));
                assertTrue((boolean)txv.get("local"));
                assertNull(txv.get("label"));
                assertFalse((boolean)txv.get("onePhaseCommit"));
                assertFalse((boolean)txv.get("internal"));
                assertEquals(0L, txv.get("timeout"));
                assertTrue(((long)txv.get("startTime")) <= System.currentTimeMillis());
            }
        }
        finally {
            latch.countDown();
        }

        boolean res = waitForCondition(() -> systemView(TXS_MON_LIST).isEmpty(), 10_000L);

        assertTrue(res);
    }

    /** */
    @Test
    public void testLocalScanQuery() throws Exception {
        IgniteCache<Integer, Integer> cache1 = ignite.createCache(
            new CacheConfiguration<Integer, Integer>("cache1")
                .setGroupName("group1"));

        int part = ignite.affinity("cache1").primaryPartitions(ignite.localNode())[0];

        List<Integer> partKeys = partitionKeys(cache1, part, 11, 0);

        for (Integer key : partKeys)
            cache1.put(key, key);

        TabularDataSupport qrySysView0 = systemView(SCAN_QRY_SYS_VIEW);

        assertNotNull(qrySysView0);

        assertEquals(0, qrySysView0.size());

        QueryCursor<Integer> qryRes1 = cache1.query(
            new ScanQuery<Integer, Integer>()
                .setFilter(new TestPredicate())
                .setLocal(true)
                .setPartition(part)
                .setPageSize(10),
            new TestTransformer());

        assertTrue(qryRes1.iterator().hasNext());

        boolean res = waitForCondition(() -> !systemView(SCAN_QRY_SYS_VIEW).isEmpty(), 5_000);

        assertTrue(res);

        CompositeData view = systemView(SCAN_QRY_SYS_VIEW).get(new Object[] {0});

        assertEquals(ignite.localNode().id().toString(), view.get("originNodeId"));
        assertEquals(0L, view.get("queryId"));
        assertEquals("cache1", view.get("cacheName"));
        assertEquals(cacheId("cache1"), view.get("cacheId"));
        assertEquals(cacheGroupId("cache1", "group1"), view.get("cacheGroupId"));
        assertEquals("group1", view.get("cacheGroupName"));
        assertTrue((Long)view.get("startTime") <= System.currentTimeMillis());
        assertTrue((Long)view.get("duration") >= 0);
        assertFalse((Boolean)view.get("canceled"));
        assertEquals(TEST_PREDICATE, view.get("filter"));
        assertTrue((Boolean)view.get("local"));
        assertEquals(part, view.get("partition"));
        assertEquals(toStringSafe(ignite.context().discovery().topologyVersionEx()), view.get("topology"));
        assertEquals(TEST_TRANSFORMER, view.get("transformer"));
        assertFalse((Boolean)view.get("keepBinary"));
        assertEquals("null", view.get("subjectId"));
        assertNull(view.get("taskName"));

        qryRes1.close();

        res = waitForCondition(() -> systemView(SCAN_QRY_SYS_VIEW).isEmpty(), 5_000);

        assertTrue(res);
    }

    /** */
    @Test
    public void testScanQuery() throws Exception {
        try(IgniteEx client1 = startClientGrid("client-1");
            IgniteEx client2 = startClientGrid("client-2")) {

            IgniteCache<Integer, Integer> cache1 = client1.createCache(
                new CacheConfiguration<Integer, Integer>("cache1")
                    .setGroupName("group1"));

            IgniteCache<Integer, Integer> cache2 = client2.createCache("cache2");

            awaitPartitionMapExchange();

            for (int i = 0; i < 100; i++) {
                cache1.put(i, i);
                cache2.put(i, i);
            }

            TabularDataSupport qrySysView0 = systemView(ignite, SCAN_QRY_SYS_VIEW);

            assertNotNull(qrySysView0);

            assertEquals(0, qrySysView0.size());

            QueryCursor<Integer> qryRes1 = cache1.query(
                new ScanQuery<Integer, Integer>()
                    .setFilter(new TestPredicate())
                    .setPageSize(10),
                new TestTransformer());

            QueryCursor<?> qryRes2 = cache2.withKeepBinary().query(new ScanQuery<>()
                .setPageSize(20));

            assertTrue(qryRes1.iterator().hasNext());
            assertTrue(qryRes2.iterator().hasNext());

            checkScanQueryView(client1, client2, ignite);

            qryRes1.close();
            qryRes2.close();

            boolean res = waitForCondition(() -> systemView(ignite, SCAN_QRY_SYS_VIEW).isEmpty(), 5_000);

            assertTrue(res);
        }
    }

    /** @throws Exception If failed. */
    @Test
    public void testIgniteKernal() throws Exception {
        DynamicMBean mbn = metricRegistry(ignite.name(), null, IGNITE_METRICS);

        assertNotNull(mbn);

        assertEquals(36, mbn.getMBeanInfo().getAttributes().length);

        assertFalse(stream(mbn.getMBeanInfo().getAttributes()).anyMatch(a-> F.isEmpty(a.getDescription())));

        assertFalse(F.isEmpty((String)mbn.getAttribute("fullVersion")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("copyright")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("osInformation")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("jdkInformation")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("vmName")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("discoverySpiFormatted")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("communicationSpiFormatted")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("deploymentSpiFormatted")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("checkpointSpiFormatted")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("collisionSpiFormatted")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("eventStorageSpiFormatted")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("failoverSpiFormatted")));
        assertFalse(F.isEmpty((String)mbn.getAttribute("loadBalancingSpiFormatted")));

        assertEquals(System.getProperty("user.name"), (String)mbn.getAttribute("osUser"));

        assertNotNull(DateFormat.getDateTimeInstance().parse((String)mbn.getAttribute("startTimestampFormatted")));
        assertNotNull(LocalTime.parse((String)mbn.getAttribute("uptimeFormatted")));

        assertTrue((boolean)mbn.getAttribute("isRebalanceEnabled"));
        assertTrue((boolean)mbn.getAttribute("isNodeInBaseline"));
        assertTrue((boolean)mbn.getAttribute("active"));

        assertTrue((long)mbn.getAttribute("startTimestamp") > 0);
        assertTrue((long)mbn.getAttribute("uptime") > 0);

        assertEquals(ignite.name(), (String)mbn.getAttribute("instanceName"));

        assertEquals(Collections.emptyList(), mbn.getAttribute("userAttributesFormatted"));
        assertEquals(Collections.emptyList(), mbn.getAttribute("lifecycleBeansFormatted"));

        assertEquals(Collections.emptyMap(), mbn.getAttribute("longJVMPauseLastEvents"));

        assertEquals(0L, mbn.getAttribute("longJVMPausesCount"));
        assertEquals(0L, mbn.getAttribute("longJVMPausesTotalDuration"));

        long clusterStateChangeTime = (long)mbn.getAttribute("lastClusterStateChangeTime");

        assertTrue(0 < clusterStateChangeTime && clusterStateChangeTime < System.currentTimeMillis());

        assertEquals(String.valueOf(ignite.configuration().getPublicThreadPoolSize()),
                mbn.getAttribute("executorServiceFormatted"));

        assertEquals(ignite.configuration().isPeerClassLoadingEnabled(), mbn.getAttribute("isPeerClassLoadingEnabled"));

        assertTrue(((String)mbn.getAttribute("currentCoordinatorFormatted"))
                .contains(ignite.localNode().id().toString()));

        assertEquals(ignite.configuration().getIgniteHome(), (String)mbn.getAttribute("igniteHome"));

        assertEquals(ignite.localNode().id(), mbn.getAttribute("localNodeId"));

        assertEquals(ignite.configuration().getGridLogger().toString(),
                (String)mbn.getAttribute("gridLoggerFormatted"));

        assertEquals(ignite.configuration().getMBeanServer().toString(),
                (String)mbn.getAttribute("mBeanServerFormatted"));

        assertEquals(ClusterState.ACTIVE.toString(), mbn.getAttribute("clusterState"));
    }

    /** */
    private void checkScanQueryView(IgniteEx client1, IgniteEx client2, IgniteEx server) throws Exception {
        boolean res = waitForCondition(() -> systemView(server, SCAN_QRY_SYS_VIEW).size() > 1, 5_000);

        assertTrue(res);

        Consumer<CompositeData> cache1checker = view -> {
            assertEquals(client1.localNode().id().toString(), view.get("originNodeId"));
            assertTrue((Long)view.get("queryId") != 0);
            assertEquals("cache1", view.get("cacheName"));
            assertEquals(cacheId("cache1"), view.get("cacheId"));
            assertEquals(cacheGroupId("cache1", "group1"), view.get("cacheGroupId"));
            assertEquals("group1", view.get("cacheGroupName"));
            assertTrue((Long)view.get("startTime") <= System.currentTimeMillis());
            assertTrue((Long)view.get("duration") >= 0);
            assertFalse((Boolean)view.get("canceled"));
            assertEquals(TEST_PREDICATE, view.get("filter"));
            assertFalse((Boolean)view.get("local"));
            assertEquals(-1, view.get("partition"));
            assertEquals(toStringSafe(client1.context().discovery().topologyVersionEx()), view.get("topology"));
            assertEquals(TEST_TRANSFORMER, view.get("transformer"));
            assertFalse((Boolean)view.get("keepBinary"));
            assertEquals("null", view.get("subjectId"));
            assertNull(view.get("taskName"));
            assertEquals(10, view.get("pageSize"));
        };

        Consumer<CompositeData> cache2checker = view -> {
            assertEquals(client2.localNode().id().toString(), view.get("originNodeId"));
            assertTrue((Long)view.get("queryId") != 0);
            assertEquals("cache2", view.get("cacheName"));
            assertEquals(cacheId("cache2"), view.get("cacheId"));
            assertEquals(cacheGroupId("cache2", null), view.get("cacheGroupId"));
            assertEquals("cache2", view.get("cacheGroupName"));
            assertTrue((Long)view.get("startTime") <= System.currentTimeMillis());
            assertTrue((Long)view.get("duration") >= 0);
            assertFalse((Boolean)view.get("canceled"));
            assertNull(view.get("filter"));
            assertFalse((Boolean)view.get("local"));
            assertEquals(-1, view.get("partition"));
            assertEquals(toStringSafe(client2.context().discovery().topologyVersionEx()), view.get("topology"));
            assertNull(view.get("transformer"));
            assertTrue((Boolean)view.get("keepBinary"));
            assertEquals("null", view.get("subjectId"));
            assertNull(view.get("taskName"));
            assertEquals(20, view.get("pageSize"));
        };

        boolean found1 = false;
        boolean found2 = false;

        TabularDataSupport qrySysView = systemView(server, SCAN_QRY_SYS_VIEW);

        for (int i=0; i < qrySysView.size(); i++) {
            CompositeData view = systemView(SCAN_QRY_SYS_VIEW).get(new Object[] {i});

            if ("cache2".equals(view.get("cacheName"))) {
                cache2checker.accept(view);
                found1 = true;
            }
            else {
                cache1checker.accept(view);
                found2 = true;
            }
        }

        assertTrue(found1 && found2);
    }

    /** */
    @Test
    public void testSysStripedExecutor() throws Exception {
        checkStripeExecutorView(ignite.context().getStripedExecutorService(),
            SYS_POOL_QUEUE_VIEW,
            "sys");
    }

    /** */
    @Test
    public void testStreamerStripedExecutor() throws Exception {
        checkStripeExecutorView(ignite.context().getDataStreamerExecutorService(),
            STREAM_POOL_QUEUE_VIEW,
            "data-streamer");
    }

    /**
     * Checks striped executor system view.
     *
     * @param execSvc Striped executor.
     * @param viewName System view.
     * @param poolName Executor name.
     */
    private void checkStripeExecutorView(StripedExecutor execSvc, String viewName, String poolName) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        execSvc.execute(0, new TestRunnable(latch, 0));
        execSvc.execute(0, new TestRunnable(latch, 1));
        execSvc.execute(1, new TestRunnable(latch, 2));
        execSvc.execute(1, new TestRunnable(latch, 3));

        try {
            boolean res = waitForCondition(() -> systemView(viewName).size() == 2, 5_000);

            assertTrue(res);

            TabularDataSupport view = systemView(viewName);

            CompositeData row0 = view.get(new Object[] {0});

            assertEquals(0, row0.get("stripeIndex"));
            assertEquals(TestRunnable.class.getSimpleName() + '1', row0.get("description"));
            assertEquals(poolName + "-stripe-0", row0.get("threadName"));
            assertEquals(TestRunnable.class.getName(), row0.get("taskName"));

            CompositeData row1 = view.get(new Object[] {1});

            assertEquals(1, row1.get("stripeIndex"));
            assertEquals(TestRunnable.class.getSimpleName() + '3', row1.get("description"));
            assertEquals(poolName + "-stripe-1", row1.get("threadName"));
            assertEquals(TestRunnable.class.getName(), row1.get("taskName"));
        }
        finally {
            latch.countDown();
        }
    }

    /** */
    @Test
    public void testPagesList() throws Exception {
        String cacheName = "cacheFL";

        IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(new CacheConfiguration<Integer, Integer>()
            .setName(cacheName).setAffinity(new RendezvousAffinityFunction().setPartitions(2)));

        // Put some data to cache to init cache partitions.
        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        TabularDataSupport view = filteredSystemView(ignite, CACHE_GRP_PAGE_LIST_VIEW, U.map(
            CachePagesListViewWalker.CACHE_GROUP_ID_FILTER, cacheId(cacheName),
            CachePagesListViewWalker.PARTITION_ID_FILTER, 0,
            CachePagesListViewWalker.BUCKET_NUMBER_FILTER, 0
        ));

        assertEquals(1, view.size());

        view = filteredSystemView(ignite, CACHE_GRP_PAGE_LIST_VIEW, U.map(
            CachePagesListViewWalker.CACHE_GROUP_ID_FILTER, cacheId(cacheName),
            CachePagesListViewWalker.BUCKET_NUMBER_FILTER, 0
        ));

        assertEquals(2, view.size());
    }

    /** */
    private void createTestHistogram(MetricRegistry mreg) {
        long[] bounds = new long[] {50, 500};

        HistogramMetricImpl histogram = mreg.histogram("histogram", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);

        histogram = mreg.histogram("histogram_with_underscore", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);
    }
}
