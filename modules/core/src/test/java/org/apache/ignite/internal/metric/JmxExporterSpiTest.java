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

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
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
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularDataSupport;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.thin.ProtocolVersion;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;
import static org.apache.ignite.spi.metric.jmx.MetricRegistryMBean.searchHistogram;
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

        DynamicMBean cacheBean = mbean(ignite, CACHE_METRICS, n);

        assertNotNull(cacheBean);

        ignite.destroyCache(n);

        assertThrowsWithCause(() -> mbean(ignite, CACHE_METRICS, n), IgniteException.class);
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
                    assertEquals(c.get("version"), ProtocolVersion.CURRENT_VER.toString());
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
            DynamicMBean caches = mbean(g, VIEWS, name);

            MBeanAttributeInfo[] attrs = caches.getMBeanInfo().getAttributes();

            assertEquals(1, attrs.length);

            return (TabularDataSupport)caches.getAttribute(VIEWS);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public DynamicMBean mbean(IgniteEx g, String grp, String name) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(g.name(), grp, name);

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            throw new IgniteException("MBean not registered.");

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, DynamicMBean.class, false);
    }

    /** */
    @Test
    public void testHistogramSearchByName() throws Exception {
        MetricRegistry mreg = new MetricRegistry("test", null);

        createTestHistogram(mreg);

        assertEquals(Long.valueOf(1), searchHistogram("histogram_0_50", mreg));
        assertEquals(Long.valueOf(2), searchHistogram("histogram_50_500", mreg));
        assertEquals(Long.valueOf(3), searchHistogram("histogram_500_inf", mreg));

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
    }

    /** */
    @Test
    public void testHistogramExport() throws Exception {
        MetricRegistry mreg = ignite.context().metric().registry("histogramTest");

        createTestHistogram(mreg);

        DynamicMBean bean = metricRegistry(ignite.name(), null, "histogramTest");

        MBeanAttributeInfo[] attrs = bean.getMBeanInfo().getAttributes();

        assertEquals(3, attrs.length);

        assertEquals(1L, bean.getAttribute("histogram_0_50"));
        assertEquals(2L, bean.getAttribute("histogram_50_500"));
        assertEquals(3L, bean.getAttribute("histogram_500_inf"));
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
    private void createTestHistogram(MetricRegistry mreg) {
        long[] bounds = new long[] {50, 500};

        HistogramMetric histogram = mreg.histogram("histogram", bounds, null);

        histogram.value(10);
        histogram.value(51);
        histogram.value(60);
        histogram.value(600);
        histogram.value(600);
        histogram.value(600);
    }
}
