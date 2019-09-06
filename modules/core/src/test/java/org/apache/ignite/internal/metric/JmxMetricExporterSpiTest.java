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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.spi.metric.list.view.CacheView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.RunnableX;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.processors.cache.CacheMetricsImpl.CACHE_METRICS;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.GC_CPU_LOAD_DESCRIPTION;
import static org.apache.ignite.internal.processors.metric.GridMetricManager.SYS_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.spi.metric.jmx.MonitoringListMBean.LIST;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/** */
public class JmxMetricExporterSpiTest extends AbstractExporterSpiTest {
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

        jmxSpi.setMetricExportFilter(mgrp -> !mgrp.name().startsWith(FILTERED_PREFIX));

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
        DynamicMBean sysMBean = metricSet(ignite.name(), null, SYS_METRICS);

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
        DynamicMBean dataRegionMBean = metricSet(ignite.name(), "io", "dataregion.default");

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

        DynamicMBean cacheBean = mbean(CACHE_METRICS, n);

        assertNotNull(cacheBean);

        ignite.destroyCache(n);

        assertThrowsWithCause(() -> mbean(CACHE_METRICS, n), IgniteException.class);
    }

    /** */
    @Test
    public void testListRemove() throws Exception {
        GridMetricManager mmgr = ignite.context().metric();

        mmgr.list("test", "description", CacheView.class);

        DynamicMBean listBean = mbean(LIST, "test");

        assertNotNull(listBean);

        mmgr.removeList("test");

        assertThrowsWithCause(() -> mbean(LIST, "test"), IgniteException.class);
    }

    /** */
    @Test
    public void testFilterAndExport() throws Exception {
        createAdditionalMetrics(ignite);

        assertThrowsWithCause(new RunnableX() {
            @Override public void runx() throws Exception {
                metricSet(ignite.name(), "filtered", "metric");
            }
        }, IgniteException.class);

        DynamicMBean bean1 = metricSet(ignite.name(), "other", "prefix");

        assertEquals(42L, bean1.getAttribute("test"));
        assertEquals(43L, bean1.getAttribute("test2"));

        DynamicMBean bean2 = metricSet(ignite.name(), "other", "prefix2");

        assertEquals(44L, bean2.getAttribute("test3"));
    }

    /** */
    @Test
    public void testCachesList() throws Exception {
        Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

        for (String name : cacheNames)
            ignite.createCache(name);

        TabularDataSupport data = monitoringList("caches");

        assertEquals(3, data.size());

        for(int i=0; i<data.size(); i++) {
            CompositeData row = data.get(new Object[] {i});

            cacheNames.remove(row.get("cacheName"));
        }

        assertTrue(cacheNames.toString(), cacheNames.isEmpty());
    }

    /** */
    @Test
    public void testCacheGroupsList() throws Exception {
        Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

        for (String grpName : grpNames)
            ignite.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

        TabularDataSupport grps = monitoringList("cacheGroups");

        assertEquals("ignite-sys, grp-1, grp-2", 3, grps.size());

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

        TabularDataSupport srvs = monitoringList("services");

        assertEquals(1, srvs.size());

        CompositeData sview = srvs.get(new Object[] {0});

        assertEquals(srvcCfg.getName(), sview.get("name"));
        assertEquals(srvcCfg.getMaxPerNodeCount(), sview.get("maxPerNodeCount"));
        assertEquals(DummyService.class.getName(), sview.get("serviceClass"));
    }

    /** */
    @Test
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

        TabularDataSupport tasks = monitoringList("tasks");

        assertEquals(5, tasks.size());

        CompositeData t = tasks.get(new Object[] {0});

        assertFalse((Boolean)t.get("internal"));
        assertNull(t.get("affinityCacheName"));
        assertEquals(-1, t.get("affinityPartitionId"));
        assertTrue(t.get("taskClassName").toString().startsWith(getClass().getName()));
        assertTrue(t.get("taskName").toString().startsWith(getClass().getName()));
        assertEquals(ignite.localNode().id().toString(), t.get("taskNodeId"));
        assertEquals("0", t.get("userVersion"));
    }

    /** */
    @Test
    public void testTransactions() throws Exception {
        IgniteCache<Integer, Integer> cache = ignite.createCache(new CacheConfiguration<Integer, Integer>("c")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

        AtomicReference<TabularDataSupport> txs = new AtomicReference<>(monitoringList("transactions"));

        assertEquals(0, txs.get().size());

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

            boolean res = waitForCondition(() -> {
                txs.set(monitoringList("transactions"));

                return txs.get().size() == 5;
            }, 5_000L);

            assertTrue(res);

            CompositeData txv = txs.get().get(new Object[] {0});

            assertEquals(ignite.localNode().id().toString(), txv.get("nodeId"));
            assertEquals(txv.get("isolation"), REPEATABLE_READ.name());
            assertEquals(txv.get("concurrency"), PESSIMISTIC.name());
            assertEquals(txv.get("state"), ACTIVE.name());
            assertNotNull(txv.get("xid"));
            assertFalse((Boolean)txv.get("system"));
            assertFalse((Boolean)txv.get("implicit"));
            assertFalse((Boolean)txv.get("implicitSingle"));
            assertTrue((Boolean)txv.get("near"));
            assertFalse((Boolean)txv.get("dht"));
            assertTrue((Boolean)txv.get("colocated"));
            assertTrue((Boolean)txv.get("local"));
            assertEquals("test", txv.get("label"));
            assertFalse((Boolean)txv.get("onePhaseCommit"));
            assertFalse((Boolean)txv.get("internal"));
            assertEquals(0L, txv.get("timeout"));
            assertTrue((Long)txv.get("startTime") <= System.currentTimeMillis());

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

            res = waitForCondition(() -> {
                txs.set(monitoringList("transactions"));

                return txs.get().size() == 10;
            }, 5_000L);

            assertTrue(res);
        }
        finally {
            latch.countDown();
        }

        boolean res = waitForCondition(() -> {
            txs.set(monitoringList("transactions"));

            return txs.get().isEmpty();
        }, 5_000L);

        assertTrue(res);
    }

    /** */
    public TabularDataSupport monitoringList(String name) {
        try {
            DynamicMBean caches = mbean(LIST, name);

            MBeanAttributeInfo[] attrs = caches.getMBeanInfo().getAttributes();

            assertEquals(1, attrs.length);

            return (TabularDataSupport)caches.getAttribute(LIST);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** */
    public DynamicMBean mbean(String grp, String name) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(ignite.name(), grp, name);

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (!mbeanSrv.isRegistered(mbeanName))
            throw new IgniteException("MBean not registered.");

        return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, DynamicMBean.class, false);
    }
}
