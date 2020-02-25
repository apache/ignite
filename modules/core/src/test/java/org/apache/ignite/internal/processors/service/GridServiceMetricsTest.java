package org.apache.ignite.internal.processors.service;

import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.platform.PlatformProcessorImpl;
import org.apache.ignite.internal.processors.platform.cluster.PlatformClusterGroup;
import org.apache.ignite.internal.processors.platform.services.PlatformServices;
import org.apache.ignite.internal.processors.service.inner.MyService;
import org.apache.ignite.internal.processors.service.inner.MyServiceFactory;
import org.apache.ignite.internal.processors.service.inner.NamingService;
import org.apache.ignite.internal.processors.service.inner.NamingServiceImpl;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICE_METRICS_ENABLED;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.METRIC_REGISTRY_INVOCATIONS;

/** */
public class GridServiceMetricsTest extends GridCommonAbstractTest {
    /** Number of service invcations. */
    private static final int INVOKE_CNT = 50;

    /** Utility holder of current grid number. */
    private final AtomicInteger gridNum = new AtomicInteger();

    /** */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        System.clearProperty(IGNITE_SERVICE_METRICS_ENABLED);
    }

    @Test
    public void fix() throws Exception {
        IgniteEx server = startGrid(0);

        PlatformProcessorImpl platformProcessor = new PlatformProcessorImpl(server.context());

        PlatformClusterGroup platformTarget = (PlatformClusterGroup)platformProcessor.processOutObject(10);

        PlatformServices platformServices = (PlatformServices)platformTarget.processOutObject(34);

    }

    /** Makes sure {@code IgniteSystemProperties#IGNITE_SERVICE_METRICS_ENABLED} works correctly. */
    @Test
    public void testMetricsEnabledDisabled() throws Exception {
        IgniteEx server = startGrid(0);

        assertFalse("Service metric registry must not be created yet.",
            findMetric(server.context().metric(), METRIC_REGISTRY_INVOCATIONS));

        String srvcName = "testMetricsEnabledService";

        server.services().deploy(serviceCfg(srvcName, 1, 1));

        assertTrue("Service metric registry must be already created.",
            findMetric(server.context().metric(), METRIC_REGISTRY_INVOCATIONS));

        stopAllGrids();

        System.setProperty(IGNITE_SERVICE_METRICS_ENABLED, "false");

        server = startGrid(0);

        assertFalse("Service metric registry must not be created again.",
            findMetric(server.context().metric(), METRIC_REGISTRY_INVOCATIONS));

        server.services().deploy(serviceCfg(srvcName, 1, 1));

        MyService srvc = server.services().service(srvcName);

        srvc.hello();

        srvc = server.services().serviceProxy(srvcName, MyService.class, false);

        srvc.hello();

        srvc = server.services().serviceProxy(srvcName, MyService.class, true);

        srvc.hello();

        IgniteEx client = startClientGrid(1);

        srvc = client.services().serviceProxy(srvcName, MyService.class, false);

        srvc.hello();

        srvc = client.services().serviceProxy(srvcName, MyService.class, true);

        srvc.hello();

        assertFalse("Service metric registry must not be created when \""+IGNITE_SERVICE_METRICS_ENABLED +
                "\" is false.", findMetric(server.context().metric(), METRIC_REGISTRY_INVOCATIONS));
    }

    /** Ensures metric are created when service is deployed and removed when service is undeployed. */
    @Test
    public void testMetricDeplotmentIndeployment() throws Exception {
        List<IgniteEx> servers = startGrids(3, false);

        String srvcName = "namingService";

        // 2 services per node.
        servers.get(0).services().deploy(serviceCfg(srvcName, servers.size(), 2));

        awaitPartitionMapExchange();

        String srvcMetricRegName = IgniteServiceProcessor.serviceMetricsName(srvcName);

        // Make sure metrics are registered.
        for (IgniteEx ignite : servers)
            assertEquals(metricsCnt(ignite, srvcMetricRegName), MyService.class.getDeclaredMethods().length);

        servers.get(0).services().cancel(srvcName);

        awaitPartitionMapExchange();

        for (IgniteEx ignite : servers)
            assertEquals(metricsCnt(ignite, srvcMetricRegName), 0);
    }

    /** Checks two different histograms are created for the same short names of service methods. */
    @Test
    public void testMetricNaming() throws Exception {
        IgniteEx ignite = startGrid(1);

        String srvcName = "namingService";

        ignite.services().deployNodeSingleton(srvcName, new NamingServiceImpl());

        NamingService srvc = ignite.services().serviceProxy(srvcName, NamingService.class, false);

        assertEquals(srvc.process(new org.apache.ignite.internal.processors.service.inner.impl.Param()),
            org.apache.ignite.internal.processors.service.inner.impl.Param.VALUE);

        assertEquals(srvc.process(new org.apache.ignite.internal.processors.service.inner.experimental.Param()),
            org.apache.ignite.internal.processors.service.inner.experimental.Param.VALUE);

        String srvcMetricRegName = IgniteServiceProcessor.serviceMetricsName(srvcName);

        MetricRegistry registry = ignite.context().metric().registry(srvcMetricRegName);

        int metricsCnt = 0;

        int invokes = 0;

        for (Metric metric : registry) {
            ++metricsCnt;

            if (metric instanceof HistogramMetric) {
                HistogramMetric histogramm = (HistogramMetric)metric;

                invokes += LongStream.of(histogramm.value()).reduce(Long::sum).getAsLong();
            }
        }

        // We did only 2 service calls.
        assertEquals(invokes, 2);

        // Ensure metrics number matches method number of the service API.
        assertEquals(metricsCnt, NamingService.class.getDeclaredMethods().length);
    }

    /** Tests local services. */
    @Test
    public void testServiceInvocationMetricsOnLocals() throws Exception {
        List<IgniteEx> grids = startGrids(3, false);

        String srvcName = "InvokeTestService";

        int callCounter = 100;

        Method mtd = MyService.class.getMethod("hello");

        String mtdMetricName = IgniteServiceProcessor.methodMetricName(mtd, 0);

        String srvcMetricRegName = IgniteServiceProcessor.serviceMetricsName(srvcName);

        grids.get(0).services().deployNodeSingleton(srvcName, MyServiceFactory.create());

        awaitPartitionMapExchange();

        // Call the service through local references.
        for (IgniteEx ignite : grids) {
            HistogramMetric invokeHistogramm = null;

            MyService srv = ignite.services().service(srvcName);

            for (int i = 0; i < callCounter; ++i)
                srv.hello();

            MetricRegistry metrics = ignite.context().metric().registry(srvcMetricRegName);

            assertNotNull("Metric registry not found.", metrics);

            for (Metric metric : metrics) {
                if (metric.name().startsWith(srvcMetricRegName) && metric.name().endsWith(mtdMetricName) &&
                    metric instanceof HistogramMetric) {
                    assertNull("Duplicated invocation histogramm found", invokeHistogramm);

                    invokeHistogramm = (HistogramMetric)metric;
                }
            }

            assertNotNull("Invocation histogramm not found", invokeHistogramm);

            long calls = LongStream.of(invokeHistogramm.value()).reduce(Long::sum).getAsLong();

            assertEquals("Wrong invocations calculated.", callCounter, calls);
        }
    }

    /** */
    @Test
    public void testServiceInvocationMetricsProxiedSingle() throws Exception {
        serviceInvocationMetricsProxied(1, 1, 1, 1);
    }

    /** */
    @Test
    public void testServiceInvocationMetricsProxiedMultyFlat() throws Exception {
        serviceInvocationMetricsProxied(3, 5, 3, 1);
    }

    /** */
    @Test
    public void testServiceInvocationMetricsProxiedMultySkew() throws Exception {
        serviceInvocationMetricsProxied(5, 3, 2, 1);
    }

    /** */
    @Test
    public void testServiceInvocationMetricsProxiedMultyFlatDuplicated() throws Exception {
        serviceInvocationMetricsProxied(3, 2, 3, 3);
    }

    /** */
    @Test
    public void testServiceInvocationMetricsProxiedMultySkewDuplicated() throws Exception {
        serviceInvocationMetricsProxied(5, 2, 2, 2);
    }

    /** Invoke service in various ways. */
    private void serviceInvocationMetricsProxied(int serverCnt, int clientCnt, int perClusterCnt, int perNodeCnt)
        throws Exception {

        List<IgniteEx> servers = startGrids(serverCnt, false);

        List<IgniteEx> clients = startGrids(clientCnt, true);

        String srvcName = "InvokeTestService";

        Method mtd = MyService.class.getMethod("hello");

        String mtdMetricName = IgniteServiceProcessor.methodMetricName(mtd, 0);

        String srvcMetricRegName = IgniteServiceProcessor.serviceMetricsName(srvcName);

        servers.get(0).services().deploy(serviceCfg(srvcName, perClusterCnt, perNodeCnt));

        awaitPartitionMapExchange();

        // Call service through the server proxies, not-sticky.
        for (int s = 0; s < INVOKE_CNT; ++s)
            servers.get(s % servers.size()).services().serviceProxy(srvcName, MyService.class, false).hello();

        // Call service through the server proxies, sticky.
        List<MyService> stickies = servers.stream()
            .map(ignite -> (MyService)ignite.services().serviceProxy(srvcName, MyService.class, true))
            .collect(Collectors.toList());

        for (int ss = 0; ss < INVOKE_CNT; ++ss)
            stickies.get(ss % stickies.size()).hello();

        // Call service through the remote client proxies, not-sticky.
        for (int c = 0; c < INVOKE_CNT; ++c)
            clients.get(c % clients.size()).services().serviceProxy(srvcName, MyService.class, false).hello();

        // Call service through the client proxies, sticky.
        stickies = clients.stream()
            .map(ignite -> (MyService)ignite.services().serviceProxy(srvcName, MyService.class, true))
            .collect(Collectors.toList());

        for (int cs = 0; cs < INVOKE_CNT; ++cs)
            stickies.get(cs % stickies.size()).hello();

        long invokeCollector = 0;

        // Calculate all the invocations.
        for (IgniteEx ignite : servers) {
            HistogramMetric invokeHistogramm = null;

            ReadOnlyMetricRegistry metrics = null;

            for (ReadOnlyMetricRegistry mr : ignite.context().metric()) {
                if (mr.name().equals(srvcMetricRegName)) {
                    metrics = mr;
                    break;
                }
            }

            // Metrics may not be deployed on this server node.
            if (metrics == null)
                continue;

            for (Metric metric : metrics) {
                if (metric.name().startsWith(srvcMetricRegName) && metric.name().endsWith(mtdMetricName) &&
                    metric instanceof HistogramMetric) {
                    assertNull("Duplicated invocation histogramm found", invokeHistogramm);

                    invokeHistogramm = (HistogramMetric)metric;
                }
            }

            assertNotNull("Invocation histogramm not found", invokeHistogramm);

            invokeCollector += LongStream.of(invokeHistogramm.value()).reduce(Long::sum).getAsLong();
        }

        // Compare all 4 call approaches.
        assertEquals("Calculated wrong service invocation number.", INVOKE_CNT * 4, invokeCollector);
    }

    /** */
    private ServiceConfiguration serviceCfg(String svcName, int perClusterCnt, int perNodeCnt) {
        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setService(MyServiceFactory.create());

        svcCfg.setName(svcName);

        svcCfg.setMaxPerNodeCount(perNodeCnt);

        svcCfg.setTotalCount(perClusterCnt);

        return svcCfg;
    }

    /** Expose ignite-references of the nodes as list. */
    private List<IgniteEx> startGrids(int cnt, boolean client) {
        return Stream.generate(() -> {
            try {
                return client ? startClientGrid(gridNum.getAndIncrement()) : startGrid(gridNum.getAndIncrement());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).limit(cnt).collect(Collectors.toList());
    }

    /**
     * @return Number of metrics contained in metric registry {@code metricRegistryName}.
     */
    private static int metricsCnt(IgniteEx ignite, String metricRegistryName) {
        MetricRegistry registry = ignite.context().metric().registry(metricRegistryName);

        int cnt = 0;

        for (Metric m : registry)
            ++cnt;

        return cnt;
    }

    /**
     * @return {@code True} if metrics registry found in {@code metricMgr} by name {@code registryName}.
     */
    private static boolean findMetric(GridMetricManager metricMgr, String registryName) {
        for (ReadOnlyMetricRegistry registry : metricMgr) {
            if (registry.name().startsWith(registryName))
                return true;
        }

        return false;
    }
}
