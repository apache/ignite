package org.apache.ignite.internal.processors.service;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.service.inner.MyService;
import org.apache.ignite.internal.processors.service.inner.MyServiceFactory;
import org.apache.ignite.internal.processors.service.inner.NamingService;
import org.apache.ignite.internal.processors.service.inner.NamingServiceImpl;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.metric.HistogramMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.spi.metric.jmx.JmxMetricExporterSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_SERVICE_METRICS_ENABLED;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.serviceMetricRegistryName;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SERVICE_METRIC_REGISTRY;

/** Tests metrics of service invocations. */
public class GridServiceMetricsTest extends GridCommonAbstractTest {
    /** Number of service invcations. */
    private static final int INVOKE_CNT = 30;

    /** Utility holder of current grid number. */
    private final AtomicInteger gridNum = new AtomicInteger();

    /** Service name used in the tests. */
    private static final String SRVC_NAME = GridServiceMetricsTest.class.getSimpleName();

    /** Error message of created metrics. */
    private static final String METRICS_MUST_NOT_BE_CREATED = "Service metric registry must not be created.";

    /** Error message of unexisting metrics. */
    private static final String METRICS_MUST_BE_CREATED = "Service metric registry not found.";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // JMX metrics exposition for debug launch mode.
        cfg.setMetricExporterSpi(new JmxMetricExporterSpi());

        return cfg;
    }

    /** Checks metric behaviour when launched several service instances. */
    @Test
    public void testMetricsMultiple() throws Throwable {
        List<IgniteEx> servers = new ArrayList<>();

        servers.add(startGrid(gridNum.getAndIncrement()));

        IgniteEx server = servers.get(0);

        IgniteEx client = startClientGrid(gridNum.getAndIncrement());

        assertNull(METRICS_MUST_NOT_BE_CREATED, findMetric(server.context().metric(), SERVICE_METRIC_REGISTRY));

        int totalInstance = 2;

        int perNode = 2;

        server.services().deployMultiple(SRVC_NAME, MyServiceFactory.create(), totalInstance, perNode);

        awaitPartitionMapExchange();

        // Call proxies on the servers.
        Stream.generate(()->server.services().serviceProxy(SRVC_NAME, MyService.class, true))
            .limit(totalInstance).forEach(srvc->((MyService)srvc).hello());

        // Call proxies on the clients.
        Stream.generate(()->client.services().serviceProxy(SRVC_NAME, MyService.class, true))
            .limit(totalInstance).forEach(srvc->((MyService)srvc).hello());

        ReadOnlyMetricRegistry metrics = findMetric(server.context().metric(), SRVC_NAME);

        // Total service calls number.
        int callsCnt = 0;

        for(Metric m : metrics) {
            if (m instanceof HistogramMetric)
                callsCnt += LongStream.of(((HistogramMetric)m).value()).reduce(Long::sum).getAsLong();
        }

        assertEquals(callsCnt, totalInstance*2);

        // Add servers more that service instances.
        servers.add(startGrid(gridNum.getAndIncrement()));

        servers.add(startGrid(gridNum.getAndIncrement()));

        awaitPartitionMapExchange();

        int deployedCnt = 0;

        int metricsCnt = 0;

        for(IgniteEx ignite : servers){
            if( ignite.services().service(SRVC_NAME) != null )
                deployedCnt++;

            if( findMetric(ignite.context().metric(), SRVC_NAME) != null )
                metricsCnt++;
        }

        assertEquals(deployedCnt, metricsCnt);

        assertEquals(metricsCnt, totalInstance);
    }

    /** Makes sure {@code IgniteSystemProperties#IGNITE_SERVICE_METRICS_ENABLED} works correctly. */
    @Test
    public void testMetricsEnabledDisabled() throws Throwable {
        IgniteEx server = startGrid(0);

        assertNull(METRICS_MUST_NOT_BE_CREATED, findMetric(server.context().metric(), SRVC_NAME));

        server.services().deploy(serviceCfg(SRVC_NAME, 1, 1));

        awaitPartitionMapExchange();

        assertNotNull(METRICS_MUST_BE_CREATED, findMetric(server.context().metric(), SRVC_NAME));

        stopAllGrids();

        String prevMetricsEnabled = System.getProperty(IGNITE_SERVICE_METRICS_ENABLED);

        try {
            System.setProperty(IGNITE_SERVICE_METRICS_ENABLED, "false");

            server = startGrid(0);

            assertNull(METRICS_MUST_NOT_BE_CREATED, findMetric(server.context().metric(), SRVC_NAME));

            server.services().deploy(serviceCfg(SRVC_NAME, 1, 1));

            MyService srvc = server.services().service(SRVC_NAME);

            srvc.hello();

            srvc = server.services().serviceProxy(SRVC_NAME, MyService.class, false);

            srvc.hello();

            srvc = server.services().serviceProxy(SRVC_NAME, MyService.class, true);

            srvc.hello();

            IgniteEx client = startClientGrid(1);

            srvc = client.services().serviceProxy(SRVC_NAME, MyService.class, false);

            srvc.hello();

            srvc = client.services().serviceProxy(SRVC_NAME, MyService.class, true);

            srvc.hello();

            assertNull(METRICS_MUST_NOT_BE_CREATED, findMetric(server.context().metric(), SRVC_NAME));
        }
        finally {
            if (prevMetricsEnabled != null)
                System.setProperty(IGNITE_SERVICE_METRICS_ENABLED, prevMetricsEnabled);
            else
                System.clearProperty(IGNITE_SERVICE_METRICS_ENABLED);
        }
    }

    /** Ensures metric are created when service is deployed and removed when service is undeployed. */
    @Test
    public void testMetricDeplotmentUndeployment() throws Exception {
        List<IgniteEx> servers = startGrids(3, false);

        // 2 services per node.
        servers.get(0).services().deploy(serviceCfg(SRVC_NAME, servers.size(), 2));

        awaitPartitionMapExchange();

        // Make sure metrics are registered.
        for (IgniteEx ignite : servers)
            assertEquals(metricsCnt(ignite, SRVC_NAME), MyService.class.getDeclaredMethods().length);

        servers.get(0).services().cancel(SRVC_NAME);

        awaitPartitionMapExchange();

        for (IgniteEx ignite : servers)
            assertEquals(metricsCnt(ignite, SRVC_NAME), 0);
    }

    /** Checks two different histograms are created for the same short names of service methods. */
    @Test
    public void testMetricNaming() throws Exception {
        IgniteEx ignite = startGrid(1);

        ignite.services().deployNodeSingleton(SRVC_NAME, new NamingServiceImpl());

        NamingService srvc = ignite.services().serviceProxy(SRVC_NAME, NamingService.class, false);

        assertEquals(srvc.process(new org.apache.ignite.internal.processors.service.inner.impl.Param()),
            org.apache.ignite.internal.processors.service.inner.impl.Param.VALUE);

        assertEquals(srvc.process(new org.apache.ignite.internal.processors.service.inner.experimental.Param()),
            org.apache.ignite.internal.processors.service.inner.experimental.Param.VALUE);

        MetricRegistry registry = ignite.context().metric().registry(serviceMetricRegistryName(SRVC_NAME));

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
    public void testServiceInvocationMetricsOnLocals() throws Throwable {
        List<IgniteEx> grids = startGrids(3, false);

        int callCounter = 100;

        Method mtd = MyService.class.getMethod("hello");

        String mtdMetricName = IgniteServiceProcessor.methodMetricName(mtd, 0);

        grids.get(0).services().deployNodeSingleton(SRVC_NAME, MyServiceFactory.create());

        awaitPartitionMapExchange();

        // Call the service through local references.
        for (IgniteEx ignite : grids) {
            HistogramMetric invokeHistogram = null;

            MyService srv = ignite.services().service(SRVC_NAME);

            GridServiceProxy<Service> proxySticky = new GridServiceProxy<>(ignite.cluster(), SRVC_NAME, Service.class,
                true, 0, ignite.context());

            GridServiceProxy<Service> proxyAny = new GridServiceProxy<>(ignite.cluster(), SRVC_NAME, Service.class,
                false, 0, ignite.context());

            Object[] args = new Object[0];

            for (int i = 0; i < callCounter; ++i) {
                srv.hello();

                proxySticky.invokeMethod(mtd, args);

                proxyAny.invokeMethod(mtd, args);
            }

            MetricRegistry metrics = ignite.context().metric().registry(serviceMetricRegistryName(SRVC_NAME));

            assertNotNull("Metric registry not found.", metrics);

            for (Metric metric : metrics) {
                if (metric.name().endsWith(mtdMetricName) &&
                    metric instanceof HistogramMetric) {
                    assertNull("Duplicated invocation histogramm found.", invokeHistogram);

                    invokeHistogram = (HistogramMetric)metric;
                }
            }

            assertNotNull("Invocation histogramm not found", invokeHistogram);

            long calls = LongStream.of(invokeHistogram.value()).reduce(Long::sum).getAsLong();

            assertEquals("Wrong invocations calculated.", 3 * callCounter, calls);
        }
    }

    /** Tests service metrics for single service instance. */
    @Test
    public void testServiceInvocationMetricsProxiedSingle() throws Throwable {
        serviceInvocationMetricsProxied(1, 1, 1, 1);
    }

    /** Tests service metrics for multy service instance: one per server. */
    @Test
    public void testServiceInvocationMetricsProxiedMultyFlat() throws Throwable {
        serviceInvocationMetricsProxied(3, 5, 3, 1);
    }

    /** Tests service metrics for multy service instance: fewer that servers cnt. */
    @Test
    public void testServiceInvocationMetricsProxiedMultySkew() throws Throwable {
        serviceInvocationMetricsProxied(5, 3, 2, 1);
    }

    /** Tests service metrics for multy service instance: serveral instances per node. */
    @Test
    public void testServiceInvocationMetricsProxiedMultyFlatDuplicated() throws Throwable {
        serviceInvocationMetricsProxied(3, 2, 3, 3);
    }

    /** Tests service metrics for multy service instance: serveral instances per node, total fewer that servers cnt. */
    @Test
    public void testServiceInvocationMetricsProxiedMultySkewDuplicated() throws Throwable {
        serviceInvocationMetricsProxied(5, 2, 2, 2);
    }

    /**
     * Invoke service in various ways.
     *
     * @param serverCnt Number of server nodes.
     * @param clientCnt Number of client nodes.
     * @param perClusterCnt Number of service instances per cluster.
     * @param perClusterCnt Number of service instances per node.
     */
    private void serviceInvocationMetricsProxied(int serverCnt, int clientCnt, int perClusterCnt, int perNodeCnt)
        throws Throwable {

        List<IgniteEx> servers = startGrids(serverCnt, false);

        List<IgniteEx> clients = startGrids(clientCnt, true);

        Method mtd = MyService.class.getMethod("hello");

        Object[] methodArgs = new Object[0];

        String mtdMetricName = IgniteServiceProcessor.methodMetricName(mtd, 0);

        servers.get(0).services().deploy(serviceCfg(SRVC_NAME, perClusterCnt, perNodeCnt));

        awaitPartitionMapExchange();

        // Call service through the server proxies, not-sticky.
        for (int s = 0; s < INVOKE_CNT; ++s) {
            IgniteEx server = servers.get(s % servers.size());

            server.services().serviceProxy(SRVC_NAME, MyService.class, false).hello();

            // Direct call of sticky client proxy.
            new GridServiceProxy<>(server.cluster(), SRVC_NAME, Service.class, true, 0, server.context())
                .invokeMethod(mtd, methodArgs);

            // Direct call of non-sticky client proxy.
            new GridServiceProxy<>(server.cluster(), SRVC_NAME, Service.class, false, 0, server.context())
                .invokeMethod(mtd, methodArgs);
        }

        // Call service through the server proxies, sticky.
        List<MyService> stickies = servers.stream()
            .map(ignite -> (MyService)ignite.services().serviceProxy(SRVC_NAME, MyService.class, true))
            .collect(Collectors.toList());

        for (int ss = 0; ss < INVOKE_CNT; ++ss)
            stickies.get(ss % stickies.size()).hello();

        // Call service through the remote client proxies, not-sticky.
        for (int c = 0; c < INVOKE_CNT; ++c) {
            IgniteEx client = clients.get(c % clients.size());

            client.services().serviceProxy(SRVC_NAME, MyService.class, false).hello();

            // Direct call of sticky client proxy.
            new GridServiceProxy<>(client.cluster(), SRVC_NAME, Service.class, true, 0, client.context())
                .invokeMethod(mtd, methodArgs);

            // Direct call of non-sticky client proxy.
            new GridServiceProxy<>(client.cluster(), SRVC_NAME, Service.class, false, 0, client.context())
                .invokeMethod(mtd, methodArgs);
        }

        // Call service through the client proxies, sticky.
        stickies = clients.stream()
            .map(ignite -> (MyService)ignite.services().serviceProxy(SRVC_NAME, MyService.class, true))
            .collect(Collectors.toList());

        for (int cs = 0; cs < INVOKE_CNT; ++cs)
            stickies.get(cs % stickies.size()).hello();

        long invokeCollector = 0;

        // Calculate all the invocations.
        for (IgniteEx ignite : servers) {
            HistogramMetric invokeHistogram = null;

            ReadOnlyMetricRegistry metrics = null;

            for (ReadOnlyMetricRegistry mr : ignite.context().metric()) {
                if (mr.name().startsWith(SERVICE_METRIC_REGISTRY)) {
                    metrics = mr;
                    break;
                }
            }

            // Metrics may not be deployed on this server node.
            if (metrics == null)
                continue;

            for (Metric metric : metrics) {
                if (metric.name().startsWith(SERVICE_METRIC_REGISTRY) && metric.name().endsWith(mtdMetricName) &&
                    metric instanceof HistogramMetric) {
                    assertNull("Duplicated invocation histogramm found.", invokeHistogram);

                    invokeHistogram = (HistogramMetric)metric;
                }
            }

            assertNotNull("Invocation histogramm not found.", invokeHistogram);

            invokeCollector += LongStream.of(invokeHistogram.value()).reduce(Long::sum).getAsLong();
        }

        // Compare all 8 call approaches.
        assertEquals("Calculated wrong service invocation number.", INVOKE_CNT * 8, invokeCollector);
    }

    /** Provides test service configuration. */
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
     * @return Number of metrics contained in metric registry for {@code srvcName}.
     */
    private static int metricsCnt(IgniteEx ignite, String srvcName) {
        MetricRegistry registry = ignite.context().metric().registry(serviceMetricRegistryName(srvcName));

        int cnt = 0;

        for (Metric m : registry)
            ++cnt;

        return cnt;
    }

    /**
     * @return Metric registry if it is found in {@code metricMgr} by name {@code srvcName}. Null otherwise.
     */
    private static ReadOnlyMetricRegistry findMetric(GridMetricManager metricMgr, String srvcName) {
        for (ReadOnlyMetricRegistry registry : metricMgr) {
            if (registry.name().equals(serviceMetricRegistryName(srvcName)))
                return registry;
        }

        return null;
    }
}
