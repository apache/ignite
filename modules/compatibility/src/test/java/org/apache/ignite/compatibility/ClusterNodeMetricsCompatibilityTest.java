package org.apache.ignite.compatibility;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.eviction.fifo.FifoEvictionPolicy;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.ClusterMetricsMXBeanImpl;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.processors.cache.persistence.DataRegionMetricsImpl;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.mxbean.ClusterMetricsMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

/**
 *
 */
public class ClusterNodeMetricsCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Size of value in bytes. */
    private static final int VAL_SIZE = 512 * 1024;

    /** Amount of cache entries. */
    private static final int MAX_VALS_AMOUNT = 400;

    /** Cache name. */
    private final String CACHE_NAME = "cache1";

    /** Client mode. */
    private boolean clientMode = false;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(spi);

        cfg.setClientMode(clientMode);

        cfg.setCacheConfiguration();
        cfg.setMetricsUpdateFrequency(500);

        CacheConfiguration<Integer, Object> ccfg = defaultCacheConfiguration();
        ccfg.setName(CACHE_NAME);
        ccfg.setStatisticsEnabled(true);

        FifoEvictionPolicy plc = new FifoEvictionPolicy();
        plc.setMaxMemorySize(MAX_VALS_AMOUNT * VAL_SIZE);
        plc.setMaxSize(0);

        ccfg.setEvictionPolicy(plc);
        ccfg.setOnheapCacheEnabled(true);

        return cfg.setCacheConfiguration(ccfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAllocatedMemory() throws Exception {
        startGrid(1, "2.3.0", new ConfigurationClosure());
        final IgniteEx ignite0 = startGrid(0);

        final IgniteCache cache = ignite0.getOrCreateCache(CACHE_NAME);

        DataRegionMetricsImpl memMetrics = getDefaultMemoryPolicyMetrics(ignite0);

        memMetrics.enableMetrics();

        int pageSize = getPageSize(ignite0);

        assertEquals(0, memMetrics.getTotalAllocatedPages());

        fillCache(cache);

        assertTrue(memMetrics.getTotalAllocatedPages() * pageSize > MAX_VALS_AMOUNT
            * VAL_SIZE);
    }

    /**
     * @param ignite Ignite instance.
     */
    private DataRegionMetricsImpl getDefaultMemoryPolicyMetrics(IgniteEx ignite) throws IgniteCheckedException {
        return ignite.context().cache().context().database().dataRegion(null).memoryMetrics();
    }

    /**
     * @param ignite Ignite instance.
     */
    private int getPageSize(IgniteEx ignite) {
        return ignite.context().cache().context().database().pageSize();
    }

    /**
     * Fill cache with values.
     * @param cache Ignite cache.
     * @throws Exception If failed.
     */
    private void fillCache(final IgniteCache<Integer, Object> cache) throws Exception{
        final byte[] val = new byte[VAL_SIZE];

        for (int i = 0; i < MAX_VALS_AMOUNT * 4; i++)
            cache.put(i, val);

        // Let metrics update twice.
        final CountDownLatch latch = new CountDownLatch(2);

        grid(0).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt.type() == EVT_NODE_METRICS_UPDATED;

                latch.countDown();

                return true;
            }
        }, EVT_NODE_METRICS_UPDATED);

        // Wait for metrics update.
        latch.await();
    }

    /**
     * @param ignite Ignite.
     * @param nodeId Node id.
     */
    private void runJobAndWaitMetrics(final Ignite ignite, UUID nodeId)
        throws InterruptedException, IgniteInterruptedCheckedException {
        ClusterGroup grp = ignite.cluster().forNodeId(nodeId);

        final int totalExecutedJobs = ignite.cluster().metrics().getTotalExecutedJobs();

        ignite.compute(grp).run(new IgniteRunnable() {
            @Override public void run() {
                doSleep(1_000);
            }
        });

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ignite.cluster().metrics().getTotalExecutedJobs() > totalExecutedJobs;
            }
        }, 5000L);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJobMetrics() throws Exception {
        final Ignite ignite1 = startGrid(1, "2.3.0", new ConfigurationClosure());
        final Ignite ignite0 = startGrid(0);

        UUID locNodeId = ignite0.cluster().localNode().id();

        // Can't use ignite1.cluster(), because it sends compute task to the remote node and broke statistics
        UUID rmtNodeId = ((IgniteProcessProxy)ignite1).getId();

        runJobAndWaitMetrics(ignite0, rmtNodeId);

        ClusterMetrics metrics1 = ignite0.cluster().metrics();

        assertEquals(1, metrics1.getTotalExecutedJobs());
        // Not exists in version 2.3.0
        assertEquals(0, metrics1.getTotalJobsExecutionTime());

        runJobAndWaitMetrics(ignite0, locNodeId);

        ClusterMetrics metrics2 = ignite0.cluster().metrics();

        assertEquals(2, metrics2.getTotalExecutedJobs());
        assertTrue(metrics2.getTotalJobsExecutionTime() >= 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClusterNodeMetrics() throws Exception {
        final Ignite ignite1 = startGrid(1, "2.3.0", new ConfigurationClosure());
        final Ignite ignite0 = startGrid(0);

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return ignite0.cluster().nodes().size() == 2 && ignite1.cluster().nodes().size() == 2;
            }
        }, 3000L);

        ClusterMetrics metrics0 = ignite0.cluster().localNode().metrics();

        ClusterMetrics nodesMetrics =
            ignite0.cluster().forNodeId(ignite0.cluster().localNode().id(), ((IgniteProcessProxy)ignite1).getId())
                .metrics();

        assertEquals(metrics0.getTotalCpus(), nodesMetrics.getTotalCpus());
        assertEquals(1, metrics0.getTotalNodes());
        assertEquals(2, nodesMetrics.getTotalNodes());

        assert metrics0.getHeapMemoryUsed() > 0;
        assert metrics0.getHeapMemoryTotal() > 0;
    }

    /**
     * Test JMX metrics.
     *
     * @throws Exception If failed.
     */
    public void testJmxClusterMetrics() throws Exception {
        startGrid(1, "2.3.0", new ConfigurationClosure());

        Ignite node = startGrid(0);

        clientMode = true;

        startGrid(2);

        awaitPartitionMapExchange();

        JmxClusterMetricsHelper helperCluster = new JmxClusterMetricsHelper(node.configuration(),
            ClusterMetricsMXBeanImpl.class);

        assertEquals(node.cluster().topologyVersion(), helperCluster.attr("TopologyVersion"));

        assertEquals(2, helperCluster.attr("TotalServerNodes"));
        assertEquals(1, helperCluster.attr("TotalClientNodes"));
    }

    /**
     * Helper class to simplify ClusterMetricsMXBean testing.
     */
    private static class JmxClusterMetricsHelper {
        /** MBean server. */
        private final MBeanServer mbeanSrv;

        /** ClusterMetrics MX bean name. */
        private final ObjectName mbean;

        /**
         * @param cfg Ignite configuration.
         * @throws MalformedObjectNameException Thrown in case of any errors.
         */
        private JmxClusterMetricsHelper(IgniteConfiguration cfg, Class<? extends ClusterMetricsMXBean> clazz) throws MalformedObjectNameException {
            this.mbeanSrv = cfg.getMBeanServer();

            this.mbean = U.makeMBeanName(cfg.getIgniteInstanceName(), "Kernal", clazz.getSimpleName());
        }

        /**
         * Get MBean attribute through MBean server.
         *
         * @param name Attribute name.
         * @return Current value of attribute.
         * @throws Exception If failed.
         */
        private Object attr(String name) throws Exception {
            return mbeanSrv.getAttribute(mbean, name);
        }
    }

    /** */
    private static class ConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            GridIoMessageFactory.registerCustom(GridTestMessage.DIRECT_TYPE, new CO<Message>() {
                @Override public Message apply() {
                    return new GridTestMessage();
                }
            });

            cfg.setLocalHost("127.0.0.1");

            TcpDiscoverySpi disco = new TcpDiscoverySpi();
            disco.setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER);

            cfg.setDiscoverySpi(disco);
        }
    }
}
