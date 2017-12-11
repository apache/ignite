package org.apache.ignite.compatibility;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.UUID;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.processors.cache.GridCacheAbstractFullApiSelfTest;
import org.apache.ignite.internal.util.typedef.CO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMBean;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;

/**
 *
 */
public class TcpCommunicationSpiStatisticsCompatibilityTest extends IgniteCompatibilityAbstractTest {
    static {
        GridIoMessageFactory.registerCustom(GridTestMessage.DIRECT_TYPE, new CO<Message>() {
            @Override public Message apply() {
                return new GridTestMessage();
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(GridCacheAbstractFullApiSelfTest.LOCAL_IP_FINDER));

        return cfg;
    }

    /**
     * Gets TcpCommunicationSpiMBean for given node.
     *
     * @param nodeIdx Node index.
     * @return MBean instance.
     */
    private TcpCommunicationSpiMBean mbean(int nodeIdx) throws MalformedObjectNameException {
        ObjectName mbeanName = U.makeMBeanName(getTestIgniteInstanceName(nodeIdx), "SPIs",
            TcpCommunicationSpi.class.getSimpleName());

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (mbeanSrv.isRegistered(mbeanName))
            return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TcpCommunicationSpiMBean.class,
                true);
        else
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return null;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStatistics() throws Exception {
        startGrid(1, "2.3.0", new ConfigurationClosure());
        startGrid(0);

        int MSG_CNT = 20;

        try {
            UUID remoteId = grid(1).localNode().id();

            // Send custom messages from node0 to node1.
            for (int i = 0; i < MSG_CNT; i++) {
                grid(0).configuration().getCommunicationSpi().sendMessage(
                    grid(0).cluster().node(remoteId), new GridTestMessage());

                doSleep(1_000);
            }

            ClusterGroup clusterGrpNode1 = grid(0).cluster().forNodeId(remoteId);

            // Send job from node0 to node1.
            grid(0).compute(clusterGrpNode1).call(new IgniteCallable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    return Boolean.TRUE;
                }
            });

            TcpCommunicationSpiMBean mbean0 = mbean(0);

            Map<String, Long> msgsSentByType0 = mbean0.getSentMessagesByType();

            assertEquals(MSG_CNT, msgsSentByType0.get(GridTestMessage.class.getSimpleName()).longValue());
        }
        finally {
            stopAllGrids();
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
