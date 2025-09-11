package org.apache.ignite.spi.discovery.datacenter;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryJoinRequestMessage;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class DataCenterResolverTest extends GridCommonAbstractTest {
    private static final String DC_ID = "DC_1";

    /** */
    private TcpDiscoverySpi tcpDiscoSpi;

    /** */
    private final AtomicReference<String> dcIdRef = new AtomicReference<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataCenterResolver(new SystemPropertyDataCenterResolver());

        if (tcpDiscoSpi != null) {
            tcpDiscoSpi.setIpFinder(((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder());

            cfg.setDiscoverySpi(tcpDiscoSpi);
        }

        cfg.setConsistentId(igniteInstanceName);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_DATA_CENTER_ID", value = DC_ID)
    public void testAttributeSetLocallyFromSysProp() throws Exception {
        IgniteEx testGrid = startGrid();

        Object dcId = testGrid.localNode().attribute(IgniteNodeAttributes.ATTR_DATA_CENTER_ID);

        assertEquals(DC_ID, dcId);
    }

    /**
     *
     * @throws Exception If failed.
     */
    @Test
    @WithSystemProperty(key = "IGNITE_DATA_CENTER_ID", value = DC_ID)
    public void testAttributeIsSentInJoinRequest() throws Exception {
        tcpDiscoSpi = new TcpDiscoverySpi() {
            @Override protected void startMessageProcess(TcpDiscoveryAbstractMessage msg) {
                if (msg instanceof TcpDiscoveryJoinRequestMessage) {
                    TcpDiscoveryJoinRequestMessage joinReq = (TcpDiscoveryJoinRequestMessage)msg;

                    dcIdRef.set(joinReq.node().getAttributes().get(IgniteNodeAttributes.ATTR_DATA_CENTER_ID).toString());
                }
            }
        };

        startGrid("crd"); //start coordinator

        tcpDiscoSpi = null;

        startGrid("srv0"); //start another server node

        waitForTopology(2);

        assertEquals(DC_ID, dcIdRef.get());
    }
}
