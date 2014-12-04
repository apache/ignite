/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp;

import org.gridgain.testframework.junits.spi.*;

/**
 *
 */
@GridSpiTest(spi = GridTcpDiscoverySpi.class, group = "Discovery SPI")
public class GridTcpDiscoverySpiConfigSelfTest extends GridSpiAbstractConfigTest<GridTcpDiscoverySpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "ipFinder", null);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "ipFinderCleanFrequency", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "localPort", 1023);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "localPortRange", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "networkTimeout", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "socketTimeout", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "ackTimeout", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "maxAckTimeout", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "heartbeatFrequency", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "threadPriority", -1);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "maxMissedHeartbeats", 0);
        checkNegativeSpiProperty(new GridTcpDiscoverySpi(), "statisticsPrintFrequency", 0);
    }
}
