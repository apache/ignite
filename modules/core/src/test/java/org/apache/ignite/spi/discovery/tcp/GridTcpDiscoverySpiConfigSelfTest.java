/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.discovery.tcp;

import org.gridgain.testframework.junits.spi.*;

/**
 *
 */
@GridSpiTest(spi = TcpDiscoverySpi.class, group = "Discovery SPI")
public class GridTcpDiscoverySpiConfigSelfTest extends GridSpiAbstractConfigTest<TcpDiscoverySpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ipFinder", null);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ipFinderCleanFrequency", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "localPort", 1023);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "localPortRange", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "networkTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "socketTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "ackTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "maxAckTimeout", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "heartbeatFrequency", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "threadPriority", -1);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "maxMissedHeartbeats", 0);
        checkNegativeSpiProperty(new TcpDiscoverySpi(), "statisticsPrintFrequency", 0);
    }
}
