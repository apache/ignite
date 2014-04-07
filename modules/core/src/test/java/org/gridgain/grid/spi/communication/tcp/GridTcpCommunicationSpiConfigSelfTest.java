/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.communication.tcp;

import org.gridgain.testframework.junits.spi.*;

/**
 * TCP communication SPI config test.
 */
@GridSpiTest(spi = GridTcpCommunicationSpi.class, group = "Communication SPI")
public class GridTcpCommunicationSpiConfigSelfTest extends GridSpiAbstractConfigTest<GridTcpCommunicationSpi> {
    /**
     * @throws Exception If failed.
     */
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "localPort", 1023);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "localPort", 65636);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "localPortRange", -1);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "idleConnectionTimeout", 0);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "connectionBufferSize", -1);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "connectionBufferFlushFrequency", 0);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "socketReceiveBuffer", -1);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "socketSendBuffer", -1);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "messageQueueLimit", -1);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "sharedMemoryPort", 0);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "sharedMemoryPort", -2);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "selectorsCount", 0);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "minimumBufferedMessageCount", -1);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "bufferSizeRatio", 0);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "connectTimeout", -1);
        checkNegativeSpiProperty(new GridTcpCommunicationSpi(), "maxConnectTimeout", -1);
    }
}
