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

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.testframework.GridTestUtils.getFreeCommPort;

/**
 * TCP communication SPI config test.
 */
@GridSpiTest(spi = TcpCommunicationSpi.class, group = "Communication SPI")
@RunWith(JUnit4.class)
public class GridTcpCommunicationSpiConfigSelfTest extends GridSpiAbstractConfigTest<TcpCommunicationSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPort", 1023);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPort", 65636);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "localPortRange", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "idleConnectionTimeout", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketReceiveBuffer", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketSendBuffer", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "messageQueueLimit", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "reconnectCount", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "selectorsCount", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "maxConnectTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "socketWriteTimeout", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "ackSendThreshold", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "ackSendThreshold", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "unacknowledgedMessagesBufferSize", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", 0);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", -1);
        checkNegativeSpiProperty(new TcpCommunicationSpi(), "connectionsPerNode", Integer.MAX_VALUE);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLocalPortRange() throws Exception {
        IgniteConfiguration cfg = getConfiguration();

        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

        commSpi.setLocalPortRange(0);
        commSpi.setLocalPort(getFreeCommPort());

        cfg.setCommunicationSpi(commSpi);

        startGrid(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }
}
