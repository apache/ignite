/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.junit.Test;

/**
 *
 */
public class GridTcpCommunicationSpiRecoveryFailureDetectionSelfTest extends GridTcpCommunicationSpiRecoverySelfTest {
    /** {@inheritDoc} */
    @Override protected TcpCommunicationSpi getSpi(int idx) {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setSharedMemoryPort(-1);
        spi.setLocalPort(port++);
        spi.setIdleConnectionTimeout(10_000);
        spi.setAckSendThreshold(5);
        spi.setSocketSendBuffer(512);
        spi.setSocketReceiveBuffer(512);
        spi.setConnectionsPerNode(1);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected long awaitForSocketWriteTimeout() {
        return IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT + 5_000;
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testFailureDetectionEnabled() throws Exception {
        for (TcpCommunicationSpi spi: spis) {
            assertTrue(spi.failureDetectionTimeoutEnabled());
            assertTrue(spi.failureDetectionTimeout() == IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT);
        }
    }
}
