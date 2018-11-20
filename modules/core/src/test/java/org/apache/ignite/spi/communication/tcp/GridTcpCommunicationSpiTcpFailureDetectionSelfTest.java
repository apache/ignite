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
import org.apache.ignite.spi.communication.CommunicationSpi;

/**
 *
 */
public class GridTcpCommunicationSpiTcpFailureDetectionSelfTest extends GridTcpCommunicationSpiTcpSelfTest {
    /** */
    private final static int SPI_COUNT = 4;

    private TcpCommunicationSpi spis[] = new TcpCommunicationSpi[SPI_COUNT];

    /** {@inheritDoc} */
    @Override protected int getSpiCount() {
        return SPI_COUNT;
    }

    /** {@inheritDoc} */
    @Override protected CommunicationSpi getSpi(int idx) {
        TcpCommunicationSpi spi = (TcpCommunicationSpi)super.getSpi(idx);

        switch (idx) {
            case 0:
                // Ignore
                break;
            case 1:
                spi.setConnectTimeout(4000);
                break;
            case 2:
                spi.setMaxConnectTimeout(TcpCommunicationSpi.DFLT_MAX_CONN_TIMEOUT);
                break;
            case 3:
                spi.setReconnectCount(2);
                break;
            default:
                assert false;
        }

        spis[idx] = spi;

        return spi;
    }

    /**
     * @throws Exception if failed.
     */
    public void testFailureDetectionEnabled() throws Exception {
        assertTrue(spis[0].failureDetectionTimeoutEnabled());
        assertTrue(spis[0].failureDetectionTimeout() == IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT);

        for (int i = 1; i < SPI_COUNT; i++) {
            assertFalse(spis[i].failureDetectionTimeoutEnabled());
            assertEquals(0, spis[i].failureDetectionTimeout());
        }
    }
}