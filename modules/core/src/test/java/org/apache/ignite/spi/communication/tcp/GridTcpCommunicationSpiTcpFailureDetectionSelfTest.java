/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class GridTcpCommunicationSpiTcpFailureDetectionSelfTest extends GridTcpCommunicationSpiTcpSelfTest {
    /** */
    private final static int SPI_COUNT = 4;

    private static TcpCommunicationSpi spis[] = new TcpCommunicationSpi[SPI_COUNT];

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
    @Test
    public void testFailureDetectionEnabled() throws Exception {
        assertTrue(spis[0].failureDetectionTimeoutEnabled());
        assertTrue(spis[0].failureDetectionTimeout() == IgniteConfiguration.DFLT_FAILURE_DETECTION_TIMEOUT);

        for (int i = 1; i < SPI_COUNT; i++) {
            assertFalse(spis[i].failureDetectionTimeoutEnabled());
            assertEquals(0, spis[i].failureDetectionTimeout());
        }
    }
}
