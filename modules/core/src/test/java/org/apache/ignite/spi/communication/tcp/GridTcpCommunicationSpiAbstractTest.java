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

import org.apache.ignite.internal.util.nio.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.testframework.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Test for {@link TcpCommunicationSpi}
 */
abstract class GridTcpCommunicationSpiAbstractTest extends GridAbstractCommunicationSelfTest<CommunicationSpi> {
    /** */
    private static final int SPI_COUNT = 3;

    /** */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** {@inheritDoc} */
    @Override protected CommunicationSpi getSpi(int idx) {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        spi.setLocalPort(GridTestUtils.getNextCommPort(getClass()));
        spi.setIdleConnectionTimeout(IDLE_CONN_TIMEOUT);
        spi.setTcpNoDelay(tcpNoDelay());

        return spi;
    }

    /**
     * @return Value of property '{@link TcpCommunicationSpi#isTcpNoDelay()}'.
     */
    protected abstract boolean tcpNoDelay();

    /** {@inheritDoc} */
    @Override protected int getSpiCount() {
        return SPI_COUNT;
    }

    /** {@inheritDoc} */
    @Override public void testSendToManyNodes() throws Exception {
        super.testSendToManyNodes();

        // Test idle clients remove.
        for (CommunicationSpi spi : spis.values()) {
            ConcurrentMap<UUID, GridTcpCommunicationClient> clients = U.field(spi, "clients");

            assertEquals(2, clients.size());

            clients.put(UUID.randomUUID(), F.first(clients.values()));
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (CommunicationSpi spi : spis.values()) {
            ConcurrentMap<UUID, GridTcpCommunicationClient> clients = U.field(spi, "clients");

            for (int i = 0; i < 20 && !clients.isEmpty(); i++) {
                info("Check failed for SPI [grid=" + GridTestUtils.getFieldValue(spi, "gridName") +
                    ", spi=" + spi + ']');

                U.sleep(1000);
            }

            assert clients.isEmpty() : "Clients: " + clients;
        }
    }
}
