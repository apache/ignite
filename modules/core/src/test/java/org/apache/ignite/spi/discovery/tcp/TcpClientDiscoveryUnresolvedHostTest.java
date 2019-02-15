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

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutException;
import org.apache.ignite.spi.IgniteSpiOperationTimeoutHelper;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Client-based discovery SPI test with unresolved server hosts.
 */
public class TcpClientDiscoveryUnresolvedHostTest extends GridCommonAbstractTest {
    /** */
    TestTcpDiscoverySpi spi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg =  super.getConfiguration(igniteInstanceName);

        spi = new TestTcpDiscoverySpi();

        cfg.setDiscoverySpi(spi.setJoinTimeout(5000).setIpFinder(new TcpDiscoveryVmIpFinder()
            .setAddresses(Collections.singletonList("test:47500"))));

        cfg.setCacheConfiguration();

        cfg.setClientMode(true);

        return cfg;
    }

    /**
     * Test that sockets closed after exception.
     *
     * @throws Exception in case of error.
     */
    @Test
    public void test() throws Exception {
        try {
            startGrid(0);
        } catch (IgniteCheckedException e) {
            //Ignore.
        }

        assertEquals(0, spi.getSockets().size());
    }

    /**
     * TcpDiscoverySpi implementation with additional storing of created sockets.
     */
    private static class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** */
        Set<Socket> sockets = new HashSet<>();

        /** {@inheritDoc} */
        @Override Socket createSocket() throws IOException {
            Socket socket = super.createSocket();

            sockets.add(socket);

            return socket;
        }

        /** {@inheritDoc} */
        @Override protected Socket openSocket(Socket sock, InetSocketAddress remAddr, IgniteSpiOperationTimeoutHelper timeoutHelper)
            throws IOException, IgniteSpiOperationTimeoutException {

            try {
                return super.openSocket(sock, remAddr, timeoutHelper);
            }
            catch (IgniteSpiOperationTimeoutException | IOException e) {
                if (sock.isClosed())
                    sockets.remove(sock);

                throw e;
            }
        }

        /**
         * Gets list of sockets opened by this discovery spi.
         *
         * @return List of sockets.
         */
        public Set<Socket> getSockets() {
            return sockets;
        }
    }
}
