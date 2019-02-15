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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridAbstractCommunicationSelfTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link TcpCommunicationSpi}
 */
@RunWith(JUnit4.class)
abstract class GridTcpCommunicationSpiAbstractTest extends GridAbstractCommunicationSelfTest<CommunicationSpi> {
    /** */
    private static final int SPI_COUNT = 3;

    /** */
    public static final int IDLE_CONN_TIMEOUT = 2000;

    /** */
    private final boolean useShmem;

    /**
     * @param useShmem Use shared mem flag.
     */
    protected GridTcpCommunicationSpiAbstractTest(boolean useShmem) {
        this.useShmem = useShmem;
    }

    /** {@inheritDoc} */
    @Override protected CommunicationSpi getSpi(int idx) {
        TcpCommunicationSpi spi = new TcpCommunicationSpi();

        if (!useShmem)
            spi.setSharedMemoryPort(-1);

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
    @Test
    @Override public void testSendToManyNodes() throws Exception {
        super.testSendToManyNodes();

        // Test idle clients remove.
        for (CommunicationSpi spi : spis.values()) {
            ConcurrentMap<UUID, GridCommunicationClient> clients = U.field(spi, "clients");

            assertEquals(getSpiCount() - 1, clients.size());

            clients.put(UUID.randomUUID(), F.first(clients.values()));
        }
    }

    /**
     *
     */
    @Test
    public void testCheckConnection1() {
        for (int i = 0; i < 100; i++) {
            for (Map.Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
                TcpCommunicationSpi spi = (TcpCommunicationSpi)entry.getValue();

                List<ClusterNode> checkNodes = new ArrayList<>(nodes);

                assert checkNodes.size() > 1;

                IgniteFuture<BitSet> fut = spi.checkConnection(checkNodes);

                BitSet res = fut.get();

                for (int n = 0; n < checkNodes.size(); n++)
                    assertTrue(res.get(n));
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCheckConnection2() throws Exception {
        final int THREADS = spis.size();

        final CyclicBarrier b = new CyclicBarrier(THREADS);

        List<IgniteInternalFuture> futs = new ArrayList<>();

        for (Map.Entry<UUID, CommunicationSpi<Message>> entry : spis.entrySet()) {
            final TcpCommunicationSpi spi = (TcpCommunicationSpi)entry.getValue();

            futs.add(GridTestUtils.runAsync(new Callable() {
                @Override public Object call() throws Exception {
                    List<ClusterNode> checkNodes = new ArrayList<>(nodes);

                    assert checkNodes.size() > 1;

                    b.await();

                    for (int i = 0; i < 100; i++) {
                        IgniteFuture<BitSet> fut = spi.checkConnection(checkNodes);

                        BitSet res = fut.get();

                        for (int n = 0; n < checkNodes.size(); n++)
                            assertTrue(res.get(n));
                    }

                    return null;
                }
            }));
        }

        for (IgniteInternalFuture f : futs)
            f.get();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        for (CommunicationSpi spi : spis.values()) {
            ConcurrentMap<UUID, GridCommunicationClient[]> clients = U.field(spi, "clients");

            for (int i = 0; i < 20; i++) {
                GridCommunicationClient client0 = null;

                for (GridCommunicationClient[] clients0 : clients.values()) {
                    for (GridCommunicationClient client : clients0) {
                        if (client != null) {
                            client0 = client;

                            break;
                        }
                    }

                    if (client0 != null)
                        break;
                }

                if (client0 == null)
                    return;

                info("Check failed for SPI [igniteInstanceName=" +
                    GridTestUtils.getFieldValue(spi, IgniteSpiAdapter.class, "igniteInstanceName") +
                    ", client=" + client0 +
                    ", spi=" + spi + ']');

                U.sleep(1000);
            }

            fail("Failed to wait when clients are closed.");
        }
    }
}
