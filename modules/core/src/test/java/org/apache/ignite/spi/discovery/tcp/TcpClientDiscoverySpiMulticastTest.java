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

package org.apache.ignite.spi.discovery.tcp;

import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.DiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_DISCONNECTED;
import static org.apache.ignite.events.EventType.EVT_CLIENT_NODE_RECONNECTED;

/**
 *
 */
public class TcpClientDiscoverySpiMulticastTest extends GridCommonAbstractTest {
    /** */
    private boolean forceSrv;

    /** */
    private ThreadLocal<Boolean> client = new ThreadLocal<>();

    /** */
    private ThreadLocal<Integer> discoPort = new ThreadLocal<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryMulticastIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();

        ipFinder.setAddressRequestAttempts(5);

        spi.setIpFinder(ipFinder);

        Boolean clientFlag = client.get();

        client.set(null);

        if (clientFlag != null && clientFlag) {
            cfg.setClientMode(true);

            spi.setForceServerMode(forceSrv);
        }
        else {
            Integer port = discoPort.get();

            discoPort.set(null);

            if (port != null)
                spi.setLocalPort(port);
        }

        cfg.setDiscoverySpi(spi);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClientStartsFirst() throws Exception {
        IgniteInternalFuture<Ignite> fut = GridTestUtils.runAsync(new Callable<Ignite>() {
            @Override public Ignite call() throws Exception {
                client.set(true);

                return startGrid(0);
            }
        }, "start-client");

        U.sleep(10_000);

        discoPort.set(TcpDiscoverySpi.DFLT_PORT);

        Ignite srv = startGrid(1);

        Ignite client = fut.get();

        final CountDownLatch reconnectLatch = new CountDownLatch(1);

        final CountDownLatch disconnectLatch = new CountDownLatch(1);

        client.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                info("Client event: " + evt);

                if (evt.type() == EVT_CLIENT_NODE_DISCONNECTED) {
                    assertEquals(1, reconnectLatch.getCount());

                    disconnectLatch.countDown();
                }
                else if (evt.type() == EVT_CLIENT_NODE_RECONNECTED) {
                    assertEquals(0, disconnectLatch.getCount());

                    reconnectLatch.countDown();
                }

                return true;
            }
        }, EVT_CLIENT_NODE_DISCONNECTED, EVT_CLIENT_NODE_RECONNECTED);

        srv.close();

        assertTrue(disconnectLatch.await(30, SECONDS));

        discoPort.set(TcpDiscoverySpi.DFLT_PORT + 100);

        startGrid(1);

        assertTrue(reconnectLatch.await(30, SECONDS));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWithMulticast() throws Exception {
        joinWithMulticast();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testJoinWithMulticastForceServer() throws Exception {
        forceSrv = true;

        joinWithMulticast();
    }

    /**
     * @throws Exception If failed.
     */
    private void joinWithMulticast() throws Exception {
        Ignite ignite0 = startGrid(0);

        assertSpi(ignite0, false);

        client.set(true);

        Ignite ignite1 = startGrid(1);

        assertTrue(ignite1.configuration().isClientMode());

        assertSpi(ignite1, !forceSrv);

        assertTrue(ignite1.configuration().isClientMode());

        assertEquals(2, ignite0.cluster().nodes().size());
        assertEquals(2, ignite1.cluster().nodes().size());

        client.set(false);

        Ignite ignite2 = startGrid(2);

        assertSpi(ignite2, false);

        assertEquals(3, ignite0.cluster().nodes().size());
        assertEquals(3, ignite1.cluster().nodes().size());
        assertEquals(3, ignite2.cluster().nodes().size());
    }

    /**
     * @param ignite Ignite.
     * @param client Expected client mode flag.
     */
    private void assertSpi(Ignite ignite, boolean client) {
        DiscoverySpi spi = ignite.configuration().getDiscoverySpi();

        assertSame(TcpDiscoverySpi.class, spi.getClass());

        TcpDiscoverySpi spi0 = (TcpDiscoverySpi)spi;

        assertSame(TcpDiscoveryMulticastIpFinder.class, spi0.getIpFinder().getClass());

        assertEquals(client, spi0.isClientMode());

        Collection<Object> addrSnds = GridTestUtils.getFieldValue(spi0.getIpFinder(), "addrSnds");

        assertEquals(client, F.isEmpty(addrSnds));
    }
}
