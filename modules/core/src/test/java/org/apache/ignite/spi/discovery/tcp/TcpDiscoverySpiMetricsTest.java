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

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;
import static org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi.SOCKET_WRITE_TIMEOUTS_CNT;

/** Tests {@link TcpDiscoverySpi} metrics registered in the {@code io.discovery} metric registry. */
public class TcpDiscoverySpiMetricsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /** */
    @Test
    public void testSocketWriteTimeoutsMetric() throws Exception {
        IgniteEx ignite = startGrid(0);

        LongMetric metric = ignite.context().metric().registry(DISCO_METRICS).findMetric(SOCKET_WRITE_TIMEOUTS_CNT);

        assertEquals(0L, metric.value());

        invokeOnTimeout((TcpDiscoverySpi)ignite.context().discovery().getInjectedDiscoverySpi(),
            new TcpDiscoveryIoSession(ignite.context(), connectedLoopbackSocket()));

        assertEquals(1L, metric.value());
    }

    /**
     * @param spi Discovery SPI.
     * @param ses IO session to pass to the timeout object.
     * @throws Exception if reflection fails.
     */
    private static void invokeOnTimeout(TcpDiscoverySpi spi, TcpDiscoveryIoSession ses) throws Exception {
        Class<?> cls = Arrays.stream(spi.getClass().getDeclaredClasses())
            .filter(c -> "SocketTimeoutObject".equals(c.getSimpleName()))
            .findFirst()
            .orElseThrow();

        Constructor<?> ctor = cls.getDeclaredConstructor(TcpDiscoverySpi.class, TcpDiscoveryIoSession.class, long.class);

        ctor.setAccessible(true);

        GridTestUtils.invoke(ctor.newInstance(spi, ses, Long.MAX_VALUE), "onTimeout");
    }

    /**
     * @return A connected client socket.
     * @throws IOException If failed.
     */
    private static Socket connectedLoopbackSocket() throws IOException {
        try (ServerSocket srv = new ServerSocket()) {
            srv.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));

            Socket client = new Socket();

            client.connect(srv.getLocalSocketAddress());

            try (Socket ignored = srv.accept()) {
                return client;
            }
        }
    }
}
