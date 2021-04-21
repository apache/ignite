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

import java.util.concurrent.TimeUnit;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for communication over discovery feature (inverse communication request).
 */
public class GridTotallyUnreachableClientTest extends GridCommonAbstractTest {
    /** */
    private boolean forceClientToSrvConnections;

    /** */
    private int locPort;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        forceClientToSrvConnections = false;
        locPort = TcpCommunicationSpi.DFLT_PORT;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(8_000);

        cfg.setCommunicationSpi(
            new TcpCommunicationSpi()
                .setForceClientToServerConnections(forceClientToSrvConnections)
                .setLocalPort(locPort)
        );

        return cfg;
    }

    /**
     * Test that you can't send anything from client to another client that has "-1" local port.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTotallyUnreachableClient() throws Exception {
        IgniteEx srv = startGrid(0);

        locPort = -1;
        IgniteEx client1 = startClientGrid(1);
        ClusterNode clientNode1 = client1.localNode();

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(() ->
            srv.context().io().sendIoTest(clientNode1, new byte[10], false).get()
        );

        fut.get(30, TimeUnit.SECONDS);

        locPort = TcpCommunicationSpi.DFLT_PORT;

        IgniteEx client2 = startClientGrid(2);

        GridTestUtils.assertThrowsAnyCause(log, () -> {
            return GridTestUtils.runAsync(() ->
                client2.context().io().sendIoTest(clientNode1, new byte[10], false).get()
            ).get(30, TimeUnit.SECONDS);
        }, IgniteSpiException.class, "Cannot send");
    }
}
