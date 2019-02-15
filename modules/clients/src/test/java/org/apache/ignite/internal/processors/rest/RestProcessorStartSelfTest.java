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

package org.apache.ignite.internal.processors.rest;

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class RestProcessorStartSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String HOST = "127.0.0.1";

    /** */
    public static final int TCP_PORT = 11222;

    /** */
    private CountDownLatch gridReady;

    /** */
    private CountDownLatch proceed;

    /** {@inheritDoc}*/
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost(HOST);

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setPort(TCP_PORT);

        cfg.setConnectorConfiguration(clientCfg);

        TestDiscoverySpi disc = new TestDiscoverySpi();

        disc.setIpFinder(sharedStaticIpFinder);

        cfg.setDiscoverySpi(disc);

        return cfg;
    }

    /** {@inheritDoc}*/
    @Override protected void beforeTest() throws Exception {
        gridReady = new CountDownLatch(1);
        proceed = new CountDownLatch(1);
    }

    /** {@inheritDoc}*/
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *  @throws Exception If failed.
     */
    @Test
    public void testTcpStart() throws Exception {
        GridClientConfiguration clCfg = new GridClientConfiguration();

        clCfg.setProtocol(GridClientProtocol.TCP);
        clCfg.setServers(Collections.singleton(HOST + ":" + TCP_PORT));

        doTest(clCfg);
    }

    /**
     * @param cfg Client configuration.
     * @throws Exception If failed.
     */
    private void doTest(final GridClientConfiguration cfg) throws Exception {
        GridTestUtils.runAsync(new IgniteCallable<Object>() {
            @Override public Object call() {
                try {
                    startGrid();
                }
                catch (Exception e) {
                    log().error("Grid start failed", e);

                    fail();
                }

                return null;
            }
        });

        try {
            gridReady.await();

            IgniteInternalFuture<GridClient> c = GridTestUtils.runAsync(new Callable<GridClient>() {
                @Override public GridClient call() throws Exception {
                    return GridClientFactory.start(cfg);
                }
            });

            try {
                proceed.countDown();

                c.get().compute().refreshTopology(false, false);
            }
            finally {
                GridClientFactory.stopAll();
            }
        }
        catch (Throwable e) {
            e.printStackTrace();
        }
        finally {
            proceed.countDown();
        }
    }

    /**
     * Test SPI.
     */
    private class TestDiscoverySpi extends TcpDiscoverySpi {
        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            gridReady.countDown();

            try {
                proceed.await();
            }
            catch (InterruptedException e) {
                throw new IgniteSpiException("Failed to await start signal.", e);
            }

            super.spiStart(igniteInstanceName);
        }
    }
}
