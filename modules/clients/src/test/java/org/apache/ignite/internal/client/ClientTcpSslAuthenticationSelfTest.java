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

package org.apache.ignite.internal.client;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.X509TrustManager;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.balancer.GridClientRoundRobinBalancer;
import org.apache.ignite.internal.client.impl.GridClientImpl;
import org.apache.ignite.internal.client.ssl.GridSslBasicContextFactory;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests
 */
public class ClientTcpSslAuthenticationSelfTest extends GridCommonAbstractTest {
    /** REST TCP port. */
    private static final int REST_TCP_PORT = 12121;

    /** Test trust manager for server. */
    private MockX509TrustManager srvTrustMgr = new MockX509TrustManager();

    /** Test trust manager for client. */
    private MockX509TrustManager clientTrustMgr = new MockX509TrustManager();

    /** Whether server should check clients. */
    private volatile boolean checkClient;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assertEquals(0, srvTrustMgr.serverCheckCallCount());
        assertEquals(0, clientTrustMgr.clientCheckCallCount());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        srvTrustMgr.reset();
        clientTrustMgr.reset();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setLocalHost(getTestResources().getLocalHost());

        assert c.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setPort(REST_TCP_PORT);
        clientCfg.setSslEnabled(true);

        clientCfg.setSslClientAuth(checkClient);
        clientCfg.setSslClientAuth(checkClient);

        GridSslBasicContextFactory factory = (GridSslBasicContextFactory)GridTestUtils.sslContextFactory();

        factory.setTrustManagers(srvTrustMgr);

        clientCfg.setSslContextFactory(factory);

        c.setConnectorConfiguration(clientCfg);

        return c;
    }

    /**
     * Creates client that will try to connect to only first node in grid.
     *
     * @return Client.
     * @throws Exception If failed to create client.
     */
    private GridClientImpl createClient() throws Exception {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setServers(Arrays.asList(U.getLocalHost().getHostAddress() + ":" + REST_TCP_PORT));
        cfg.setBalancer(new GridClientRoundRobinBalancer());

        GridSslBasicContextFactory factory = (GridSslBasicContextFactory)GridTestUtils.sslContextFactory();

        factory.setTrustManagers(clientTrustMgr);

        cfg.setSslContextFactory(factory);

        return (GridClientImpl)GridClientFactory.start(cfg);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerAuthenticated() throws Exception {
        checkServerAuthenticatedByClient(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testServerNotAuthenticatedByClient() throws Exception {
        try {
            checkServerAuthenticatedByClient(true);
        }
        catch (GridClientDisconnectedException e) {
            assertTrue(X.hasCause(e, GridServerUnreachableException.class));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientAuthenticated() throws Exception {
        checkClientAuthenticatedByServer(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClientNotAuthenticated() throws Exception {
        try {
            checkServerAuthenticatedByClient(true);
        }
        catch (GridClientDisconnectedException e) {
            assertTrue(X.hasCause(e, GridServerUnreachableException.class));
        }
    }

    /**
     * @param fail Should client trust manager fail.
     * @throws Exception If failed.
     */
    private void checkServerAuthenticatedByClient(boolean fail) throws Exception {
        checkClient = false;
        srvTrustMgr.shouldFail(false);
        clientTrustMgr.shouldFail(fail);

        startGrid();

        try {
            try (GridClientImpl c = createClient()) {
                c.compute().refreshTopology(false, false);
            }
        }
        finally {
            G.stopAll(false);
        }

        assertEquals(0, srvTrustMgr.clientCheckCallCount());
        assertEquals(1, clientTrustMgr.serverCheckCallCount());
    }

    /**
     * @param fail Should server trust manager fail.
     * @throws Exception If failed.
     */
    private void checkClientAuthenticatedByServer(boolean fail) throws Exception {
        checkClient = true;
        srvTrustMgr.shouldFail(fail);
        clientTrustMgr.shouldFail(false);

        startGrid();

        try {
            try (GridClientImpl c = createClient()) {
                c.compute().refreshTopology(false, false);
            }
        }
        finally {
            G.stopAll(false);
        }

        assertEquals(1, srvTrustMgr.clientCheckCallCount());
        assertEquals(1, clientTrustMgr.serverCheckCallCount());
    }

    /**
     * Test trust manager to emulate certificate check failures.
     */
    private static class MockX509TrustManager implements X509TrustManager {
        /** Empty array. */
        private static final X509Certificate[] EMPTY = new X509Certificate[0];

        /** Whether checks should fail. */
        private volatile boolean shouldFail;

        /** Client check call count. */
        private AtomicInteger clientCheckCallCnt = new AtomicInteger();

        /** Server check call count. */
        private AtomicInteger srvCheckCallCnt = new AtomicInteger();

        /**
         * @param shouldFail Whether checks should fail.
         */
        private void shouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }

        /** {@inheritDoc} */
        @Override public void checkClientTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
            clientCheckCallCnt.incrementAndGet();

            if (shouldFail)
                throw new CertificateException("Client check failed.");
        }

        /** {@inheritDoc} */
        @Override public void checkServerTrusted(X509Certificate[] x509Certificates, String s)
            throws CertificateException {
            srvCheckCallCnt.incrementAndGet();

            if (shouldFail)
                throw new CertificateException("Server check failed.");
        }

        /** {@inheritDoc} */
        @Override public X509Certificate[] getAcceptedIssuers() {
            return EMPTY;
        }

        /**
         * @return Call count to checkClientTrusted method.
         */
        public int clientCheckCallCount() {
            return clientCheckCallCnt.get();
        }

        /**
         * @return Call count to checkServerTrusted method.
         */
        public int serverCheckCallCount() {
            return srvCheckCallCnt.get();
        }

        /**
         * Clears should fail flag and resets call counters.
         */
        public void reset() {
            shouldFail = false;
            clientCheckCallCnt.set(0);
            srvCheckCallCnt.set(0);
        }
    }
}