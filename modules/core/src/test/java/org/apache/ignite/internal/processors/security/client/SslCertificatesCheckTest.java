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

package org.apache.ignite.internal.processors.security.client;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientAuthenticationException;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSslSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.plugin.security.SecurityCredentialsBasicProvider;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_OPS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.ALLOW_ALL;

/**
 * Security tests for thin client.
 */
@RunWith(JUnit4.class)
public class SslCertificatesCheckTest extends AbstractSecurityTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(false, log);

    /** */
    private final LogListener lsnr = LogListener.matches("SSL certificates are not found.").build();

    {
        listeningLog.registerListener(lsnr);
    }

    /** Client that has system permissions. */
    protected static final String CLIENT_ADMIN_OPER = "client_admin_oper";

    /** */
    private boolean failServer;

    /** */
    private boolean failClient;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        lsnr.reset();
    }

    /**
     * @return Test data.
     */
    protected TestSecurityData[] clientData() {
        return new TestSecurityData[]{new TestSecurityData(CLIENT_ADMIN_OPER,
            SecurityPermissionSetBuilder.create().defaultAllowAll(false)
                .appendSystemPermissions(ADMIN_OPS)
                .build()
        )};
    }

    /**
     * @param instanceName Instance name.
     */
    @Override protected IgniteConfiguration getConfiguration(String instanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(instanceName);
        cfg.setActiveOnStart(false);

        cfg.setPluginProviders(new TestSslSecurityPluginProvider("srv_" + instanceName, null, ALLOW_ALL,
            true, clientData()));

        if (!failServer) {
            SslContextFactory sslFactory = (SslContextFactory) GridTestUtils.sslFactory();

            cfg.setSslContextFactory(sslFactory);
            cfg.setConnectorConfiguration(new ConnectorConfiguration()
                .setSslEnabled(true)
                .setSslClientAuth(!failClient)
                .setSslFactory(sslFactory));
        }

        if (instanceName.endsWith("0"))
            cfg.setGridLogger(listeningLog);

        if (instanceName.endsWith("2"))
            cfg.setClientMode(true);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testSslCertificates() throws Exception {
        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        startGrid(2);

        assertEquals(3, ignite.cluster().topologyVersion());
        assertFalse(ignite.cluster().active());

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration(CLIENT_ADMIN_OPER, ""))) {
            assertTrue(client.connected());
            client.state().active(true);
        }

        assertFalse(lsnr.check());
    }

    /**
     *
     */
    @Test
    public void testSslCertificatesThinClientFail() throws Exception {
        failClient = true;

        Ignite ignite = startGrids(2);

        assertEquals(2, ignite.cluster().topologyVersion());

        startGrid(2);

        assertEquals(3, ignite.cluster().topologyVersion());

        try (GridClient client = GridClientFactory.start(getGridClientConfiguration(CLIENT_ADMIN_OPER, ""))) {
            assertFalse(client.connected());
            GridTestUtils.assertThrowsAnyCause(log,
                ()-> {
                    client.throwLastError();
                    return null;
                },
                GridClientAuthenticationException.class,
                "Client authentication failed");
        }

        assertTrue(lsnr.check());
    }

    /**
     *
     */
    @Test
    public void testSslCertificatesClientFail() throws Exception {
        failServer = true;

        Ignite ignite = startGrids(1);

        assertEquals(1, ignite.cluster().topologyVersion());

        GridTestUtils.assertThrowsAnyCause(log,
            ()-> {
                startGrid(2);
                return null;
            },
            IgniteSpiException.class,
            "Authentication failed");

        assertEquals(1, ignite.cluster().topologyVersion());
        assertTrue(lsnr.check());
    }

    /**
     *
     */
    @Test
    public void testSslCertificatesServerFail() throws Exception {
        failServer = true;

        Ignite ignite = startGrid(0);

        GridTestUtils.assertThrowsAnyCause(log,
            ()-> {
                startGrid(1);
                return null;
            },
            IgniteAuthenticationException.class,
            "Authentication failed");

        assertEquals(1, ignite.cluster().topologyVersion());
        assertTrue(lsnr.check());
    }

    /**
     * @param login Login
     * @param pwd Password
     * @return Client configuration.
     */
    protected GridClientConfiguration getGridClientConfiguration(String login, String pwd) {
        SslContextFactory sslFactory = (SslContextFactory) GridTestUtils.sslFactory();

        sslFactory.setKeyStoreFilePath(U.resolveIgnitePath(GridTestProperties.getProperty("ssl.keystore.client.path"))
            .getAbsolutePath());

        return new GridClientConfiguration()
            .setSslContextFactory(sslFactory::create)
            .setRouters(Collections.singletonList("127.0.0.1:11211"))
            .setSecurityCredentialsProvider(
                new SecurityCredentialsBasicProvider(new SecurityCredentials(login, pwd)));
    }
}
