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

package org.apache.ignite.client;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that a thin client rebuilds its SSL context out of the files on disk once a TLS handshake has been refused,
 * so that an operator can repair it by placing new certificates without restarting the application.
 */
public class ThinClientSslContextReloadTest extends GridCommonAbstractTest {
    /** Key store the server runs on; replaced on disk to rotate its certificate. */
    private Path srvKeyStore;

    /** Trust store the client runs on; replaced on disk to let it trust the rotated server. */
    private Path cliTrustStore;

    /** Key store the client presents; replaced on disk to see whether the client picks it up. */
    private Path cliKeyStore;

    /** Whether the server demands a certificate from the client and checks who signed it. */
    private boolean srvClientAuth;

    /** Port the node took; a suite running next to this one may hold the default. */
    private int port;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setSslContextFactory(serverSslContextFactory());

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(srvClientAuth)
            .setUseIgniteSslContextFactory(true));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        srvKeyStore = Files.createTempFile("ignite-thin-srv-", ".jks");
        cliTrustStore = Files.createTempFile("ignite-thin-trust-", ".jks");
        cliKeyStore = Files.createTempFile("ignite-thin-cli-", ".jks");

        placeStore("node01", srvKeyStore);
        placeStore("trustone", cliTrustStore);
        placeStore("node01", cliKeyStore);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        srvClientAuth = false;

        Files.deleteIfExists(srvKeyStore);
        Files.deleteIfExists(cliTrustStore);
        Files.deleteIfExists(cliKeyStore);
    }

    /** A client that stopped trusting the server must recover once the widened trust store is on its disk. */
    @Test
    public void testClientRecoversOnRotatedTrustStore() throws Exception {
        startNode();

        try (IgniteClient cli = Ignition.startClient(clientConfiguration())) {
            assertNotNull(cli.cacheNames());

            stopAllGrids();

            // The server comes back with a certificate issued by an authority the client does not trust yet.
            placeStore("node02", srvKeyStore);

            startNode();

            assertFalse("The client must not trust the rotated server yet", reachable(cli));

            placeStore("trustboth", cliTrustStore);

            assertTrue("The client must pick the widened trust store up from disk",
                GridTestUtils.waitForCondition(() -> reachable(cli), 10_000));
        }
    }

    /**
     * A connection that merely dropped must not make the client present a different certificate: the operator may
     * have staged the next key store on disk long before anything is meant to trust its authority.
     */
    @Test
    public void testDroppedConnectionKeepsPresentedCertificate() throws Exception {
        srvClientAuth = true;

        startNode();

        try (IgniteClient cli = Ignition.startClient(clientConfiguration())) {
            assertNotNull(cli.cacheNames());

            stopAllGrids();

            // Staged, and signed by an authority the cluster does not trust yet.
            placeStore("node02", cliKeyStore);

            try (ServerSocket ignored = acceptAndClose()) {
                assertFalse("The handshake must not complete against a socket that closes at once", reachable(cli));
            }

            startNode();

            assertTrue("A dropped connection must not rotate the certificate the client presents",
                GridTestUtils.waitForCondition(() -> reachable(cli), 10_000));
        }
    }

    /** A refused handshake must name the reason, or a rotation done out of order cannot be diagnosed. */
    @Test
    public void testRefusedHandshakeNamesTheReason() throws Exception {
        placeStore("node02", srvKeyStore);

        startNode();

        Throwable e = GridTestUtils.assertThrows(log, () -> Ignition.startClient(clientConfiguration()),
            Exception.class, null);

        assertTrue("The TLS failure must reach the caller, got: " + e,
            X.hasCause(e, SSLHandshakeException.class));
    }

    /**
     * Starts the node under test and remembers the port it took.
     */
    private void startNode() throws Exception {
        port = startGrid(0).context().clientListener().port();
    }

    /**
     * @param cli Client to probe.
     * @return {@code True} if the client can talk to the cluster.
     */
    private boolean reachable(IgniteClient cli) {
        try {
            cli.cacheNames();

            return true;
        }
        catch (Exception ignored) {
            return false;
        }
    }

    /**
     * @return Configuration of the client under test, reading its trust store from {@link #cliTrustStore}.
     */
    private ClientConfiguration clientConfiguration() {
        return new ClientConfiguration()
            .setAddresses("127.0.0.1:" + port)
            .setSslMode(SslMode.REQUIRED)
            .setSslTrustCertificateKeyStorePath(cliTrustStore.toString())
            .setSslTrustCertificateKeyStorePassword(GridTestUtils.keyStorePassword())
            .setSslClientCertificateKeyStorePath(cliKeyStore.toString())
            .setSslClientCertificateKeyStorePassword(GridTestUtils.keyStorePassword());
    }

    /**
     * @return Socket on the client connector port that accepts connections and closes them at once, so that a
     *      handshake never completes and TLS is not at fault.
     */
    private ServerSocket acceptAndClose() throws Exception {
        ServerSocket srvSock = new ServerSocket(port, 0, InetAddress.getLoopbackAddress());

        // accept() throws once the test closes the socket, and the async runner absorbs that into a future
        // nobody waits on.
        GridTestUtils.runAsync(() -> {
            while (!srvSock.isClosed())
                srvSock.accept().close();
        });

        return srvSock;
    }

    /**
     * @return SSL context factory of the server, reading its certificate from {@link #srvKeyStore}.
     */
    private Factory<SSLContext> serverSslContextFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(srvKeyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());

        if (srvClientAuth) {
            factory.setTrustStoreFilePath(GridTestUtils.keyStorePath("trustone"));
            factory.setTrustStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        }
        else
            factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     * @param dest File to replace.
     */
    private void placeStore(String name, Path dest) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), dest, StandardCopyOption.REPLACE_EXISTING);
    }
}
