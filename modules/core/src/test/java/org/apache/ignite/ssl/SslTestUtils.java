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

package org.apache.ignite.ssl;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.UUID;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.X509KeyManager;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.AbstractSslTask;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.management.ssl.SslStatusTask;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.jetbrains.annotations.Nullable;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/** Runs the {@code --ssl} commands on every node of a cluster, as control.sh does, and probes the certificates TLS servers present. */
public class SslTestUtils {
    /** Timeout for a probe connection or TLS handshake. */
    private static final int SOCKET_TIMEOUT = 30_000;

    /** */
    private SslTestUtils() {
        // No-op.
    }

    /**
     * @param node Node that receives the command; it runs on every server and client node of the cluster.
     * @return Report of {@code --ssl reload}.
     */
    public static String reload(IgniteEx node) throws Exception {
        return execute(SslReloadTask.class, node);
    }

    /**
     * @param node Node that receives the command; it runs on every server and client node of the cluster.
     * @return Report of {@code --ssl status}.
     */
    public static String status(IgniteEx node) throws Exception {
        return execute(SslStatusTask.class, node);
    }

    /**
     * @param node Node that receives {@code --ssl reload}, which must fail.
     * @return Exception stack trace with all causes, including any wrappers added by compute.
     */
    public static String reloadFailure(IgniteEx node) {
        return X.getFullStackTrace(GridTestUtils.assertThrows(null, () -> reload(node), Exception.class, null));
    }

    /**
     * @param probe Context to connect with; discovery and communication ask the client for a certificate they trust.
     * @param port Loopback port to connect to.
     * @return Certificate the server presents on a new TLS connection to that port.
     */
    public static X509Certificate servedCertificate(SSLContext probe, int port) throws Exception {
        try (SSLSocket sock = (SSLSocket)probe.getSocketFactory().createSocket()) {
            sock.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), SOCKET_TIMEOUT);
            sock.setSoTimeout(SOCKET_TIMEOUT);

            sock.startHandshake();

            return (X509Certificate)sock.getSession().getPeerCertificates()[0];
        }
    }

    /**
     * Checks that the server accepts a trusted client and rejects both an untrusted certificate and no certificate.
     * Each probe uses a fresh context to prevent session resumption. TLS 1.2 completes the handshake only after the server
     * has checked the client certificate.
     *
     * @param port Server port.
     * @param trustedKeys Single-key store trusted by the server.
     * @param untrustedKeys Single-key store not trusted by the server.
     * @param trustStore Authorities trusted by all three clients.
     */
    public static void assertClientAuthentication(int port, KeyStore trustedKeys, KeyStore untrustedKeys, KeyStore trustStore)
        throws Exception {
        try (SSLSocket sock = tls12Socket(clientContext(trustedKeys, trustStore), port)) {
            sock.startHandshake();
        }
        catch (IOException e) {
            throw new AssertionError("Trusted client must connect [port=" + port + ']', e);
        }

        assertHandshakeRefused(clientContext(untrustedKeys, trustStore), port, "untrusted certificate");
        assertHandshakeRefused(clientContext(null, trustStore), port, "no certificate");
    }

    /**
     * Creates a client that presents the sole private key of the store even if the server does not advertise its authority.
     * This lets tests distinguish an untrusted certificate from a missing certificate.
     *
     * @param keys Single-key store, or {@code null} to present no certificate.
     * @param trustStore Authorities trusted by the client.
     * @return Fresh client context.
     */
    public static SSLContext clientContext(@Nullable KeyStore keys, KeyStore trustStore) throws Exception {
        KeyManager[] keyManagers = new KeyManager[0];

        if (keys != null) {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            kmf.init(keys, TestCertificateAuthority.PWD);

            X509KeyManager km = (X509KeyManager)kmf.getKeyManagers()[0];

            String alias = keys.aliases().nextElement();

            assertNotNull("No private key for test alias " + alias, km.getPrivateKey(alias));

            X509KeyManager always = new X509KeyManager() {
                /** {@inheritDoc} */
                @Override public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket sock) {
                    return km.chooseClientAlias(keyType, null, sock);
                }

                /** {@inheritDoc} */
                @Override public String[] getClientAliases(String keyType, Principal[] issuers) {
                    return km.getClientAliases(keyType, null);
                }

                /** {@inheritDoc} */
                @Override public String[] getServerAliases(String keyType, Principal[] issuers) {
                    return km.getServerAliases(keyType, issuers);
                }

                /** {@inheritDoc} */
                @Override public String chooseServerAlias(String keyType, Principal[] issuers, Socket sock) {
                    return km.chooseServerAlias(keyType, issuers, sock);
                }

                /** {@inheritDoc} */
                @Override public X509Certificate[] getCertificateChain(String alias) {
                    return km.getCertificateChain(alias);
                }

                /** {@inheritDoc} */
                @Override public PrivateKey getPrivateKey(String alias) {
                    return km.getPrivateKey(alias);
                }
            };

            keyManagers = new KeyManager[] {always};
        }

        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(keyManagers, TestCertificateAuthority.trustManagers(trustStore), null);

        return ctx;
    }

    /** @return Node UUID and consistent ID in command output format. */
    public static String node(IgniteEx node) {
        return node.localNode().id() + " [consistentId=" + node.localNode().consistentId() + ']';
    }

    /** @return Discovery port of the node. */
    public static int discoveryPort(IgniteEx node) {
        return ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort();
    }

    /**
     * @param task Task of the command.
     * @param node Node that receives the command.
     * @return Report of the command.
     */
    private static String execute(Class<? extends AbstractSslTask> task, IgniteEx node) throws Exception {
        List<UUID> ids = node.cluster().nodes().stream().map(ClusterNode::id).toList();

        return node.compute(node.cluster()).execute(task, new VisorTaskArgument<>(ids, new NoArg(), false)).result();
    }

    /**
     * @param ctx Client context.
     * @param port Server port.
     * @param client Description of the refused client.
     */
    private static void assertHandshakeRefused(SSLContext ctx, int port, String client) throws Exception {
        try (SSLSocket sock = tls12Socket(ctx, port)) {
            try {
                sock.startHandshake();

                fail("Server must refuse the client [port=" + port + ", client=" + client + ']');
            }
            catch (SSLException | SocketException expected) {
                // A TLS alert or a connection reset is a refusal. A timeout must fail the test.
            }
        }
        catch (IOException e) {
            throw new AssertionError("Client refusal check failed [port=" + port + ", client=" + client + ']', e);
        }
    }

    /**
     * @param ctx Client context.
     * @param port Server port.
     * @return Connected socket limited to TLS 1.2.
     */
    private static SSLSocket tls12Socket(SSLContext ctx, int port) throws IOException {
        SSLSocket sock = (SSLSocket)ctx.getSocketFactory().createSocket();

        try {
            sock.setEnabledProtocols(new String[] {"TLSv1.2"});
            sock.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), SOCKET_TIMEOUT);
            sock.setSoTimeout(SOCKET_TIMEOUT);

            return sock;
        }
        catch (IOException | RuntimeException e) {
            U.closeQuiet(sock);

            throw e;
        }
    }
}
