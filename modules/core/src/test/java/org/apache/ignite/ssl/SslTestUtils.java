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
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyStore;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509KeyManager;
import org.apache.ignite.cluster.ClusterGroup;
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
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.testframework.GridTestUtils.keyStorePassword;
import static org.apache.ignite.testframework.GridTestUtils.keyStorePath;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

/** Runs the {@code --ssl} commands the way control.sh does, probes what the transports serve, and places the test stores. */
public class SslTestUtils {
    /** Timeout for a probe connection or TLS handshake. */
    private static final int SOCKET_TIMEOUT = 30_000;

    /** */
    private SslTestUtils() {
        // No-op.
    }

    /**
     * @param nodes Nodes whose certificates are reloaded. The first node submits the command.
     * @return Report of the command.
     */
    public static String reload(IgniteEx... nodes) throws Exception {
        return execute(SslReloadTask.class, nodes);
    }

    /**
     * @param nodes Nodes whose certificate status is requested. The first node submits the command.
     * @return Report of the command.
     */
    public static String status(IgniteEx... nodes) throws Exception {
        return execute(SslStatusTask.class, nodes);
    }

    /**
     * @param nodes Nodes whose certificates are reloaded. The first node submits the command.
     * @return Exception stack trace with all causes, including any wrappers added by compute.
     */
    public static String reloadFailure(IgniteEx... nodes) {
        return X.getFullStackTrace(GridTestUtils.assertThrows(null, () -> reload(nodes), Exception.class, null));
    }

    /**
     * @param probe Context to connect with; discovery asks the client for a certificate it trusts.
     * @param port Port to connect to.
     * @return Certificate the node presents on a new TLS connection to that port.
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
     * @param port Port to connect to; the probe presents node01 and trusts both test authorities.
     * @return Subject of the certificate the node presents on a new TLS connection to that port.
     */
    public static String servedSubject(int port) throws Exception {
        return servedCertificate(GridTestUtils.sslTrustedFactory("node01", "trustboth").create(), port).getSubjectX500Principal().getName();
    }

    /**
     * Checks that the server accepts a trusted client and rejects both an untrusted certificate and no certificate.
     * Each probe uses a fresh context to prevent session resumption. TLS 1.2 completes the handshake only after the server
     * has checked the client certificate.
     *
     * @param port Server port.
     * @param trustedKey Test key store trusted by the server.
     * @param untrustedKey Test key store not trusted by the server.
     */
    public static void assertClientAuthentication(int port, String trustedKey, String untrustedKey) throws Exception {
        try (SSLSocket sock = tls12Socket(clientContext(trustedKey), port)) {
            sock.startHandshake();
        }
        catch (IOException e) {
            throw new AssertionError("Trusted client must connect [port=" + port + ", client=" + trustedKey + ']', e);
        }

        assertHandshakeRefused(untrustedKey, port);
        assertHandshakeRefused(null, port);
    }

    /**
     * Creates a client that trusts both test authorities and presents the selected certificate even if the server does not
     * advertise its authority. This lets tests distinguish an untrusted certificate from a missing certificate.
     *
     * @param keyStore Single-key test store, or {@code null} for a client without a certificate.
     * @return Fresh client context.
     */
    public static SSLContext clientContext(@Nullable String keyStore) throws Exception {
        KeyManager[] keyManagers = new KeyManager[0];

        if (keyStore != null) {
            KeyStore keys = store(keyStore);

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            kmf.init(keys, keyStorePassword().toCharArray());

            X509KeyManager km = (X509KeyManager)kmf.getKeyManagers()[0];

            String alias = keys.aliases().nextElement();

            assertNotNull("No private key in test store " + keyStore, km.getPrivateKey(alias));

            X509KeyManager always = new X509KeyManager() {
                @Override public String chooseClientAlias(String[] keyType, Principal[] issuers, Socket sock) {
                    return alias;
                }

                @Override public String[] getClientAliases(String keyType, Principal[] issuers) {
                    return new String[] {alias};
                }

                @Override public String[] getServerAliases(String keyType, Principal[] issuers) {
                    return km.getServerAliases(keyType, issuers);
                }

                @Override public String chooseServerAlias(String keyType, Principal[] issuers, Socket sock) {
                    return km.chooseServerAlias(keyType, issuers, sock);
                }

                @Override public X509Certificate[] getCertificateChain(String alias) {
                    return km.getCertificateChain(alias);
                }

                @Override public PrivateKey getPrivateKey(String alias) {
                    return km.getPrivateKey(alias);
                }
            };

            keyManagers = new KeyManager[] {always};
        }

        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        tmf.init(store("trustboth"));

        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(keyManagers, tmf.getTrustManagers(), null);

        return ctx;
    }

    /** @param name Test store. */
    private static KeyStore store(String name) throws Exception {
        KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());

        try (InputStream in = Files.newInputStream(Path.of(keyStorePath(name)))) {
            store.load(in, keyStorePassword().toCharArray());
        }

        return store;
    }

    /**
     * @param keyStore Untrusted test store, or {@code null} for a client without a certificate.
     * @param port Server port.
     */
    private static void assertHandshakeRefused(@Nullable String keyStore, int port) throws Exception {
        String client = keyStore == null ? "no certificate" : keyStore;

        try (SSLSocket sock = tls12Socket(clientContext(keyStore), port)) {
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

    /** @return Node UUID and consistent ID in command output format. */
    public static String node(IgniteEx node) {
        return node.localNode().id() + " [consistentId=" + node.localNode().consistentId() + ']';
    }

    /** @return Discovery port of the node. */
    public static int discoveryPort(IgniteEx node) {
        return ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort();
    }

    /**
     * @param name Test store, as {@code tests.properties} names it.
     * @param dest File to replace.
     */
    public static void place(String name, Path dest) throws IOException {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), dest, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * @param task Task of the command.
     * @param nodes Nodes to run on.
     * @return Report of the command.
     */
    private static String execute(Class<? extends AbstractSslTask> task, IgniteEx... nodes) throws Exception {
        List<UUID> ids = Arrays.stream(nodes).map(n -> n.localNode().id()).collect(Collectors.toList());

        ClusterGroup serversAndClients = nodes[0].cluster();

        return nodes[0].compute(serversAndClients).execute(task, new VisorTaskArgument<>(ids, new NoArg(), false)).result();
    }
}
