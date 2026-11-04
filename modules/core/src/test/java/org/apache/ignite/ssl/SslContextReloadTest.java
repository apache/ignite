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

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.management.ssl.SslStatusTask;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.ssl.SslContextRegistry.BINARY_REST;
import static org.apache.ignite.internal.ssl.SslContextRegistry.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextRegistry.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextRegistry.DISCOVERY;
import static org.apache.ignite.ssl.SslTestUtils.assertClientAuthentication;
import static org.apache.ignite.ssl.SslTestUtils.discoveryPort;
import static org.apache.ignite.ssl.SslTestUtils.node;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.reloadFailure;
import static org.apache.ignite.ssl.SslTestUtils.servedCertificate;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.TestCertificateAuthority.chain;
import static org.apache.ignite.testframework.TestCertificateAuthority.context;
import static org.apache.ignite.testframework.TestCertificateAuthority.merged;
import static org.apache.ignite.testframework.TestCertificateAuthority.save;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests TLS reload and status on running nodes. Each node reads its own stores. By default its client connector shares
 * the node factory, and the node trusts both certificate authorities used during rotation.
 */
public class SslContextReloadTest extends GridCommonAbstractTest {
    /** Transports of a node whose client connector shares the node factory. */
    private static final String ALL_TRANSPORTS = CLIENT_CONNECTOR + ", " + COMMUNICATION + ", " + DISCOVERY;

    /** */
    private static final long HOUR = TimeUnit.HOURS.toMillis(1);

    /** Alias and subject of the key every node starts on. */
    private static final String OLD_KEY = "old-key";

    /** Alias and subject of the replacement key. */
    private static final String NEW_KEY = "new-key";

    /** Directory with the store files of the nodes. */
    private Path dir;

    /** Authority of the key every node starts on. */
    private TestCertificateAuthority oldCa;

    /** Replacement authority. */
    private TestCertificateAuthority newCa;

    /** Key every node starts on, issued by {@link #oldCa}. */
    private KeyStore oldKeys;

    /** Replacement key, issued by {@link #newCa}. */
    private KeyStore newKeys;

    /** Certificate of {@link #oldKeys}. */
    private X509Certificate oldCert;

    /** Certificate of {@link #newKeys}. */
    private X509Certificate newCert;

    /** Both authorities: nodes trust them at start, probes always. */
    private KeyStore trustBoth;

    /** Discovery addresses shared only by nodes of this test. */
    private TcpDiscoveryVmIpFinder ipFinder;

    /** */
    private final ListeningTestLogger nodeLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName).setGridLogger(nodeLog);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (!Files.exists(keyFile(igniteInstanceName)))
            save(oldKeys, keyFile(igniteInstanceName));

        if (!Files.exists(trustFile(igniteInstanceName)))
            save(trustBoth, trustFile(igniteInstanceName));

        return cfg.setSslContextFactory(storeFactory(keyFile(igniteInstanceName), trustFile(igniteInstanceName)))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(false));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ipFinder = new TcpDiscoveryVmIpFinder(true);
        dir = Files.createTempDirectory("ignite-ssl-reload-");
        oldCa = new TestCertificateAuthority("old-ca");
        newCa = new TestCertificateAuthority("new-ca");
        oldKeys = oldCa.keyStoreWithPrivateKey(OLD_KEY);
        newKeys = newCa.keyStoreWithPrivateKey(NEW_KEY);
        oldCert = chain(oldKeys).get(0);
        newCert = chain(newKeys).get(0);
        trustBoth = TestCertificateAuthority.trustStore(oldCa, newCa);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();

        U.delete(dir);
    }

    /**
     * After reload, new connections to the client connector, communication and discovery get the new certificate, while an open
     * thin client connection keeps working.
     */
    @Test
    public void testNewConnectionsGetReloadedCertificate() throws Exception {
        IgniteEx g = startGrid(0);
        int clientPort = g.context().clientListener().port();
        // The client trusts only the old CA, so it cannot reconnect after the node switches to a certificate from the new CA.
        SSLContext clientCtx = context(oldKeys, oldCa.trustStore());

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:" + clientPort)
            .setSslMode(SslMode.REQUIRED).setSslContextFactory(() -> clientCtx)
            .setPartitionAwarenessEnabled(false).setClusterDiscoveryEnabled(false))) {
            placeKeys(newKeys);

            reload(g);

            cli.cacheNames();
        }

        SSLContext probe = context(oldKeys, trustBoth);

        assertEquals(newCert, servedCertificate(probe, clientPort));
        assertEquals(newCert, servedCertificate(probe, communicationPort(g)));
        assertEquals(newCert, servedCertificate(probe, discoveryPort(g)));
    }

    /** Every node logs the alias and subject it loaded and the node that received the command. */
    @Test
    public void testReloadAudited() throws Exception {
        IgniteEx g0 = startGrid(0);

        startGrid(1);

        LogListener audited = LogListener.matches(Pattern.compile(Pattern.quote("TLS certificates reloaded [transports=" +
            ALL_TRANSPORTS + ", alias=" + NEW_KEY + ", subject=CN=" + NEW_KEY + ", ") + ".*" +
            Pattern.quote(", initiator=management command, originNodeId=" + g0.localNode().id() + ']'))).times(2).build();

        nodeLog.registerListener(audited);
        placeKeys(newKeys);

        reload(g0);

        assertTrue(audited.check());
    }

    /**
     * A factory shared by every transport, binary REST included, is loaded once on start and once per reload; status does not load
     * it. A factory that is not an {@link AbstractSslContextFactory} gives no certificate metadata but still reloads.
     */
    @Test
    public void testSharedFactoryLoadedOncePerReload() throws Exception {
        String name = getTestIgniteInstanceName(0);
        CountingFactory factory = new CountingFactory(keyFile(name), trustFile(name));
        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory);
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true));
        });
        String transports = BINARY_REST + ", " + ALL_TRANSPORTS;

        assertEquals("Start", 1, factory.loads.get());
        assertEquals(node(g) + ": " + transports + '\n' + DOUBLE_INDENT + "certificate metadata is unavailable\n", status(g));
        assertEquals("Status", 1, factory.loads.get());

        placeKeys(newKeys);

        assertEquals(node(g) + ": reloaded " + transports + '\n', reload(g));
        assertEquals("Reload", 2, factory.loads.get());

        SSLContext probe = context(oldKeys, trustBoth);

        assertEquals(newCert, servedCertificate(probe, discoveryPort(g)));
        assertEquals(newCert, servedCertificate(probe, g.context().clientListener().port()));
        assertEquals(newCert, servedCertificate(probe, g.localNode().attribute(ATTR_REST_TCP_PORT)));
    }

    /** A client connector and binary REST with factories of their own get providers of their own, each reading its own store. */
    @Test
    public void testSeparateFactoriesReloadedSeparately() throws Exception {
        KeyStore clientKeys = oldCa.keyStoreWithPrivateKey("client-connector-key");
        Path clientFile = dir.resolve("client-connector-key.jks");
        Path restFile = dir.resolve("binary-rest-key.jks");

        save(clientKeys, clientFile);
        save(oldCa.keyStoreWithPrivateKey("binary-rest-key"), restFile);

        IgniteEx g = startGrid(0, cfg -> {
            Path trust = trustFile(cfg.getIgniteInstanceName());

            cfg.getClientConnectorConfiguration().setUseIgniteSslContextFactory(false)
                .setSslContextFactory(storeFactory(clientFile, trust));
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true).setSslFactory(storeFactory(restFile, trust)));
        });

        save(newKeys, restFile);

        reload(g);

        SSLContext probe = context(oldKeys, trustBoth);

        assertEquals(oldCert, servedCertificate(probe, discoveryPort(g)));
        assertEquals(chain(clientKeys).get(0), servedCertificate(probe, g.context().clientListener().port()));
        assertEquals(newCert, servedCertificate(probe, g.localNode().attribute(ATTR_REST_TCP_PORT)));
    }

    /**
     * A broken certificate provider is reported without skipping other factories on the node.
     * The node factory is registered first, so the client connector line shows that reload goes on after a failure.
     */
    @Test
    public void testFailingFactoryDoesNotSkipOthers() throws Exception {
        String name = getTestIgniteInstanceName(0);
        SslContextFactory nodeFactory = storeFactory(keyFile(name), trustFile(name));
        AtomicBoolean fail = new AtomicBoolean();
        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(() -> {
                if (fail.get())
                    throw new ServiceConfigurationError("Certificate provider class is unavailable");

                return nodeFactory.create();
            });
            cfg.getClientConnectorConfiguration().setUseIgniteSslContextFactory(false)
                .setSslContextFactory(storeFactory(keyFile(name), trustFile(name)));
        });
        LogListener logged = LogListener.matches("Failed to reload TLS certificates, the ones in use stay [transports=" + COMMUNICATION +
            ", " + DISCOVERY + ", initiator=management command, originNodeId=" + g.localNode().id() +
            ", reason=Certificate provider class is unavailable]").build();

        nodeLog.registerListener(logged);
        placeKeys(newKeys);
        fail.set(true);

        String res = reloadFailure(g);

        assertContains(log, res, node(g) + ": failed on " + COMMUNICATION + ", " + DISCOVERY +
            " (Certificate provider class is unavailable)\n" +
            node(g) + ": reloaded " + CLIENT_CONNECTOR + "; loaded alias=" + NEW_KEY + ", ");
        assertTrue(logged.check());

        String status = status(g);

        assertContains(log, status, "last reload failed 1 time(s) in a row");
        assertContains(log, status, "Certificate provider class is unavailable");
        assertContains(log, status, "last reload succeeded at");
    }

    /** Both commands report a node that is unavailable when jobs are mapped and still run on the other nodes. */
    @Test
    public void testUnavailableNodeReported() throws Exception {
        IgniteEx g = startGrid(0);
        UUID unavailable = UUID.randomUUID();
        VisorTaskArgument<NoArg> arg = new VisorTaskArgument<>(List.of(g.localNode().id(), unavailable), new NoArg(), false);
        String report = "Failed on 1 node(s):\n" + unavailable + ": node was unavailable to run the command\n\n" +
            "Succeeded on 1 node(s):\n" + node(g) + ": ";

        Throwable reloadErr = GridTestUtils.assertThrows(log, () -> g.compute().execute(SslReloadTask.class, arg).result(),
            IgniteException.class, null);
        Throwable statusErr = GridTestUtils.assertThrows(log, () -> g.compute().execute(SslStatusTask.class, arg).result(),
            IgniteException.class, null);

        assertContains(log, reloadErr.getMessage(), report);
        assertContains(log, statusErr.getMessage(), report);
    }

    /** The report lists failures first, a failed job named by node UUID and consistent ID, then successes, and fails the command. */
    @Test
    public void testReportListsFailuresFirst() {
        GridTestNode departed = new GridTestNode(UUID.randomUUID());
        ComputeJobResult succeeded = mock(ComputeJobResult.class);
        ComputeJobResult thrown = mock(ComputeJobResult.class);
        ComputeJobResult refused = mock(ComputeJobResult.class);

        departed.setConsistentId("departed-node");

        when(succeeded.getNode()).thenReturn(new GridTestNode(UUID.randomUUID()));
        when(succeeded.getData()).thenReturn(new IgniteBiTuple<>(true, "node-a: reloaded discovery"));
        when(thrown.getNode()).thenReturn(departed);
        when(thrown.getException()).thenReturn(new IgniteException("node left"));
        when(refused.getNode()).thenReturn(new GridTestNode(UUID.randomUUID()));
        when(refused.getData()).thenReturn(new IgniteBiTuple<>(false, "node-c: failed on discovery (expired)"));

        Throwable err = GridTestUtils.assertThrows(log, () -> new SslReloadTask().reduce(List.of(succeeded, thrown, refused)).result(),
            IgniteException.class, null);

        assertEquals("Failed on 2 node(s):\n" +
            departed.id() + " [consistentId=departed-node]: node left\n" +
            "node-c: failed on discovery (expired)\n" +
            "\n" +
            "Succeeded on 1 node(s):\n" +
            "node-a: reloaded discovery\n", err.getMessage());
    }

    /**
     * Reload and status name every reported chain of the store; for a chain whose authority expires before its leaf, status shows the
     * expiry of the authority.
     */
    @Test
    public void testReportsNameEveryChain() throws Exception {
        // X.509 keeps whole seconds.
        long now = System.currentTimeMillis() / 1000 * 1000;
        long caExpiry = now + HOUR;
        TestCertificateAuthority shortCa = new TestCertificateAuthority("short-ca", now - HOUR, caExpiry);
        IgniteEx g = startGrid(0);

        placeKeys(merged(oldKeys, shortCa.keyStoreWithPrivateKey(NEW_KEY, now - HOUR, now + 2 * HOUR)));

        String reloaded = reload(g);
        String status = status(g);

        assertContains(log, reloaded, "alias=" + OLD_KEY + ", subject=CN=" + OLD_KEY + ", ");
        assertContains(log, reloaded, "alias=" + NEW_KEY + ", subject=CN=" + NEW_KEY + ", ");
        assertContains(log, status, "loaded alias=" + OLD_KEY + ", subject=CN=" + OLD_KEY + ", ");
        assertContains(log, status, "loaded alias=" + NEW_KEY + ", subject=CN=" + NEW_KEY + ", ");
        assertContains(log, status, ", chainNotAfter=" + Instant.ofEpochMilli(caExpiry));
    }

    /** Status reports the expired node chain and the client connector with no key, and startup logs both problems. */
    @Test
    public void testNodeOnUnusableMaterialReported() throws Exception {
        long now = System.currentTimeMillis();
        String name = getTestIgniteInstanceName(0);
        LogListener expired = LogListener.matches("The TLS certificate chain in use is not valid now; put a valid certificate in place " +
            "and run control.sh --ssl reload [alias=expired-key, subject=CN=expired-key, ").build();
        LogListener noChains = LogListener.matches("The key material has no X.509 certificate chains").build();

        save(oldCa.keyStoreWithPrivateKey("expired-key", now - 2 * HOUR, now - HOUR), keyFile(name));
        nodeLog.registerListener(expired);
        nodeLog.registerListener(noChains);

        IgniteEx g = startGrid(0, cfg -> {
            cfg.getClientConnectorConfiguration().setUseIgniteSslContextFactory(false)
                .setSslContextFactory(storeFactory(trustFile(name), trustFile(name)));
        });

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> status(g), Exception.class, null));
        String problem = "\n" + DOUBLE_INDENT + "loaded alias=expired-key, subject=CN=expired-key, [^\\n]+\n" + DOUBLE_INDENT +
            "PROBLEM: the loaded certificate chain is not valid now \\[alias=expired-key]";

        assertTrue(expired.check());
        assertTrue(noChains.check());
        assertContains(log, res, node(g) + ": " + CLIENT_CONNECTOR + '\n' + DOUBLE_INDENT + "no X.509 certificate chains");
        assertTrue(res, Pattern.compile(Pattern.quote(node(g) + ": " + COMMUNICATION + ", " + DISCOVERY) + problem).matcher(res).find());
    }

    /** Without SSL both commands report that no SSL context factory is configured. */
    @Test
    public void testWithoutSsl() throws Exception {
        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(null).setClientConnectorConfiguration(new ClientConnectorConfiguration());
        });

        assertEquals(node(g) + ": no SSL context factory is configured\n", reload(g));
        assertEquals(node(g) + ": no SSL context factory is configured\n", status(g));
    }

    /**
     * The authority is replaced with three reloads: trust both authorities, present a key of the new one, trust only the new one.
     * After the first reload, clients of both authorities connect while the node still presents its old certificate.
     * Then the discovery, communication, client connector and binary REST ports accept a client of the new authority and refuse
     * one of the old authority and one without a certificate, a reopened communication connection works, and a node of the new
     * authority joins.
     */
    @Test
    public void testCertificateAuthorityReplaced() throws Exception {
        save(oldCa.trustStore(), trustFile(getTestIgniteInstanceName(0)));
        save(oldCa.trustStore(), trustFile(getTestIgniteInstanceName(1)));

        IgniteEx g0 = startGrid(0, cfg -> {
            cfg.getClientConnectorConfiguration().setSslClientAuth(true);
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true).setSslClientAuth(true));
        });
        IgniteEx g1 = startGrid(1);

        placeTrust(trustBoth);
        reload(g0);

        for (KeyStore keys : List.of(oldKeys, newKeys)) {
            SSLContext clientCtx = context(keys, trustBoth);

            try (IgniteClient cli = Ignition.startClient(new ClientConfiguration()
                .setAddresses("127.0.0.1:" + g0.context().clientListener().port())
                .setSslMode(SslMode.REQUIRED).setSslContextFactory(() -> clientCtx)
                .setPartitionAwarenessEnabled(false).setClusterDiscoveryEnabled(false))) {
                cli.cacheNames();
            }
        }

        placeKeys(newKeys);
        reload(g0);
        placeTrust(newCa.trustStore());
        reload(g0);

        assertClientAuthentication(discoveryPort(g0), newKeys, oldKeys, trustBoth);
        assertClientAuthentication(communicationPort(g0), newKeys, oldKeys, trustBoth);
        assertClientAuthentication(g0.context().clientListener().port(), newKeys, oldKeys, trustBoth);
        assertClientAuthentication(g0.localNode().attribute(ATTR_REST_TCP_PORT), newKeys, oldKeys, trustBoth);

        ConnectionClientPool pool = U.field(g0.configuration().getCommunicationSpi(), "clientPool");

        pool.forceCloseConnection(g1.localNode().id());

        assertEquals((Integer)1, g0.compute(g0.cluster().forNodeId(g1.localNode().id())).call(() -> 1));

        save(newCa.keyStoreWithPrivateKey("joining-key"), keyFile(getTestIgniteInstanceName(2)));
        save(newCa.trustStore(), trustFile(getTestIgniteInstanceName(2)));

        assertEquals(3, startGrid(2).cluster().nodes().size());
    }

    /** @param store Key store to write for every running node. */
    private void placeKeys(KeyStore store) throws Exception {
        for (Ignite node : G.allGrids())
            save(store, keyFile(node.name()));
    }

    /** @param store Trust store to write for every running node. */
    private void placeTrust(KeyStore store) throws Exception {
        for (Ignite node : G.allGrids())
            save(store, trustFile(node.name()));
    }

    /** @return Key store file of the node. */
    private Path keyFile(String igniteInstanceName) {
        return dir.resolve(igniteInstanceName + "-key.jks");
    }

    /** @return Trust store file of the node. */
    private Path trustFile(String igniteInstanceName) {
        return dir.resolve(igniteInstanceName + "-trust.jks");
    }

    /** @return Communication port of the node. */
    private static int communicationPort(IgniteEx node) {
        return ((TcpCommunicationSpi)node.configuration().getCommunicationSpi()).boundPort();
    }

    /** @return Factory reading the JKS files. */
    private static SslContextFactory storeFactory(Path keys, Path trust) {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreType("JKS");
        factory.setTrustStoreType("JKS");
        factory.setKeyStoreFilePath(keys.toString());
        factory.setKeyStorePassword(TestCertificateAuthority.PWD);
        factory.setTrustStoreFilePath(trust.toString());
        factory.setTrustStorePassword(TestCertificateAuthority.PWD);

        return factory;
    }

    /** Factory that is not an {@link AbstractSslContextFactory}, so it gives no certificate metadata; counts its loads. */
    private static class CountingFactory implements Factory<SSLContext> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Key store file. */
        private final Path keys;

        /** Trust store file. */
        private final Path trust;

        /** Calls of {@link #create()}. */
        private final AtomicInteger loads = new AtomicInteger();

        /**
         * @param keys Key store file.
         * @param trust Trust store file.
         */
        private CountingFactory(Path keys, Path trust) {
            this.keys = keys;
            this.trust = trust;
        }

        /** {@inheritDoc} */
        @Override public SSLContext create() {
            loads.incrementAndGet();

            return storeFactory(keys, trust).create();
        }
    }
}
