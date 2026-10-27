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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
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
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.ssl.SslCertificates;
import org.apache.ignite.internal.ssl.SslContextProvider;
import org.apache.ignite.internal.ssl.SslContextRegistry;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.ssl.SslContextRegistry.BINARY_REST;
import static org.apache.ignite.internal.ssl.SslContextRegistry.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextRegistry.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextRegistry.DISCOVERY;
import static org.apache.ignite.ssl.SslTestUtils.assertClientAuthentication;
import static org.apache.ignite.ssl.SslTestUtils.discoveryPort;
import static org.apache.ignite.ssl.SslTestUtils.node;
import static org.apache.ignite.ssl.SslTestUtils.place;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.reloadFailure;
import static org.apache.ignite.ssl.SslTestUtils.servedSubject;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Tests {@code --ssl reload} and {@code --ssl status} on running nodes. Each node has separate key and trust stores so that tests
 * can replace or corrupt them independently. node01 is signed by the old CA (oneca); node02, node03 and the expired node02old
 * are signed by the new CA (twoca).
 */
public class SslContextReloadTest extends GridCommonAbstractTest {
    /** Transports of a node whose client connector shares the factory of the node. */
    private static final String ALL_TRANSPORTS = CLIENT_CONNECTOR + ", " + COMMUNICATION + ", " + DISCOVERY;

    /** Directory the stores of the nodes are placed in. */
    private Path dir;

    /** Whether the client connector runs on a factory of its own. */
    private boolean ownClientConnectorFactory;

    /** Test trust store every node starts on, unless one is placed for it. */
    private String initTrustStore = "trustboth";

    /** */
    private final ListeningTestLogger nodeLog = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName).setGridLogger(nodeLog);

        if (!Files.exists(keyStore(igniteInstanceName)))
            place("node01", keyStore(igniteInstanceName));

        if (!Files.exists(trustStore(igniteInstanceName)))
            place(initTrustStore, trustStore(igniteInstanceName));

        ClientConnectorConfiguration cliCfg = new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(false)
            .setUseIgniteSslContextFactory(!ownClientConnectorFactory);

        if (ownClientConnectorFactory)
            cliCfg.setSslContextFactory(storeFactory(igniteInstanceName));

        return cfg.setSslContextFactory(storeFactory(igniteInstanceName)).setClientConnectorConfiguration(cliCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        dir = Files.createTempDirectory("ignite-ssl-reload-");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();

        U.delete(dir);
    }

    /**
     * A rotation on a running cluster reaches new connections, the report, the node log and status, and leaves established
     * connections working.
     */
    @Test
    public void testReloadOnRunningCluster() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        LogListener logged = LogListener.matches(Pattern.compile("TLS certificates reloaded \\[transports=" + ALL_TRANSPORTS +
            ", subject=CN=node02, .*initiator=management command, originNodeId=" + g0.localNode().id())).times(2).build();

        nodeLog.registerListener(logged);

        // The client trusts only the old CA, so it cannot reconnect after the node switches to a certificate from the new CA.
        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:" + g0.context().clientListener().port())
            .setSslMode(SslMode.REQUIRED).setSslContextFactory(GridTestUtils.sslTrustedFactory("node01", "trustone"))
            .setPartitionAwarenessEnabled(false).setClusterDiscoveryEnabled(false))) {
            placeKeys("node02");

            String res = reload(g0, g1);

            cli.cacheNames();

            assertReloaded(res, g0, ALL_TRANSPORTS);
            assertReloaded(res, g1, ALL_TRANSPORTS);
            assertTrue(logged.check());
        }

        assertEquals("CN=node02", servedSubject(g0.context().clientListener().port()));
        assertEquals("CN=node02", servedSubject(((TcpCommunicationSpi)g0.configuration().getCommunicationSpi()).boundPort()));
        assertEquals("CN=node02", servedSubject(discoveryPort(g0)));

        SslContextProvider p = g0.context().sslContexts().providers().iterator().next();

        assertEquals("CN=node02", p.certificate().getSubjectX500Principal().getName());
        assertContains(log, p.certificate().getIssuerX500Principal().getName(), "CN=twoca");
        assertTrue(p.reloadOutcome().successTime() > 0);

        String status = status(g0);

        assertContains(log, status, "chainNotAfter=" + Instant.ofEpochMilli(SslCertificates.chainNotAfter(p.certificateChain())));
        assertContains(log, status, "last reload succeeded at " + Instant.ofEpochMilli(p.reloadOutcome().successTime()));

        stopGrid(0);

        IgniteEx restarted = startGrid(0);

        assertEquals("CN=node02", servedSubject(restarted.context().clientListener().port()));
        assertEquals("CN=node02", servedSubject(discoveryPort(restarted)));
    }

    /** One node fails: the others still take the new certificate and appear in the report; the command fails. */
    @Test
    public void testPartialFailureAcrossNodes() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        place("node02", keyStore(g0.name()));
        Files.write(keyStore(g1.name()), "not a key store".getBytes());

        String res = reloadFailure(g0, g1);

        assertReloaded(res, g0, ALL_TRANSPORTS);
        assertFailed(res, g1, ALL_TRANSPORTS);
        assertContains(log, res, "Failed to initialize key store");

        String failed = "Failed on 1 node(s):\n" + node(g1) + ": failed on ";
        String succeeded = "\n\nSucceeded on 1 node(s):\n" + node(g0) + ": reloaded ";

        assertTrue("The failed node must be listed first, in a separate block: " + res,
            res.contains(failed) && res.indexOf(succeeded) > res.indexOf(failed));

        assertEquals("CN=node02", servedSubject(g0.context().clientListener().port()));
        assertEquals("CN=node01", servedSubject(g1.context().clientListener().port()));

        String status = status(g1);

        assertContains(log, status, "last reload failed 1 time(s) in a row");
        assertContains(log, status, "Failed to initialize key store");
        assertContains(log, status, "loaded subject=CN=node01,");
    }

    /**
     * A factory that is not an {@link AbstractSslContextFactory} and returns the context in use fails the reload: the node gets no
     * new certificates from it.
     */
    @Test
    public void testCachingFactoryNotReloaded() throws Exception {
        IgniteEx g = startGrid(0, cfg -> {
            SSLContext ctx = cfg.getSslContextFactory().create();

            cfg.setSslContextFactory(() -> ctx);
        });

        placeKeys("node02");

        String res = reloadFailure(g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "returns the context in use");

        assertEquals("CN=node01", servedSubject(g.context().clientListener().port()));
    }

    /** A shared custom factory loads once at startup and once per reload. Status does not load it. */
    @Test
    public void testCustomFactoryReloaded() throws Exception {
        TestFactory factory = new TestFactory();

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory);
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true));
        });

        SslContextRegistry registry = g.context().sslContexts();
        SSLContext ctx = registry.register(factory, COMMUNICATION);

        assertSame(ctx, registry.register(factory, DISCOVERY));
        assertSame(ctx, registry.register(factory, CLIENT_CONNECTOR));
        assertSame(ctx, registry.register(factory, BINARY_REST));
        assertEquals(1, registry.providers().size());
        assertEquals(1, factory.calls);

        String transports = BINARY_REST + ", " + ALL_TRANSPORTS;

        assertContains(log, status(g), node(g) + ": " + transports);
        assertEquals("Status must not load the factory", 1, factory.calls);

        placeKeys("node02");

        assertReloaded(reload(g), g, transports);
        assertEquals("Reload must load the shared factory once", 2, factory.calls);
        assertSame(ctx, registry.register(factory, COMMUNICATION));
        assertContains(log, status(g), "loaded subject=CN=node02,");
        assertEquals("Status must not load the factory", 2, factory.calls);

        assertEquals("CN=node02", servedSubject(discoveryPort(g)));
        assertEquals("CN=node02", servedSubject(g.context().clientListener().port()));
        assertEquals("CN=node02", servedSubject(g.localNode().attribute(ATTR_REST_TCP_PORT)));
    }

    /** Equal but distinct factories reload independently, including when one of them fails. */
    @Test
    public void testEqualFactoriesReloadIndependently() throws Exception {
        TestFactory nodeFactory = new TestFactory();
        TestFactory clientFactory = new TestFactory();

        assertNotSame(nodeFactory, clientFactory);
        assertEquals(nodeFactory, clientFactory);
        assertEquals(nodeFactory.hashCode(), clientFactory.hashCode());

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(nodeFactory);
            cfg.getClientConnectorConfiguration().setUseIgniteSslContextFactory(false).setSslContextFactory(clientFactory);
        });

        SslContextRegistry registry = g.context().sslContexts();

        assertEquals(2, registry.providers().size());
        assertNotSame(registry.register(nodeFactory, COMMUNICATION), registry.register(clientFactory, CLIENT_CONNECTOR));
        assertEquals(1, nodeFactory.calls);
        assertEquals(1, clientFactory.calls);

        placeKeys("node02");

        String res = reload(g);

        assertReloaded(res, g, COMMUNICATION + ", " + DISCOVERY);
        assertReloaded(res, g, CLIENT_CONNECTOR);
        assertEquals(2, nodeFactory.calls);
        assertEquals(2, clientFactory.calls);

        nodeFactory.fail = true;
        placeKeys("node03");

        res = reloadFailure(g);

        assertFailed(res, g, COMMUNICATION + ", " + DISCOVERY);
        assertReloaded(res, g, CLIENT_CONNECTOR);
        assertEquals(3, nodeFactory.calls);
        assertEquals(3, clientFactory.calls);
        assertEquals("CN=node02", servedSubject(discoveryPort(g)));
        assertEquals("CN=node03", servedSubject(g.context().clientListener().port()));
    }

    /** An expired certificate stays out of use, and the report, log and status identify the rejected chain. */
    @Test
    public void testExpiredCertificateNotApplied() throws Exception {
        LogListener expired = LogListener.matches(Pattern.compile("Failed to reload TLS certificates, the ones in use stay .*" +
            "reason=The new certificate chain is not valid now \\[subject=CN=node02old,")).build();

        nodeLog.registerListener(expired);

        IgniteEx g = startGrid(0);

        placeKeys("node02old");

        String res = reloadFailure(g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "The new certificate chain is not valid now [subject=CN=node02old,");
        assertTrue(expired.check());
        assertEquals("CN=node01", servedSubject(discoveryPort(g)));

        String status = status(g);

        assertContains(log, status, "loaded subject=CN=node01, issuer=");
        assertContains(log, status, "last reload failed 1 time(s) in a row");
        assertContains(log, status, "The new certificate chain is not valid now");
    }

    /** A trust-only store must not replace a working node's keys; new connections still receive the original certificate. */
    @Test
    public void testKeyStoreWithoutKeyNotApplied() throws Exception {
        IgniteEx g = startGrid(0);

        placeKeys("trustboth");

        String res = reloadFailure(g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "The new key material has no private key, so the node would present no certificate");
        assertEquals("CN=node01", servedSubject(g.context().clientListener().port()));
        assertEquals("CN=node01", servedSubject(discoveryPort(g)));
        assertClientConnected(g, "node01");
    }

    /** A client connector and binary REST with factories of their own reload separately from the node-wide factory. */
    @Test
    public void testClientTransportsWithSeparateFactories() throws Exception {
        initTrustStore = "trustone";
        ownClientConnectorFactory = true;

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true)
                .setSslFactory(storeFactory(cfg.getIgniteInstanceName())));
        });

        placeKeys("node02");

        String res = reload(g);

        assertReloaded(res, g, CLIENT_CONNECTOR);
        assertReloaded(res, g, BINARY_REST);
        assertReloaded(res, g, COMMUNICATION + ", " + DISCOVERY);

        assertEquals("CN=node02", servedSubject(g.context().clientListener().port()));
        assertEquals("CN=node02", servedSubject(g.localNode().attribute(ATTR_REST_TCP_PORT)));
        assertEquals("CN=node02", servedSubject(discoveryPort(g)));
    }

    /** Manual reload accepts several keys, reports unavailable metadata and keeps existing and new TLS connections working. */
    @Test
    public void testKeyStoreWithMultipleKeysReloadedWithoutMetadata() throws Exception {
        ownClientConnectorFactory = true;

        IgniteEx g = startGrid(0);

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:" + g.context().clientListener().port())
            .setSslMode(SslMode.REQUIRED).setSslContextFactory(GridTestUtils.sslTrustedFactory("node01", "trustboth"))
            .setPartitionAwarenessEnabled(false).setClusterDiscoveryEnabled(false))) {
            placeKeys("node0102");

            String res = reload(g);

            assertContains(log, res, node(g) + ": reloaded " + CLIENT_CONNECTOR);
            assertContains(log, res, node(g) + ": reloaded " + COMMUNICATION + ", " + DISCOVERY);

            cli.cacheNames();
        }

        String subj = servedSubject(g.context().clientListener().port());

        assertTrue(subj, List.of("CN=node01", "CN=node02").contains(subj));

        String status = status(g);

        assertContains(log, status, "certificate metadata is unavailable");
        assertContains(log, status, "last reload succeeded at");
        assertNotContains(log, status, "loaded subject=");
    }

    /**
     * The authority is replaced in steps: trust the new one, present its certificates, drop the old one. Every TCP transport
     * accepts certificates from the new authority and rejects old certificates and clients without certificates. The nodes
     * reconnect and a node of the new authority joins.
     */
    @Test
    public void testCertificateAuthorityReplaced() throws Exception {
        initTrustStore = "trustone";

        IgniteEx g0 = startGrid(0, cfg -> {
            cfg.getClientConnectorConfiguration().setSslClientAuth(true);
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true).setSslClientAuth(true));
        });
        IgniteEx g1 = startGrid(1, cfg -> {
            cfg.getClientConnectorConfiguration().setSslClientAuth(true);
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true).setSslClientAuth(true));
        });

        assertTransportClientAuthentication(g0, "node01", "node02");

        placeTrust("trustboth");
        reload(g0, g1);

        assertClientConnected(g0, "node01");
        assertClientConnected(g0, "node02");

        placeKeys("node02");
        reload(g0, g1);

        placeTrust("trusttwo");
        reload(g0, g1);

        String reloadedStatus = status(g0, g1);

        assertTrue(reloadedStatus, Pattern.compile("issuer=[^;\\n]*CN=twoca").matcher(reloadedStatus).find());

        for (IgniteEx node : List.of(g0, g1)) {
            assertClientConnected(node, "node02");
            assertTransportClientAuthentication(node, "node02", "node01");
        }

        ConnectionClientPool pool = U.field(g0.configuration().getCommunicationSpi(), "clientPool");

        pool.forceCloseConnection(g1.localNode().id());

        assertEquals((Integer)1, g0.compute(g0.cluster().forNodeId(g1.localNode().id())).call(() -> 1));

        place("node03", keyStore(getTestIgniteInstanceName(2)));
        place("trusttwo", trustStore(getTestIgniteInstanceName(2)));

        IgniteEx g2 = startGrid(2);

        assertEquals(3, g2.cluster().nodes().size());

        g2.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        assertEquals((Integer)1, g0.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(1));
    }

    /** Status names the certificate of every transport, a client connector on its own factory included, and fails on an expired one. */
    @Test
    public void testStatus() throws Exception {
        ownClientConnectorFactory = true;

        place("node02old", keyStore(getTestIgniteInstanceName(0)));

        IgniteEx g = startGrid(0);

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> status(g), Exception.class, null));
        String id = node(g);

        assertContains(log, res, id + ": " + CLIENT_CONNECTOR + "\n    loaded subject=CN=node02old, issuer=");
        assertContains(log, res, id + ": " + COMMUNICATION + ", " + DISCOVERY + "\n    loaded subject=CN=node02old, issuer=");
        assertContains(log, res, "CN=twoca");
        assertContains(log, res, "PROBLEM: the loaded certificate is not valid now");
    }

    /** A node that starts on an expired certificate logs a warning. */
    @Test
    public void testExpiredCertificateWarnedAtStart() throws Exception {
        place("node02old", keyStore(getTestIgniteInstanceName(0)));

        LogListener expired = LogListener.matches("The TLS certificate chain in use is not valid now").build();

        nodeLog.registerListener(expired);

        startGrid(0);

        assertTrue(expired.check());
    }

    /** A failed job result still identifies the node by UUID and consistent ID. */
    @Test
    public void testFailedJobNamesNode() {
        GridTestNode n = new GridTestNode(UUID.randomUUID());

        n.setConsistentId("cid");

        ComputeJobResult res = Mockito.mock(ComputeJobResult.class);

        Mockito.when(res.getNode()).thenReturn(n);
        Mockito.when(res.getException()).thenReturn(new IgniteException("node left"));

        Throwable e = GridTestUtils.assertThrows(log, () -> new SslReloadTask().reduce(List.of(res)).result(), IgniteException.class, null);

        assertContains(log, e.getMessage(), "Failed on 1 node(s):\n" + n.id() + " [consistentId=cid]: node left");
    }

    /** Both commands report that no SSL context factory is configured. */
    @Test
    public void testWithoutSsl() throws Exception {
        IgniteEx g = startGrid(getTestIgniteInstanceName(0), cfg -> cfg.setSslContextFactory(null)
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()));

        assertContains(log, reload(g), node(g) + ": no SSL context factory is configured");
        assertContains(log, status(g), node(g) + ": no SSL context factory is configured");
    }

    /** Checks client authentication on discovery, communication, the client connector and binary REST. */
    private void assertTransportClientAuthentication(IgniteEx node, String trustedKey, String untrustedKey) throws Exception {
        for (int port : new int[] {
            discoveryPort(node),
            ((TcpCommunicationSpi)node.configuration().getCommunicationSpi()).boundPort(),
            node.context().clientListener().port(),
            node.localNode().attribute(ATTR_REST_TCP_PORT)
        })
            assertClientAuthentication(port, trustedKey, untrustedKey);
    }

    /** Opens a new authenticated client connection; its own trust store accepts both test authorities. */
    private void assertClientConnected(IgniteEx node, String keyStore) {
        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:" + node.context().clientListener().port())
            .setSslMode(SslMode.REQUIRED).setSslContextFactory(GridTestUtils.sslTrustedFactory(keyStore, "trustboth"))
            .setPartitionAwarenessEnabled(false).setClusterDiscoveryEnabled(false))) {
            cli.cacheNames();
        }
    }

    /** @return Factory reading the stores of the node. */
    private Factory<SSLContext> storeFactory(String igniteInstanceName) {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore(igniteInstanceName).toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustStoreFilePath(trustStore(igniteInstanceName).toString());
        factory.setTrustStorePassword(GridTestUtils.keyStorePassword().toCharArray());

        return factory;
    }

    /** */
    private Path keyStore(String igniteInstanceName) {
        return dir.resolve(igniteInstanceName + "-key.jks");
    }

    /** */
    private Path trustStore(String igniteInstanceName) {
        return dir.resolve(igniteInstanceName + "-trust.jks");
    }

    /** @param name Test key store to place for every running node. */
    private void placeKeys(String name) throws IOException {
        for (Ignite node : G.allGrids())
            place(name, keyStore(node.name()));
    }

    /** @param name Test trust store to place for every running node. */
    private void placeTrust(String name) throws IOException {
        for (Ignite node : G.allGrids())
            place(name, trustStore(node.name()));
    }

    /** */
    private void assertReloaded(String res, IgniteEx node, String transports) {
        assertContains(log, res, node(node) + ": reloaded " + transports + "; loaded ");
    }

    /** */
    private void assertFailed(String res, IgniteEx node, String transports) {
        assertContains(log, res, node(node) + ": failed on " + transports + " (");
    }

    /** Counts loads and deliberately considers distinct instances equal. */
    private class TestFactory implements Factory<SSLContext> {
        /** Factory calls, including failed loads. */
        private int calls;

        /** Whether loads fail. */
        private boolean fail;

        /** {@inheritDoc} */
        @Override public SSLContext create() {
            calls++;

            if (fail)
                throw new IgniteException("Certificate source is unavailable");

            return storeFactory(getTestIgniteInstanceName(0)).create();
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof TestFactory;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return 0;
        }
    }
}
