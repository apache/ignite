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
 * Tests {@code --ssl reload} and {@code --ssl status} on running nodes. Every node runs on a key store and a trust store of its own, so
 * that nodes can be rotated and broken one by one. node01 is issued by oneca; node02, node03 and the expired node02old by twoca.
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
    @Override protected void afterTest() throws Exception {
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

        // Trusts oneca only: it cannot connect again once the node serves node02.
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

        assertEquals("CN=node02", p.servedCertificate().getSubjectX500Principal().getName());
        assertContains(log, p.servedCertificate().getIssuerX500Principal().getName(), "CN=twoca");
        assertTrue(p.reloadOutcome().successTime() > 0);

        String status = status(g0);

        assertContains(log, status, "chainNotAfter=" + Instant.ofEpochMilli(SslCertificates.chainNotAfter(p.servedChain())));
        assertContains(log, status, "last reload succeeded at " + Instant.ofEpochMilli(p.reloadOutcome().successTime()));
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

        assertEquals("A manual reload failure must not count as an automatic renewal", "0", metric(g1, "ConsecutiveRenewalFailures"));

        String status = status(g1);

        assertContains(log, status, "last reload failed 1 time(s) in a row");
        assertContains(log, status, "Failed to initialize key store");
        assertContains(log, status, "serving subject=CN=node01,");
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

    /** A factory that is not an {@link AbstractSslContextFactory} and builds a new context on every call is reloaded. */
    @Test
    public void testCustomFactoryReloaded() throws Exception {
        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(() -> storeFactory(cfg.getIgniteInstanceName()).create());
        });

        placeKeys("node02");

        assertReloaded(reload(g), g, ALL_TRANSPORTS);
        assertEquals("CN=node02", servedSubject(discoveryPort(g)));
    }

    /** Neither a certificate the node's own trust store rejects nor an expired one is put in use; the report, log and status say why. */
    @Test
    public void testRejectedCertificateNotApplied() throws Exception {
        initTrustStore = "trustone";

        LogListener untrusted = LogListener.matches(Pattern.compile("Failed to reload TLS certificates, the ones in use stay " +
            "\\[transports=" + ALL_TRANSPORTS + ", initiator=management command, .*reason=The new certificate fails a test handshake " +
            "with this node's own trust store.*subject=CN=node02,")).build();

        LogListener expired = LogListener.matches(Pattern.compile("Failed to reload TLS certificates, the ones in use stay .*" +
            "reason=The new certificate chain is not valid now \\[subject=CN=node02old,")).build();

        nodeLog.registerListener(untrusted);
        nodeLog.registerListener(expired);

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        String res = reloadFailure(g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "The new certificate fails a test handshake with this node's own trust store");
        assertContains(log, res, "subject=CN=node02,");
        assertTrue(untrusted.check());

        assertEquals("CN=node01", servedSubject(discoveryPort(g)));

        String status = status(g);

        assertContains(log, status, "serving subject=CN=node01, issuer=");
        assertContains(log, status, "last reload failed 1 time(s) in a row");
        assertContains(log, status, "The new certificate fails a test handshake with this node's own trust store");

        placeTrust("trustboth");
        placeKeys("node02old");

        res = reloadFailure(g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "The new certificate chain is not valid now [subject=CN=node02old,");
        assertTrue(expired.check());

        assertEquals("CN=node01", servedSubject(discoveryPort(g)));
    }

    /**
     * A client connector and binary REST with factories of their own reload separately and skip the handshake check: nodes do not use
     * their trust stores.
     */
    @Test
    public void testOwnClientConnectorFactoryReloadedApart() throws Exception {
        initTrustStore = "trustone";
        ownClientConnectorFactory = true;

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setConnectorConfiguration(new ConnectorConfiguration().setSslEnabled(true)
                .setSslFactory(storeFactory(cfg.getIgniteInstanceName())));
        });

        placeKeys("node02");

        String res = reloadFailure(g);

        assertReloaded(res, g, CLIENT_CONNECTOR);
        assertReloaded(res, g, BINARY_REST);
        assertFailed(res, g, COMMUNICATION + ", " + DISCOVERY);

        assertEquals("CN=node02", servedSubject(g.context().clientListener().port()));
        assertEquals("CN=node02", servedSubject(g.localNode().attribute(ATTR_REST_TCP_PORT)));
        assertEquals("CN=node01", servedSubject(discoveryPort(g)));
    }

    /** A key store that gives clients no certificate, such as one without a private key, is not put in use on a client connector. */
    @Test
    public void testKeyStoreWithoutKeyNotApplied() throws Exception {
        ownClientConnectorFactory = true;

        IgniteEx g = startGrid(0);

        placeKeys("trustboth");

        assertContains(log, reloadFailure(g), node(g) + ": failed on " + CLIENT_CONNECTOR +
            " (The new SSL context presents no certificate to a client");

        assertEquals("CN=node01", servedSubject(g.context().clientListener().port()));
    }

    /**
     * The authority is replaced in steps: trust the new one, present its certificates, drop the old one; then the nodes open new
     * connections to each other, and a node of the new one joins. Status shows which authorities every node trusts before the change
     * and after each change of the trust store.
     */
    @Test
    public void testCertificateAuthorityReplaced() throws Exception {
        initTrustStore = "trustone";

        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        assertNotContains(log, status(g0, g1), "CN=twoca");

        placeTrust("trustboth");
        reload(g0, g1);

        String trustsBoth = status(g0, g1);

        assertEquals(2, trustsBoth.split("    trusts: ", -1).length - 1);
        assertContains(log, trustsBoth, "CN=oneca, EMAILADDRESS=oneca@ignite.apache.org; ");
        assertContains(log, trustsBoth, "CN=twoca, EMAILADDRESS=twoca@ignite.apache.org");

        placeKeys("node02");
        reload(g0, g1);

        placeTrust("trusttwo");
        reload(g0, g1);

        String trustsTwo = status(g0, g1);

        assertNotContains(log, trustsTwo, "CN=oneca");
        assertContains(log, trustsTwo, "trusts: C=RU, ST=SPb, L=SPb, O=Ignite, OU=Dev, CN=twoca");

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

        assertContains(log, res, id + ": " + CLIENT_CONNECTOR + "\n    serving subject=CN=node02old, issuer=");
        assertContains(log, res, id + ": " + COMMUNICATION + ", " + DISCOVERY + "\n    serving subject=CN=node02old, issuer=");
        assertContains(log, res, "CN=twoca");
        assertContains(log, res, "PROBLEM: the certificate is not valid now, peers refuse it");

        String trusts = res.substring(res.indexOf("    trusts: "), res.indexOf('\n', res.indexOf("    trusts: ")));

        assertContains(log, trusts, "CN=oneca");
        assertContains(log, trusts, "CN=twoca");
    }

    /** A node whose own trust store does not accept its certificate starts, but warns that reloads and renewals will refuse it. */
    @Test
    public void testOwnTrustStoreRefusingOwnCertificateWarned() throws Exception {
        initTrustStore = "trusttwo";

        LogListener warned = LogListener.matches("The node's own trust store does not accept its certificate").build();

        nodeLog.registerListener(warned);

        startGrid(0);

        assertTrue(warned.check());
    }

    /** A node that starts on an expired certificate warns that peers refuse it, not that its own trust store does. */
    @Test
    public void testExpiredCertificateWarnedAtStart() throws Exception {
        place("node02old", keyStore(getTestIgniteInstanceName(0)));

        LogListener expired = LogListener.matches("The TLS certificate chain in use is not valid now").build();
        LogListener untrusted = LogListener.matches("The node's own trust store does not accept its certificate").build();

        nodeLog.registerListener(expired);
        nodeLog.registerListener(untrusted);

        startGrid(0);

        assertTrue(expired.check());
        assertFalse(untrusted.check());
    }

    /** A node whose job did not finish is named in the report like the others. */
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

    /** Both commands say so on a node without SSL. */
    @Test
    public void testWithoutSsl() throws Exception {
        IgniteEx g = startGrid(getTestIgniteInstanceName(0), cfg -> cfg.setSslContextFactory(null)
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()));

        assertContains(log, reload(g), node(g) + ": no SSL context factory is configured");
        assertContains(log, status(g), node(g) + ": no SSL context factory is configured");
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

    /** @return Metric of the client connector of the node, as a string. */
    private static String metric(IgniteEx node, String name) {
        return node.context().metric().registry("ssl.client.connector").findMetric(name).getAsString();
    }

    /** */
    private void assertReloaded(String res, IgniteEx node, String transports) {
        assertContains(log, res, node(node) + ": reloaded " + transports + "; serving ");
    }

    /** */
    private void assertFailed(String res, IgniteEx node, String transports) {
        assertContains(log, res, node(node) + ": failed on " + transports + " (");
    }
}
