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
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.regex.Pattern;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.ssl.SslContextReloadable.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextReloadable.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextReloadable.DISCOVERY;
import static org.apache.ignite.ssl.SslTestUtils.discoveryPort;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.reloadFailure;
import static org.apache.ignite.ssl.SslTestUtils.servedCertificate;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

/**
 * Tests {@code --ssl reload} and {@code --ssl status} on running nodes. Every node runs on a key store and a trust store of its own, so
 * that nodes can be rotated and broken one by one; a node trusts any peer unless a trust store is placed for it. node01 is issued by oneca;
 * node02, node03 and the expired node02old by twoca.
 */
public class SslContextReloadNodeTest extends GridCommonAbstractTest {
    /** Transports of a node whose client connector shares the factory of the node. */
    private static final String ALL_TRANSPORTS = CLIENT_CONNECTOR + ", " + COMMUNICATION + ", " + DISCOVERY;

    /** Directory the stores of the nodes are placed in. */
    private Path dir;

    /** Whether the node being started uses SSL. */
    private boolean ssl = true;

    /** Whether the factory of the node hands back the same context every time, the way a factory caching it does. */
    private boolean cachingFactory;

    /** Whether the client connector runs on a factory of its own. */
    private boolean ownClientConnectorFactory;

    /** Test trust store every node starts on, unless one is placed for it; {@code null} to trust any peer. */
    private String trustStore;

    /** */
    private ListeningTestLogger nodeLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName).setGridLogger(nodeLog);

        if (!ssl)
            return cfg;

        if (!Files.exists(keyStore(igniteInstanceName)))
            place("node01", keyStore(igniteInstanceName));

        if (trustStore != null && !Files.exists(trustStore(igniteInstanceName)))
            place(trustStore, trustStore(igniteInstanceName));

        Factory<SSLContext> factory = storeFactory(igniteInstanceName);

        if (cachingFactory) {
            SSLContext ctx = factory.create();

            factory = () -> ctx;
        }

        ClientConnectorConfiguration cliCfg = new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(false)
            .setUseIgniteSslContextFactory(!ownClientConnectorFactory);

        if (ownClientConnectorFactory)
            cliCfg.setSslContextFactory(storeFactory(igniteInstanceName));

        return cfg.setSslContextFactory(factory).setClientConnectorConfiguration(cliCfg);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        dir = Files.createTempDirectory("ignite-ssl-reload-node-");
        nodeLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ssl = true;
        cachingFactory = false;
        ownClientConnectorFactory = false;
        trustStore = null;

        U.delete(dir);
    }

    /** A rotation on a running cluster reaches new connections of every transport, the report, the node log, status and metrics. */
    @Test
    public void testReloadOnRunningCluster() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        // Both nodes name the node the command came through.
        LogListener logged = LogListener.matches(Pattern.compile("TLS certificates reloaded \\[transports=" + ALL_TRANSPORTS +
            ", subject=CN=node02, .*initiator=management command, originNodeId=" + g0.localNode().id())).times(2).build();

        nodeLog.registerListener(logged);

        IgniteCache<Integer, Integer> cache = g0.getOrCreateCache(DEFAULT_CACHE_NAME);

        placeKeys("node02");

        String res = reload(g0, g1);

        assertReloaded(res, g0, ALL_TRANSPORTS);
        assertReloaded(res, g1, ALL_TRANSPORTS);
        assertContains(log, res, "serving subject=CN=node02, issuer=");
        assertTrue(logged.check());

        assertEquals("CN=node02", served(g0.context().clientListener().port()));

        // Discovery accepts on a plain socket and secures every connection separately, so the listening socket does not pin a certificate.
        assertEquals("CN=node02", served(discoveryPort(g0)));

        String status = status(g0);

        assertContains(log, status, "serving subject=CN=node02, issuer=");
        assertContains(log, status, "last reload succeeded at ");

        assertEquals("CN=node02", metric(g0, "CertificateSubject"));
        assertContains(log, metric(g0, "CertificateIssuer"), "CN=twoca");
        assertTrue(Long.parseLong(metric(g0, "LastReloadTime")) > 0);

        // twoca expires before node02 itself, and peers refuse the chain from then on.
        assertContains(log, status, "chainNotAfter=" + Instant.ofEpochMilli(Long.parseLong(metric(g0, "CertificateNotAfter"))));

        cache.put(1, 1);

        assertEquals((Integer)1, g1.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(1));
    }

    /** A node that fails holds the others back neither from the new certificate nor from the report, and the command fails. */
    @Test
    public void testPartialFailureAcrossNodes() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        place("node02", keyStore(g0.name()));
        Files.write(keyStore(g1.name()), "not a key store".getBytes());

        String res = reloadFailure(log, g0, g1);

        assertReloaded(res, g0, ALL_TRANSPORTS);
        assertFailed(res, g1, ALL_TRANSPORTS);
        assertContains(log, res, "Failed to initialize key store");

        assertEquals("CN=node02", served(g0.context().clientListener().port()));
        assertEquals("CN=node01", served(g1.context().clientListener().port()));

        assertEquals("1", metric(g1, "ReloadFailures"));
        assertContains(log, metric(g1, "LastReloadFailure"), "Failed to initialize key store");
        assertEquals("CN=node01", metric(g1, "CertificateSubject"));
    }

    /** A factory that hands back the context in use leaves nothing to read again, which fails the reload with that reason. */
    @Test
    public void testCachingFactoryNotReloaded() throws Exception {
        cachingFactory = true;

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        String res = reloadFailure(log, g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "hands back the context in use");

        assertEquals("CN=node01", served(g.context().clientListener().port()));
    }

    /** Neither a certificate the node's own trust store rejects nor an expired one is put in use; the report, log and status say why. */
    @Test
    public void testRejectedCertificateNotApplied() throws Exception {
        trustStore = "trustone";

        LogListener untrusted = LogListener.matches(Pattern.compile("Failed to reload TLS certificates, the ones in use stay " +
            "\\[transports=" + ALL_TRANSPORTS + ", initiator=management command, .*reason=A handshake between nodes on the new " +
            "certificate was refused.*subject=CN=node02,")).build();

        LogListener expired = LogListener.matches(Pattern.compile("Failed to reload TLS certificates, the ones in use stay .*" +
            "reason=The new certificate chain is not valid now \\[subject=CN=node02old,")).build();

        nodeLog.registerListener(untrusted);
        nodeLog.registerListener(expired);

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        String res = reloadFailure(log, g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "A handshake between nodes on the new certificate was refused");
        assertContains(log, res, "subject=CN=node02,");
        assertTrue(untrusted.check());

        assertEquals("CN=node01", served(discoveryPort(g)));

        String status = status(g);

        assertContains(log, status, "serving subject=CN=node01, issuer=");
        assertContains(log, status, "last reload failed 1 time(s) in a row");
        assertContains(log, status, "A handshake between nodes on the new certificate was refused");

        placeTrust("trustboth");
        placeKeys("node02old");

        res = reloadFailure(log, g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "The new certificate chain is not valid now [subject=CN=node02old,");
        assertTrue(expired.check());

        assertEquals("CN=node01", served(discoveryPort(g)));
    }

    /** A client connector on a factory of its own reloads apart, without the handshake between nodes its trust store has no say in. */
    @Test
    public void testOwnClientConnectorFactoryReloadedApart() throws Exception {
        trustStore = "trustone";
        ownClientConnectorFactory = true;

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        String res = reloadFailure(log, g);

        assertReloaded(res, g, CLIENT_CONNECTOR);
        assertFailed(res, g, COMMUNICATION + ", " + DISCOVERY);

        assertEquals("CN=node02", served(g.context().clientListener().port()));
        assertEquals("CN=node01", served(discoveryPort(g)));
    }

    /** The authority is replaced by steps: trust the new one, present its certificates, drop the old one; a node of the new one joins. */
    @Test
    public void testCertificateAuthorityReplaced() throws Exception {
        trustStore = "trustone";

        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        placeTrust("trustboth");
        reload(g0, g1);

        placeKeys("node02");
        reload(g0, g1);

        placeTrust("trusttwo");
        reload(g0, g1);

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
        String id = g.localNode().id().toString();

        assertContains(log, res, id + ": " + CLIENT_CONNECTOR + "\n    serving subject=CN=node02old, issuer=");
        assertContains(log, res, id + ": " + COMMUNICATION + ", " + DISCOVERY + "\n    serving subject=CN=node02old, issuer=");
        assertContains(log, res, "CN=twoca");
        assertContains(log, res, "PROBLEM: the certificate is not valid now, peers refuse it");
    }

    /** Both commands say so on a node without SSL. */
    @Test
    public void testWithoutSsl() throws Exception {
        ssl = false;

        IgniteEx g = startGrid(0);

        assertContains(log, reload(g), g.localNode().id() + ": SSL is not configured");
        assertContains(log, status(g), g.localNode().id() + ": SSL is not configured");
    }

    /** @return Factory reading the stores of the node, trusting any peer if the node has no trust store. */
    private Factory<SSLContext> storeFactory(String igniteInstanceName) {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore(igniteInstanceName).toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());

        if (Files.exists(trustStore(igniteInstanceName))) {
            factory.setTrustStoreFilePath(trustStore(igniteInstanceName).toString());
            factory.setTrustStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        }
        else
            factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

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

    /**
     * @param name Test store, as {@code tests.properties} names it.
     * @param dest File to replace.
     */
    private static void place(String name, Path dest) throws IOException {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), dest, StandardCopyOption.REPLACE_EXISTING);
    }

    /** @return Subject of the certificate the node presents on a new connection to the port. */
    private static String served(int port) throws Exception {
        // A fresh context has an empty session cache, so the handshake cannot resume a session on the certificate served before.
        SSLContext probe = GridTestUtils.sslTrustedFactory("node01", "trustboth").create();

        return servedCertificate(probe, port).getSubjectX500Principal().getName();
    }

    /** @return Metric of the client connector of the node, as a string. */
    private static String metric(IgniteEx node, String name) {
        return node.context().metric().registry("ssl.client.connector").findMetric(name).getAsString();
    }

    /** */
    private void assertReloaded(String res, IgniteEx node, String transports) {
        assertContains(log, res, node.localNode().id() + ": reloaded " + transports + "; serving ");
    }

    /** */
    private void assertFailed(String res, IgniteEx node, String transports) {
        assertContains(log, res, node.localNode().id() + ": failed on " + transports + " (");
    }
}
