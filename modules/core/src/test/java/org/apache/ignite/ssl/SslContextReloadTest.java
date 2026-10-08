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
import java.util.regex.Pattern;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignite;
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

import static org.apache.ignite.internal.ssl.SslContextRegistry.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextRegistry.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextRegistry.DISCOVERY;
import static org.apache.ignite.ssl.SslTestUtils.discoveryPort;
import static org.apache.ignite.ssl.SslTestUtils.place;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.reloadFailure;
import static org.apache.ignite.ssl.SslTestUtils.servedSubject;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;

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

    /** A rotation on a running cluster reaches new connections, the report, the node log, status and metrics. */
    @Test
    public void testReloadOnRunningCluster() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        LogListener logged = LogListener.matches(Pattern.compile("TLS certificates reloaded \\[transports=" + ALL_TRANSPORTS +
            ", subject=CN=node02, .*initiator=management command, originNodeId=" + g0.localNode().id())).times(2).build();

        nodeLog.registerListener(logged);

        placeKeys("node02");

        String res = reload(g0, g1);

        assertReloaded(res, g0, ALL_TRANSPORTS);
        assertReloaded(res, g1, ALL_TRANSPORTS);
        assertTrue(logged.check());

        assertEquals("CN=node02", servedSubject(g0.context().clientListener().port()));

        assertEquals("CN=node02", servedSubject(discoveryPort(g0)));

        assertEquals("CN=node02", metric(g0, "CertificateSubject"));
        assertContains(log, metric(g0, "CertificateIssuer"), "CN=twoca");
        assertTrue(Long.parseLong(metric(g0, "LastReloadSuccessTime")) > 0);

        long chainNotAfter = Long.parseLong(metric(g0, "ChainNotAfter"));

        assertContains(log, status(g0), "chainNotAfter=" + Instant.ofEpochMilli(chainNotAfter));
    }

    /** A node that fails holds the others back neither from the new certificate nor from the report, and the command fails. */
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

        String failed = "Failed on 1 node(s):\n" + g1.localNode().id() + ": failed on ";
        String succeeded = "\n\nSucceeded on 1 node(s):\n" + g0.localNode().id() + ": reloaded ";

        assertTrue("The failed node must be listed first, apart from the other one: " + res,
            res.contains(failed) && res.indexOf(succeeded) > res.indexOf(failed));

        assertEquals("CN=node02", servedSubject(g0.context().clientListener().port()));
        assertEquals("CN=node01", servedSubject(g1.context().clientListener().port()));

        assertEquals("1", metric(g1, "ConsecutiveReloadFailures"));
        assertContains(log, metric(g1, "LastReloadFailureReason"), "Failed to initialize key store");
        assertEquals("CN=node01", metric(g1, "CertificateSubject"));
    }

    /** A factory that hands back the context in use leaves nothing to read again, which fails the reload with that reason. */
    @Test
    public void testCachingFactoryNotReloaded() throws Exception {
        IgniteEx g = startGrid(0, cfg -> {
            SSLContext ctx = cfg.getSslContextFactory().create();

            cfg.setSslContextFactory(() -> ctx);
        });

        placeKeys("node02");

        String res = reloadFailure(g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "hands back the context in use");

        assertEquals("CN=node01", servedSubject(g.context().clientListener().port()));
    }

    /** Neither a certificate the node's own trust store rejects nor an expired one is put in use; the report, log and status say why. */
    @Test
    public void testRejectedCertificateNotApplied() throws Exception {
        initTrustStore = "trustone";

        LogListener untrusted = LogListener.matches(Pattern.compile("Failed to reload TLS certificates, the ones in use stay " +
            "\\[transports=" + ALL_TRANSPORTS + ", initiator=management command, .*reason=A handshake between nodes on the new " +
            "certificate was refused.*subject=CN=node02,")).build();

        LogListener expired = LogListener.matches(Pattern.compile("Failed to reload TLS certificates, the ones in use stay .*" +
            "reason=The new certificate chain is not valid now \\[subject=CN=node02old,")).build();

        nodeLog.registerListener(untrusted);
        nodeLog.registerListener(expired);

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        String res = reloadFailure(g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "A handshake between nodes on the new certificate was refused");
        assertContains(log, res, "subject=CN=node02,");
        assertTrue(untrusted.check());

        assertEquals("CN=node01", servedSubject(discoveryPort(g)));

        String status = status(g);

        assertContains(log, status, "serving subject=CN=node01, issuer=");
        assertContains(log, status, "last reload failed 1 time(s) in a row");
        assertContains(log, status, "A handshake between nodes on the new certificate was refused");

        placeTrust("trustboth");
        placeKeys("node02old");

        res = reloadFailure(g);

        assertFailed(res, g, ALL_TRANSPORTS);
        assertContains(log, res, "The new certificate chain is not valid now [subject=CN=node02old,");
        assertTrue(expired.check());

        assertEquals("CN=node01", servedSubject(discoveryPort(g)));
    }

    /** A client connector on a factory of its own reloads apart, with no handshake check between nodes: they do not use its trust store. */
    @Test
    public void testOwnClientConnectorFactoryReloadedApart() throws Exception {
        initTrustStore = "trustone";
        ownClientConnectorFactory = true;

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        String res = reloadFailure(g);

        assertReloaded(res, g, CLIENT_CONNECTOR);
        assertFailed(res, g, COMMUNICATION + ", " + DISCOVERY);

        assertEquals("CN=node02", servedSubject(g.context().clientListener().port()));
        assertEquals("CN=node01", servedSubject(discoveryPort(g)));
    }

    /** The authority is replaced by steps: trust the new one, present its certificates, drop the old one; a node of the new one joins. */
    @Test
    public void testCertificateAuthorityReplaced() throws Exception {
        initTrustStore = "trustone";

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
        IgniteEx g = startGrid(getTestIgniteInstanceName(0), cfg -> cfg.setSslContextFactory(null)
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()));

        assertContains(log, reload(g), g.localNode().id() + ": SSL is not configured");
        assertContains(log, status(g), g.localNode().id() + ": SSL is not configured");
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
        assertContains(log, res, node.localNode().id() + ": reloaded " + transports + "; serving ");
    }

    /** */
    private void assertFailed(String res, IgniteEx node, String transports) {
        assertContains(log, res, node.localNode().id() + ": failed on " + transports + " (");
    }
}
