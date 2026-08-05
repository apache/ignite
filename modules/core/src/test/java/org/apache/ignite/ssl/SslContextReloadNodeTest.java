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

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.ssl.SslReloadCommandArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.VisorTaskResult;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.ssl.SslContextReloadable.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextReloadable.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextReloadable.DISCOVERY;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Tests {@code --ssl reload} on running nodes: it must move the SSL-enabled transports onto the key store that
 * replaced the one on disk while the cluster keeps operating, and with {@code --dry-run} report the same outcome
 * without changing anything.
 */
public class SslContextReloadNodeTest extends GridCommonAbstractTest {
    /** Reason the failing factory reports, standing in for an unreadable key store. */
    private static final String FAILURE_MSG = "Key store is unreadable";

    /** Switches the failing factory to failing; static, so that it is reachable from inside the node. */
    private static final AtomicBoolean FAIL_RELOAD = new AtomicBoolean();

    /** Key store file shared by the nodes; replaced on disk to simulate certificate rotation. */
    private Path keyStore;

    /** Whether SSL should be configured for the node being started. */
    private boolean ssl = true;

    /** Whether the node uses a custom factory that caches the context and therefore cannot be reloaded. */
    private boolean cachingFactory;

    /** Whether the node uses a custom factory that fails to rebuild the context. */
    private boolean failingFactory;

    /** Test trust store the node runs on, {@code null} to trust any peer. */
    private String trustStore;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (ssl) {
            cfg.setSslContextFactory(nodeSslContextFactory());

            cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setSslClientAuth(false)
                .setUseIgniteSslContextFactory(true));
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        keyStore = Files.createTempFile("ignite-ssl-reload-node-", ".jks");

        copyKeyStore("node01");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        ssl = true;
        cachingFactory = false;
        failingFactory = false;
        trustStore = null;

        FAIL_RELOAD.set(false);

        if (keyStore != null)
            Files.deleteIfExists(keyStore);
    }

    /**
     * @return SSL context factory the node under test runs on, according to the flags set by the test.
     */
    private Factory<SSLContext> nodeSslContextFactory() {
        Factory<SSLContext> factory = reloadableFactory();

        if (cachingFactory) {
            // A ready-made context, the way a factory caching it internally hands it over: the node gets the very
            // same instance back, so there is nothing to read again.
            SSLContext ctx = factory.create();

            return () -> ctx;
        }

        if (failingFactory) {
            return () -> {
                if (FAIL_RELOAD.get())
                    throw new IgniteException(FAILURE_MSG);

                return factory.create();
            };
        }

        return factory;
    }

    /** Certificate reload on every SSL transport of a running two-node cluster must succeed and keep it operational. */
    @Test
    public void testReloadOnRunningCluster() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        IgniteCache<Integer, Integer> cache = g0.getOrCreateCache(DEFAULT_CACHE_NAME);

        cache.put(1, 1);

        assertEquals("Cluster must be operational before the reload",
            (Integer)1, g1.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(1));

        X509Certificate cliCertBefore = servedCertificate(clientConnectorPort(g0));
        X509Certificate discoCertBefore = servedCertificate(discoveryPort(g0));

        // Rotate the certificate on disk.
        copyKeyStore("node02");

        String res = reload(g0, g1);

        assertReloaded(res, g0, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);
        assertReloaded(res, g1, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);

        // The report names the certificate now in use, so a rotation can be verified without probing the ports.
        assertContains(log, res, "serving CN=node02");

        assertRotated(CLIENT_CONNECTOR, cliCertBefore, servedCertificate(clientConnectorPort(g0)));

        // Discovery accepts on a plain socket and secures every connection separately, so the listening socket
        // does not pin the certificate it was bound with.
        assertRotated(DISCOVERY, discoCertBefore, servedCertificate(discoveryPort(g0)));

        cache.put(2, 2);

        assertEquals("Established sessions must survive the reload",
            (Integer)2, g1.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(2));
    }

    /**
     * A second rotation has to take effect as well. Each reload compares what it built against the context in use,
     * so a component that kept comparing against the one it started with would report the second rotation as
     * nothing to do.
     */
    @Test
    public void testSecondReloadRotatesAgain() throws Exception {
        IgniteEx g = startGrid(0);

        copyKeyStore("node02");

        assertContains(log, reload(g), "serving CN=node02");

        X509Certificate afterFirst = servedCertificate(discoveryPort(g));

        copyKeyStore("node03");

        assertContains(log, reload(g), "serving CN=node03");

        assertRotated("A second reload", afterFirst, servedCertificate(discoveryPort(g)));
    }

    /** A caching custom factory cannot be reloaded, and the command must report that instead of a success. */
    @Test
    public void testCachingFactoryReportedAsNotReloaded() throws Exception {
        cachingFactory = true;

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(clientConnectorPort(g));

        copyKeyStore("node02");

        String res = reload(g);

        assertContains(log, res, "not reloaded");

        // A successful list is always printed right after the node id, so its absence means nothing was reloaded.
        assertNotContains(log, res, ": reloaded");

        // The operator must be told why, not just that nothing happened.
        assertContains(log, res, "handed over ready-made");

        assertKept("A caching factory", certBefore, servedCertificate(clientConnectorPort(g)));
    }

    /** A factory that cannot rebuild the context must be reported per component, and the old one must stay in use. */
    @Test
    public void testFailingFactoryReportedPerComponent() throws Exception {
        failingFactory = true;

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(clientConnectorPort(g));

        FAIL_RELOAD.set(true);

        // The whole chain, so that the assertions do not depend on how the compute framework wraps the failure.
        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null));

        // Every SSL transport is attempted, so a single broken one does not hide the state of the others.
        assertContains(log, res, COMMUNICATION);
        assertContains(log, res, DISCOVERY);
        assertContains(log, res, CLIENT_CONNECTOR);
        assertContains(log, res, FAILURE_MSG);

        assertKept("A failed reload", certBefore, servedCertificate(clientConnectorPort(g)));
    }

    /**
     * A certificate the node's own trust store rejects must not reach the inter-node transports: applying it would
     * leave the node unable to open new connections to the rest of the cluster.
     */
    @Test
    public void testUntrustedCertificateNotApplied() throws Exception {
        trustStore = "trustone";

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(discoveryPort(g));

        // node02 is issued by "twoca", which the "trust-one" store does not contain.
        copyKeyStore("node02");

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null));

        assertContains(log, res, "would fail on");
        assertContains(log, res, COMMUNICATION);
        assertContains(log, res, DISCOVERY);

        assertKept("A certificate the trust store rejects", certBefore, servedCertificate(discoveryPort(g)));
    }

    /** A dry run must accept a valid rotation and still leave the node on the certificate it is running. */
    @Test
    public void testDryRunAcceptsWithoutApplying() throws Exception {
        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(discoveryPort(g));

        copyKeyStore("node02");

        String res = dryRun(g);

        assertContains(log, res, "can be reloaded");
        assertContains(log, res, DISCOVERY);

        assertKept("An accepted but not applied certificate", certBefore, servedCertificate(discoveryPort(g)));
    }

    /** A dry run must reject a certificate that a reload would refuse, before any node has been touched. */
    @Test
    public void testDryRunRejectsUntrustedCertificate() throws Exception {
        trustStore = "trustone";

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(discoveryPort(g));

        copyKeyStore("node02");

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> dryRun(g), Exception.class, null));

        assertContains(log, res, "would fail on");
        assertContains(log, res, DISCOVERY);

        assertKept("A rejected certificate", certBefore, servedCertificate(discoveryPort(g)));
    }

    /** A client node runs the same inter-node transports, so the command must cover it as well. */
    @Test
    public void testClientNodeReloaded() throws Exception {
        IgniteEx srv = startGrid(0);
        IgniteEx cli = startClientGrid(1);

        copyKeyStore("node02");

        assertReloaded(reload(srv, cli), cli, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);
    }

    /** Reload must report nothing to reload on a node that does not use SSL. */
    @Test
    public void testReloadWithoutSsl() throws Exception {
        ssl = false;

        IgniteEx g = startGrid(0);

        String res = reload(g);

        assertContains(log, res, "SSL is not configured");
    }

    /** @param nodes Nodes to reload certificates on. */
    private String reload(IgniteEx... nodes) throws Exception {
        return execute(false, nodes);
    }

    /** @param nodes Nodes to check certificates on. */
    private String dryRun(IgniteEx... nodes) throws Exception {
        return execute(true, nodes);
    }

    /**
     * @param dryRun Whether the certificates are only checked.
     * @param nodes Nodes to run on, submitting from the first one.
     * @return Aggregated task result.
     */
    private String execute(boolean dryRun, IgniteEx... nodes) throws Exception {
        List<UUID> ids = new ArrayList<>();

        for (IgniteEx node : nodes)
            ids.add(node.localNode().id());

        SslReloadCommandArg arg = new SslReloadCommandArg();

        arg.dryRun(dryRun);
        arg.token(UUID.randomUUID());

        String prepared = run(arg, ids, nodes[0]);

        if (dryRun)
            return prepared;

        // Both phases, the way the command drives them: prepare reports what would happen, commit puts it in use.
        arg.commit(true);

        return run(arg, ids, nodes[0]);
    }

    /**
     * @param arg Argument carrying the phase.
     * @param ids Nodes to run on.
     * @param from Node to run from.
     * @return Report of that phase.
     */
    private String run(SslReloadCommandArg arg, List<UUID> ids, IgniteEx from) throws Exception {
        // Over the whole cluster, as the command itself does: the default facade covers server nodes only.
        VisorTaskResult<String> res = from.compute(from.cluster()).execute(SslReloadTask.class,
            new VisorTaskArgument<>(ids, arg, false));

        return res.result();
    }

    /**
     * @return SSL context factory reading the certificate from {@link #keyStore}, trusting either {@link #trustStore}
     *      or any peer.
     */
    private Factory<SSLContext> reloadableFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());

        if (trustStore == null)
            factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());
        else {
            factory.setTrustStoreFilePath(GridTestUtils.keyStorePath(trustStore));
            factory.setTrustStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        }

        return factory;
    }

    /**
     * @return SSL context factory of the probing client: a fixed identity every configuration under test keeps
     *      trusting, so that probing works before and after the rotation.
     */
    private Factory<SSLContext> probeFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(GridTestUtils.keyStorePath("node01"));
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /**
     * @param name Test key store name (see {@code tests.properties}).
     */
    private void copyKeyStore(String name) throws Exception {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), keyStore, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * @param res Aggregated report.
     * @param node Node whose line the report must carry.
     * @param comps Transports the node must have reloaded; the report lists them sorted by name.
     */
    private void assertReloaded(String res, IgniteEx node, String... comps) {
        assertContains(log, res, node.localNode().id() + ": reloaded " + String.join(", ", comps));
    }

    /**
     * @param name Transport that was expected to pick the rotated certificate up.
     * @param before Certificate served before the reload.
     * @param after Certificate served after the reload.
     */
    private void assertRotated(String name, X509Certificate before, X509Certificate after) {
        assertFalse(name + " must serve the rotated certificate to new connections", before.equals(after));
    }

    /**
     * @param what What was expected to leave the certificate alone.
     * @param before Certificate served before the reload.
     * @param after Certificate served after the reload.
     */
    private void assertKept(String what, X509Certificate before, X509Certificate after) {
        assertTrue(what + " must keep the previously loaded certificate", before.equals(after));
    }

    /** @param node Node to connect to. */
    private int clientConnectorPort(IgniteEx node) {
        return node.context().clientListener().port();
    }

    /** @param node Node to connect to. */
    private int discoveryPort(IgniteEx node) {
        return ((TcpDiscoverySpi)node.configuration().getDiscoverySpi()).getLocalPort();
    }

    /**
     * @param port Port to connect to.
     * @return Certificate the node presents on a new TLS connection to that port.
     */
    private X509Certificate servedCertificate(int port) throws Exception {
        // A fresh context every time: it has an empty session cache, so the handshake cannot be resumed and always
        // reports the certificate the node serves right now.
        SSLContext cliCtx = probeFactory().create();

        try (SSLSocket sock = (SSLSocket)cliCtx.getSocketFactory()
            .createSocket(InetAddress.getLoopbackAddress(), port)) {

            sock.startHandshake();

            return (X509Certificate)sock.getSession().getPeerCertificates()[0];
        }
    }
}
