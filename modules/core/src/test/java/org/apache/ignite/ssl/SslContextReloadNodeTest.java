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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import javax.cache.configuration.Factory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.CommandWarningException;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadCommandArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.management.ssl.SslStatusTask;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.ssl.SslMetrics;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.VisorTaskResult;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.ssl.SslContextReloadable.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextReloadable.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextReloadable.DISCOVERY;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Tests {@code --ssl reload} on running nodes: every node must move its SSL-enabled transports onto the stores that
 * replaced the ones on disk, on its own and without waiting for the others, and with {@code --dry-run} report the same
 * outcome without changing anything.
 * <p>
 * Every node runs on a key store and a trust store of its own, so that nodes can be rotated, and broken, one by one.
 * A node trusts any peer unless a trust store is placed for it.
 */
public class SslContextReloadNodeTest extends GridCommonAbstractTest {
    /** Reason the failing factory reports, standing in for an unreadable key store. */
    private static final String FAILURE_MSG = "Key store is unreadable";

    /** Switches the failing factory to failing; static, so that it is reachable from inside the node. */
    private static final AtomicBoolean FAIL_RELOAD = new AtomicBoolean();

    /** Directory the stores of the nodes are placed in. */
    private Path dir;

    /** Whether SSL should be configured for the node being started. */
    private boolean ssl = true;

    /** Whether the node uses a custom factory that caches the context and therefore cannot be reloaded. */
    private boolean cachingFactory;

    /** Whether the client connector runs on a factory of its own, rather than sharing the one of the node. */
    private boolean ownClientConnectorFactory;

    /** Whether the own factory of the client connector fails to rebuild the context once told to. */
    private boolean failingClientConnectorFactory;

    /** Test trust store every node starts on, unless one is placed for it; {@code null} to trust any peer. */
    private String trustStore;

    /** Log of the nodes under test, to see what a reload leaves there. */
    private ListeningTestLogger nodeLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(nodeLog);

        if (ssl) {
            if (!Files.exists(keyStore(igniteInstanceName)))
                place("node01", keyStore(igniteInstanceName));

            if (trustStore != null && !Files.exists(trustStore(igniteInstanceName)))
                place(trustStore, trustStore(igniteInstanceName));

            cfg.setSslContextFactory(nodeSslContextFactory(igniteInstanceName));

            ClientConnectorConfiguration cliCfg = new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setSslClientAuth(false);

            if (ownClientConnectorFactory) {
                Factory<SSLContext> factory = reloadableFactory(igniteInstanceName);

                cliCfg.setUseIgniteSslContextFactory(false)
                    .setSslContextFactory(failingClientConnectorFactory ? failing(factory) : factory);
            }
            else
                cliCfg.setUseIgniteSslContextFactory(true);

            cfg.setClientConnectorConfiguration(cliCfg);
        }

        return cfg;
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
        failingClientConnectorFactory = false;
        trustStore = null;

        FAIL_RELOAD.set(false);

        U.delete(dir);
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

        placeKeys("node02");

        String res = reload(g0, g1);

        assertReloaded(res, g0, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);
        assertReloaded(res, g1, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);

        // The report names the certificate now in use, and the authority that issued it, so a rotation can be
        // verified without probing the ports.
        assertContains(log, res, "serving CN=node02");
        assertContains(log, res, "issued by ");

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

        placeKeys("node02");

        assertContains(log, reload(g), "serving CN=node02");

        X509Certificate afterFirst = servedCertificate(discoveryPort(g));

        placeKeys("node03");

        assertContains(log, reload(g), "serving CN=node03");

        assertRotated("A second reload", afterFirst, servedCertificate(discoveryPort(g)));
    }

    /**
     * Nodes reload on their own: one that fails must not hold the others back, and the command must fail while still
     * reporting the nodes that moved.
     */
    @Test
    public void testPartialFailureAcrossNodes() throws Exception {
        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        X509Certificate cert1Before = servedCertificate(clientConnectorPort(g1));

        place("node02", keyStore(g0.name()));
        Files.write(keyStore(g1.name()), "not a key store".getBytes());

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> reload(g0, g1), Exception.class, null));

        assertReloaded(res, g0, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);
        assertContains(log, res, g1.localNode().id() + ": failed on " + CLIENT_CONNECTOR + ", " + COMMUNICATION +
            ", " + DISCOVERY);

        // The reason names the store, not only the wrapper the failure came in.
        assertContains(log, res, "Failed to initialize key store");

        assertEquals("CN=node02", servedCertificate(clientConnectorPort(g0)).getSubjectX500Principal().getName());
        assertKept("A node that failed", cert1Before, servedCertificate(clientConnectorPort(g1)));
    }

    /**
     * A caching custom factory cannot be reloaded. The command must say so, and end with a warning rather than as a
     * success, since the certificate in use stays.
     */
    @Test
    public void testCachingFactoryReportedAsNotReloaded() throws Exception {
        cachingFactory = true;

        LogListener lsnr = LogListener.matches("TLS certificates cannot be reloaded, the SSL context is handed over " +
            "ready-made").build();

        nodeLog.registerListener(lsnr);

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(clientConnectorPort(g));

        placeKeys("node02");

        Throwable e = GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null);

        assertTrue("A reload that left a certificate in place must end with a warning",
            X.hasCause(e, CommandWarningException.class));

        String res = X.getFullStackTrace(e);

        assertContains(log, res, "not reloaded");
        assertNotContains(log, res, ": reloaded");

        // The operator must be told why, not just that nothing happened.
        assertContains(log, res, "handed over ready-made");

        assertTrue("The node log must say the certificate stays", lsnr.check());

        assertKept("A caching factory", certBefore, servedCertificate(clientConnectorPort(g)));
    }

    /**
     * One factory that cannot rebuild the context must neither hide the state of the rest nor hold them back. The
     * broken one is the own factory of the client connector here, which the node reloads first, so the healthy one
     * of the node comes after the failure.
     */
    @Test
    public void testFailingFactoryReportedPerComponent() throws Exception {
        ownClientConnectorFactory = true;
        failingClientConnectorFactory = true;

        IgniteEx g = startGrid(0);

        X509Certificate cliCertBefore = servedCertificate(clientConnectorPort(g));
        X509Certificate discoCertBefore = servedCertificate(discoveryPort(g));

        placeKeys("node02");

        FAIL_RELOAD.set(true);

        // The whole chain, so that the assertions do not depend on how the compute framework wraps the failure.
        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null));

        assertContains(log, res, "failed on " + CLIENT_CONNECTOR);

        // The reason lies one cause deep, where the factory put it.
        assertContains(log, res, FAILURE_MSG);

        assertReloaded(res, g, COMMUNICATION, DISCOVERY);

        assertKept("A factory that failed", cliCertBefore, servedCertificate(clientConnectorPort(g)));
        assertRotated("A factory after the failed one", discoCertBefore, servedCertificate(discoveryPort(g)));
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
        placeKeys("node02");

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null));

        assertContains(log, res, "failed on " + CLIENT_CONNECTOR + ", " + COMMUNICATION + ", " + DISCOVERY);

        // The reason says which check refused the certificate, and which certificate it was.
        assertContains(log, res, "A handshake between nodes on the new certificate was refused");
        assertContains(log, res, "subject=CN=node02");

        assertKept("A certificate the trust store rejects", certBefore, servedCertificate(discoveryPort(g)));
    }

    /** A certificate the node's own trust store accepts must be put in use. */
    @Test
    public void testTrustedCertificateApplied() throws Exception {
        trustStore = "trustboth";

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        assertReloaded(reload(g), g, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);

        assertEquals("CN=node02", servedCertificate(clientConnectorPort(g)).getSubjectX500Principal().getName());
    }

    /** An expired certificate must not be put in use, even when its authority is trusted. */
    @Test
    public void testExpiredCertificateNotApplied() throws Exception {
        trustStore = "trustboth";

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(clientConnectorPort(g));

        placeKeys("node02old");

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null));

        assertContains(log, res, "failed on");
        assertContains(log, res, "subject=CN=node02old");

        assertKept("An expired certificate", certBefore, servedCertificate(clientConnectorPort(g)));
    }

    /**
     * A client connector on a factory of its own is only checked to build: its trust store is configured for the
     * clients, so it has no say over the certificate of the node.
     */
    @Test
    public void testOwnClientConnectorFactoryOnlyBuilt() throws Exception {
        trustStore = "trustone";
        ownClientConnectorFactory = true;

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null));

        assertContains(log, res, "failed on " + COMMUNICATION + ", " + DISCOVERY);
        assertReloaded(res, g, CLIENT_CONNECTOR);

        assertEquals("CN=node02", servedCertificate(clientConnectorPort(g)).getSubjectX500Principal().getName());
    }

    /**
     * A node that trusts only the authority of the rotated certificates must be able to join and to exchange data,
     * which it can only if the running nodes present the rotated certificates on discovery and communication alike,
     * on connections they accept and on the ones they open.
     */
    @Test
    public void testJoinAfterRotation() throws Exception {
        trustStore = "trustboth";

        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        placeKeys("node02");

        reload(g0, g1);

        place("node03", keyStore(getTestIgniteInstanceName(2)));
        place("trusttwo", trustStore(getTestIgniteInstanceName(2)));

        IgniteEx g2 = startGrid(2);

        assertEquals("The joining node must reach the rotated cluster", 3, g2.cluster().nodes().size());

        g2.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        assertEquals("Traffic between nodes must go over the rotated certificates",
            (Integer)1, g0.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(1));
    }

    /**
     * The certificate authority is replaced the way the documentation describes: trust the new one, present
     * certificates it issued, stop trusting the old one. Every step must reload, and the cluster must keep working
     * and keep letting nodes of the new authority in.
     */
    @Test
    public void testCertificateAuthorityReplaced() throws Exception {
        trustStore = "trustone";

        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        placeTrust("trustboth");
        assertReloaded(reload(g0, g1), g1, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);

        placeKeys("node02");
        assertReloaded(reload(g0, g1), g1, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);

        placeTrust("trusttwo");
        assertReloaded(reload(g0, g1), g1, CLIENT_CONNECTOR, COMMUNICATION, DISCOVERY);

        place("node03", keyStore(getTestIgniteInstanceName(2)));
        place("trusttwo", trustStore(getTestIgniteInstanceName(2)));

        IgniteEx g2 = startGrid(2);

        assertEquals(3, g2.cluster().nodes().size());

        g2.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        assertEquals((Integer)1, g0.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(1));
    }

    /** Every certificate put in use must be named in the node log, together with who asked for it. */
    @Test
    public void testReloadLogged() throws Exception {
        LogListener lsnr = LogListener.matches(Pattern.compile("TLS certificates reloaded \\[transports=" +
            CLIENT_CONNECTOR + ", " + COMMUNICATION + ", " + DISCOVERY + ", subject=CN=node02, .*" +
            "initiator=management command, originNodeId=")).build();

        nodeLog.registerListener(lsnr);

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        reload(g);

        assertTrue("The node log must name the certificate put in use and who asked for it", lsnr.check());
    }

    /** A reload that failed must leave its reason in the node log, not only in the answer to the command. */
    @Test
    public void testFailedReloadLogged() throws Exception {
        trustStore = "trustone";

        LogListener lsnr = LogListener.matches(Pattern.compile("Failed to reload TLS certificates, the ones in use " +
            "stay \\[transports=" + CLIENT_CONNECTOR + ", " + COMMUNICATION + ", " + DISCOVERY + ", .*reason=A " +
            "handshake between nodes on the new certificate was refused.*subject=CN=node02")).build();

        nodeLog.registerListener(lsnr);

        IgniteEx g = startGrid(0);

        // node02 is issued by "twoca", which the "trust-one" store does not contain.
        placeKeys("node02");

        GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null);

        assertTrue("The node log must name the reason a reload failed", lsnr.check());
    }

    /** A dry run must accept a valid rotation and still leave the node on the certificate it is running. */
    @Test
    public void testDryRunAcceptsWithoutApplying() throws Exception {
        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(discoveryPort(g));

        placeKeys("node02");

        String res = dryRun(g);

        assertContains(log, res, "can be reloaded");
        assertContains(log, res, DISCOVERY);
        assertNotContains(log, res, ": reloaded");

        assertKept("An accepted but not applied certificate", certBefore, servedCertificate(discoveryPort(g)));
    }

    /** A dry run must reject a certificate that a reload would refuse, without touching the node. */
    @Test
    public void testDryRunRejectsUntrustedCertificate() throws Exception {
        trustStore = "trustone";

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(discoveryPort(g));

        placeKeys("node02");

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

        placeKeys("node02");

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

    /**
     * Status must name, for every transport, the certificate it serves, the authorities it trusts and how its last
     * reload went, including a client connector that runs on a factory of its own.
     */
    @Test
    public void testStatusReportsEveryTransport() throws Exception {
        trustStore = "trustboth";
        ownClientConnectorFactory = true;

        IgniteEx g = startGrid(0);

        String res = status(g);

        String id = g.localNode().id().toString();

        assertContains(log, res, id + ": " + CLIENT_CONNECTOR + "\n    serving CN=node01, issued by ");
        assertContains(log, res, id + ": " + COMMUNICATION + ", " + DISCOVERY + "\n    serving CN=node01, issued by ");
        assertContains(log, res, "CN=oneca");
        assertContains(log, res, "    trusts ");
        assertContains(log, res, "CN=twoca");
        assertContains(log, res, "not reloaded since the node started");
    }

    /** After a reload, status must name the new certificate and say when the reload succeeded. */
    @Test
    public void testStatusAfterReload() throws Exception {
        IgniteEx g = startGrid(0);

        placeKeys("node02");

        reload(g);

        String res = status(g);

        assertContains(log, res, "serving CN=node02");
        assertContains(log, res, "last reload succeeded at ");
    }

    /** A failed reload must make status end with a warning that names the reason, while the old certificate stays. */
    @Test
    public void testStatusWarnsAfterFailedReload() throws Exception {
        trustStore = "trustone";

        IgniteEx g = startGrid(0);

        placeKeys("node02");

        GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null);

        Throwable e = GridTestUtils.assertThrows(log, () -> status(g), Exception.class, null);

        assertTrue("A failed reload must make status end with a warning", X.hasCause(e, CommandWarningException.class));

        String res = X.getFullStackTrace(e);

        assertContains(log, res, "serving CN=node01");
        assertContains(log, res, "last reload failed 1 time(s) in a row");
        assertContains(log, res, "A handshake between nodes on the new certificate was refused");
    }

    /** A certificate that is not valid any more must fail status, not only be listed. */
    @Test
    public void testStatusFailsOnExpiredCertificate() throws Exception {
        place("node02old", keyStore(getTestIgniteInstanceName(0)));

        IgniteEx g = startGrid(0);

        Throwable e = GridTestUtils.assertThrows(log, () -> status(g), Exception.class, null);

        assertFalse("An expired certificate is a failure, not a warning", X.hasCause(e, CommandWarningException.class));

        assertContains(log, X.getFullStackTrace(e), "PROBLEM: the certificate is not valid now");
    }

    /** The metrics of a transport must follow the certificate it serves and the outcome of its reloads. */
    @Test
    public void testMetrics() throws Exception {
        trustStore = "trustboth";

        IgniteEx g = startGrid(0);

        MetricRegistryImpl reg = g.context().metric().registry(SslMetrics.registryName(CLIENT_CONNECTOR));

        assertEquals("CN=node01", reg.<ObjectMetric<String>>findMetric("CertificateSubject").value());
        assertContains(log, reg.<ObjectMetric<String>>findMetric("TrustedAuthorities").value(), "CN=twoca");
        assertEquals(0, reg.<LongMetric>findMetric("LastReloadTime").value());

        placeKeys("node02");

        reload(g);

        X509Certificate cert = servedCertificate(clientConnectorPort(g));

        assertEquals("CN=node02", reg.<ObjectMetric<String>>findMetric("CertificateSubject").value());
        assertEquals(cert.getNotAfter().getTime(), reg.<LongMetric>findMetric("CertificateNotAfter").value());
        assertTrue(reg.<LongMetric>findMetric("LastReloadTime").value() > 0);

        Files.write(keyStore(g.name()), "not a key store".getBytes());

        GridTestUtils.assertThrows(log, () -> reload(g), Exception.class, null);

        assertEquals(1, reg.<IntMetric>findMetric("ReloadFailures").value());
        assertContains(log, reg.<ObjectMetric<String>>findMetric("LastReloadFailure").value(),
            "Failed to initialize key store");

        // The certificate in use stays, and so do its metrics.
        assertEquals("CN=node02", reg.<ObjectMetric<String>>findMetric("CertificateSubject").value());
    }

    /**
     * @param node Node to run on.
     * @return Status report of that node.
     */
    private String status(IgniteEx node) throws Exception {
        VisorTaskResult<String> res = node.compute(node.cluster()).execute(SslStatusTask.class,
            new VisorTaskArgument<>(node.localNode().id(), new NoArg(), false));

        return res.result();
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

        // Over the whole cluster, as the command itself does: the default facade covers server nodes only.
        VisorTaskResult<String> res = nodes[0].compute(nodes[0].cluster()).execute(SslReloadTask.class,
            new VisorTaskArgument<>(ids, arg, false));

        return res.result();
    }

    /**
     * @param igniteInstanceName Node the factory is for.
     * @return SSL context factory the node runs on, according to the flags set by the test.
     */
    private Factory<SSLContext> nodeSslContextFactory(String igniteInstanceName) {
        Factory<SSLContext> factory = reloadableFactory(igniteInstanceName);

        if (cachingFactory) {
            // A ready-made context, the way a factory caching it internally hands it over: the node gets the very
            // same instance back, so there is nothing to read again.
            SSLContext ctx = factory.create();

            return () -> ctx;
        }

        return factory;
    }

    /**
     * @param factory Factory to wrap.
     * @return Factory that fails once {@link #FAIL_RELOAD} is set, with its reason one cause deep.
     */
    private static Factory<SSLContext> failing(Factory<SSLContext> factory) {
        return () -> {
            if (FAIL_RELOAD.get())
                throw new IgniteException("Failed to build the SSL context", new IOException(FAILURE_MSG));

            return factory.create();
        };
    }

    /**
     * @param igniteInstanceName Node the factory is for.
     * @return SSL context factory reading the stores of that node, trusting any peer if it has no trust store.
     */
    private Factory<SSLContext> reloadableFactory(String igniteInstanceName) {
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

    /**
     * @return SSL context factory of the probing client: it presents a certificate every node trusts while it
     *      probes, and trusts any node.
     */
    private Factory<SSLContext> probeFactory() {
        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(GridTestUtils.keyStorePath("node01"));
        factory.setKeyStorePassword(GridTestUtils.keyStorePassword().toCharArray());
        factory.setTrustManagers(SslContextFactory.getDisabledTrustManager());

        return factory;
    }

    /** @param igniteInstanceName Node. */
    private Path keyStore(String igniteInstanceName) {
        return dir.resolve(igniteInstanceName + "-key.jks");
    }

    /** @param igniteInstanceName Node. */
    private Path trustStore(String igniteInstanceName) {
        return dir.resolve(igniteInstanceName + "-trust.jks");
    }

    /** @param name Test key store to place for every running node (see {@code tests.properties}). */
    private void placeKeys(String name) throws IOException {
        for (Ignite node : G.allGrids())
            place(name, keyStore(node.name()));
    }

    /** @param name Test trust store to place for every running node (see {@code tests.properties}). */
    private void placeTrust(String name) throws IOException {
        for (Ignite node : G.allGrids())
            place(name, trustStore(node.name()));
    }

    /**
     * @param name Test store name (see {@code tests.properties}).
     * @param dest File to replace.
     */
    private static void place(String name, Path dest) throws IOException {
        Files.copy(Path.of(GridTestUtils.keyStorePath(name)), dest, StandardCopyOption.REPLACE_EXISTING);
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
