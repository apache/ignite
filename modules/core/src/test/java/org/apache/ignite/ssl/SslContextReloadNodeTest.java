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
import java.util.Arrays;
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
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslEnsureTask;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.management.ssl.SslTask;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.internal.visor.VisorTaskResult;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Tests the {@code --ssl} commands on a running node: {@code reload} must move the SSL-enabled transports onto the
 * key store that replaced the one on disk while the cluster keeps operating, and {@code ensure} must report the same
 * outcome without changing anything.
 */
public class SslContextReloadNodeTest extends GridCommonAbstractTest {
    /** Reason {@link FailingSslContextFactory} reports, standing in for an unreadable key store. */
    private static final String FAILURE_MSG = "Key store is unreadable";

    /** Switches {@link FailingSslContextFactory} to failing; static, as the factory lives inside the node. */
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

    /** Key store of the probing client; fixed, so that probing does not depend on the rotation under test. */
    private Path probeKeyStore;

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
        probeKeyStore = Files.createTempFile("ignite-ssl-reload-probe-", ".jks");

        copyKeyStore("node01");

        Files.copy(Path.of(GridTestUtils.keyStorePath("node01")), probeKeyStore, StandardCopyOption.REPLACE_EXISTING);
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

        if (probeKeyStore != null)
            Files.deleteIfExists(probeKeyStore);
    }

    /**
     * @return SSL context factory the node under test runs on, according to the flags set by the test.
     */
    private Factory<SSLContext> nodeSslContextFactory() {
        if (cachingFactory)
            return new CachingSslContextFactory(reloadableFactory());

        if (failingFactory)
            return new FailingSslContextFactory(reloadableFactory());

        return reloadableFactory();
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

        assertContains(log, res, "communication");
        assertContains(log, res, "discovery");
        assertContains(log, res, "client connector");

        assertRotated("client connector", cliCertBefore, servedCertificate(clientConnectorPort(g0)));

        // Discovery accepts on a plain socket and secures every connection separately, so the listening socket
        // does not pin the certificate it was bound with.
        assertRotated("discovery", discoCertBefore, servedCertificate(discoveryPort(g0)));

        cache.put(2, 2);

        assertEquals("Established sessions must survive the reload",
            (Integer)2, g1.<Integer, Integer>cache(DEFAULT_CACHE_NAME).get(2));
    }

    /** A caching custom factory cannot be reloaded, and the command must report that instead of a success. */
    @Test
    public void testCachingFactoryReportedAsNotReloaded() throws Exception {
        cachingFactory = true;

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(clientConnectorPort(g));

        copyKeyStore("node02");

        String res = reload(g);

        assertContains(log, res, "NOT reloaded");

        // A successful list is always printed right after the node id, so its absence means nothing was reloaded.
        assertNotContains(log, res, ": reloaded ");

        // The operator must be told how to fix it, not just that it failed.
        assertContains(log, res, AbstractSslContextFactory.class.getName());

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
        assertContains(log, res, "communication");
        assertContains(log, res, "discovery");
        assertContains(log, res, "client connector");
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

        assertContains(log, res, "failed to reload");
        assertContains(log, res, "communication");
        assertContains(log, res, "discovery");

        assertKept("A certificate the trust store rejects", certBefore, servedCertificate(discoveryPort(g)));
    }

    /** Ensure must accept a valid rotation and still leave the node on the certificate it is running. */
    @Test
    public void testEnsureAcceptsWithoutApplying() throws Exception {
        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(discoveryPort(g));

        copyKeyStore("node02");

        String res = ensure(g);

        assertContains(log, res, "can be reloaded");
        assertContains(log, res, "discovery");

        assertKept("An accepted but not applied certificate", certBefore, servedCertificate(discoveryPort(g)));
    }

    /** Ensure must reject a certificate that a reload would refuse, before any node has been touched. */
    @Test
    public void testEnsureRejectsUntrustedCertificate() throws Exception {
        trustStore = "trustone";

        IgniteEx g = startGrid(0);

        X509Certificate certBefore = servedCertificate(discoveryPort(g));

        copyKeyStore("node02");

        String res = X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> ensure(g), Exception.class, null));

        assertContains(log, res, "would fail to reload");
        assertContains(log, res, "discovery");

        assertKept("A rejected certificate", certBefore, servedCertificate(discoveryPort(g)));
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
        return execute(SslReloadTask.class, nodes);
    }

    /** @param nodes Nodes to check certificates on. */
    private String ensure(IgniteEx... nodes) throws Exception {
        return execute(SslEnsureTask.class, nodes);
    }

    /**
     * @param task Task to run, submitting it from the first node.
     * @param nodes Nodes to run it on.
     * @return Aggregated task result.
     */
    private String execute(Class<? extends SslTask> task, IgniteEx... nodes) throws Exception {
        List<UUID> ids = new ArrayList<>();

        for (IgniteEx node : nodes)
            ids.add(node.localNode().id());

        VisorTaskResult<String> res = nodes[0].compute().execute(task,
            new VisorTaskArgument<>(ids, new NoArg(), false));

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

        factory.setKeyStoreFilePath(probeKeyStore.toString());
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
     * @param name Transport that was expected to pick the rotated certificate up.
     * @param before Certificate served before the reload.
     * @param after Certificate served after the reload.
     */
    private void assertRotated(String name, X509Certificate before, X509Certificate after) throws Exception {
        assertFalse(name + " must serve the rotated certificate to new connections",
            Arrays.equals(before.getEncoded(), after.getEncoded()));
    }

    /**
     * @param what What was expected to leave the certificate alone.
     * @param before Certificate served before the reload.
     * @param after Certificate served after the reload.
     */
    private void assertKept(String what, X509Certificate before, X509Certificate after) throws Exception {
        assertTrue(what + " must keep the previously loaded certificate",
            Arrays.equals(before.getEncoded(), after.getEncoded()));
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

    /**
     * Not an {@link AbstractSslContextFactory} and caches the created context, so its certificate cannot be rotated
     * without a node restart.
     */
    private static class CachingSslContextFactory implements Factory<SSLContext> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Factory<SSLContext> delegate;

        /** Cached context, created once and never rebuilt. */
        private volatile SSLContext ctx;

        /** */
        private CachingSslContextFactory(Factory<SSLContext> delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public SSLContext create() {
            if (ctx == null)
                ctx = delegate.create();

            return ctx;
        }
    }

    /**
     * Builds the context through the delegate until {@link #FAIL_RELOAD} is set, and fails afterwards the way a
     * factory reading a corrupted store on disk would.
     */
    private static class FailingSslContextFactory implements Factory<SSLContext> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final Factory<SSLContext> delegate;

        /** */
        private FailingSslContextFactory(Factory<SSLContext> delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public SSLContext create() {
            if (FAIL_RELOAD.get())
                throw new IgniteException(FAILURE_MSG);

            return delegate.create();
        }
    }
}
