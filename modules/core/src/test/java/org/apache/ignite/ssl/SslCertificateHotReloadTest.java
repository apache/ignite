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
import java.util.Date;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.GridComponent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SSL_HOT_RELOAD_BEFORE_EXPIRY;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
public class SslCertificateHotReloadTest extends GridCommonAbstractTest {
    /** Upper bound for waiting for an event that must happen. */
    private static final long TIMEOUT_MS = 30_000;

    /**
     * Given: the factory has loaded due certificates of one CA, and the files then get certificates of another CA.
     * When: the node starts, and its hot reload attempt runs.
     * Then: the node installs the new certificates, and a client that trusts and presents only them connects.
     */
    @Test
    public void testNodeHotReloadsCertificatesForNewConnections() throws Exception {
        long now = System.currentTimeMillis();
        TestCertificateAuthority ca1 = new TestCertificateAuthority("ca1", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        TestCertificateAuthority ca2 = new TestCertificateAuthority("ca2", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        Path dir = Files.createTempDirectory("ssl-hot-reload");
        Path keys = dir.resolve("keys.jks");
        Path trust = dir.resolve("trust.jks");

        try {
            save(ca1.keyStore("node", now - MINUTES.toMillis(110), now + MINUTES.toMillis(10)), keys);
            save(ca1.trustStore(), trust);

            SslContextFactory serverFactory = configure(new SslContextFactory(), keys, trust);

            SSLContext initCtx = serverFactory.create();

            assertTrue("Hot reload must be due at startup", serverFactory.certificatesExpiry() - DFLT_SSL_HOT_RELOAD_BEFORE_EXPIRY < now);

            save(ca2.keyStore("node", now - MINUTES.toMillis(1), now + DAYS.toMillis(10)), keys);
            save(ca2.trustStore(), trust);

            IgniteEx node = startGrid(0, cfg -> {
                cfg.setClientConnectorConfiguration(sslClientConnector(serverFactory));
            });

            assertTrue("The reloaded context was not installed", waitForCondition(() -> serverFactory.create() != initCtx, TIMEOUT_MS));

            ClientConfiguration clientCfg = new ClientConfiguration()
                .setAddresses("127.0.0.1:" + node.context().clientListener().port())
                .setSslMode(SslMode.REQUIRED)
                .setSslContextFactory(configure(new SslContextFactory(), keys, trust));

            try (IgniteClient client = Ignition.startClient(clientCfg)) {
                client.cacheNames();
            }
        }
        finally {
            stopAllGrids();
            U.delete(dir);
        }
    }

    /**
     * Given: a node starts with expired certificates, so hot reload stays due.
     * When: the automatic attempt reloads the same certificates.
     * Then: the node counts the failure and logs the transport and the reason.
     */
    @Test
    public void testNodeReportsHotReloadFailureForExpiredCertificates() throws Exception {
        long now = System.currentTimeMillis();
        TestCertificateAuthority ca = new TestCertificateAuthority("ca", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        Path dir = Files.createTempDirectory("ssl-hot-reload");
        Path keys = dir.resolve("keys.jks");
        Path trust = dir.resolve("trust.jks");
        ListeningTestLogger nodeLog = new ListeningTestLogger(log);
        LogListener failure = LogListener.matches("TLS hot reload failed [transports=client connector").build();
        LogListener reason = LogListener.matches("Loaded certificates are still due for hot reload").build();

        nodeLog.registerListener(failure);
        nodeLog.registerListener(reason);

        try {
            save(ca.keyStore("node", now - MINUTES.toMillis(120), now - MINUTES.toMillis(1)), keys);
            save(ca.trustStore(), trust);

            SslContextFactory serverFactory = configure(new SslContextFactory(), keys, trust);

            IgniteEx node = startGrid(0, cfg -> {
                cfg.setGridLogger(nodeLog).setClientConnectorConfiguration(sslClientConnector(serverFactory));
            });
            LongMetric failures = node.context().metric().registry("ssl").findMetric("ConsecutiveHotReloadFailures");

            assertTrue("The automatic attempt did not fail", waitForCondition(() -> failures.value() > 0, TIMEOUT_MS));
            assertTrue(failure.check(TIMEOUT_MS));
            assertTrue(reason.check(TIMEOUT_MS));
        }
        finally {
            stopAllGrids();
            U.delete(dir);
        }
    }

    /**
     * Given: a node with a TLS transport runs its hot reload thread.
     * When: the node stops.
     * Then: the hot reload thread ends.
     */
    @Test
    public void testNodeStopEndsHotReloadThread() throws Exception {
        long now = System.currentTimeMillis();
        TestCertificateAuthority ca = new TestCertificateAuthority("ca", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        Path dir = Files.createTempDirectory("ssl-hot-reload");
        Path keys = dir.resolve("keys.jks");
        Path trust = dir.resolve("trust.jks");

        try {
            save(ca.keyStore("node", now - DAYS.toMillis(1), now + DAYS.toMillis(10)), keys);
            save(ca.trustStore(), trust);

            SslContextFactory serverFactory = configure(new SslContextFactory(), keys, trust);

            IgniteEx node = startGrid(0, cfg -> {
                cfg.setClientConnectorConfiguration(sslClientConnector(serverFactory));
            });

            Thread hotReload = null;

            for (Thread thread : Thread.getAllStackTraces().keySet()) {
                if (thread.getName().startsWith("ssl-hot-reload") && thread.getName().contains(node.name()))
                    hotReload = thread;
            }

            assertNotNull("The hot reload thread is not running", hotReload);

            stopAllGrids();
            hotReload.join(TIMEOUT_MS);

            assertFalse("The hot reload thread is still running", hotReload.isAlive());
        }
        finally {
            stopAllGrids();
            U.delete(dir);
        }
    }

    /**
     * Given: the node configuration disables hot reload, and the certificates of a TLS transport are due for hot reload.
     * When: the node starts.
     * Then: the node starts no hot reload thread.
     */
    @Test
    public void testNodeWithHotReloadDisabledStartsNoHotReloadThread() throws Exception {
        long now = System.currentTimeMillis();
        TestCertificateAuthority ca = new TestCertificateAuthority("ca", now - DAYS.toMillis(1), now + DAYS.toMillis(10));
        Path dir = Files.createTempDirectory("ssl-hot-reload");
        Path keys = dir.resolve("keys.jks");
        Path trust = dir.resolve("trust.jks");

        try {
            save(ca.keyStore("node", now - MINUTES.toMillis(110), now + MINUTES.toMillis(10)), keys);
            save(ca.trustStore(), trust);

            SslContextFactory serverFactory = configure(new SslContextFactory(), keys, trust);

            IgniteEx node = startGrid(0, cfg -> {
                cfg.setSslHotReloadEnabled(false).setClientConnectorConfiguration(sslClientConnector(serverFactory));
            });

            for (Thread thread : Thread.getAllStackTraces().keySet()) {
                String name = thread.getName();

                assertFalse(name, name.startsWith("ssl-hot-reload") && name.contains(node.name()));
            }
        }
        finally {
            stopAllGrids();
            U.delete(dir);
        }
    }

    /**
     * Given: the node configuration sets a hot reload lead time of an hour, and the certificates of a TLS transport
     *     expire in 10 days.
     * When: the node starts.
     * Then: the node logs that hot reload is due an hour before the certificates expire.
     */
    @Test
    public void testNodeUsesHotReloadLeadTimeFromConfiguration() throws Exception {
        long now = System.currentTimeMillis();
        long expiry = now + DAYS.toMillis(10);
        TestCertificateAuthority ca = new TestCertificateAuthority("ca", now - DAYS.toMillis(1), now + DAYS.toMillis(20));
        Path dir = Files.createTempDirectory("ssl-hot-reload");
        Path keys = dir.resolve("keys.jks");
        Path trust = dir.resolve("trust.jks");
        ListeningTestLogger nodeLog = new ListeningTestLogger(log);
        LogListener due = LogListener.matches("TLS hot reload due [transports=client connector, hotReloadTime="
            + new Date(expiry - HOURS.toMillis(1)) + ']').build();

        nodeLog.registerListener(due);

        try {
            save(ca.keyStore("node", now - DAYS.toMillis(1), expiry), keys);
            save(ca.trustStore(), trust);

            SslContextFactory serverFactory = configure(new SslContextFactory(), keys, trust);

            startGrid(0, cfg -> {
                cfg.setGridLogger(nodeLog)
                    .setSslHotReloadBeforeExpiry(HOURS.toMillis(1))
                    .setClientConnectorConfiguration(sslClientConnector(serverFactory));
            });

            assertTrue(due.check(TIMEOUT_MS));
        }
        finally {
            stopAllGrids();
            U.delete(dir);
        }
    }

    /**
     * Given: a node with the client connector and REST, which stop their servers in their own {@code onKernalStop}.
     * When: the node starts its components.
     * Then: the SSL factory registry comes after both, so the node, which stops components in reverse order,
     *     stops hot reload before them.
     */
    @Test
    public void testHotReloadStopsBeforeClientConnectorAndRest() throws Exception {
        IgniteEx node = startGrid(0);

        try {
            List<GridComponent> comps = node.context().components();
            int clientConnector = comps.indexOf(node.context().clientListener());
            int rest = comps.indexOf(node.context().rest());

            assertTrue(clientConnector >= 0 && rest >= 0);
            assertTrue(comps.indexOf(node.context().ssl()) > Math.max(clientConnector, rest));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Creates a client connector configuration that uses the given factory and requires client certificates.
     * The connector uses this factory instead of the node factory.
     */
    private static ClientConnectorConfiguration sslClientConnector(SslContextFactory factory) {
        return new ClientConnectorConfiguration()
            .setSslEnabled(true)
            .setSslClientAuth(true)
            .setUseIgniteSslContextFactory(false)
            .setSslContextFactory(factory);
    }

    /**
     * Points the factory to the given key and trust store files with the test password.
     * It returns the same factory, so a test can configure it inline.
     */
    private static SslContextFactory configure(SslContextFactory factory, Path keys, Path trust) {
        factory.setKeyStoreFilePath(keys.toString());
        factory.setKeyStorePassword(PASSWORD);
        factory.setTrustStoreFilePath(trust.toString());
        factory.setTrustStorePassword(PASSWORD);

        return factory;
    }
}
