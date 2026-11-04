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

package org.apache.ignite.internal.ssl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.ssl.AbstractSslContextFactory.ReloadOutcome;
import org.apache.ignite.ssl.SslBundle;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.Level;
import org.junit.Test;

import static org.apache.ignite.internal.ssl.SslContextRegistry.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextRegistry.DISCOVERY;
import static org.apache.ignite.ssl.SslTestUtils.discoveryPort;
import static org.apache.ignite.ssl.SslTestUtils.servedCertificate;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.TestCertificateAuthority.chain;
import static org.apache.ignite.testframework.TestCertificateAuthority.context;
import static org.apache.ignite.testframework.TestCertificateAuthority.save;

/**
 * Renewal on a node: a factory without certificate metadata stops the node from starting with renewal; the node exposes the sum of
 * unsuccessful renewals of its factories as the only SSL metric; stopping the node interrupts an attempt without a failure and
 * stops the renewal thread; the file-based factory puts a replaced key store in use with no command. Nodes run
 * {@link SslContextFactory} on key store files of the test and trust the test authority. Nodes that run renewal start on
 * a certificate with 10 of its 120 minutes left, inside the default window of 18 minutes, so the first attempt comes at once.
 */
public class SslRenewalNodeTest extends GridCommonAbstractTest {
    /** */
    private static final long SECOND = 1_000L;

    /** */
    private static final long MINUTE = 60 * SECOND;

    /** */
    private static final long HOUR = 60 * MINUTE;

    /** How renewal logs an attempt that got the chains in use, before the expiry state. */
    private static final String WAITING = "The SSL context factory has no newer TLS certificates yet, the ones in use stay";

    /** Authority of the test certificates. */
    private TestCertificateAuthority ca;

    /** Directory with the key store files of the test. */
    private Path dir;

    /** What renewal logs, with levels. */
    private final MemorizingAppender appender = new MemorizingAppender();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("renewal-ca");
        dir = Files.createTempDirectory("ignite-ssl-renewal-");

        appender.start();
        appender.installSelfOn(SslRenewal.class);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        try {
            stopAllGrids();
        }
        finally {
            appender.removeSelfFrom(SslRenewal.class);
            appender.stop();

            U.delete(dir);
        }
    }

    /** A node whose factory enables renewal but gives no certificate metadata does not start. */
    @Test
    public void testUnknownCertificateRefusedAtStart() throws Exception {
        Path keys = dir.resolve("node.jks");
        SslContextFactory factory = readingKeys(new SslContextFactory() {
            /** {@inheritDoc} */
            @Override protected SslBundle createSslBundle() throws SSLException {
                return new SslBundle(super.createSslBundle().context(), null);
            }
        }, keys);

        save(ca.keyStoreWithPrivateKey("node"), keys);
        factory.setRenewalEnabled(true);

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory);
        }), IgniteException.class, "The SSL context factory must provide at least one X.509 certificate chain");
    }

    /**
     * The node exposes the unsuccessful renewals of all its factories as {@code ssl.ConsecutiveRenewalFailures}, the only metric
     * of the {@code ssl} registry; a renewal clears the count of its factory only. Renewal is off, so that renewal attempts of the
     * node do not change the counts while the test renews by hand; the node then plans no renewal and runs no renewal thread.
     */
    @Test
    public void testRenewalFailuresMetric() throws Exception {
        long now = System.currentTimeMillis();
        Path nodeKeys = dir.resolve("node.jks");
        Path clientKeys = dir.resolve("client-connector.jks");
        KeyStore inUse = ca.keyStoreWithPrivateKey("in-use", now - HOUR, now + HOUR);
        SslContextFactory nodeFactory = readingKeys(new SslContextFactory(), nodeKeys);
        SslContextFactory clientFactory = readingKeys(new SslContextFactory(), clientKeys);

        save(inUse, nodeKeys);
        save(inUse, clientKeys);

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(nodeFactory).setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSslEnabled(true).setSslClientAuth(false).setUseIgniteSslContextFactory(false).setSslContextFactory(clientFactory));
        });
        SslContextProvider node = provider(g, COMMUNICATION);
        ReadOnlyMetricRegistry ssl = g.context().metric().registry("ssl");
        Iterator<Metric> metrics = ssl.iterator();
        IntMetric failures = ssl.findMetric("ConsecutiveRenewalFailures");

        assertEquals("ssl.ConsecutiveRenewalFailures", metrics.next().name());
        assertFalse(metrics.hasNext());

        nodeFactory.renew(nodeFactory.create());
        clientFactory.renew(clientFactory.create());
        clientFactory.renew(clientFactory.create());

        assertEquals("Both factories", 3, failures.value());

        save(ca.keyStoreWithPrivateKey("renewed", now - HOUR, now + 2 * HOUR), nodeKeys);

        nodeFactory.renew(nodeFactory.create());

        assertEquals("Node factory renewed", 2, failures.value());
        assertEquals(0, node.nextRenewalTime());
        assertFalse(renewalThreadAlive(g.name()));
    }

    /**
     * Stopping a node interrupts an attempt in progress without counting or logging a failure, and the renewal thread stops with
     * the node.
     */
    @Test
    public void testStopDuringRenewalLogsNoFailure() throws Exception {
        long now = System.currentTimeMillis();
        Path keys = dir.resolve("node.jks");
        CountDownLatch attempting = new CountDownLatch(1);
        SslContextFactory factory = readingKeys(new SslContextFactory() {
            /** {@inheritDoc} */
            @Override protected SslBundle createSslBundle() throws SSLException {
                if (Thread.currentThread().getName().startsWith("ssl-renewal")) {
                    attempting.countDown();

                    try {
                        Thread.sleep(Long.MAX_VALUE);
                    }
                    catch (InterruptedException ignored) {
                        throw new SSLException("Key store read cancelled");
                    }
                }

                return super.createSslBundle();
            }
        }, keys);

        save(ca.keyStoreWithPrivateKey("in-window", now - 110 * MINUTE, now + 10 * MINUTE), keys);
        factory.setRenewalEnabled(true);

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory);
        });
        String name = g.name();
        SslContextProvider p = provider(g, COMMUNICATION);

        assertTrue(attempting.await(10, TimeUnit.SECONDS));
        assertTrue(renewalThreadAlive(name));

        stopGrid(0);

        assertEquals(0, p.renewalFailures());
        assertEquals(new ReloadOutcome(0, 0, null, 0), p.reloadOutcome());
        assertEquals(List.of(), logged(Level.WARN));
        assertEquals(List.of(), logged(Level.ERROR));
        assertTrue(waitForCondition(() -> !renewalThreadAlive(name), 10_000));
    }

    /** Restarting a node with the same factory attaches renewal to the new executor and picks up replacement material. */
    @Test
    public void testSameFactoryRenewsAfterNodeRestart() throws Exception {
        long now = System.currentTimeMillis();
        Path keys = dir.resolve("node.jks");
        SslContextFactory factory = readingKeys(new SslContextFactory(), keys);
        KeyStore replacement = ca.keyStoreWithPrivateKey("replacement", now - HOUR, now + 2 * HOUR);

        save(ca.keyStoreWithPrivateKey("in-window", now - 110 * MINUTE, now + 10 * MINUTE), keys);
        factory.setRenewalEnabled(true);

        startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory);
        });

        assertTrue(waitForCondition(() -> factory.renewalFailures() > 0, 10_000));

        stopGrid(0);

        assertEquals(0, factory.nextRenewalTime());

        save(replacement, keys);

        IgniteEx restarted = startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory);
        });

        SslContextProvider discovery = provider(restarted, DISCOVERY);

        assertTrue(waitForCondition(() -> discovery.certificateChains().containsKey("replacement"), 10_000));
        assertEquals(0, factory.renewalFailures());
        assertEquals(chain(replacement).get(0), servedCertificate(context(ca.keyStoreWithPrivateKey("probe"), ca.trustStore()),
            discoveryPort(restarted)));
        assertTrue(renewalThreadAlive(restarted.name()));
    }

    /**
     * The file-based factory with renewal enabled waits while its key store file holds the certificate in use and puts the file
     * replaced on disk in use with no command: the node logs the renewal, new discovery connections get the new certificate, and
     * status shows the next renewal at the default window of the new certificate, 18 minutes before its expiry.
     */
    @Test
    public void testReplacedKeyStoreFileRenewed() throws Exception {
        // X.509 keeps whole seconds.
        long now = System.currentTimeMillis() / SECOND * SECOND;
        Path keys = dir.resolve("node.jks");
        KeyStore renewed = ca.keyStoreWithPrivateKey("renewed", now, now + 2 * HOUR);
        long nextRenewal = now + 2 * HOUR - 18 * MINUTE;
        SslContextFactory factory = readingKeys(new SslContextFactory(), keys);
        long minPause = GridTestUtils.getFieldValue(SslRenewal.class, "minPause");
        long maxPause = GridTestUtils.getFieldValue(SslRenewal.class, "maxPause");

        save(ca.keyStoreWithPrivateKey("in-window", now - 110 * MINUTE, now + 10 * MINUTE), keys);
        factory.setRenewalEnabled(true);

        // Retries at least a minute apart would make the test wait that long for the replaced file.
        GridTestUtils.setFieldValue(SslRenewal.class, "minPause", 100L);
        GridTestUtils.setFieldValue(SslRenewal.class, "maxPause", 1_000L);

        try {
            IgniteEx g = startGrid(0, cfg -> {
                cfg.setSslContextFactory(factory);
            });
            SslContextProvider p = provider(g, COMMUNICATION);

            assertTrue(waitForCondition(() -> logged(Level.WARN).stream().anyMatch(msg -> msg.startsWith(WAITING)), 10_000));

            save(renewed, keys);

            assertTrue(waitForCondition(() -> logged(Level.INFO).stream().anyMatch(msg -> msg.startsWith("TLS certificates reloaded " +
                "[transports=" + COMMUNICATION + ", " + DISCOVERY + ", alias=renewed, subject=CN=renewed, ") &&
                msg.endsWith(", initiator=automatic renewal]")), 10_000));

            assertEquals(chain(renewed).get(0), servedCertificate(context(ca.keyStoreWithPrivateKey("probe"), ca.trustStore()),
                discoveryPort(g)));

            // The renewal thread plans the next attempt after it logs the renewal.
            assertTrue(waitForCondition(() -> p.nextRenewalTime() == nextRenewal, 10_000));
            assertContains(log, status(g), "next automatic renewal at " + Instant.ofEpochMilli(nextRenewal));
        }
        finally {
            GridTestUtils.setFieldValue(SslRenewal.class, "minPause", minPause);
            GridTestUtils.setFieldValue(SslRenewal.class, "maxPause", maxPause);
        }
    }

    /**
     * @param factory Factory to set up.
     * @param keys Key store file.
     * @return The factory, reading the key store file and trusting the test authority.
     */
    private <F extends SslContextFactory> F readingKeys(F factory, Path keys) throws Exception {
        factory.setKeyStoreType("JKS");
        factory.setKeyStoreFilePath(keys.toString());
        factory.setKeyStorePassword(TestCertificateAuthority.PWD);
        factory.setTrustManagers(TestCertificateAuthority.trustManagers(ca.trustStore()));

        return factory;
    }

    /**
     * @param level Level.
     * @return Messages renewal logged at the level, in log order.
     */
    private List<String> logged(Level level) {
        return appender.events().stream().filter(e -> e.getLevel() == level).map(e -> e.getMessage().getFormattedMessage()).toList();
    }

    /**
     * @param g Node.
     * @param transport Transport.
     * @return Provider of the transport.
     */
    private static SslContextProvider provider(IgniteEx g, String transport) {
        return g.context().sslContexts().providers().stream().filter(p -> p.transports().contains(transport)).findFirst()
            .orElseThrow();
    }

    /**
     * @param name Node name.
     * @return Whether the renewal thread of the node is alive.
     */
    private static boolean renewalThreadAlive(String name) {
        return Thread.getAllStackTraces().keySet().stream()
            .anyMatch(t -> t.isAlive() && t.getName().startsWith("ssl-renewal") && t.getName().contains(name));
    }
}
