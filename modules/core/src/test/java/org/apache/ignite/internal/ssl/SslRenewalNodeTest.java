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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.ssl.SslFactoryAccess.ReloadOutcome;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.IntMetric;
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
import static org.apache.ignite.ssl.AbstractSslContextFactory.NODE_ACCESS;
import static org.apache.ignite.ssl.SslTestUtils.discoveryPort;
import static org.apache.ignite.ssl.SslTestUtils.servedCertificate;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.TestCertificateAuthority.chain;
import static org.apache.ignite.testframework.TestCertificateAuthority.context;
import static org.apache.ignite.testframework.TestCertificateAuthority.save;

/**
 * Tests renewal with running nodes, including certificate delivery over discovery, restart, shutdown and node metrics.
 */
public class SslRenewalNodeTest extends GridCommonAbstractTest {
    /** */
    private static final long SECOND = 1_000L;

    /** */
    private static final long MINUTE = 60 * SECOND;

    /** */
    private static final long HOUR = 60 * MINUTE;

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

    /** A node configured to renew certificates must have a chain whose expiry can be monitored. */
    @Test
    public void testNodeWithoutChainsRefusesRenewal() throws Exception {
        Path keys = dir.resolve("node.jks");
        SslContextFactory factory = readingKeys(new SslContextFactory(), keys);

        save(ca.trustStore(), keys);
        factory.setRenewalEnabled(true);

        GridTestUtils.assertThrowsAnyCause(log, () -> startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory);
        }), IgniteException.class, "The SSL context factory must provide at least one X.509 certificate chain");
    }

    /** The node counts automatic attempts across distinct factories. Advancing one factory's expiry clears only its count. */
    @Test
    public void testRenewalFailuresMetric() throws Exception {
        long now = System.currentTimeMillis();
        Path nodeKeys = dir.resolve("node.jks");
        Path clientKeys = dir.resolve("client-connector.jks");
        KeyStore inWindow = ca.keyStoreWithPrivateKey("in-window", now - 110 * MINUTE, now + 10 * MINUTE);
        SslContextFactory nodeFactory = readingKeys(new SslContextFactory(), nodeKeys);
        SslContextFactory clientFactory = readingKeys(new SslContextFactory(), clientKeys);

        nodeFactory.setRenewalEnabled(true);
        clientFactory.setRenewalEnabled(true);
        save(inWindow, nodeKeys);
        save(inWindow, clientKeys);

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(nodeFactory).setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSslEnabled(true).setSslClientAuth(false).setUseIgniteSslContextFactory(false).setSslContextFactory(clientFactory));
        });
        IntMetric failures = g.context().metric().registry("ssl").findMetric("ConsecutiveRenewalFailures");

        assertTrue("One automatic attempt per factory", waitForCondition(() -> failures.value() == 2, 10_000));

        save(ca.keyStoreWithPrivateKey("renewed", now - HOUR, now + 2 * HOUR), nodeKeys);
        provider(g, COMMUNICATION).reload();

        assertEquals("Only the client connector is still waiting", 1, failures.value());
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

        assertTrue(waitForCondition(() -> NODE_ACCESS.renewalFailures(factory) > 0, 10_000));

        stopGrid(0);

        assertEquals(0, NODE_ACCESS.nextRenewalTime(factory));

        save(replacement, keys);

        IgniteEx restarted = startGrid(0, cfg -> {
            cfg.setSslContextFactory(factory);
        });

        SslContextProvider discovery = provider(restarted, DISCOVERY);

        assertTrue(waitForCondition(() -> discovery.certificateChains().containsKey("replacement"), 10_000));
        assertEquals(0, NODE_ACCESS.renewalFailures(factory));
        assertEquals(chain(replacement).get(0), servedCertificate(context(ca.keyStoreWithPrivateKey("probe"), ca.trustStore()),
            discoveryPort(restarted)));
        assertTrue(renewalThreadAlive(restarted.name()));
    }

    /** The first renewal reads the replacement file. Discovery serves its certificate and status shows its next renewal. */
    @Test
    public void testReplacedKeyStoreFileRenewed() throws Exception {
        long now = System.currentTimeMillis() / SECOND * SECOND;
        Path keys = dir.resolve("node.jks");
        CountDownLatch transportsReady = new CountDownLatch(1);
        KeyStore renewed = ca.keyStoreWithPrivateKey("renewed", now, now + 2 * HOUR);
        long nextRenewal = now + 2 * HOUR - 18 * MINUTE;
        SslContextFactory factory = readingKeys(new SslContextFactory() {
            /** {@inheritDoc} */
            @Override protected SslBundle createSslBundle() throws SSLException {
                if (Thread.currentThread().getName().startsWith("ssl-renewal")) {
                    try {
                        transportsReady.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new SSLException(e);
                    }
                }

                return super.createSslBundle();
            }
        }, keys);

        save(ca.keyStoreWithPrivateKey("in-window", now - 110 * MINUTE, now + 10 * MINUTE), keys);
        factory.setRenewalEnabled(true);

        IgniteEx g;

        try {
            g = startGrid(0, cfg -> {
                cfg.setSslContextFactory(factory);
            });

            save(renewed, keys);
        }
        finally {
            transportsReady.countDown();
        }

        assertTrue(waitForCondition(() -> logged(Level.INFO).stream().anyMatch(msg -> msg.startsWith("TLS certificates reloaded " +
            "[transports=" + COMMUNICATION + ", " + DISCOVERY + ", alias=renewed, subject=CN=renewed, ") &&
            msg.endsWith(", initiator=automatic renewal]")), 10_000));
        assertEquals(chain(renewed).get(0), servedCertificate(context(ca.keyStoreWithPrivateKey("probe"), ca.trustStore()),
            discoveryPort(g)));

        SslContextProvider p = provider(g, COMMUNICATION);

        assertTrue(waitForCondition(() -> p.nextRenewalTime() == nextRenewal, 10_000));
        assertContains(log, status(g), "next automatic renewal at " + Instant.ofEpochMilli(nextRenewal));
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
