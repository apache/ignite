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

import java.io.Serial;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.ssl.SslContextRegistry.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextRegistry.COMMUNICATION;
import static org.apache.ignite.internal.ssl.SslContextRegistry.DISCOVERY;
import static org.apache.ignite.ssl.AbstractSslContextFactory.DFLT_RENEW_BEFORE_FRACTION;
import static org.apache.ignite.ssl.SslTestUtils.discoveryPort;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.servedCertificate;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertArrayEquals;

/** Tests that a node renews the certificates of a factory with renewal enabled before they expire. */
public class SslRenewalTest extends GridCommonAbstractTest {
    /** */
    private static final long MINUTE = 60_000L;

    /** */
    private static final long HOUR = 60 * MINUTE;

    /** How much earlier than planned a timer may fire, measured by a finer clock than the one it was set by. */
    private static final long CLOCK_SLACK = 50;

    /** Failure of an issuer that cannot be reached. */
    private static final String ISSUER_DOWN = "Issuer is unavailable";

    /** What the node logs while the factory returns the certificates in use. */
    private static final String WAITING = "The SSL context factory has no newer TLS certificates yet, the ones in use stay";

    /** Transports that share the node-wide SSL context, as the log lists them. */
    private static final String NODE_TRANSPORTS = "transports=communication, discovery";

    /** Operation context attribute standing for whoever runs a reload. */
    private static final OperationContextAttribute<String> OPERATOR = OperationContextAttribute.newInstance();

    /** Authority behind every certificate the nodes get. */
    private TestCertificateAuthority ca;

    /** What the renewals log, with the levels. */
    private final MemorizingAppender appender = new MemorizingAppender();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("renewalca");

        appender.start();
        appender.installSelfOn(SslRenewal.class);

        pauses(100, 1_000);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        try {
            stopAllGrids();
        }
        finally {
            appender.removeSelfFrom(SslRenewal.class);
            appender.stop();

            pauses(MINUTE, HOUR);
        }
    }

    /** A node must renew in the window and not before, serve and plan by the new certificate, and stop renewing on stop. */
    @Test
    public void testRenewsBeforeExpiry() throws Exception {
        long now = System.currentTimeMillis();
        long notAfter = (now + 40_000) / 1000 * 1000;
        long notBefore = notAfter - 233_000;
        long due = notAfter - (long)((notAfter - notBefore) * DFLT_RENEW_BEFORE_FRACTION);

        assertTrue("The window must open seconds after the node starts, well before expiry", due - now > 3_000 && notAfter - due > 30_000);

        IgniteEx g = startNode(new Issuer(valid(notBefore, notAfter), fresh(HOUR)));

        assertTrue(waitForCondition(() -> automaticRenewals("transports=" + COMMUNICATION) == 1, 30_000));

        assertTrue("The certificate must have been renewed", chainNotAfter(g) > now + 30 * MINUTE);
        assertTrue("The renewal must not come before the window opens",
            provider(g).reloadOutcome().successTime() >= due - CLOCK_SLACK);
        assertEquals("New connections must get the renewed certificate", chainNotAfter(g),
            servedCertificate(probe(), discoveryPort(g)).getNotAfter().getTime());

        long next = dueTime(g);

        assertTrue(waitForCondition(() -> nextRenewalTime(g) == next, 10_000));

        assertTrue("A node renewing certificates must run the renewal thread", renewalThreadAlive(g.name()));

        stopGrid(0);

        assertTrue("The renewal thread must stop with the node", waitForCondition(() -> !renewalThreadAlive(g.name()), 10_000));
    }

    /** Plans renewal from the authority's expiry when it expires before the leaf certificate. */
    @Test
    public void testRenewalPlannedBeforeAuthorityExpiry() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore keys = ca.issue("node", now - HOUR, now + TimeUnit.DAYS.toMillis(400));

        X509Certificate leaf = (X509Certificate)keys.getCertificate("node");
        X509Certificate authority = (X509Certificate)keys.getCertificateChain("node")[1];

        long notBefore = leaf.getNotBefore().getTime();
        long notAfter = authority.getNotAfter().getTime();
        long due = notAfter - (long)((notAfter - notBefore) * DFLT_RENEW_BEFORE_FRACTION);

        assertTrue("The authority must expire before the leaf", notAfter < leaf.getNotAfter().getTime());

        IgniteEx g = startNode(new Issuer(() -> keys));

        assertTrue("Renewal must be planned by the authority's expiry",
            waitForCondition(() -> nextRenewalTime(g) == due, 10_000));
    }

    /** With less than half of the window left, failures must be logged as errors and counted, and the status must show the next attempt. */
    @Test
    public void testLateFailureLoggedAsError() throws Exception {
        long now = System.currentTimeMillis();

        IgniteEx g = startNode(new Issuer(valid(now - HOUR, now + 2 * MINUTE), failure()));

        String late = "Failed to reload TLS certificates, the ones in use stay and expire soon [" + NODE_TRANSPORTS +
            ", initiator=automatic renewal, reason=" + ISSUER_DOWN;

        assertTrue(waitForCondition(() -> logged(Level.ERROR, late) >= 2, 30_000));

        assertTrue(metrics(g).<IntMetric>findMetric("ConsecutiveRenewalFailures").value() >= 2);
        assertContains(log, status(g), "next automatic renewal at");
    }

    /** Changing a returned chain must not change the provider's certificate or chain. */
    @Test
    public void testCertificateChainCannotChangeProvider() {
        SslContextProvider p = new SslContextProvider(new Issuer(fresh(HOUR)));

        X509Certificate[] chain = p.certificateChain();

        assertNotNull(chain);

        X509Certificate[] expected = chain.clone();

        chain[0] = null;

        assertSame(expected[0], p.certificate());
        assertArrayEquals(expected, p.certificateChain());
    }

    /** A certificate that expires no later than the one in use must not be put in use. */
    @Test
    public void testRenewalNotMovingExpiryRejected() {
        long now = System.currentTimeMillis();

        SslContextProvider p = new SslContextProvider(new Issuer(valid(now - HOUR, now + 5 * MINUTE)));

        assertRenewalRejected(p, "The new certificate chain expires no later than the one in use");
    }

    /** Nodes with different certificates from one authority keep communicating when only one node reloads. */
    @Test
    public void testReloadWithDifferentCertificatesFromSameAuthority() throws Exception {
        checkNodeRotation(ca, false);
    }

    /** Nodes need to trust each other's authorities, not their own, when one node reloads its certificate. */
    @Test
    public void testReloadWithDifferentAuthorities() throws Exception {
        checkNodeRotation(new TestCertificateAuthority("peerca"), false);
    }

    /** An automatic renewal preserves connections to a peer whose certificate and different authority stay unchanged. */
    @Test
    public void testRenewalWithDifferentAuthorities() throws Exception {
        checkNodeRotation(new TestCertificateAuthority("peerca"), true);
    }

    /** Reload installs the administrator's material; an actual TLS connection still rejects an authority the peer does not trust. */
    @Test
    public void testReloadDoesNotBypassPeerTrust() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority peerCa = new TestCertificateAuthority("peerca");
        TestCertificateAuthority untrusted = new TestCertificateAuthority("untrustedca");
        AtomicReference<KeyStore> keys = new AtomicReference<>(ca.issue("node0", now - MINUTE, now + HOUR));
        KeyStore peerKeys = peerCa.issue("node1", now - MINUTE, now + HOUR);

        Issuer issuer = issuer(keys::get, peerCa.trustStore(), false);
        Issuer peerIssuer = issuer(() -> peerKeys, ca.trustStore(), false);

        IgniteEx g0 = startNode(0, issuer);
        IgniteEx g1 = startNode(1, peerIssuer);

        X509Certificate peerCert = provider(g1).certificate();

        assertEquals(provider(g0).certificate(), servedCertificate(peerIssuer.createNew(), discoveryPort(g0)));

        keys.set(untrusted.issue("node0-new", now, now + 2 * HOUR));

        assertContains(log, reload(g0), ": reloaded " + COMMUNICATION + ", " + DISCOVERY);
        assertTrue(provider(g0).reloadOutcome().successTime() > 0);
        assertEquals("CN=untrustedca", provider(g0).certificate().getIssuerX500Principal().getName());
        assertEquals(peerCert, provider(g1).certificate());

        assertThrowsWithCause(() -> servedCertificate(peerIssuer.createNew(), discoveryPort(g0)), SSLException.class);
    }

    /** An expired certificate can be replaced with a valid chain from another authority. */
    @Test
    public void testExpiredCertificateRenewed() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        Issuer issuer = new Issuer(valid(now - 2 * HOUR, now - HOUR), () -> other.issue("node", now, now + HOUR));

        SslContextProvider p = new SslContextProvider(issuer);

        p.addTransport(COMMUNICATION);

        issuer.trust = other.trustStore();

        assertNotNull(p.renew(p.context()));
        assertEquals("CN=otherca", p.certificate().getIssuerX500Principal().getName());
    }

    /** A renewal logs the certificate it installed even if a manual reload replaces it before the log is written. */
    @Test
    public void testRenewalLogsAppliedCertificate() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = new Issuer(valid(now - HOUR, now + MINUTE),
            () -> ca.issue("renewed", now, now + HOUR), () -> ca.issue("manual", now, now + 2 * HOUR));

        SslContextProvider p = new SslContextProvider(issuer) {
            @Override X509Certificate renew(SSLContext expected) throws IgniteCheckedException {
                X509Certificate renewed = super.renew(expected);

                if (renewed != null)
                    super.reload();

                return renewed;
            }
        };

        p.addTransport(COMMUNICATION);

        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1);

        try {
            p.startRenewal(exec, log.getLogger(SslRenewal.class));

            assertTrue(waitForCondition(() -> automaticRenewals("transports=" + COMMUNICATION) == 1, 10_000));
            assertEquals("CN=manual", p.certificate().getSubjectX500Principal().getName());
            assertEquals(1, logged(Level.INFO, "TLS certificates reloaded [transports=communication, subject=CN=renewed,"));
        }
        finally {
            exec.shutdownNow();

            assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /** A certificate that is not valid yet must not be put in use. */
    @Test
    public void testRenewalNotValidYetRejected() {
        long now = System.currentTimeMillis();

        SslContextProvider p = new SslContextProvider(new Issuer(fresh(HOUR), valid(now + 10 * MINUTE, now + 2 * HOUR)));

        assertRenewalRejected(p, "The new certificate chain is not valid now");
    }

    /**
     * A renewal planned before a reload must not ask for a certificate or replace the one the reload has put in use. The reload
     * must leave the factory's {@code create()} returning the first context.
     */
    @Test
    public void testRenewalPlannedBeforeReloadSuperseded() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + HOUR);

        Issuer issuer = new Issuer(() -> inUse);

        SslContextProvider p = new SslContextProvider(issuer);

        SSLContext planned = p.context();

        assertNull(p.renew(planned));
        assertEquals(1, p.renewalFailures());

        p.reload();

        SSLContext reloaded = p.context();

        assertNull(p.renew(planned));
        assertEquals(1, p.renewalFailures());
        assertSame(reloaded, p.context());
        assertEquals("The superseded renewal must not ask for another certificate", 3, issuer.calls.size());
        assertSame(planned, issuer.create());
    }

    /** A reload during planning must not let an attempt for the old certificate rebuild the new context. */
    @Test
    public void testReloadDuringRenewalPlanning() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = new Issuer(valid(now - HOUR, now + 9 * MINUTE), fresh(2 * HOUR), fresh(3 * HOUR));

        CountDownLatch planRead = new CountDownLatch(1);
        CountDownLatch continuePlanning = new CountDownLatch(1);
        CountDownLatch reloaded = new CountDownLatch(1);
        CountDownLatch allowReplan = new CountDownLatch(1);
        CountDownLatch attemptStarted = new CountDownLatch(1);

        AtomicBoolean firstPlan = new AtomicBoolean(true);
        AtomicReference<Thread> reloadThread = new AtomicReference<>();

        SslContextProvider p = new SslContextProvider(issuer) {
            @Override public ReloadOutcome reloadOutcome() {
                ReloadOutcome outcome = super.reloadOutcome();

                if (firstPlan.getAndSet(false)) {
                    planRead.countDown();

                    U.awaitQuiet(continuePlanning);
                }

                return outcome;
            }

            @Override X509Certificate renew(SSLContext expected) throws IgniteCheckedException {
                attemptStarted.countDown();

                return super.renew(expected);
            }
        };

        ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1) {
            @Override public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
                if (Thread.currentThread() == reloadThread.get()) {
                    reloaded.countDown();

                    U.awaitQuiet(allowReplan);
                }

                return super.schedule(task, delay, unit);
            }
        };
        IgniteInternalFuture<?> reload = null;

        try {
            p.startRenewal(exec, log);

            assertTrue(planRead.await(10, TimeUnit.SECONDS));

            reload = GridTestUtils.runAsync(() -> {
                reloadThread.set(Thread.currentThread());

                p.reload();

                return null;
            });

            assertTrue(reloaded.await(10, TimeUnit.SECONDS));

            SSLContext inUse = p.context();

            // Let the old plan start its attempt before the reload queues a new plan.
            continuePlanning.countDown();

            assertTrue(attemptStarted.await(10, TimeUnit.SECONDS));

            allowReplan.countDown();

            reload.get(10_000);
            exec.submit(() -> {}).get(10, TimeUnit.SECONDS);

            assertEquals("Only the initial load and the manual reload may call the factory", 2, issuer.calls.size());
            assertSame(inUse, p.context());
            assertTrue("The next attempt must follow the new certificate", p.nextRenewalTime() > now + HOUR);
        }
        finally {
            continuePlanning.countDown();
            allowReplan.countDown();

            exec.shutdownNow();

            if (reload != null)
                reload.get(10_000);

            assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /** A successful reload by the command must cancel the planned retry and plan the next renewal by the new certificate. */
    @Test
    public void testReloadByCommandReplans() throws Exception {
        long now = System.currentTimeMillis();

        pauses(MINUTE, MINUTE);

        IgniteEx g = startNode(new Issuer(valid(now - HOUR, now + 9 * MINUTE), failure(), fresh(2 * HOUR), fresh(HOUR)));

        assertTrue(waitForCondition(() -> logged(Level.WARN, ISSUER_DOWN) == 1, 30_000));

        long retry = nextRenewalTime(g);

        assertTrue("A retry must be planned after the pause",
            retry >= provider(g).reloadOutcome().failureTime() + MINUTE - CLOCK_SLACK);

        reload(g);

        long due = dueTime(g);

        assertTrue(waitForCondition(() -> nextRenewalTime(g) == due, 10_000));

        assertTrue("The next renewal must follow the new certificate", due > now + HOUR);

        ScheduledThreadPoolExecutor exec = GridTestUtils.getFieldValue(g.context().sslContexts(), "renewalExec", "delegate");

        assertTrue("The cancelled retry must not stay planned", exec.getQueue().stream().map(t -> (RunnableScheduledFuture<?>)t)
            .filter(f -> !f.isCancelled()).allMatch(f -> f.getDelay(TimeUnit.MILLISECONDS) > HOUR));
    }

    /** Renewals must run in an operation context of their own, not in the one of whoever ran a reload before them. */
    @Test
    public void testReloadContextNotCarriedIntoRenewals() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = new Issuer(fresh(HOUR), valid(now - HOUR, now + 9 * MINUTE), fresh(HOUR));

        IgniteEx g = startNode(issuer);

        try (Scope ignored = OperationContext.set(OPERATOR, "operator")) {
            provider(g).reload();
        }

        assertTrue(waitForCondition(() -> automaticRenewals(NODE_TRANSPORTS) == 1, 30_000));

        assertEquals("operator", issuer.calls.get(1).operator);
        assertNull("The renewal must not run as the operator", issuer.calls.get(2).operator);
    }

    /**
     * A failed renewal must be retried until it succeeds. Each pause must double, from the shortest to the longest one. While more
     * than half of the window is left, each failure must be logged as a warning. The success must clear the count of failures.
     */
    @Test
    public void testFailedRenewalRetried() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = new Issuer(valid(now - HOUR, now + 9 * MINUTE),
            failure(), failure(), failure(), failure(), failure(), failure(), fresh(HOUR));

        pauses(100, 400);

        IgniteEx g = startNode(issuer);

        assertTrue(waitForCondition(() -> automaticRenewals("transports=" + COMMUNICATION) == 1, 30_000));

        long[] pauses = {100, 200, 400, 400, 400};

        for (int i = 0; i < pauses.length; i++) {
            long pause = issuer.calls.get(i + 2).time - issuer.calls.get(i + 1).time;

            assertTrue("Pause " + (i + 1) + " must last at least " + pauses[i] + " ms: " + pause, pause >= pauses[i] - CLOCK_SLACK);
        }

        LogEvent fifth = events(Level.WARN, ISSUER_DOWN).get(4);

        assertTrue("The longest pause must hold, a doubling one would be 1600 ms by now", plannedPause(fifth) <= 600);

        assertEquals(8, issuer.calls.size());
        assertEquals(6, logged(Level.WARN, "initiator=automatic renewal, reason=" + ISSUER_DOWN));
        assertEquals(0, logged(Level.ERROR, ISSUER_DOWN));
        assertEquals(0, metrics(g).<IntMetric>findMetric("ConsecutiveRenewalFailures").value());
        assertTrue(chainNotAfter(g) > now + 30 * MINUTE);
    }

    /** A failure must be an error once the next attempt would come only after expiry, even with more than half of the window left. */
    @Test
    public void testFailureBeforeExpiringRetryLoggedAsError() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = new Issuer(valid(now - HOUR, now + 2 * MINUTE), failure());

        issuer.setRenewBeforeFraction(0.05);
        pauses(5 * MINUTE, 5 * MINUTE);

        startNode(issuer);

        assertTrue(waitForCondition(() -> logged(Level.ERROR, "initiator=automatic renewal, reason=" + ISSUER_DOWN) == 1, 30_000));
    }

    /** A certificate due for renewal as soon as it is issued must not make the node renew more often than allowed. */
    @Test
    public void testRenewalsNoMoreOftenThanMinPause() throws Exception {
        AtomicInteger issued = new AtomicInteger();

        Callable<KeyStore> dueOnIssue = () -> {
            long t = System.currentTimeMillis();

            // A second later each time: notAfter is rounded down to a second, and a renewal that does not move it is refused.
            return ca.issue("node", t - HOUR, t + 5 * MINUTE + issued.incrementAndGet() * 1_000L);
        };

        Issuer issuer = new Issuer(dueOnIssue);

        pauses(300, 300);

        startNode(issuer);

        assertTrue(waitForCondition(() -> automaticRenewals("transports=" + COMMUNICATION) >= 4, 30_000));
        assertTrue("Every attempt must renew the certificate",
            appender.events().stream().noneMatch(e -> e.getLevel().isMoreSpecificThan(Level.WARN)));

        for (int i = 2; i < 5; i++) {
            long pause = issuer.calls.get(i).time - issuer.calls.get(i - 1).time;

            assertTrue("Renewals must be 300 ms apart at least: " + pause, pause >= 300 - CLOCK_SLACK);
        }
    }

    /** A {@code renewBeforeFraction} out of range must be refused at once. */
    @Test
    public void testInvalidSettingsRefused() {
        SslContextFactory factory = new SslContextFactory();

        assertThrowsWithCause(() -> factory.setRenewBeforeFraction(1), IllegalArgumentException.class);
        assertThrowsWithCause(() -> factory.setRenewBeforeFraction(0), IllegalArgumentException.class);
    }

    /** The client connector with a factory of its own must be renewed on its own. */
    @Test
    public void testClientConnectorRenewed() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = new Issuer(valid(now - HOUR, now + 5 * MINUTE), fresh(HOUR));

        IgniteEx g = startClientConnectorOn(issuer);

        assertTrue(waitForCondition(() -> automaticRenewals("transports=" + CLIENT_CONNECTOR) == 1, 30_000));

        SslContextProvider p = provider(g, CLIENT_CONNECTOR);

        long notAfter = SslCertificates.chainNotAfter(p.certificateChain());

        assertTrue(notAfter > now + 30 * MINUTE);
        assertTrue(waitForCondition(() -> p.nextRenewalTime() > now + 30 * MINUTE, 10_000));

        assertEquals("New connections must get the renewed certificate", notAfter,
            servedCertificate(probe(), g.context().clientListener().port()).getNotAfter().getTime());
    }

    /** Renewal must be off unless enabled: the node must not ask the factory again, plan a renewal or run the renewal thread. */
    @Test
    public void testRenewalOffByDefault() throws Exception {
        assertFalse(new SslContextFactory().isRenewalEnabled());

        long now = System.currentTimeMillis();

        Issuer issuer = new Issuer(valid(now - HOUR, now + 5 * MINUTE));

        issuer.setRenewalEnabled(false);

        IgniteEx g = startNode(issuer);

        assertEquals("A certificate in the window must stay", 1, issuer.calls.size());
        assertEquals(0, nextRenewalTime(g));
        assertFalse(renewalThreadAlive(g.name()));
        assertTrue(appender.events().isEmpty());
    }

    /** The node sums failures once per factory, and a recovery clears only that factory's contribution. */
    @Test
    public void testRenewalFailuresMetric() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            assertEquals(0, metrics(g).<IntMetric>findMetric("ConsecutiveRenewalFailures").value());
        }

        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + HOUR);

        Issuer nodeIssuer = new Issuer(() -> inUse, () -> inUse, fresh(2 * HOUR));
        Issuer clientIssuer = new Issuer(() -> inUse, () -> inUse, () -> inUse, fresh(2 * HOUR));

        nodeIssuer.setRenewalEnabled(false);
        clientIssuer.setRenewalEnabled(false);

        IgniteEx g = startGrid(0, cfg -> {
            cfg.setSslContextFactory(nodeIssuer);
            cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(false)
                .setUseIgniteSslContextFactory(false).setSslContextFactory(clientIssuer));
        });

        SslContextProvider node = provider(g);
        SslContextProvider client = provider(g, CLIENT_CONNECTOR);

        assertSame(node, provider(g, DISCOVERY));

        for (ReadOnlyMetricRegistry reg : g.context().metric())
            assertFalse("Transport aliases must not be registered: " + reg.name(), reg.name().startsWith("ssl."));

        IntMetric failures = metrics(g).findMetric("ConsecutiveRenewalFailures");

        assertEquals(0, failures.value());

        assertNull(node.renew(node.context()));
        assertEquals(1, failures.value());

        assertNull(client.renew(client.context()));
        assertNull(client.renew(client.context()));
        assertEquals(3, failures.value());

        assertNotNull(node.renew(node.context()));
        assertEquals(2, failures.value());

        assertNotNull(client.renew(client.context()));
        assertEquals(0, failures.value());
    }

    /**
     * While the factory returns the certificate in use, as a file-based one does until the files are replaced, the node must wait
     * rather than throw. It must count unsuccessful renewals and log one warning without a stack trace for many attempts. A reload
     * must start the wait over: the warning comes again and names every transport of the node, and the pause is the shortest one again. The
     * first warning may come before discovery is registered, so only the warning after the reload must name the transports.
     */
    @Test
    public void testWaitsForNewerCertificate() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + 9 * MINUTE);

        Issuer issuer = new Issuer(() -> inUse);

        IgniteEx g = startNode(issuer);

        assertTrue(waitForCondition(() -> provider(g).renewalFailures() >= 5, 30_000));

        long grown = issuer.calls.get(5).time - issuer.calls.get(4).time;

        assertTrue("The pause must double while waiting, up to 800 ms by the fifth one: " + grown, grown >= 800 - CLOCK_SLACK);

        assertEquals(1, logged(Level.WARN, WAITING));
        assertEquals(0, logged(Level.ERROR, WAITING));
        assertTrue("The wait must be logged without a stack trace", appender.events().stream().allMatch(e -> e.getThrown() == null));

        ReadOnlyMetricRegistry reg = metrics(g);

        assertTrue(reg.<IntMetric>findMetric("ConsecutiveRenewalFailures").value() >= 5);
        assertNull(provider(g).reloadOutcome().failureReason());

        Iterator<Metric> metrics = reg.iterator();

        assertEquals("ssl.ConsecutiveRenewalFailures", metrics.next().name());
        assertFalse("Only the renewal problem counter must be exposed", metrics.hasNext());

        reload(g);

        assertTrue(waitForCondition(() -> logged(Level.WARN, WAITING) == 2, 30_000));

        LogEvent afterReload = events(Level.WARN, WAITING).get(1);

        assertContains(log, afterReload.getMessage().getFormattedMessage(), WAITING + " [" + NODE_TRANSPORTS);
        assertTrue("The pause must be the shortest one again, not 800 ms or more", plannedPause(afterReload) < 400);
    }

    /**
     * A wait logged as a warning must be logged as an error at once when less than half of the window is left, once more when the
     * certificates expire, and then not again.
     */
    @Test
    public void testWaitTurningLateLoggedAsErrorAtOnce() throws Exception {
        AtomicReference<KeyStore> inUse = new AtomicReference<>();

        // Issued on the first request, so that the time left does not depend on how long the node takes to start.
        Issuer issuer = new Issuer(() -> {
            if (inUse.get() == null) {
                long t = System.currentTimeMillis();

                inUse.set(ca.issue("node", t - 80_000, t + 20_000));
            }

            return inUse.get();
        });

        issuer.setRenewBeforeFraction(0.22);

        startNode(issuer);

        assertTrue(waitForCondition(() -> logged(Level.ERROR, WAITING + " and expire soon") == 1, 30_000));

        int calls = issuer.calls.size();

        assertTrue(waitForCondition(() -> issuer.calls.size() >= calls + 2, 30_000));

        assertEquals(1, logged(Level.WARN, WAITING));
        assertEquals(1, logged(Level.ERROR, WAITING));

        assertTrue(waitForCondition(() -> logged(Level.ERROR, WAITING + ", though they have expired") == 1, 30_000));

        int expiredCalls = issuer.calls.size();

        assertTrue(waitForCondition(() -> issuer.calls.size() >= expiredCalls + 2, 30_000));

        assertEquals(1, logged(Level.WARN, WAITING));
        assertEquals(2, logged(Level.ERROR, WAITING));
    }

    /** A node that stops in the middle of a renewal attempt must interrupt it and log no failure. */
    @Test
    public void testStopDuringRenewalLogsNoFailure() throws Exception {
        long now = System.currentTimeMillis();

        CountDownLatch entered = new CountDownLatch(1);

        Issuer issuer = new Issuer(valid(now - HOUR, now + 9 * MINUTE), () -> {
            entered.countDown();

            Thread.sleep(Long.MAX_VALUE);

            return null;
        });

        IgniteEx g = startNode(issuer);

        assertTrue(entered.await(30, TimeUnit.SECONDS));

        SslContextProvider p = provider(g);

        stopGrid(0);

        assertEquals(0, p.renewalFailures());
        assertTrue(appender.events().stream().noneMatch(e -> e.getLevel().isMoreSpecificThan(Level.WARN)));
    }

    /** Cached and expired unchanged chains stay in use and count as unsuccessful renewal attempts. */
    @Test
    public void testSameCertificatesUnchanged() throws Exception {
        long now = System.currentTimeMillis();

        SSLContext cached = TestCertificateAuthority.context(ca.issue("node", now - HOUR, now + HOUR), ca.trustStore());

        SslContextProvider p = new SslContextProvider(() -> cached);

        assertNull(p.renew(p.context()));
        assertEquals(1, p.renewalFailures());

        assertNull(p.renew(p.context()));
        assertEquals(2, p.renewalFailures());
        assertEquals(0, p.reloadOutcome().failures());

        KeyStore expired = ca.issue("node", now - 2 * HOUR, now - HOUR);

        SslContextProvider expiredProvider = new SslContextProvider(new Issuer(() -> expired));

        SSLContext expiredCtx = expiredProvider.context();

        assertNull(expiredProvider.renew(expiredCtx));
        assertSame(expiredCtx, expiredProvider.context());
        assertEquals(1, expiredProvider.renewalFailures());
    }

    /** Returning the same certificate and throwing both count until a renewal extends the chain's lifetime. */
    @Test
    public void testUnsuccessfulRenewalsCounted() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + HOUR);

        SslContextProvider p = new SslContextProvider(new Issuer(() -> inUse, () -> inUse, failure(), () -> inUse, fresh(2 * HOUR)));

        assertNull(p.renew(p.context()));
        assertEquals(1, p.renewalFailures());

        GridTestUtils.assertThrows(log, () -> p.renew(p.context()), IgniteException.class, ISSUER_DOWN);

        assertEquals(2, p.renewalFailures());
        assertNull(p.renew(p.context()));
        assertEquals(3, p.renewalFailures());
        assertEquals(1, p.reloadOutcome().failures());

        assertNotNull(p.renew(p.context()));
        assertEquals(0, p.renewalFailures());
    }

    /** Manual reloads neither count as renewal attempts nor clear their failures unless the chain expires later. */
    @Test
    public void testReloadClearsRenewalFailuresOnlyWithLaterExpiry() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + HOUR);

        Issuer issuer = new Issuer(() -> inUse, () -> inUse, failure(), () -> inUse, () -> inUse,
            valid(now - HOUR, now + HOUR), fresh(2 * HOUR));

        SslContextProvider p = new SslContextProvider(issuer);

        assertNull(p.renew(p.context()));
        assertEquals(1, p.renewalFailures());

        // A failed manual reload is not an automatic renewal attempt.
        GridTestUtils.assertThrows(log, p::reload, IgniteException.class, ISSUER_DOWN);

        assertEquals(1, p.renewalFailures());

        // Reloading the same certificate clears the reload error, but not the renewal failures.
        p.reload();

        assertEquals(1, p.renewalFailures());
        assertNull(p.reloadOutcome().failureReason());

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        issuer.trust = TestCertificateAuthority.trustStore(ca, other);

        // Updating only the trust store does not renew the certificate.
        p.reload();

        assertEquals(1, p.renewalFailures());

        // A different certificate with the same expiry still does not extend the chain's lifetime.
        p.reload();

        assertFalse(inUse.getCertificate("node").equals(p.certificate()));
        assertEquals(1, p.renewalFailures());

        // Only the final certificate extends the chain's lifetime.
        p.reload();

        assertEquals(0, p.renewalFailures());
    }

    /** Extending only the leaf does not clear failures when the authority still expires first. */
    @Test
    public void testLaterLeafExpiryDoesNotClearRenewalFailures() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + TimeUnit.DAYS.toMillis(400));

        SslContextProvider p = new SslContextProvider(new Issuer(() -> inUse, () -> inUse,
            valid(now - HOUR, now + TimeUnit.DAYS.toMillis(450))));

        long chainExpiry = SslCertificates.chainNotAfter(p.certificateChain());
        long leafExpiry = p.certificate().getNotAfter().getTime();

        assertNull(p.renew(p.context()));

        p.reload();

        assertTrue(p.certificate().getNotAfter().getTime() > leafExpiry);
        assertEquals(chainExpiry, SslCertificates.chainNotAfter(p.certificateChain()));
        assertEquals(1, p.renewalFailures());
    }

    /** The file-based factory with renewal enabled must put in use the key store replaced on disk, with no command. */
    @Test
    public void testReplacedKeyStoreFileRenewed() throws Exception {
        long now = System.currentTimeMillis();

        Path keyStore = Files.createTempFile("ignite-ssl-renewal-", ".p12");

        try {
            TestCertificateAuthority.save(ca.issue("node", now - HOUR, now + 9 * MINUTE), keyStore);

            SslContextFactory factory = new SslContextFactory();

            factory.setKeyStoreFilePath(keyStore.toString());
            factory.setKeyStorePassword(TestCertificateAuthority.PWD);
            factory.setKeyStoreType("PKCS12");
            factory.setTrustManagers(TestCertificateAuthority.trustManagers(ca.trustStore()));
            factory.setRenewalEnabled(true);

            IgniteEx g = startNode(factory);

            assertTrue(waitForCondition(() -> logged(Level.WARN, WAITING) == 1, 30_000));

            TestCertificateAuthority.save(ca.issue("node", now, now + HOUR), keyStore);

            assertTrue(waitForCondition(() -> automaticRenewals(NODE_TRANSPORTS) == 1, 30_000));
            assertTrue(chainNotAfter(g) > now + 30 * MINUTE);
        }
        finally {
            Files.delete(keyStore);
        }
    }

    /** Automatic renewal without certificate metadata must stop the node from starting. */
    @Test
    public void testUnknownCertificateRefusedAtStart() {
        Issuer issuer = new Issuer(SslRenewalTest::noKey);

        GridTestUtils.assertThrowsAnyCause(log, () -> startClientConnectorOn(issuer), IgniteException.class,
            "The SSL context factory provides no certificate chain");
    }

    /** Without renewal, a factory with no certificate metadata can be reloaded, and status reports the missing metadata. */
    @Test
    public void testUnknownCertificateWithoutRenewal() throws Exception {
        Issuer issuer = new Issuer(SslRenewalTest::noKey);

        issuer.setRenewalEnabled(false);

        IgniteEx g = startClientConnectorOn(issuer);

        SslContextProvider p = provider(g, CLIENT_CONNECTOR);

        assertNull(p.certificateChain());
        assertEquals(0, p.nextRenewalTime());
        assertContains(log, status(g), "certificate metadata is unavailable");
        assertContains(log, reload(g), ": reloaded " + CLIENT_CONNECTOR);
        assertTrue(appender.events().isEmpty());
    }

    /** Without renewal, replacing one key with several keys must not prevent manual reload. */
    @Test
    public void testReloadCanLoseMetadataWithoutRenewal() throws Exception {
        Issuer issuer = new Issuer(fresh(HOUR), this::twoKeys);

        issuer.setRenewalEnabled(false);

        SslContextProvider p = new SslContextProvider(issuer);
        SSLContext inUse = p.context();

        assertNotNull(p.certificateChain());

        p.reload();

        assertNotSame(inUse, p.context());
        assertNull(p.certificateChain());
        assertTrue(p.reloadOutcome().successTime() > 0);
        assertEquals(0, p.reloadOutcome().failures());
    }

    /** Renewal settings are fixed at registration; changing the factory must not let reload lose the chain used by renewal. */
    @Test
    public void testReloadCannotLoseMetadataWithRenewal() {
        Issuer issuer = new Issuer(fresh(HOUR), this::twoKeys);
        SslContextProvider p = new SslContextProvider(issuer);

        issuer.setRenewalEnabled(false);

        SSLContext inUse = p.context();
        X509Certificate[] chain = p.certificateChain();
        String reason = "The new SSL context provides no certificate chain, so it cannot be renewed automatically";

        assertNotNull(chain);

        GridTestUtils.assertThrows(log, () -> {
            p.reload();

            return null;
        }, IgniteCheckedException.class, reason);

        assertSame(inUse, p.context());
        assertArrayEquals(chain, p.certificateChain());

        assertRenewalRejected(p, reason);
    }

    /** @return Store with two valid private keys and therefore no single chain for renewal metadata. */
    private KeyStore twoKeys() throws Exception {
        long now = System.currentTimeMillis();
        KeyStore keys = ca.issue("node", now - MINUTE, now + HOUR);
        KeyStore other = ca.issue("other", now - MINUTE, now + HOUR);

        keys.setKeyEntry("other", other.getKey("other", TestCertificateAuthority.PWD), TestCertificateAuthority.PWD,
            other.getCertificateChain("other"));

        return keys;
    }

    /** @return Key store with no private key. */
    private static KeyStore noKey() throws Exception {
        KeyStore noKey = KeyStore.getInstance(KeyStore.getDefaultType());

        noKey.load(null, null);

        return noKey;
    }

    /**
     * @param factory SSL context factory of the client connector.
     * @return Started node whose client connector runs on the factory and whose other transports run without SSL.
     */
    private IgniteEx startClientConnectorOn(AbstractSslContextFactory factory) throws Exception {
        return startGrid(getTestIgniteInstanceName(0), cfg -> cfg
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(false)
                .setUseIgniteSslContextFactory(false).setSslContextFactory(factory)));
    }

    /**
     * @param factory SSL context factory of the node.
     * @return Started node.
     */
    private IgniteEx startNode(AbstractSslContextFactory factory) throws Exception {
        return startNode(0, factory);
    }

    /**
     * @param idx Node index.
     * @param factory SSL context factory of the node.
     * @return Started node.
     */
    private IgniteEx startNode(int idx, AbstractSslContextFactory factory) throws Exception {
        return startGrid(getTestIgniteInstanceName(idx), cfg -> cfg.setSslContextFactory(factory));
    }

    /**
     * @param source Key store to use on each request.
     * @param trust Authorities the node accepts.
     * @param renewal Whether the node renews automatically.
     * @return Configured issuer.
     */
    private Issuer issuer(Callable<KeyStore> source, KeyStore trust, boolean renewal) {
        Issuer issuer = new Issuer(source);

        issuer.trust = trust;
        issuer.setRenewalEnabled(renewal);

        return issuer;
    }

    /**
     * Rotates one node's certificate, then restarts each node to check that they load their current certificates and reconnect.
     *
     * @param peerCa Authority of the peer whose certificate stays unchanged.
     * @param automatic Whether the first node renews automatically instead of being reloaded by a command.
     */
    private void checkNodeRotation(TestCertificateAuthority peerCa, boolean automatic) throws Exception {
        long now = System.currentTimeMillis();

        AtomicReference<KeyStore> keys = new AtomicReference<>(ca.issue("node0", now - HOUR, now + 9 * MINUTE));
        KeyStore peerKeys = peerCa.issue("node1", now - MINUTE, now + HOUR);
        Issuer issuer = issuer(keys::get, peerCa.trustStore(), automatic);
        Issuer peerIssuer = issuer(() -> peerKeys, ca.trustStore(), false);

        IgniteEx g0 = startNode(0, issuer);
        IgniteEx g1 = startNode(1, peerIssuer);

        X509Certificate peerCert = provider(g1).certificate();

        assertFalse(provider(g0).certificate().equals(peerCert));
        assertComputeWorks(g0, g1);

        KeyStore replacement = ca.issue("node0-new", now, now + 2 * HOUR);
        X509Certificate renewed = (X509Certificate)replacement.getCertificate("node0-new");

        keys.set(replacement);

        if (automatic)
            assertTrue(waitForCondition(() -> renewed.equals(provider(g0).certificate()), 30_000));
        else
            assertContains(log, reload(g0), ": reloaded " + COMMUNICATION + ", " + DISCOVERY);

        assertEquals(renewed, provider(g0).certificate());
        assertEquals(peerCert, provider(g1).certificate());
        assertEquals(0, provider(g1).reloadOutcome().successTime());
        assertEquals(renewed, servedCertificate(peerIssuer.createNew(), discoveryPort(g0)));
        assertEquals(peerCert, servedCertificate(issuer.createNew(), discoveryPort(g1)));

        assertComputeWorks(g0, g1);

        stopGrid(1);

        g1 = startNode(1, issuer(() -> peerKeys, ca.trustStore(), false));

        assertEquals(renewed, provider(g0).certificate());
        assertEquals(peerCert, provider(g1).certificate());
        assertComputeWorks(g0, g1);

        stopGrid(0);

        IgniteEx restarted = startNode(0, issuer(keys::get, peerCa.trustStore(), automatic));

        assertEquals(renewed, provider(restarted).certificate());
        assertEquals(renewed, servedCertificate(peerIssuer.createNew(), discoveryPort(restarted)));
        assertEquals(peerCert, provider(g1).certificate());
        assertComputeWorks(restarted, g1);
    }

    /** Runs a job from each node on its peer. */
    private void assertComputeWorks(IgniteEx g0, IgniteEx g1) {
        for (IgniteEx from : List.of(g0, g1)) {
            IgniteEx to = from == g0 ? g1 : g0;

            assertEquals((Integer)1, from.compute(from.cluster().forNodeId(to.localNode().id())).withTimeout(10_000).call(() -> 1));
        }
    }

    /**
     * @param notBefore Time the certificate becomes valid.
     * @param notAfter Time it expires.
     * @return Step that issues a certificate valid for that time.
     */
    private Callable<KeyStore> valid(long notBefore, long notAfter) {
        return () -> ca.issue("node", notBefore, notAfter);
    }

    /**
     * @param lifetime Lifetime of the certificate.
     * @return Step that issues a certificate valid from the moment it is issued.
     */
    private Callable<KeyStore> fresh(long lifetime) {
        return () -> {
            long now = System.currentTimeMillis();

            return ca.issue("node", now, now + lifetime);
        };
    }

    /** @return Step that fails the way an issuer that cannot be reached does. */
    private static Callable<KeyStore> failure() {
        return () -> {
            throw new IgniteException(ISSUER_DOWN);
        };
    }

    /** @return Context to connect to the nodes with; a new one each time, so that no session is resumed with an old certificate. */
    private SSLContext probe() throws Exception {
        long now = System.currentTimeMillis();

        return TestCertificateAuthority.context(ca.issue("probe", now - MINUTE, now + HOUR), ca.trustStore());
    }

    /**
     * @param min Pause after the first renewal attempt that puts nothing in use, in milliseconds.
     * @param max Longest pause, in milliseconds.
     */
    private static void pauses(long min, long max) {
        GridTestUtils.setFieldValue(SslRenewal.class, "minPause", min);
        GridTestUtils.setFieldValue(SslRenewal.class, "maxPause", max);
    }

    /**
     * @param p Provider.
     * @param reason Reason the renewal must be rejected for.
     */
    private static void assertRenewalRejected(SslContextProvider p, String reason) {
        SSLContext inUse = p.context();
        X509Certificate[] chain = p.certificateChain();
        int failures = p.renewalFailures();

        GridTestUtils.assertThrows(log, () -> p.renew(inUse), IgniteCheckedException.class, reason);

        assertSame(inUse, p.context());
        assertArrayEquals(chain, p.certificateChain());
        assertEquals(failures + 1, p.renewalFailures());
    }

    /**
     * @param level Level.
     * @param text Text the message contains.
     * @return How many messages with the text the renewals logged at the level.
     */
    private long logged(Level level, String text) {
        return events(level, text).size();
    }

    /**
     * @param level Level.
     * @param text Text the message contains.
     * @return Messages with the text the renewals logged at the level, in the order logged.
     */
    private List<LogEvent> events(Level level, String text) {
        return appender.events().stream().filter(e -> e.getLevel() == level && e.getMessage().getFormattedMessage().contains(text))
            .collect(Collectors.toList());
    }

    /**
     * @param transports Transports as the log names them.
     * @return How many times the renewals put new certificates in use.
     */
    private long automaticRenewals(String transports) {
        return logged(Level.INFO, "TLS certificates reloaded [" + transports);
    }

    /**
     * @param e Event logged by an attempt that put nothing in use.
     * @return Time from the event to the next attempt it planned, in milliseconds.
     */
    private static long plannedPause(LogEvent e) {
        String msg = e.getMessage().getFormattedMessage();
        int from = msg.indexOf("nextRenewal=") + "nextRenewal=".length();

        return Instant.parse(msg.substring(from, msg.indexOf(']', from))).toEpochMilli() - e.getTimeMillis();
    }

    /**
     * @param g Node.
     * @return Time the certificate the node serves is due for renewal by the default share.
     */
    private static long dueTime(IgniteEx g) {
        long notBefore = provider(g).certificate().getNotBefore().getTime();
        long notAfter = chainNotAfter(g);

        return notAfter - (long)((notAfter - notBefore) * DFLT_RENEW_BEFORE_FRACTION);
    }

    /** */
    private static long nextRenewalTime(IgniteEx g) {
        return provider(g).nextRenewalTime();
    }

    /** */
    private static long chainNotAfter(IgniteEx g) {
        return SslCertificates.chainNotAfter(provider(g).certificateChain());
    }

    /**
     * @param g Node.
     * @return SSL metrics of the node.
     */
    private static ReadOnlyMetricRegistry metrics(IgniteEx g) {
        for (ReadOnlyMetricRegistry reg : g.context().metric()) {
            if (reg.name().equals("ssl"))
                return reg;
        }

        throw new AssertionError("SSL metrics are not registered");
    }

    /**
     * @param g Node.
     * @return Provider of the context communication takes.
     */
    private static SslContextProvider provider(IgniteEx g) {
        return provider(g, COMMUNICATION);
    }

    /**
     * @param g Node.
     * @param transport Transport.
     * @return Provider of the context the transport takes.
     */
    private static SslContextProvider provider(IgniteEx g, String transport) {
        for (SslContextProvider p : g.context().sslContexts().providers()) {
            if (p.transports().contains(transport))
                return p;
        }

        throw new AssertionError("Nothing serves " + transport);
    }

    /**
     * @param name Node name.
     * @return Whether a renewal thread of the node is alive.
     */
    private static boolean renewalThreadAlive(String name) {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.isAlive() && t.getName().startsWith("ssl-renewal") && t.getName().contains(name))
                return true;
        }

        return false;
    }

    /** One request to an issuer. */
    private static class Call {
        /** When it came. */
        private final long time = System.currentTimeMillis();

        /** Operation context attribute standing for the operator, as the request saw it. */
        private final @Nullable String operator = OperationContext.get(OPERATOR);
    }

    /** Factory that answers each request the way the test scripted it, the last step repeated for the rest. */
    private class Issuer extends AbstractSslContextFactory {
        /** */
        @Serial
        private static final long serialVersionUID = 0L;

        /** Answers to the requests in order, the last one repeated for the rest. */
        private final List<Callable<KeyStore>> steps;

        /** Requests so far. */
        private final List<Call> calls = new CopyOnWriteArrayList<>();

        /** Trust store of the contexts built from now on, {@code null} for the one of the test authority alone. */
        private volatile KeyStore trust;

        /** @param steps Answers to the requests in order, the last one repeated for the rest. */
        @SafeVarargs
        private Issuer(Callable<KeyStore>... steps) {
            this.steps = List.of(steps);

            setRenewalEnabled(true);
        }

        /** {@inheritDoc} */
        @Override protected void checkParameters() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected KeyManager[] createKeyManagers() throws SSLException {
            return createKeyMaterial().keyManagers();
        }

        /** {@inheritDoc} */
        @Override protected KeyMaterial createKeyMaterial() throws SSLException {
            Callable<KeyStore> step = steps.get(Math.min(calls.size(), steps.size() - 1));

            calls.add(new Call());

            try {
                KeyStore store = step.call();

                return KeyMaterial.fromKeyStore(TestCertificateAuthority.keyManagers(store), store);
            }
            catch (IgniteException e) {
                throw e;
            }
            catch (Exception e) {
                throw new SSLException(e);
            }
        }

        /** {@inheritDoc} */
        @Override protected TrustManager[] createTrustManagers() throws SSLException {
            try {
                return TestCertificateAuthority.trustManagers(trust != null ? trust : ca.trustStore());
            }
            catch (Exception e) {
                throw new SSLException(e);
            }
        }
    }
}
