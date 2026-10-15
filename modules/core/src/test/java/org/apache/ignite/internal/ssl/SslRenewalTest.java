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
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
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
    @Override protected void afterTest() throws Exception {
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
            metrics(g, "ssl.communication").<LongMetric>findMetric("LastReloadSuccessTime").value() >= due - CLOCK_SLACK);
        assertEquals("New connections must get the renewed certificate", chainNotAfter(g),
            servedCertificate(probe(), discoveryPort(g)).getNotAfter().getTime());

        long next = dueTime(g);

        assertTrue(waitForCondition(() -> nextRenewalTime(g) == next, 10_000));

        assertTrue("A node renewing certificates must run the renewal thread", renewalThreadAlive(g.name()));

        stopGrid(0);

        assertTrue("The renewal thread must stop with the node", waitForCondition(() -> !renewalThreadAlive(g.name()), 10_000));
    }

    /** With less than half of the window left, failures must be logged as errors and counted, and the status must show the next attempt. */
    @Test
    public void testLateFailureLoggedAsError() throws Exception {
        long now = System.currentTimeMillis();

        IgniteEx g = startNode(new Issuer(valid(now - HOUR, now + 2 * MINUTE), failure()));

        String late = "Failed to reload TLS certificates, the ones in use stay and expire soon [" + NODE_TRANSPORTS +
            ", initiator=automatic renewal, reason=" + ISSUER_DOWN;

        assertTrue(waitForCondition(() -> logged(Level.ERROR, late) >= 2, 30_000));

        assertTrue(metrics(g, "ssl.communication").<IntMetric>findMetric("ConsecutiveReloadFailures").value() >= 2);
        assertContains(log, status(g), "next automatic renewal at");
    }

    /** A certificate that expires no later than the one in use must not be put in use. */
    @Test
    public void testRenewalNotMovingExpiryRejected() throws Exception {
        long now = System.currentTimeMillis();

        SslContextProvider p = new SslContextProvider(new Issuer(valid(now - HOUR, now + 5 * MINUTE)));

        assertRenewalRejected(p, "The new certificate chain expires no later than the one in use");
    }

    /** A certificate the other nodes would refuse must not be put in use. */
    @Test
    public void testRenewalRefusedBetweenNodesRejected() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        SslContextProvider p = new SslContextProvider(new Issuer(fresh(HOUR), () -> other.issue("node", now, now + HOUR)));

        p.addTransport(COMMUNICATION);

        assertRenewalRejected(p, "The new certificate fails a test handshake with this node's own trust store");
    }

    /** A renewal to an authority that the trust store in use does not accept yet must be refused, until a reload puts it there. */
    @Test
    public void testRenewalToAuthorityNotTrustedYetRejected() throws Exception {
        for (String transport : new String[] {COMMUNICATION, DISCOVERY}) {
            long now = System.currentTimeMillis();

            TestCertificateAuthority other = new TestCertificateAuthority("otherca");

            Issuer issuer = new Issuer(fresh(HOUR), () -> other.issue("node", now, now + 2 * HOUR));

            SslContextProvider p = new SslContextProvider(issuer);

            p.addTransport(transport);

            issuer.trust = TestCertificateAuthority.trustStore(ca, other);

            assertRenewalRejected(p, "The trust store in use does not accept the new certificate");
        }
    }

    /** A renewal of a transport nodes do not connect on must not be checked against the trust store in use. */
    @Test
    public void testClientConnectorRenewalNotCheckedAgainstTrustInUse() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        Issuer issuer = new Issuer(fresh(HOUR), () -> other.issue("node", now, now + 2 * HOUR));

        SslContextProvider p = new SslContextProvider(issuer);

        p.addTransport(CLIENT_CONNECTOR);

        issuer.trust = TestCertificateAuthority.trustStore(ca, other);

        assertEquals(SslContextProvider.RenewalResult.RENEWED, p.renew(p.context()));
    }

    /** A renewal that drops the authority of the certificate in use must be refused: peers that have not renewed yet still present it. */
    @Test
    public void testRenewalDroppingAuthorityInUseRejected() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        Issuer issuer = new Issuer(fresh(HOUR), () -> other.issue("node", now, now + 2 * HOUR));

        issuer.trust = TestCertificateAuthority.trustStore(ca, other);

        SslContextProvider p = new SslContextProvider(issuer);

        p.addTransport(COMMUNICATION);

        issuer.trust = other.trustStore();

        assertRenewalRejected(p, "The new trust store does not accept the certificate in use");
    }

    /** Once the certificate in use has expired, peers refuse it anyway, so a renewal must not be checked against it. */
    @Test
    public void testExpiredCertificateInUseNotCheckedAgainst() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        Issuer issuer = new Issuer(valid(now - 2 * HOUR, now - HOUR), () -> other.issue("node", now, now + HOUR));

        SslContextProvider p = new SslContextProvider(issuer);

        p.addTransport(COMMUNICATION);

        issuer.trust = other.trustStore();

        assertEquals(SslContextProvider.RenewalResult.RENEWED, p.renew(p.context()));
        assertEquals("CN=otherca", p.servedCertificate().getIssuerX500Principal().getName());
    }

    /** The authorities a context announces must come sorted by name, whatever order the trust store holds them in. */
    @Test
    public void testTrustedAuthoritiesSorted() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority[] cas = {ca, new TestCertificateAuthority("eca"), new TestCertificateAuthority("bca"),
            new TestCertificateAuthority("dca"), new TestCertificateAuthority("aca")};

        SSLContext ctx = TestCertificateAuthority.context(ca.issue("node", now - HOUR, now + HOUR),
            TestCertificateAuthority.trustStore(cas));

        assertEquals(List.of("CN=aca", "CN=bca", "CN=dca", "CN=eca", "CN=renewalca"),
            SslCertificates.trustedAuthorities(ctx).stream().map(X500Principal::getName).collect(Collectors.toList()));
    }

    /** A trust manager that names no authorities must leave the node to start, with the authorities unknown. */
    @Test
    public void testTrustManagerNamingNoAuthorities() throws Exception {
        long now = System.currentTimeMillis();

        X509TrustManager namingNone = new X509TrustManager() {
            @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {
                // No-op.
            }

            @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {
                // No-op.
            }

            @Override public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };

        SSLContext ctx = SSLContext.getInstance("TLS");

        ctx.init(TestCertificateAuthority.keyManagers(ca.issue("node", now - HOUR, now + HOUR)), new TrustManager[] {namingNone}, null);

        assertTrue(new SslContextProvider(() -> ctx).trustedAuthorities().isEmpty());
    }

    /** Once a reload has put a new authority in the trust store in use, a renewal to that authority must go through. */
    @Test
    public void testRenewalToTrustedAuthorityAccepted() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        Issuer issuer = new Issuer(fresh(HOUR), fresh(HOUR), () -> other.issue("node", now, now + 2 * HOUR));

        SslContextProvider p = new SslContextProvider(issuer);

        p.addTransport(COMMUNICATION);

        issuer.trust = TestCertificateAuthority.trustStore(ca, other);

        p.reload();

        assertEquals(SslContextProvider.RenewalResult.RENEWED, p.renew(p.context()));
        assertEquals("CN=otherca", p.servedCertificate().getIssuerX500Principal().getName());
    }

    /** A certificate that is not valid yet must not be put in use, even where no handshake would catch it. */
    @Test
    public void testRenewalNotValidYetRejected() throws Exception {
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
        Issuer issuer = new Issuer(fresh(HOUR));

        SslContextProvider p = new SslContextProvider(issuer);

        SSLContext planned = p.context();

        p.reload();

        SSLContext reloaded = p.context();

        assertEquals(SslContextProvider.RenewalResult.SUPERSEDED, p.renew(planned));
        assertSame(reloaded, p.context());
        assertEquals("The renewal must not ask for another certificate", 2, issuer.calls.size());
        assertSame(planned, issuer.create());
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
            retry >= provider(g).lastFailureTime() + MINUTE - CLOCK_SLACK);

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
        assertEquals(0, metrics(g, "ssl.communication").<IntMetric>findMetric("ConsecutiveReloadFailures").value());
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

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 5, 30_000));

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

        IgniteEx g = startGrid(getTestIgniteInstanceName(0), cfg -> cfg
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(false)
                .setUseIgniteSslContextFactory(false).setSslContextFactory(issuer)));

        assertTrue(waitForCondition(() -> automaticRenewals("transports=" + CLIENT_CONNECTOR) == 1, 30_000));

        MetricRegistryImpl reg = metrics(g, "ssl.client.connector");

        long notAfter = reg.<LongMetric>findMetric("ChainNotAfter").value();

        assertTrue(notAfter > now + 30 * MINUTE);
        assertTrue(waitForCondition(() -> reg.<LongMetric>findMetric("NextRenewalTime").value() > now + 30 * MINUTE, 10_000));

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

    /**
     * While the factory returns the certificate in use, as a file-based one does until the files are replaced, the node must wait
     * rather than fail. It must count no failures and log one warning without a stack trace for many attempts. A reload must start
     * the wait over: the warning comes again and names every transport of the node, and the pause is the shortest one again. The
     * first warning may come before discovery is registered, so only the warning after the reload must name the transports.
     */
    @Test
    public void testWaitsForNewerCertificate() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + 9 * MINUTE);

        Issuer issuer = new Issuer(() -> inUse);

        IgniteEx g = startNode(issuer);

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 6, 30_000));

        long grown = issuer.calls.get(5).time - issuer.calls.get(4).time;

        assertTrue("The pause must double while waiting, up to 800 ms by the fifth one: " + grown, grown >= 800 - CLOCK_SLACK);

        assertEquals(1, logged(Level.WARN, WAITING));
        assertEquals(0, logged(Level.ERROR, WAITING));
        assertTrue("The wait must be logged without a stack trace", appender.events().stream().allMatch(e -> e.getThrown() == null));

        MetricRegistryImpl reg = metrics(g, "ssl.communication");

        assertEquals(0, reg.<IntMetric>findMetric("ConsecutiveReloadFailures").value());
        assertNull(reg.<ObjectMetric<String>>findMetric("LastReloadFailureReason").value());

        reload(g);

        assertTrue(waitForCondition(() -> logged(Level.WARN, WAITING) == 2, 30_000));

        LogEvent afterReload = events(Level.WARN, WAITING).get(1);

        assertContains(log, afterReload.getMessage().getFormattedMessage(), WAITING + " [" + NODE_TRANSPORTS);
        assertTrue("The pause must be the shortest one again, not 800 ms or more", plannedPause(afterReload) < 400);
    }

    /** A wait logged as a warning must be logged as an error at once when less than half of the window is left, and then not again. */
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
    }

    /** A factory that returns the certificates in use, as the very same context or as an expired chain, must leave nothing to renew. */
    @Test
    public void testSameCertificatesUnchanged() throws Exception {
        long now = System.currentTimeMillis();

        SSLContext cached = TestCertificateAuthority.context(ca.issue("node", now - HOUR, now + HOUR), ca.trustStore());

        SslContextProvider p = new SslContextProvider(() -> cached);

        assertEquals(SslContextProvider.RenewalResult.UNCHANGED, p.renew(p.context()));

        KeyStore expired = ca.issue("node", now - 2 * HOUR, now - HOUR);

        SslContextProvider expiredProvider = new SslContextProvider(new Issuer(() -> expired));

        assertEquals(SslContextProvider.RenewalResult.UNCHANGED, expiredProvider.renew(expiredProvider.context()));
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

    /** If the node cannot tell when the certificate expires, here with no key in the key store, it must not renew or count a failure. */
    @Test
    public void testUnknownCertificateNotRenewed() throws Exception {
        Issuer issuer = new Issuer(() -> {
            KeyStore noKey = KeyStore.getInstance(KeyStore.getDefaultType());

            noKey.load(null, null);

            return noKey;
        });

        IgniteEx g = startGrid(getTestIgniteInstanceName(0), cfg -> cfg
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(false)
                .setUseIgniteSslContextFactory(false).setSslContextFactory(issuer)));

        assertTrue(waitForCondition(() -> logged(Level.WARN, "Cannot tell when the TLS certificate expires") == 1, 30_000));

        MetricRegistryImpl reg = metrics(g, "ssl.client.connector");

        assertEquals(0, reg.<IntMetric>findMetric("ConsecutiveReloadFailures").value());
        assertEquals(0, reg.<LongMetric>findMetric("NextRenewalTime").value());
        assertEquals("Nothing must be renewed", 1, issuer.calls.size());
    }

    /**
     * @param factory SSL context factory of the node.
     * @return Started node.
     */
    private IgniteEx startNode(AbstractSslContextFactory factory) throws Exception {
        return startGrid(getTestIgniteInstanceName(0), cfg -> cfg.setSslContextFactory(factory));
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

        GridTestUtils.assertThrows(log, () -> p.renew(inUse), IgniteCheckedException.class, reason);

        assertSame(inUse, p.context());
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
        long notBefore = provider(g).servedCertificate().getNotBefore().getTime();
        long notAfter = chainNotAfter(g);

        return notAfter - (long)((notAfter - notBefore) * DFLT_RENEW_BEFORE_FRACTION);
    }

    /** */
    private static long nextRenewalTime(IgniteEx g) {
        return metrics(g, "ssl.communication").<LongMetric>findMetric("NextRenewalTime").value();
    }

    /** */
    private static long chainNotAfter(IgniteEx g) {
        return metrics(g, "ssl.communication").<LongMetric>findMetric("ChainNotAfter").value();
    }

    /**
     * @param g Node.
     * @param name Registry name, as the documentation gives it.
     * @return Metrics of a transport.
     */
    private static MetricRegistryImpl metrics(IgniteEx g, String name) {
        return g.context().metric().registry(name);
    }

    /**
     * @param g Node.
     * @return Provider of the context communication takes.
     */
    private static SslContextProvider provider(IgniteEx g) {
        for (SslContextProvider p : g.context().sslContexts().providers()) {
            if (p.transports().contains(COMMUNICATION))
                return p;
        }

        throw new AssertionError("Nothing serves " + COMMUNICATION);
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
            Callable<KeyStore> step = steps.get(Math.min(calls.size(), steps.size() - 1));

            calls.add(new Call());

            try {
                return TestCertificateAuthority.keyManagers(step.call());
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
