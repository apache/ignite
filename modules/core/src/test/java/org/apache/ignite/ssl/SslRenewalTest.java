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
import java.security.KeyStore;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.ssl.SslContextProvider;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.ssl.SslRenewal;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.Level;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.ssl.SslContextReloadable.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextReloadable.COMMUNICATION;
import static org.apache.ignite.ssl.AbstractSslContextFactory.DFLT_RENEW_BEFORE_FRACTION;
import static org.apache.ignite.ssl.SslTestUtils.discoveryPort;
import static org.apache.ignite.ssl.SslTestUtils.reload;
import static org.apache.ignite.ssl.SslTestUtils.servedCertificate;
import static org.apache.ignite.ssl.SslTestUtils.status;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests that a node renews the certificates of a factory with renewal enabled before they expire. */
public class SslRenewalTest extends GridCommonAbstractTest {
    /** */
    private static final long MIN = 60_000L;

    /** */
    private static final long HOUR = 60 * MIN;

    /** How much earlier than planned a timer may fire, measured by a finer clock than the one it was set by. */
    private static final long CLOCK_SLACK = 50;

    /** Failure of an issuer that cannot be reached. */
    private static final String ISSUER_DOWN = "Issuer is unavailable";

    /** What the node logs while the factory hands back the certificates in use. */
    private static final String WAITING = "The SSL context factory has no newer TLS certificates yet, the ones in use stay";

    /** Transports a node with SSL configured for the whole node serves on one context, as the log names them. */
    private static final String NODE_TRANSPORTS = "transports=communication, discovery";

    /** Operation context attribute standing for whoever runs a reload. */
    private static final OperationContextAttribute<String> OPERATOR = OperationContextAttribute.newInstance();

    /** Authority behind every certificate the nodes get. */
    private TestCertificateAuthority ca;

    /** Issuer of each node, by node name; a node without one gets certificates for an hour. */
    private final Map<String, Issuer> issuers = new ConcurrentHashMap<>();

    /** Log of the nodes under test. */
    private final ListeningTestLogger nodeLog = new ListeningTestLogger(log);

    /** What the renewals log, with the levels. */
    private final MemorizingAppender appender = new MemorizingAppender();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(nodeLog)
            .setSslContextFactory(issuers.computeIfAbsent(igniteInstanceName, name -> new Issuer().then(fresh(HOUR))));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("renewalca");

        appender.installSelfOn(SslRenewal.class);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        appender.removeSelfFrom(SslRenewal.class);

        stopAllGrids();
    }

    /** Nodes must renew in the window and not before, serve and plan by the new certificate, let a node join, and stop renewing on stop. */
    @Test
    public void testRenewsBeforeExpiry() throws Exception {
        long now = System.currentTimeMillis();
        long notAfter = (now + 40_000) / 1000 * 1000;
        long notBefore = notAfter - 233_000;
        long due = notAfter - (long)((notAfter - notBefore) * DFLT_RENEW_BEFORE_FRACTION);

        assertTrue("The window must open seconds after the nodes start, well before expiry", due - now > 3_000 && notAfter - due > 30_000);

        issuer(0).then(valid(notBefore, notAfter)).then(fresh(HOUR));
        issuer(1).then(valid(notBefore, notAfter)).then(fresh(HOUR));

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).times(2).build();

        nodeLog.registerListener(renewed);

        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        assertTrue(renewed.check(30_000));

        for (IgniteEx g : new IgniteEx[] {g0, g1}) {
            assertTrue("The certificate must have been renewed", certificateNotAfter(g) > now + 30 * MIN);
            assertTrue("The renewal must not come before the window opens",
                metrics(g, "ssl.communication").<LongMetric>findMetric("LastReloadTime").value() >= due);
            assertEquals("New connections must get the renewed certificate", certificateNotAfter(g),
                servedCertificate(probe(), discoveryPort(g)).getNotAfter().getTime());

            long next = dueTime(g);

            assertTrue(waitForCondition(() -> nextRenewalTime(g) == next, 10_000));
        }

        IgniteEx g2 = startGrid(2);

        assertEquals(3, g0.cluster().nodes().size());

        g2.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        assertEquals(1, g0.cache(DEFAULT_CACHE_NAME).get(1));

        assertTrue("A node renewing certificates must run the renewal thread", renewalThreadAlive(g0.name()));

        stopGrid(0);

        assertTrue("The renewal thread must stop with the node", waitForCondition(() -> !renewalThreadAlive(g0.name()), 10_000));
    }

    /** Failures once less than half of the window remains must be logged as errors, and the status must show them with the next attempt. */
    @Test
    public void testLateFailureLoggedAsError() throws Exception {
        long now = System.currentTimeMillis();

        issuer(0).then(valid(now - HOUR, now + 2 * MIN)).then(failure());

        IgniteEx g = startGrid(0);

        String late = "Failed to reload TLS certificates, the ones in use stay and expire soon [" + NODE_TRANSPORTS +
            ", initiator=automatic renewal, reason=" + ISSUER_DOWN;

        assertTrue(waitForCondition(() -> logged(Level.ERROR, late) >= 2, 30_000));

        MetricRegistryImpl reg = metrics(g, "ssl.communication");

        assertTrue(reg.<IntMetric>findMetric("ReloadFailures").value() >= 2);
        assertContains(log, reg.<ObjectMetric<String>>findMetric("LastReloadFailure").value(), ISSUER_DOWN);

        String report = status(g);

        assertContains(log, report, "last reload failed");
        assertContains(log, report, ISSUER_DOWN);
        assertContains(log, report, "next automatic renewal at");
    }

    /** A certificate that expires no later than the one in use must not be put in use. */
    @Test
    public void testRenewalNotMovingExpiryRejected() throws Exception {
        long now = System.currentTimeMillis();

        SslContextProvider p = new SslContextProvider(new Issuer().then(valid(now - HOUR, now + 5 * MIN)));

        assertRenewalRejected(p, "The new certificate chain expires no later than the one in use");
    }

    /** A certificate the other nodes would refuse must not be put in use. */
    @Test
    public void testRenewalRefusedBetweenNodesRejected() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        SslContextProvider p = new SslContextProvider(new Issuer().then(fresh(HOUR)).then(() -> other.issue("node", now, now + HOUR)));

        p.addTransport(COMMUNICATION);

        assertRenewalRejected(p, "A handshake between nodes on the new certificate was refused");
    }

    /** A certificate that is not valid yet must not be put in use, even where no handshake would catch it. */
    @Test
    public void testRenewalNotValidYetRejected() throws Exception {
        long now = System.currentTimeMillis();

        SslContextProvider p = new SslContextProvider(new Issuer().then(fresh(HOUR)).then(valid(now + 10 * MIN, now + 2 * HOUR)));

        assertRenewalRejected(p, "The new certificate chain is not valid now");
    }

    /** A renewal planned before a reload must give way to it rather than replace the certificate the reload has put in use. */
    @Test
    public void testRenewalPlannedBeforeReloadDropped() throws Exception {
        Issuer issuer = new Issuer().then(fresh(HOUR));

        SslContextProvider p = new SslContextProvider(issuer);

        SSLContext planned = p.context();

        p.reload();

        SSLContext reloaded = p.context();

        assertEquals(SslContextProvider.Renewed.SUPERSEDED, p.renew(planned));
        assertSame(reloaded, p.context());
        assertEquals("The renewal must not ask for another certificate", 2, issuer.calls.size());
    }

    /** A successful reload by the command must cancel the planned retry and plan the next renewal by the new certificate. */
    @Test
    public void testReloadByCommandReplans() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 9 * MIN)).then(failure()).then(fresh(2 * HOUR)).then(fresh(HOUR));

        issuer.setRenewalRetryMinInterval(2_000);
        issuer.setRenewalRetryMaxInterval(2_000);

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).build();

        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(0);

        assertTrue(waitForCondition(() -> logged(Level.WARN, ISSUER_DOWN) == 1, 30_000));

        long retry = nextRenewalTime(g);

        assertTrue("A retry must be planned after the pause",
            retry >= provider(g, COMMUNICATION).lastFailureTime() + issuer.getRenewalRetryMinInterval() - CLOCK_SLACK);

        reload(g);

        long due = dueTime(g);

        assertTrue(waitForCondition(() -> nextRenewalTime(g) == due, 10_000));

        assertTrue("The next renewal must follow the new certificate", due > now + HOUR);
        assertEquals(0, metrics(g, "ssl.communication").<IntMetric>findMetric("ReloadFailures").value());

        long untilPastRetry = retry + 1_500 - System.currentTimeMillis();

        assertFalse(waitForCondition(() -> issuer.calls.size() > 3, untilPastRetry));

        assertFalse("The cancelled retry must not run", renewed.check());
    }

    /** Renewals must run in a context of their own, not in the one of whoever ran a reload before them. */
    @Test
    public void testReloadContextNotCarriedIntoRenewals() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(fresh(HOUR)).then(valid(now - HOUR, now + 9 * MIN)).then(fresh(HOUR));

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).times(1).build();

        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(0);

        try (Scope ignored = OperationContext.set(OPERATOR, "operator")) {
            SslContextProvider p = provider(g, COMMUNICATION);

            p.reload();
            p.onReloaded(log, "operator");
        }

        assertTrue(renewed.check(30_000));

        assertEquals("operator", issuer.calls.get(1).operator);
        assertNull("The renewal must not run as the operator", issuer.calls.get(2).operator);
    }

    /**
     * A failed renewal must be retried until it succeeds, each pause doubling from the shortest to the longest one, each failure logged
     * as a warning while more than half of the window is left, and the count of failures cleared by the success.
     */
    @Test
    public void testFailedRenewalRetried() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 9 * MIN));

        for (int i = 0; i < 6; i++)
            issuer.then(failure());

        issuer.then(fresh(HOUR));

        issuer.setRenewalRetryMinInterval(100);
        issuer.setRenewalRetryMaxInterval(400);

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).times(1).build();

        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(0);

        assertTrue(renewed.check(30_000));

        long[] pauses = {100, 200, 400, 400, 400};

        for (int i = 0; i < pauses.length; i++) {
            long pause = issuer.calls.get(i + 2).time - issuer.calls.get(i + 1).time;

            assertTrue("Pause " + (i + 1) + " must last at least " + pauses[i] + " ms: " + pause, pause >= pauses[i] - CLOCK_SLACK);
        }

        long last = issuer.calls.get(6).time - issuer.calls.get(5).time;

        assertTrue("The longest pause must hold, a doubling one would be 1600 ms by now: " + last, last < 1_200);

        assertEquals(8, issuer.calls.size());
        assertEquals(6, logged(Level.WARN, "initiator=automatic renewal, reason=" + ISSUER_DOWN));
        assertEquals(0, logged(Level.ERROR, ISSUER_DOWN));
        assertEquals(0, metrics(g, "ssl.communication").<IntMetric>findMetric("ReloadFailures").value());
        assertTrue(certificateNotAfter(g) > now + 30 * MIN);
    }

    /** A failure must be an error once the next attempt would come only after expiry, even with more than half of the window left. */
    @Test
    public void testFailureBeforeExpiringRetryLoggedAsError() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 2 * MIN)).then(failure());

        issuer.setRenewBefore(3 * MIN);
        issuer.setRenewalRetryMinInterval(5 * MIN);
        issuer.setRenewalRetryMaxInterval(5 * MIN);

        startGrid(0);

        assertTrue(waitForCondition(() -> logged(Level.ERROR, "initiator=automatic renewal, reason=" + ISSUER_DOWN) == 1, 30_000));
    }

    /** The longest pause must be cut down to a quarter of the window, so that a short window holds several attempts. */
    @Test
    public void testRetryPauseCutToWindow() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 5_000)).then(failure());

        issuer.setRenewBefore(6_000);
        issuer.setRenewalRetryMinInterval(1_000);
        issuer.setRenewalRetryMaxInterval(HOUR);

        startGrid(0);

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 5, 30_000));

        for (int i = 2; i < 5; i++) {
            long pause = issuer.calls.get(i).time - issuer.calls.get(i - 1).time;

            assertTrue("Pause " + (i - 1) + " must stay within a quarter of the window, a doubling third one would be 4 s: " + pause,
                pause >= 1_000 - CLOCK_SLACK && pause < 3_000);
        }
    }

    /**
     * A certificate due for renewal as soon as it is issued must not make the node renew more often than allowed, whatever the factory
     * says after the start.
     */
    @Test
    public void testRenewalsNoMoreOftenThanMinRetry() throws Exception {
        Step dueOnIssue = () -> {
            long t = System.currentTimeMillis();

            return ca.issue("node", t - HOUR, t + 5 * MIN);
        };

        Issuer issuer = issuer(0).then(dueOnIssue);

        issuer.setRenewalRetryMinInterval(300);
        issuer.setRenewalRetryMaxInterval(300);

        startGrid(0);

        issuer.setRenewalRetryMinInterval(1);
        issuer.setRenewalRetryMaxInterval(1);

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 5, 30_000));

        for (int i = 2; i < 5; i++) {
            long pause = issuer.calls.get(i).time - issuer.calls.get(i - 1).time;

            assertTrue("Renewals must be 300 ms apart at least: " + pause, pause >= 300 - CLOCK_SLACK);
        }
    }

    /** An absolute window must apply when it is smaller than the share of the lifetime, and only then. */
    @Test
    public void testRenewBeforeTakesSmallerWindow() throws Exception {
        long now = System.currentTimeMillis();

        issuer(0).then(valid(now, now + HOUR)).setRenewBefore(2 * MIN);
        issuer(1).then(valid(now, now + HOUR)).setRenewBefore(30 * MIN);

        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        assertTrue(waitForCondition(() -> nextRenewalTime(g0) == certificateNotAfter(g0) - 2 * MIN, 10_000));
        assertTrue(waitForCondition(() -> nextRenewalTime(g1) == dueTime(g1), 10_000));
    }

    /** The jitter must bring the renewal forward by a random share of the window, different on each node. */
    @Test
    public void testJitterBringsRenewalForward() throws Exception {
        long now = System.currentTimeMillis();

        for (int i = 0; i < 2; i++)
            issuer(i).then(valid(now, now + HOUR)).setRenewalJitter(1);

        startGrid(0);
        startGrid(1);

        long[] next = new long[2];

        for (int i = 0; i < 2; i++) {
            IgniteEx g = grid(i);

            assertTrue(waitForCondition(() -> nextRenewalTime(g) > 0, 10_000));

            long due = dueTime(g);
            long window = certificateNotAfter(g) - due;

            next[i] = nextRenewalTime(g);

            assertTrue("The renewal must come within the window [next=" + next[i] + ", due=" + due + ']',
                next[i] <= due && next[i] >= due - window);
        }

        assertTrue("Nodes with certificates issued at once must not renew at once", next[0] != next[1]);
    }

    /** Settings out of range must keep the node from starting, before the issuer is asked for anything. */
    @Test
    public void testInvalidSettingsFailNodeStart() throws Exception {
        assertStartFails(i -> i.setRenewBeforeFraction(1), "renewBeforeFraction must be greater than 0 and less than 1");
        assertStartFails(i -> i.setRenewBeforeFraction(0), "renewBeforeFraction must be greater than 0 and less than 1");
        assertStartFails(i -> i.setRenewBefore(-1), "renewBefore must not be negative");
        assertStartFails(i -> i.setRenewalJitter(1.5), "renewalJitter must be from 0 to 1");
        assertStartFails(i -> {
            i.setRenewBeforeFraction(0.6);
            i.setRenewalJitter(1);
        }, "renewBeforeFraction * (1 + renewalJitter) must be less than 1");
        assertStartFails(i -> i.setRenewalRetryMinInterval(0), "renewalRetryMinInterval must be positive");
        assertStartFails(i -> i.setRenewalRetryMaxInterval(i.getRenewalRetryMinInterval() - 1),
            "renewalRetryMaxInterval must not be less than renewalRetryMinInterval");
    }

    /** The client connector with a factory of its own must be renewed on its own. */
    @Test
    public void testClientConnectorRenewed() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 5 * MIN)).then(fresh(HOUR));

        LogListener renewed = automaticRenewals("transports=" + CLIENT_CONNECTOR).times(1).build();

        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(getTestIgniteInstanceName(0), cfg -> cfg.setSslContextFactory(null)
            .setClientConnectorConfiguration(new ClientConnectorConfiguration().setSslEnabled(true).setSslClientAuth(false)
                .setUseIgniteSslContextFactory(false).setSslContextFactory(issuer)));

        assertTrue(renewed.check(30_000));

        MetricRegistryImpl reg = metrics(g, "ssl.client.connector");

        long notAfter = reg.<LongMetric>findMetric("CertificateNotAfter").value();

        assertTrue(notAfter > now + 30 * MIN);
        assertTrue(reg.<LongMetric>findMetric("NextRenewalTime").value() > now + 30 * MIN);

        assertEquals("New connections must get the renewed certificate", notAfter,
            servedCertificate(probe(), g.context().clientListener().port()).getNotAfter().getTime());
    }

    /** Renewal must be off unless enabled: the node must not ask the factory again, plan a renewal or run the renewal thread. */
    @Test
    public void testRenewalOffByDefault() throws Exception {
        assertFalse(new SslContextFactory().isRenewalEnabled());

        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 5 * MIN));

        issuer.setRenewalEnabled(false);

        IgniteEx g = startGrid(0);

        int calls = issuer.calls.size();

        assertFalse("A certificate in the window must stay", waitForCondition(() -> issuer.calls.size() > calls, 2_000));
        assertEquals(0, nextRenewalTime(g));
        assertFalse(renewalThreadAlive(g.name()));
        assertTrue(appender.events().isEmpty());
    }

    /**
     * While the factory hands back the certificate in use, as a file-based one does until the files are replaced, the node must wait
     * rather than fail: no failures counted, one warning without a stack trace for many attempts, then the newer certificate put in use.
     */
    @Test
    public void testWaitsForNewerCertificate() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + 9 * MIN);

        Issuer issuer = issuer(0).then(() -> inUse);

        IgniteEx g = startGrid(0);

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 6, 30_000));

        long[] pauses = {100, 200, 400, 800};

        for (int i = 0; i < pauses.length; i++) {
            long pause = issuer.calls.get(i + 2).time - issuer.calls.get(i + 1).time;

            assertTrue("Pause " + (i + 1) + " must last at least " + pauses[i] + " ms: " + pause, pause >= pauses[i] - CLOCK_SLACK);
        }

        assertEquals(1, logged(Level.WARN, WAITING + " [" + NODE_TRANSPORTS));
        assertEquals(0, logged(Level.ERROR, WAITING));
        assertTrue("The wait must be logged without a stack trace", appender.events().stream().allMatch(e -> e.getThrown() == null));

        MetricRegistryImpl reg = metrics(g, "ssl.communication");

        assertEquals(0, reg.<IntMetric>findMetric("ReloadFailures").value());
        assertNull(reg.<ObjectMetric<String>>findMetric("LastReloadFailure").value());

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).times(1).build();

        nodeLog.registerListener(renewed);

        issuer.then(fresh(HOUR));

        assertTrue(renewed.check(30_000));
        assertTrue(certificateNotAfter(g) > now + 30 * MIN);
    }

    /** Waiting once less than half of the window is left must be logged as an error, still once for many attempts and not as a failure. */
    @Test
    public void testLateWaitLoggedAsError() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + 2 * MIN);

        Issuer issuer = issuer(0).then(() -> inUse);

        IgniteEx g = startGrid(0);

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 6, 30_000));

        assertEquals(1, logged(Level.ERROR, WAITING + " and expire soon [" + NODE_TRANSPORTS));
        assertEquals(0, logged(Level.WARN, WAITING));
        assertEquals(0, metrics(g, "ssl.communication").<IntMetric>findMetric("ReloadFailures").value());
    }

    /** A wait logged as a warning must be logged as an error at once when less than half of the window is left, and then not again. */
    @Test
    public void testWaitTurningLateLoggedAsErrorAtOnce() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + 20_000);

        Issuer issuer = issuer(0).then(() -> inUse);

        issuer.setRenewBefore(22_000);

        startGrid(0);

        assertTrue(waitForCondition(() -> logged(Level.ERROR, WAITING + " and expire soon") == 1, 30_000));

        int calls = issuer.calls.size();

        assertTrue(waitForCondition(() -> issuer.calls.size() >= calls + 2, 30_000));

        assertEquals(1, logged(Level.WARN, WAITING));
        assertEquals(1, logged(Level.ERROR, WAITING));
    }

    /** A reload by the command must start the wait afresh: logged again, and with the shortest pause again. */
    @Test
    public void testReloadRestartsWait() throws Exception {
        long now = System.currentTimeMillis();

        KeyStore inUse = ca.issue("node", now - HOUR, now + 9 * MIN);

        Issuer issuer = issuer(0).then(() -> inUse);

        IgniteEx g = startGrid(0);

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 6, 30_000));

        reload(g);

        int reloaded = issuer.calls.size();

        assertTrue(waitForCondition(() -> logged(Level.WARN, WAITING) == 2, 30_000));
        assertTrue(waitForCondition(() -> issuer.calls.size() >= reloaded + 2, 30_000));

        long pause = issuer.calls.get(reloaded + 1).time - issuer.calls.get(reloaded).time;

        assertTrue("The pause must be the shortest one again, not 800 ms or more: " + pause, pause < 400);
    }

    /** Certificates handed back as they are in use, even the very same context or an expired chain, must leave nothing to renew yet. */
    @Test
    public void testSameCertificatesUnchanged() throws Exception {
        long now = System.currentTimeMillis();

        SSLContext cached = TestCertificateAuthority.context(ca.issue("node", now - HOUR, now + HOUR), ca.trustStore());

        SslContextProvider p = new SslContextProvider(() -> cached);

        assertEquals(SslContextProvider.Renewed.UNCHANGED, p.renew(p.context()));

        KeyStore expired = ca.issue("node", now - 2 * HOUR, now - HOUR);

        SslContextProvider expiredProvider = new SslContextProvider(new Issuer().then(() -> expired));

        expiredProvider.addTransport(COMMUNICATION);

        assertEquals(SslContextProvider.Renewed.UNCHANGED, expiredProvider.renew(expiredProvider.context()));
    }

    /** The file-based factory with renewal enabled must put in use the key store replaced on disk, with no command. */
    @Test
    public void testReplacedKeyStoreFileRenewed() throws Exception {
        long now = System.currentTimeMillis();

        Path dir = Files.createTempDirectory("ignite-ssl-renewal-");

        try {
            renewKeyStoreFile(dir, now);
        }
        finally {
            U.delete(dir);
        }
    }

    /**
     * @param dir Directory for the stores.
     * @param now Current time.
     */
    private void renewKeyStoreFile(Path dir, long now) throws Exception {
        Path keyStore = dir.resolve("node.p12");
        Path trustStore = dir.resolve("trust.p12");

        TestCertificateAuthority.save(ca.issue("node", now - HOUR, now + 9 * MIN), keyStore);
        TestCertificateAuthority.save(ca.trustStore(), trustStore);

        SslContextFactory factory = new SslContextFactory();

        factory.setKeyStoreFilePath(keyStore.toString());
        factory.setKeyStorePassword(TestCertificateAuthority.PASSWORD.toCharArray());
        factory.setKeyStoreType("PKCS12");
        factory.setTrustStoreFilePath(trustStore.toString());
        factory.setTrustStorePassword(TestCertificateAuthority.PASSWORD.toCharArray());
        factory.setTrustStoreType("PKCS12");
        factory.setRenewalEnabled(true);
        factory.setRenewalRetryMinInterval(100);
        factory.setRenewalRetryMaxInterval(500);

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).times(1).build();

        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(getTestIgniteInstanceName(0), cfg -> cfg.setSslContextFactory(factory));

        assertTrue(waitForCondition(() -> logged(Level.WARN, WAITING) == 1, 30_000));

        TestCertificateAuthority.save(ca.issue("node", now, now + HOUR), keyStore);

        assertTrue(renewed.check(30_000));
        assertTrue(certificateNotAfter(g) > now + 30 * MIN);
    }

    /**
     * @param idx Node index.
     * @return Issuer of the node, created empty.
     */
    private Issuer issuer(int idx) {
        Issuer issuer = new Issuer();

        issuers.put(getTestIgniteInstanceName(idx), issuer);

        return issuer;
    }

    /**
     * @param notBefore Time the certificate becomes valid.
     * @param notAfter Time it expires.
     * @return Step that issues a certificate valid for that time.
     */
    private Step valid(long notBefore, long notAfter) {
        return () -> ca.issue("node", notBefore, notAfter);
    }

    /**
     * @param lifetime Lifetime of the certificate.
     * @return Step that issues a certificate valid from the moment it is issued.
     */
    private Step fresh(long lifetime) {
        return () -> {
            long now = System.currentTimeMillis();

            return ca.issue("node", now, now + lifetime);
        };
    }

    /** @return Step that fails the way an issuer that cannot be reached does. */
    private static Step failure() {
        return () -> {
            throw new IgniteException(ISSUER_DOWN);
        };
    }

    /** @return Context to connect to the nodes with; a new one each time, so that no session is resumed with an old certificate. */
    private SSLContext probe() throws Exception {
        long now = System.currentTimeMillis();

        return TestCertificateAuthority.context(ca.issue("probe", now - MIN, now + HOUR), ca.trustStore());
    }

    /**
     * @param setup Puts a setting of the issuer out of range.
     * @param reason Reason the node must give for not starting.
     */
    private void assertStartFails(Consumer<Issuer> setup, String reason) {
        Issuer issuer = issuer(0).then(fresh(HOUR));

        setup.accept(issuer);

        assertContains(log, X.getFullStackTrace(GridTestUtils.assertThrows(log, () -> startGrid(0), Exception.class, null)), reason);

        assertTrue("The issuer must not be asked: " + reason, issuer.calls.isEmpty());
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
        return appender.events().stream().filter(e -> e.getLevel() == level && e.getMessage().getFormattedMessage().contains(text)).count();
    }

    /**
     * @param transports Transports as the log names them.
     * @return Listener of the certificates the renewals put in use.
     */
    private static LogListener.Builder automaticRenewals(String transports) {
        return LogListener.matches(s -> s.contains("TLS certificates reloaded [" + transports) &&
            s.contains("initiator=automatic renewal"));
    }

    /**
     * @param g Node.
     * @return Time the certificate the node serves is due for renewal by the default share, without jitter.
     */
    private static long dueTime(IgniteEx g) {
        long notBefore = provider(g, COMMUNICATION).servedCertificate().getNotBefore().getTime();
        long notAfter = certificateNotAfter(g);

        return notAfter - (long)((notAfter - notBefore) * DFLT_RENEW_BEFORE_FRACTION);
    }

    /** */
    private static long nextRenewalTime(IgniteEx g) {
        return metrics(g, "ssl.communication").<LongMetric>findMetric("NextRenewalTime").value();
    }

    /** */
    private static long certificateNotAfter(IgniteEx g) {
        return metrics(g, "ssl.communication").<LongMetric>findMetric("CertificateNotAfter").value();
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
     * @param transport Transport.
     * @return Provider of the context the transport takes.
     */
    private static SslContextProvider provider(IgniteEx g, String transport) {
        for (SslContextReloadable comp : g.context().internalSubscriptionProcessor().sslContexts().reloadables()) {
            if (comp.transports().contains(transport))
                return (SslContextProvider)comp;
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

    /** What an issuer does on one request. */
    @FunctionalInterface
    private interface Step {
        /** @return Key store with the certificate issued. */
        KeyStore issue() throws Exception;
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

        /** Steps not taken yet. */
        private final Queue<Step> steps = new ConcurrentLinkedQueue<>();

        /** Requests so far. */
        private final List<Call> calls = new CopyOnWriteArrayList<>();

        /** Step taken last. */
        private volatile Step last;

        /** Renews, with short pauses, so that retries do not hold the test up. */
        private Issuer() {
            setRenewalEnabled(true);
            setRenewalRetryMinInterval(100);
            setRenewalRetryMaxInterval(1_000);
        }

        /**
         * @param step Step to take on the next request not scripted yet.
         * @return {@code this} for chaining.
         */
        private Issuer then(Step step) {
            steps.add(step);

            return this;
        }

        /** {@inheritDoc} */
        @Override protected void checkParameters() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected KeyManager[] createKeyManagers() throws SSLException {
            calls.add(new Call());

            Step step = steps.poll();

            if (step != null)
                last = step;

            try {
                return TestCertificateAuthority.keyManagers(last.issue());
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
                return TestCertificateAuthority.trustManagers(ca.trustStore());
            }
            catch (Exception e) {
                throw new SSLException(e);
            }
        }
    }
}
