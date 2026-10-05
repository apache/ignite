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
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.management.api.CommandWarningException;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.management.ssl.SslReloadCommandArg;
import org.apache.ignite.internal.management.ssl.SslReloadTask;
import org.apache.ignite.internal.management.ssl.SslStatusTask;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.ssl.SslContextProvider;
import org.apache.ignite.internal.ssl.SslContextReloadable;
import org.apache.ignite.internal.ssl.SslMetrics;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorTaskArgument;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.ObjectMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.ssl.SslContextReloadable.CLIENT_CONNECTOR;
import static org.apache.ignite.internal.ssl.SslContextReloadable.COMMUNICATION;
import static org.apache.ignite.ssl.RenewableSslContextFactory.DFLT_RENEW_BEFORE_FRACTION;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests that a node renews the certificates a {@link RenewableSslContextFactory} issues before they expire. */
public class SslRenewalTest extends GridCommonAbstractTest {
    /** */
    private static final long MIN = 60_000L;

    /** */
    private static final long HOUR = 60 * MIN;

    /** How much earlier than planned a timer may fire, measured by a finer clock than the one it was set by. */
    private static final long CLOCK_SLACK = 20;

    /** Failure of an issuer that cannot be reached. */
    private static final String ISSUER_DOWN = "Issuer is unavailable";

    /** Transports a node with SSL configured for the whole node serves on one context, as the log names them. */
    private static final String NODE_TRANSPORTS = "transports=communication, discovery";

    /** Operation context attribute standing for whoever runs a reload. */
    private static final OperationContextAttribute<String> OPERATOR = OperationContextAttribute.newInstance();

    /** Authority behind every certificate the nodes get. */
    private TestCertificateAuthority ca;

    /** Issuer of each node, by node name; a node without one gets certificates for an hour. */
    private final Map<String, Issuer> issuers = new ConcurrentHashMap<>();

    /** Whether the issuer serves the client connector only, while the rest of the node runs without SSL. */
    private boolean clientConnectorOnly;

    /** Log of the nodes under test. */
    private ListeningTestLogger nodeLog;

    /** What the nodes logged as errors. */
    private final Queue<String> errors = new ConcurrentLinkedQueue<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setGridLogger(nodeLog);

        Issuer issuer = issuers.computeIfAbsent(igniteInstanceName, name -> new Issuer().then(fresh(HOUR)));

        if (clientConnectorOnly) {
            cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setSslEnabled(true)
                .setSslClientAuth(false)
                .setUseIgniteSslContextFactory(false)
                .setSslContextFactory(issuer));
        }
        else
            cfg.setSslContextFactory(issuer);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("renewalca");

        nodeLog = new ListeningTestLogger(new ErrorLog(log, errors::add));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        issuers.clear();
        errors.clear();

        clientConnectorOnly = false;
    }

    /**
     * Each node must plan the renewal by the window of its certificate, renew it then and not before, serve the new
     * certificate to new connections and plan the next renewal by it; a node joining afterwards must find the cluster
     * working.
     */
    @Test
    public void testRenewsBeforeExpiry() throws Exception {
        long now = System.currentTimeMillis();

        // The window of 15% opens in about five seconds, and the certificate leaves the nodes forty to start in. The
        // dates are cut to seconds, the way the certificate carries them.
        long notBefore = (now - 193_000) / 1000 * 1000;
        long notAfter = (now + 40_000) / 1000 * 1000;

        long due = notAfter - (long)((notAfter - notBefore) * DFLT_RENEW_BEFORE_FRACTION);

        issuer(0).then(valid(notBefore, notAfter)).then(fresh(HOUR));
        issuer(1).then(valid(notBefore, notAfter)).then(fresh(HOUR));

        // Planned as soon as the first transport serves the context, so the line may not list the others yet.
        LogListener planned = LogListener.matches(s -> s.contains("TLS certificates will be renewed automatically") &&
            s.contains(", at=" + Instant.ofEpochMilli(due) + ',')).times(2).build();

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).times(2).build();

        nodeLog.registerListener(planned);
        nodeLog.registerListener(renewed);

        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        // The renewals are planned as the nodes start, so on a machine slow enough to start them past the due time
        // they are planned for right away instead.
        if (System.currentTimeMillis() < due)
            assertTrue(planned.check(10_000));

        assertTrue(renewed.check(30_000));

        for (IgniteEx g : new IgniteEx[] {g0, g1}) {
            assertTrue("The certificate must have been renewed", certificateNotAfter(g) > now + 30 * MIN);

            assertTrue("The renewal must not come before the window opens", longMetric(g, "LastReloadTime") >= due);

            assertEquals("New connections must get the renewed certificate", certificateNotAfter(g),
                servedCertificate(discoveryPort(g)).getNotAfter().getTime());

            long next = dueTime(g);

            assertTrue(waitForCondition(() -> nextRenewalTime(g) == next, 10_000));
        }

        IgniteEx g2 = startGrid(2);

        assertEquals(3, g0.cluster().nodes().size());

        g2.getOrCreateCache(DEFAULT_CACHE_NAME).put(1, 1);

        assertEquals(1, g0.cache(DEFAULT_CACHE_NAME).get(1));
    }

    /** A failed renewal must be retried until it succeeds, each failure logged as a warning and counted. */
    @Test
    public void testFailedRenewalRetried() throws Exception {
        long now = System.currentTimeMillis();

        // The window is open from the start, and less than half of it is left only minutes later.
        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 9 * MIN)).then(failure()).then(failure())
            .then(fresh(HOUR));

        LogListener failed = LogListener.matches(s -> s.contains("Failed to reload TLS certificates") &&
            s.contains("initiator=automatic renewal, reason=" + ISSUER_DOWN)).times(2).build();

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).times(1).build();

        nodeLog.registerListener(failed);
        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(0);

        assertTrue(renewed.check(30_000));
        assertTrue(failed.check());

        assertEquals(4, issuer.calls.size());
        assertEquals(0, registry(g).<IntMetric>findMetric("ReloadFailures").value());
        assertTrue(certificateNotAfter(g) > now + 30 * MIN);

        assertTrue("Early failures must be warnings: " + errors,
            errors.stream().noneMatch(e -> e.contains(ISSUER_DOWN)));
    }

    /**
     * Failures once less than half of the window remains must be logged as errors, and the status must show them
     * along with the next attempt.
     */
    @Test
    public void testLateFailureLoggedAsError() throws Exception {
        long now = System.currentTimeMillis();

        issuer(0).then(valid(now - HOUR, now + 2 * MIN)).then(failure());

        LogListener late = LogListener.matches("Failed to reload TLS certificates, the ones in use stay and " +
            "expire soon [" + NODE_TRANSPORTS + ", initiator=automatic renewal, reason=" + ISSUER_DOWN)
            .atLeast(2).build();

        nodeLog.registerListener(late);

        IgniteEx g = startGrid(0);

        assertTrue(late.check(30_000));

        assertTrue("Late failures must be errors: " + errors,
            errors.stream().anyMatch(e -> e.contains("and expire soon") && e.contains(ISSUER_DOWN)));

        assertTrue(registry(g).<IntMetric>findMetric("ReloadFailures").value() >= 2);
        assertContains(log, registry(g).<ObjectMetric<String>>findMetric("LastReloadFailure").value(), ISSUER_DOWN);

        Throwable e = GridTestUtils.assertThrows(log, () -> status(g), IgniteException.class, null);

        assertTrue(X.hasCause(e, CommandWarningException.class));

        String report = X.getFullStackTrace(e);

        assertContains(log, report, "last reload failed");
        assertContains(log, report, ISSUER_DOWN);
        assertContains(log, report, "next automatic renewal at");
    }

    /** A certificate that expires no later than the one in use must not be put in use, however often it comes. */
    @Test
    public void testRenewalNotMovingExpiryRejected() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 5 * MIN));

        LogListener rejected = LogListener.matches("The new certificate chain expires no later than the one in use")
            .atLeast(2).build();

        nodeLog.registerListener(rejected);

        IgniteEx g = startGrid(0);

        assertTrue(rejected.check(30_000));

        assertEquals("The certificate issued first must stay", issuer.calls.get(0).serial,
            registry(g).<ObjectMetric<String>>findMetric("CertificateSerialNumber").value());
    }

    /** A renewed certificate the other nodes would refuse must not be put in use. */
    @Test
    public void testRenewalRefusedBetweenNodesRejected() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 5 * MIN))
            .then(() -> other.issue("node", now, now + HOUR));

        LogListener refused = LogListener.matches(s -> s.contains("Failed to reload TLS certificates") &&
            s.contains("A handshake between nodes on the new certificate was refused")).atLeast(2).build();

        nodeLog.registerListener(refused);

        IgniteEx g = startGrid(0);

        assertTrue(refused.check(30_000));

        assertEquals("The certificate issued first must stay", issuer.calls.get(0).serial,
            registry(g).<ObjectMetric<String>>findMetric("CertificateSerialNumber").value());
        assertEquals("CN=renewalca", registry(g).<ObjectMetric<String>>findMetric("CertificateIssuer").value());
    }

    /** A certificate that is not valid yet must not be put in use, even where no handshake would catch it. */
    @Test
    public void testRenewalNotValidYetRejected() throws Exception {
        clientConnectorOnly = true;

        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 5 * MIN))
            .then(valid(now + 10 * MIN, now + 2 * HOUR));

        LogListener rejected = LogListener.matches("The new certificate chain is not valid now").atLeast(1).build();

        nodeLog.registerListener(rejected);

        IgniteEx g = startGrid(0);

        assertTrue(rejected.check(30_000));

        MetricRegistryImpl reg = g.context().metric().registry(SslMetrics.registryName(CLIENT_CONNECTOR));

        assertEquals(issuer.calls.get(0).serial,
            reg.<ObjectMetric<String>>findMetric("CertificateSerialNumber").value());
    }

    /**
     * A successful reload by the command must cancel the planned retry and plan the next renewal by the new
     * certificate.
     */
    @Test
    public void testReloadByCommandReplans() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 9 * MIN)).then(failure()).then(fresh(2 * HOUR))
            .then(fresh(HOUR));

        issuer.minRetry = 2_000;
        issuer.maxRetry = 2_000;

        LogListener failed = LogListener.matches(s -> s.contains("Failed to reload TLS certificates") &&
            s.contains(ISSUER_DOWN)).times(1).build();

        LogListener renewed = automaticRenewals(NODE_TRANSPORTS).build();

        nodeLog.registerListener(failed);
        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(0);

        assertTrue(failed.check(30_000));

        long retry = nextRenewalTime(g);

        assertTrue("A retry must be planned after the pause",
            retry >= longMetric(g, "LastReloadFailureTime") + issuer.minRetry - CLOCK_SLACK);

        reload(g);

        long due = dueTime(g);

        assertTrue(waitForCondition(() -> nextRenewalTime(g) == due, 10_000));

        assertTrue("The next renewal must follow the new certificate", due > now + HOUR);
        assertEquals(0, registry(g).<IntMetric>findMetric("ReloadFailures").value());

        // Past the time the cancelled retry was due.
        assertFalse(waitForCondition(() -> issuer.calls.size() > 3, retry + 1_500 - System.currentTimeMillis()));

        assertFalse("The cancelled retry must not run", renewed.check());
    }

    /**
     * An attempt that falls due while the command is reloading must give way to the reload rather than replace the
     * certificate the reload has just put in use.
     */
    @Test
    public void testAttemptWaitingForReloadDropped() throws Exception {
        long now = System.currentTimeMillis();

        CountDownLatch inReload = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Step reloaded = fresh(2 * HOUR);

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 9 * MIN)).then(failure())
            .then(() -> {
                inReload.countDown();

                release.await();

                return reloaded.issue();
            })
            .then(fresh(HOUR));

        issuer.minRetry = 500;
        issuer.maxRetry = 500;

        LogListener failed = LogListener.matches(s -> s.contains("Failed to reload TLS certificates") &&
            s.contains(ISSUER_DOWN)).atLeast(1).build();

        nodeLog.registerListener(failed);

        IgniteEx g = startGrid(0);

        assertTrue(failed.check(30_000));

        long retry = nextRenewalTime(g);

        IgniteInternalFuture<?> reloadFut = GridTestUtils.runAsync(() -> reload(g));

        assertTrue(inReload.await(10, TimeUnit.SECONDS));

        // The retry falls due while the command holds the provider, and waits for it.
        assertTrue(waitForCondition(() -> System.currentTimeMillis() > retry + 300, 10_000));

        release.countDown();

        reloadFut.get(10_000);

        long due = dueTime(g);

        assertTrue(waitForCondition(() -> nextRenewalTime(g) == due, 10_000));

        assertFalse("The waiting attempt must not ask for another certificate",
            waitForCondition(() -> issuer.calls.size() > 3, 1_000));

        assertEquals(0, registry(g).<IntMetric>findMetric("ReloadFailures").value());
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
            provider(g).reload();
        }

        assertTrue(renewed.check(30_000));

        assertEquals("operator", issuer.calls.get(1).operator);
        assertNull("The renewal must not run as the operator", issuer.calls.get(2).operator);
    }

    /** Each pause between failed attempts must double from the shortest to the longest one. */
    @Test
    public void testRetryPausesDouble() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 9 * MIN)).then(failure());

        issuer.minRetry = 100;
        issuer.maxRetry = 400;

        startGrid(0);

        // The first attempt and five retries.
        assertTrue(waitForCondition(() -> issuer.calls.size() >= 7, 30_000));

        long[] pauses = {100, 200, 400, 400, 400};

        for (int i = 0; i < pauses.length; i++) {
            long pause = issuer.calls.get(i + 2).time - issuer.calls.get(i + 1).time;

            assertTrue("Pause " + (i + 1) + " must last at least " + pauses[i] + " ms: " + pause,
                pause >= pauses[i] - CLOCK_SLACK);
        }

        long last = issuer.calls.get(6).time - issuer.calls.get(5).time;

        // A pause that kept doubling would be 1600 ms by now.
        assertTrue("The longest pause must hold: " + last, last < 1_200);
    }

    /** The longest pause must be cut down to a quarter of the window, so that a short window holds several attempts. */
    @Test
    public void testRetryPauseCutToWindow() throws Exception {
        long now = System.currentTimeMillis();

        // A window of six seconds, a quarter of which is a second and a half.
        long notBefore = (now - 35_000) / 1000 * 1000;

        Issuer issuer = issuer(0).then(valid(notBefore, notBefore + 40_000)).then(failure());

        issuer.minRetry = 1_000;
        issuer.maxRetry = HOUR;

        startGrid(0);

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 5, 30_000));

        for (int i = 2; i < 5; i++) {
            long pause = issuer.calls.get(i).time - issuer.calls.get(i - 1).time;

            // Doubling up to the hour would make the third pause four seconds at least.
            assertTrue("Pause " + (i - 1) + " must stay within a quarter of the window: " + pause,
                pause >= 1_000 - CLOCK_SLACK && pause < 3_000);
        }
    }

    /** A certificate due for renewal as soon as it is issued must not make the node renew more often than allowed. */
    @Test
    public void testRenewalsNoMoreOftenThanMinRetry() throws Exception {
        Issuer issuer = issuer(0).then(() -> {
            long t = System.currentTimeMillis();

            // Expires later each time, so it is put in use, and is in its window from the start.
            return ca.issue("node", t - HOUR, t + 5 * MIN);
        });

        issuer.minRetry = 300;
        issuer.maxRetry = 300;

        startGrid(0);

        assertTrue(waitForCondition(() -> issuer.calls.size() >= 5, 30_000));

        for (int i = 2; i < 5; i++) {
            long pause = issuer.calls.get(i).time - issuer.calls.get(i - 1).time;

            assertTrue("Renewals must be " + issuer.minRetry + " ms apart at least: " + pause,
                pause >= issuer.minRetry - CLOCK_SLACK);
        }
    }

    /** An absolute window must apply when it is smaller than the share of the lifetime, and only then. */
    @Test
    public void testRenewBeforeTakesSmallerWindow() throws Exception {
        long now = System.currentTimeMillis();

        Issuer smaller = issuer(0).then(valid(now, now + HOUR));
        Issuer larger = issuer(1).then(valid(now, now + HOUR));

        smaller.renewBefore = 2 * MIN;
        larger.renewBefore = 30 * MIN;

        IgniteEx g0 = startGrid(0);
        IgniteEx g1 = startGrid(1);

        assertTrue(waitForCondition(() -> nextRenewalTime(g0) == certificateNotAfter(g0) - 2 * MIN, 10_000));
        assertTrue(waitForCondition(() -> nextRenewalTime(g1) == dueTime(g1), 10_000));
    }

    /** The jitter must bring the renewal forward by a random share of the window, different on each node. */
    @Test
    public void testJitterBringsRenewalForward() throws Exception {
        long now = System.currentTimeMillis();

        for (int i = 0; i < 2; i++) {
            Issuer issuer = issuer(i).then(valid(now, now + HOUR));

            issuer.jitter = 1;
        }

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
        List<Consumer<Issuer>> invalid = new ArrayList<>();
        List<String> reasons = new ArrayList<>();

        invalid.add(i -> i.fraction = 1);
        reasons.add("renewBeforeFraction must be greater than 0 and less than 1");

        invalid.add(i -> i.fraction = 0);
        reasons.add("renewBeforeFraction must be greater than 0 and less than 1");

        invalid.add(i -> i.renewBefore = -1);
        reasons.add("renewBefore must not be negative");

        invalid.add(i -> i.jitter = 1.5);
        reasons.add("renewalJitter must be from 0 to 1");

        invalid.add(i -> {
            i.fraction = 0.6;
            i.jitter = 1;
        });
        reasons.add("renewBeforeFraction * (1 + renewalJitter) must be less than 1");

        invalid.add(i -> i.minRetry = 0);
        reasons.add("renewalRetryMinInterval must be positive");

        invalid.add(i -> i.maxRetry = i.minRetry - 1);
        reasons.add("renewalRetryMaxInterval must not be less than renewalRetryMinInterval");

        for (int k = 0; k < invalid.size(); k++) {
            Issuer issuer = issuer(0).then(fresh(HOUR));

            invalid.get(k).accept(issuer);

            Throwable e = GridTestUtils.assertThrows(log, () -> startGrid(0), Exception.class, null);

            assertContains(log, X.getFullStackTrace(e), reasons.get(k));

            assertEquals("The issuer must not be asked: " + reasons.get(k), 0, issuer.calls.size());
        }
    }

    /** The client connector with a factory of its own must be renewed on its own. */
    @Test
    public void testClientConnectorRenewed() throws Exception {
        clientConnectorOnly = true;

        long now = System.currentTimeMillis();

        issuer(0).then(valid(now - HOUR, now + 5 * MIN)).then(fresh(HOUR));

        LogListener renewed = automaticRenewals("transports=" + CLIENT_CONNECTOR).times(1).build();

        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(0);

        assertTrue(renewed.check(30_000));

        MetricRegistryImpl reg = g.context().metric().registry(SslMetrics.registryName(CLIENT_CONNECTOR));

        long notAfter = reg.<LongMetric>findMetric("CertificateNotAfter").value();

        assertTrue(notAfter > now + 30 * MIN);
        assertTrue(reg.<LongMetric>findMetric("NextRenewalTime").value() > now + 30 * MIN);

        assertEquals("New connections must get the renewed certificate", notAfter,
            servedCertificate(g.context().clientListener().port()).getNotAfter().getTime());
    }

    /** The renewal thread must stop with the node. */
    @Test
    public void testRenewalThreadLifecycle() throws Exception {
        IgniteEx g = startGrid(0);

        assertTrue(waitForCondition(() -> nextRenewalTime(g) > 0, 10_000));

        assertTrue("A node renewing certificates must run the renewal thread", renewalThreadAlive(g.name()));

        stopGrid(0);

        assertTrue("The renewal thread must stop with the node",
            waitForCondition(() -> !renewalThreadAlive(g.name()), 10_000));
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
        long notBefore = longMetric(g, "CertificateNotBefore");
        long notAfter = certificateNotAfter(g);

        return notAfter - (long)((notAfter - notBefore) * DFLT_RENEW_BEFORE_FRACTION);
    }

    /** */
    private static long nextRenewalTime(IgniteEx g) {
        return longMetric(g, "NextRenewalTime");
    }

    /** */
    private static long certificateNotAfter(IgniteEx g) {
        return longMetric(g, "CertificateNotAfter");
    }

    /** */
    private static long longMetric(IgniteEx g, String name) {
        return registry(g).<LongMetric>findMetric(name).value();
    }

    /**
     * @param g Node.
     * @return Metrics of the context communication and discovery share.
     */
    private static MetricRegistryImpl registry(IgniteEx g) {
        return g.context().metric().registry(SslMetrics.registryName(COMMUNICATION));
    }

    /**
     * @param g Node.
     * @return Provider of the context communication and discovery share.
     */
    private static SslContextProvider provider(IgniteEx g) {
        for (SslContextReloadable comp : g.context().internalSubscriptionProcessor().getSslContextReloadables()) {
            if (comp instanceof SslContextProvider && comp.users().contains(COMMUNICATION))
                return (SslContextProvider)comp;
        }

        throw new AssertionError("No provider serves communication");
    }

    /** @param g Node to connect to. */
    private static int discoveryPort(IgniteEx g) {
        return ((TcpDiscoverySpi)g.configuration().getDiscoverySpi()).getLocalPort();
    }

    /**
     * @param port Port to connect to.
     * @return Certificate the node presents on a new TLS connection to that port.
     */
    private X509Certificate servedCertificate(int port) throws Exception {
        long now = System.currentTimeMillis();

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        // Discovery asks the client for a certificate of its own.
        kmf.init(ca.issue("probe", now - MIN, now + HOUR), TestCertificateAuthority.PWD);

        SSLContext probe = SSLContext.getInstance("TLS");

        probe.init(kmf.getKeyManagers(), new TrustManager[] {SslContextFactory.getDisabledTrustManager()}, null);

        try (SSLSocket sock = (SSLSocket)probe.getSocketFactory()
            .createSocket(InetAddress.getLoopbackAddress(), port)) {
            sock.startHandshake();

            return (X509Certificate)sock.getSession().getPeerCertificates()[0];
        }
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

    /**
     * @param g Node to run on.
     * @return Status report of the node.
     */
    private static String status(IgniteEx g) throws Exception {
        return g.compute(g.cluster()).execute(SslStatusTask.class,
            new VisorTaskArgument<>(g.localNode().id(), new NoArg(), false)).result();
    }

    /**
     * @param g Node to reload certificates on.
     * @return Reload report of the node.
     */
    private static String reload(IgniteEx g) throws Exception {
        return g.compute(g.cluster()).execute(SslReloadTask.class,
            new VisorTaskArgument<>(g.localNode().id(), new SslReloadCommandArg(), false)).result();
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

        /** Serial number of the certificate issued, {@code null} if the request failed. */
        private volatile String serial;
    }

    /** Factory that answers each request the way the test scripted it, the last step repeated for the rest. */
    private class Issuer implements RenewableSslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** Steps not taken yet. */
        private final Queue<Step> steps = new ConcurrentLinkedQueue<>();

        /** Requests so far. */
        private final List<Call> calls = new CopyOnWriteArrayList<>();

        /** Step taken last. */
        private volatile Step last;

        /** */
        private volatile double fraction = DFLT_RENEW_BEFORE_FRACTION;

        /** */
        private volatile long renewBefore;

        /** */
        private volatile double jitter;

        /** Short, so that retries do not hold the test up. */
        private volatile long minRetry = 100;

        /** */
        private volatile long maxRetry = 1_000;

        /**
         * @param step Step to take on the next request not scripted yet.
         * @return {@code this} for chaining.
         */
        private Issuer then(Step step) {
            steps.add(step);

            return this;
        }

        /** {@inheritDoc} */
        @Override public SSLContext create() {
            Call call = new Call();

            calls.add(call);

            Step step = steps.poll();

            if (step == null)
                step = last;
            else
                last = step;

            try {
                KeyStore keyStore = step.issue();

                call.serial = ((X509Certificate)keyStore.getCertificate("node")).getSerialNumber().toString(16);

                return TestCertificateAuthority.context(keyStore, ca.trustStore());
            }
            catch (IgniteException e) {
                throw e;
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public double getRenewBeforeFraction() {
            return fraction;
        }

        /** {@inheritDoc} */
        @Override public long getRenewBefore() {
            return renewBefore;
        }

        /** {@inheritDoc} */
        @Override public double getRenewalJitter() {
            return jitter;
        }

        /** {@inheritDoc} */
        @Override public long getRenewalRetryMinInterval() {
            return minRetry;
        }

        /** {@inheritDoc} */
        @Override public long getRenewalRetryMaxInterval() {
            return maxRetry;
        }
    }

    /** Echo of the node log that also hands over what is logged as an error. */
    private static class ErrorLog implements IgniteLogger {
        /** */
        private final IgniteLogger delegate;

        /** */
        private final Consumer<String> errors;

        /**
         * @param delegate Logger to echo to.
         * @param errors Receiver of the error messages.
         */
        private ErrorLog(IgniteLogger delegate, Consumer<String> errors) {
            this.delegate = delegate;
            this.errors = errors;
        }

        /** {@inheritDoc} */
        @Override public IgniteLogger getLogger(Object ctgr) {
            return this;
        }

        /** {@inheritDoc} */
        @Override public void trace(String msg) {
            delegate.trace(msg);
        }

        /** {@inheritDoc} */
        @Override public void debug(String msg) {
            delegate.debug(msg);
        }

        /** {@inheritDoc} */
        @Override public void info(String msg) {
            delegate.info(msg);
        }

        /** {@inheritDoc} */
        @Override public void warning(String msg, @Nullable Throwable e) {
            delegate.warning(msg, e);
        }

        /** {@inheritDoc} */
        @Override public void error(String msg, @Nullable Throwable e) {
            errors.accept(msg);

            delegate.error(msg, e);
        }

        /** {@inheritDoc} */
        @Override public boolean isTraceEnabled() {
            return delegate.isTraceEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return delegate.isDebugEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isInfoEnabled() {
            return delegate.isInfoEnabled();
        }

        /** {@inheritDoc} */
        @Override public boolean isQuiet() {
            return delegate.isQuiet();
        }

        /** {@inheritDoc} */
        @Override public String fileName() {
            return delegate.fileName();
        }
    }
}
