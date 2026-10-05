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

import java.security.KeyStore;
import java.time.Instant;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;
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
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.visor.VisorTaskArgument;
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
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests that a node renews the certificates a {@link RenewableSslContextFactory} issues before they expire. */
public class SslRenewalTest extends GridCommonAbstractTest {
    /** */
    private static final long MIN = 60_000L;

    /** */
    private static final long HOUR = 60 * MIN;

    /** Failure of an issuer that cannot be reached. */
    private static final String ISSUER_DOWN = "Issuer is unavailable";

    /** Transports a node with SSL configured for the whole node serves on one context, as the log names them. */
    private static final String NODE_TRANSPORTS = "transports=communication, discovery";

    /** Authority behind every certificate the nodes get. */
    private TestCertificateAuthority ca;

    /** Issuer of each node, by node name; a node without one gets certificates for an hour. */
    private final Map<String, Issuer> issuers = new ConcurrentHashMap<>();

    /** Whether the issuer serves the client connector only, while the rest of the node runs without SSL. */
    private boolean clientConnectorOnly;

    /** Log of the nodes under test. */
    private ListeningTestLogger nodeLog;

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

        nodeLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        issuers.clear();

        clientConnectorOnly = false;
    }

    /**
     * Each node must plan the renewal by the window of its certificate, renew it then and not before, and plan the
     * next renewal by the new one; a node joining afterwards must find the cluster working.
     */
    @Test
    public void testRenewsBeforeExpiry() throws Exception {
        long now = System.currentTimeMillis();

        // A hundred seconds of lifetime with twenty left: the default window of 15% opens in about five seconds. The
        // dates are cut to seconds, the way the certificate carries them.
        long notBefore = (now - 80_000) / 1000 * 1000;
        long notAfter = (now + 20_000) / 1000 * 1000;

        long due = notAfter - (long)((notAfter - notBefore) * RenewableSslContextFactory.DFLT_RENEW_BEFORE_FRACTION);

        issuer(0).then(valid(notBefore, notAfter)).then(fresh(HOUR));
        issuer(1).then(valid(notBefore, notAfter)).then(fresh(HOUR));

        LogListener planned = LogListener.matches("TLS certificates will be renewed automatically [" +
            NODE_TRANSPORTS + ", at=" + Instant.ofEpochMilli(due)).times(2).build();

        LogListener renewed = LogListener.matches(s -> s.contains("TLS certificates reloaded [" + NODE_TRANSPORTS) &&
            s.contains("initiator=automatic renewal")).times(2).build();

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

            long next = dueTime(g, RenewableSslContextFactory.DFLT_RENEW_BEFORE_FRACTION);

            assertTrue(waitForCondition(() -> nextRenewalTime(g) == next, 10_000));
        }

        startGrid(2);

        assertEquals(3, g0.cluster().nodes().size());
    }

    /** A failed renewal must be retried until it succeeds, each failure logged and counted. */
    @Test
    public void testFailedRenewalRetried() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 5 * MIN)).then(failure()).then(failure())
            .then(fresh(HOUR));

        LogListener failed = LogListener.matches("Failed to reload TLS certificates, the ones in use stay [" +
            NODE_TRANSPORTS + ", initiator=automatic renewal, reason=" + ISSUER_DOWN).times(2).build();

        LogListener renewed = LogListener.matches(s -> s.contains("TLS certificates reloaded [" + NODE_TRANSPORTS) &&
            s.contains("initiator=automatic renewal")).times(1).build();

        nodeLog.registerListener(failed);
        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(0);

        assertTrue(renewed.check(30_000));
        assertTrue(failed.check());

        assertEquals(4, issuer.calls.get());
        assertEquals(0, registry(g).<IntMetric>findMetric("ReloadFailures").value());
        assertTrue(certificateNotAfter(g) > now + 30 * MIN);
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

        assertTrue(registry(g).<IntMetric>findMetric("ReloadFailures").value() >= 2);
        assertContains(log, registry(g).<ObjectMetric<String>>findMetric("LastReloadFailure").value(), ISSUER_DOWN);

        Throwable e = GridTestUtils.assertThrows(log, () -> status(g), IgniteException.class, null);

        assertTrue(X.hasCause(e, CommandWarningException.class));

        String report = X.getFullStackTrace(e);

        assertContains(log, report, "last reload failed");
        assertContains(log, report, ISSUER_DOWN);
        assertContains(log, report, "next automatic renewal at");
    }

    /** A certificate that expires no later than the one in use must not be put in use. */
    @Test
    public void testRenewalNotMovingExpiryRejected() throws Exception {
        long now = System.currentTimeMillis();

        issuer(0).then(valid(now - HOUR, now + 5 * MIN));

        LogListener rejected = LogListener.matches("The new certificate expires no later than the one in use")
            .atLeast(1).build();

        nodeLog.registerListener(rejected);

        IgniteEx g = startGrid(0);

        String serial = registry(g).<ObjectMetric<String>>findMetric("CertificateSerialNumber").value();

        assertTrue(rejected.check(30_000));

        assertEquals(serial, registry(g).<ObjectMetric<String>>findMetric("CertificateSerialNumber").value());
    }

    /** A renewed certificate the other nodes would refuse must not be put in use. */
    @Test
    public void testRenewalRefusedBetweenNodesRejected() throws Exception {
        long now = System.currentTimeMillis();

        TestCertificateAuthority other = new TestCertificateAuthority("otherca");

        issuer(0).then(valid(now - HOUR, now + 5 * MIN)).then(() -> other.issue("node", now, now + HOUR));

        LogListener refused = LogListener.matches(s -> s.contains("Failed to reload TLS certificates") &&
            s.contains("A handshake between nodes on the new certificate was refused")).atLeast(1).build();

        nodeLog.registerListener(refused);

        IgniteEx g = startGrid(0);

        long notAfter = certificateNotAfter(g);

        assertTrue(refused.check(30_000));

        assertEquals(notAfter, certificateNotAfter(g));
    }

    /** A successful reload by the command must stop the retries and plan the next renewal by the new certificate. */
    @Test
    public void testReloadByCommandReplans() throws Exception {
        long now = System.currentTimeMillis();

        Issuer issuer = issuer(0).then(valid(now - HOUR, now + 5 * MIN)).then(failure()).then(fresh(2 * HOUR));

        // Long enough for the command to come before the next attempt.
        issuer.minRetry = MIN;
        issuer.maxRetry = MIN;

        LogListener failed = LogListener.matches(s -> s.contains("Failed to reload TLS certificates") &&
            s.contains(ISSUER_DOWN)).times(1).build();

        nodeLog.registerListener(failed);

        IgniteEx g = startGrid(0);

        assertTrue(failed.check(30_000));

        long retry = nextRenewalTime(g);

        assertTrue("A retry must be planned", retry > now);

        reload(g);

        long due = dueTime(g, RenewableSslContextFactory.DFLT_RENEW_BEFORE_FRACTION);

        assertTrue(waitForCondition(() -> nextRenewalTime(g) == due, 10_000));

        assertTrue("The next renewal must follow the new certificate", due > now + HOUR);
        assertEquals(0, registry(g).<IntMetric>findMetric("ReloadFailures").value());
        assertEquals(3, issuer.calls.get());
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

            long due = dueTime(g, RenewableSslContextFactory.DFLT_RENEW_BEFORE_FRACTION);

            long window = certificateNotAfter(g) - due;

            next[i] = nextRenewalTime(g);

            assertTrue("The renewal must come within the window [next=" + next[i] + ", due=" + due + ']',
                next[i] <= due && next[i] >= due - window);
        }

        assertTrue("Nodes with certificates issued at once must not renew at once", next[0] != next[1]);
    }

    /** The settings of the factory must be checked when the node starts. */
    @Test
    public void testInvalidSettingsFailNodeStart() throws Exception {
        Issuer issuer = issuer(0).then(fresh(HOUR));

        issuer.fraction = 1;

        Throwable e = GridTestUtils.assertThrows(log, () -> startGrid(0), Exception.class, null);

        assertContains(log, X.getFullStackTrace(e), "renewBeforeFraction must be greater than 0 and less than 1");
    }

    /** The client connector with a factory of its own must be renewed on its own. */
    @Test
    public void testClientConnectorRenewed() throws Exception {
        clientConnectorOnly = true;

        long now = System.currentTimeMillis();

        issuer(0).then(valid(now - HOUR, now + 5 * MIN)).then(fresh(HOUR));

        LogListener renewed = LogListener.matches(s -> s.contains("TLS certificates reloaded [transports=" +
            CLIENT_CONNECTOR) && s.contains("initiator=automatic renewal")).times(1).build();

        nodeLog.registerListener(renewed);

        IgniteEx g = startGrid(0);

        assertTrue(renewed.check(30_000));

        MetricRegistryImpl reg = g.context().metric().registry(SslMetrics.registryName(CLIENT_CONNECTOR));

        assertTrue(reg.<LongMetric>findMetric("CertificateNotAfter").value() > now + 30 * MIN);
        assertTrue(reg.<LongMetric>findMetric("NextRenewalTime").value() > now + 30 * MIN);
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
     * @param g Node.
     * @param fraction Share of the lifetime the window takes.
     * @return Time the certificate the node serves is due for renewal, without jitter.
     */
    private static long dueTime(IgniteEx g, double fraction) {
        long notBefore = registry(g).<LongMetric>findMetric("CertificateNotBefore").value();
        long notAfter = registry(g).<LongMetric>findMetric("CertificateNotAfter").value();

        return notAfter - (long)((notAfter - notBefore) * fraction);
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

    /** Factory that answers each request the way the test scripted it, the last step repeated for the rest. */
    private class Issuer implements RenewableSslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** Steps not taken yet. */
        private final Queue<Step> steps = new ConcurrentLinkedQueue<>();

        /** Requests so far. */
        private final AtomicInteger calls = new AtomicInteger();

        /** Step taken last. */
        private volatile Step last;

        /** */
        private volatile double fraction = DFLT_RENEW_BEFORE_FRACTION;

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
            calls.incrementAndGet();

            Step step = steps.poll();

            if (step == null)
                step = last;
            else
                last = step;

            try {
                return TestCertificateAuthority.context(step.issue(), ca.trustStore());
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
}
