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

import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.AbstractSslContextFactory.ReloadOutcome;
import org.apache.ignite.ssl.SslBundle;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MemorizingAppender;
import org.apache.ignite.testframework.TestCertificateAuthority;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor;
import static org.apache.ignite.ssl.AbstractSslContextFactory.NODE_ACCESS;
import static org.apache.ignite.testframework.GridTestUtils.assertContains;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.testframework.TestCertificateAuthority.bundle;
import static org.apache.ignite.testframework.TestCertificateAuthority.chain;
import static org.apache.ignite.testframework.TestCertificateAuthority.context;
import static org.apache.ignite.testframework.TestCertificateAuthority.merged;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Renewal without nodes: its settings, {@link AbstractSslContextFactory#renew(SSLContext)}, the count of unsuccessful renewals and the
 * scheduler. Renewal is off by default. renew installs all new chains, even in place of an expired certificate. Its refusals
 * follow the order of the checks in the code: it applies the checks of reload, waits while the factory returns the chains in
 * use, refuses a cached context with new metadata and refuses chains that do not advance the earliest expiry. It ignores a
 * replaced context and changes nothing when interrupted. Reload keeps the metadata renewal needs, renewal needs chains
 * to start, and unsuccessful renewals count until the earliest expiry advances, by renewal or reload. The scheduler tries first
 * at the window of the earliest chain, then no sooner than a minute after a success, backs off on failures and logs each one,
 * replans on reload, drops an outdated attempt and runs without the operator context. The log names the installed certificate,
 * sets the level of a failure by the time left and reports a wait once per lateness, again on expiry. Tests enable renewal on
 * a factory and script its fresh bundles; pauses are the production ones, a minute to an hour. Renewal plans and attempts in
 * tasks of an executor that records them with their delays and runs one only when a test calls {@code runNext()}. The operator
 * context test runs on the kind of executor a node uses, and a concurrent initialization test coordinates two threads. The expiry test
 * advances the scheduler's clock explicitly. An appender on the logger of {@link SslRenewal} captures what renewal logs.
 */
public class SslRenewalTest extends GridCommonAbstractTest {
    /** */
    private static final long SECOND = 1_000L;

    /** */
    private static final long MINUTE = 60 * SECOND;

    /** */
    private static final long HOUR = 60 * MINUTE;

    /** Renewal window of a quarter of the lifetime: the last 30 minutes of a certificate that lives 2 hours. */
    private static final double QUARTER = 0.25;

    /** Failure of a factory whose issuer cannot be reached. */
    private static final String ISSUER_DOWN = "Issuer is unavailable";

    /** How renewal logs a failed attempt, before the expiry state. */
    private static final String FAILED = "Failed to reload TLS certificates, the ones in use stay";

    /** How renewal logs an attempt that got the chains in use, before the expiry state. */
    private static final String WAITING = "The SSL context factory has no newer TLS certificates yet, the ones in use stay";

    /** Operation context attribute standing for an operator. */
    private static final OperationContextAttribute<String> OPERATOR = OperationContextAttribute.newInstance();

    /** Authority of the test certificates. */
    private TestCertificateAuthority ca;

    /** Logger renewal writes to; {@link #appender} captures it. */
    private IgniteLogger renewalLog;

    /** What renewal logs, with levels. */
    private final MemorizingAppender appender = new MemorizingAppender();

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ca = new TestCertificateAuthority("renewal-ca");
        renewalLog = log.getLogger(SslRenewal.class);

        appender.start();
        appender.installSelfOn(SslRenewal.class);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        appender.removeSelfFrom(SslRenewal.class);
        appender.stop();
    }

    /** Renewal is off by default, and its window must be a share of the lifetime greater than 0 and less than 1. */
    @Test
    public void testInvalidSettingsRefused() {
        SslContextFactory factory = new SslContextFactory();

        assertFalse(factory.isRenewalEnabled());
        assertThrowsWithCause(() -> factory.setRenewBeforeFraction(0), IllegalArgumentException.class);
        assertThrowsWithCause(() -> factory.setRenewBeforeFraction(1), IllegalArgumentException.class);
    }

    /** renew installs all new chains together; their aliases need not match the ones in use. */
    @Test
    public void testRenewInstallsAllNewChains() throws Exception {
        long now = System.currentTimeMillis();
        KeyStore old1 = ca.keyStoreWithPrivateKey("old-1", now - HOUR, now + HOUR);
        KeyStore old2 = ca.keyStoreWithPrivateKey("old-2", now - HOUR, now + HOUR);
        KeyStore new1 = ca.keyStoreWithPrivateKey("new-1", now - HOUR, now + 2 * HOUR);
        KeyStore new2 = ca.keyStoreWithPrivateKey("new-2", now - HOUR, now + 2 * HOUR);
        SslBundle renewed = bundle(merged(new1, new2), ca.trustStore());
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(bundle(merged(old1, old2), ca.trustStore()), renewed).when(f).createSslBundle();

        f.renew(f.create());

        assertSame(renewed.context(), f.create());
        assertEquals(Map.of("new-1", chain(new1), "new-2", chain(new2)), NODE_ACCESS.current(f).certificateChains());
    }

    /** Removing the first-expiring chain advances the earliest expiry when the remaining chain expires later. */
    @Test
    public void testRenewalCanRemoveEarliestChain() throws Exception {
        long now = System.currentTimeMillis();
        KeyStore removed = ca.keyStoreWithPrivateKey("removed", now - HOUR, now + HOUR);
        KeyStore remaining = ca.keyStoreWithPrivateKey("remaining", now - HOUR, now + 2 * HOUR);
        SslBundle replacement = bundle(remaining, ca.trustStore());
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(bundle(merged(removed, remaining), ca.trustStore()), replacement).when(f).createSslBundle();

        assertSame(replacement, f.renew(f.create()));
        assertEquals(Map.of("remaining", chain(remaining)), NODE_ACCESS.current(f).certificateChains());
    }

    /** renew replaces an expired certificate in use with a valid one. */
    @Test
    public void testExpiredCertificateRenewed() throws Exception {
        long now = System.currentTimeMillis();
        KeyStore valid = ca.keyStoreWithPrivateKey("valid", now - HOUR, now + HOUR);
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("expired", now - 2 * HOUR, now - HOUR), ca.trustStore()),
            bundle(valid, ca.trustStore())).when(f).createSslBundle();

        f.renew(f.create());

        assertEquals(Map.of("valid", chain(valid)), NODE_ACCESS.current(f).certificateChains());
    }

    /** renew applies the checks of reload: here a candidate without certificate chains and one that is not valid yet are refused. */
    @Test
    public void testRenewalAppliesReloadChecks() throws Exception {
        long now = System.currentTimeMillis();
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("startup", now - HOUR, now + HOUR), ca.trustStore()),
            bundle(ca.trustStore(), ca.trustStore()),
            bundle(ca.keyStoreWithPrivateKey("not-yet-valid", now + 10 * MINUTE, now + 2 * HOUR), ca.trustStore()))
            .when(f).createSslBundle();

        f.create();

        assertRenewalRejected("No certificate chains", f,
            "The new key material has no X.509 certificate chains, so the node would present no certificate");
        assertRenewalRejected("Not valid yet", f, "The new certificate chain is not valid now [alias=not-yet-valid, ");
    }

    /**
     * The chains in use mean that the factory has nothing newer yet: renew returns {@code null} and counts an unsuccessful renewal,
     * but records no reload failure. Here a caching factory returns the bundle in use, and another factory returns the expired
     * chains in use, which the validity check would refuse.
     */
    @Test
    public void testUnchangedCertificatesAwaited() throws Exception {
        long now = System.currentTimeMillis();
        SslBundle cached = bundle(ca.keyStoreWithPrivateKey("cached"), ca.trustStore());
        KeyStore expired = ca.keyStoreWithPrivateKey("expired", now - 2 * HOUR, now - HOUR);
        TestSslContextFactory caching = renewingFactory(QUARTER);
        TestSslContextFactory expiredAgain = renewingFactory(QUARTER);

        doReturn(cached).when(caching).createSslBundle();
        doReturn(bundle(expired, ca.trustStore()), bundle(expired, ca.trustStore())).when(expiredAgain).createSslBundle();

        assertNull("Cached bundle", caching.renew(caching.create()));
        assertEquals("Cached bundle", 1, caching.renewalFailures());
        assertEquals("Cached bundle", new ReloadOutcome(0, 0, null, 0), caching.reloadOutcome());

        assertNull("Expired chains", expiredAgain.renew(expiredAgain.create()));
        assertEquals("Expired chains", 1, expiredAgain.renewalFailures());
        assertEquals("Expired chains", new ReloadOutcome(0, 0, null, 0), expiredAgain.reloadOutcome());
    }

    /** New metadata cannot make the context in use, which a caching factory returns again, serve a renewed certificate. */
    @Test
    public void testChangedMetadataCannotRenewCachedContext() throws Exception {
        long now = System.currentTimeMillis();
        SslBundle startup = bundle(ca.keyStoreWithPrivateKey("startup", now - HOUR, now + HOUR), ca.trustStore());
        KeyStore renewed = ca.keyStoreWithPrivateKey("renewed", now - HOUR, now + 2 * HOUR);
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(startup, new SslBundle(startup.context(), Map.of("renewed", chain(renewed)))).when(f).createSslBundle();

        f.create();

        assertRenewalRejected("Cached context", f, "The SSL context factory returns the context in use");
    }

    /**
     * renew refuses new chains unless the earliest expiry across them, authorities included, is later than the one in use: here
     * another key with the same expiry, one of two keys renewed, and a later leaf from an authority that expires no later.
     */
    @Test
    public void testRenewalMustAdvanceEarliestExpiry() throws Exception {
        long now = System.currentTimeMillis();
        String reason = "The new certificate chains expire no later than the ones in use";
        TestCertificateAuthority shortCa = new TestCertificateAuthority("short-ca", now - HOUR, now + HOUR);
        KeyStore a = ca.keyStoreWithPrivateKey("a", now - HOUR, now + HOUR);
        KeyStore b = ca.keyStoreWithPrivateKey("b", now - HOUR, now + HOUR);
        KeyStore renewedA = ca.keyStoreWithPrivateKey("a", now - HOUR, now + 2 * HOUR);
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(
            bundle(merged(a, b), ca.trustStore()),
            bundle(ca.keyStoreWithPrivateKey("other", now - HOUR, now + HOUR), ca.trustStore()),
            bundle(merged(renewedA, b), ca.trustStore()),
            bundle(shortCa.keyStoreWithPrivateKey("short-ca-leaf", now - HOUR, now + 2 * HOUR), ca.trustStore())).when(f).createSslBundle();

        f.create();

        assertRenewalRejected("Another key with the same expiry", f, reason);
        assertRenewalRejected("One of two keys renewed", f, reason);
        assertRenewalRejected("A later leaf from an authority that expires no later", f, reason);
    }

    /** renew planned for a context that a reload has replaced does not call the factory and changes nothing. */
    @Test
    public void testRenewForReplacedContextDoesNothing() throws Exception {
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()),
            bundle(ca.keyStoreWithPrivateKey("reloaded"), ca.trustStore())).when(f).createSslBundle();

        SSLContext planned = f.create();
        SslBundle reloaded = NODE_ACCESS.reload(f);

        assertNull(f.renew(planned));
        assertSame(reloaded, NODE_ACCESS.current(f));
        assertEquals(0, f.renewalFailures());
        verify(f, times(2)).createSslBundle();
    }

    /**
     * renew interrupted while the factory loads changes nothing and keeps the interrupt flag, whether the factory returns the chains
     * in use or new ones: the interrupt is checked before both the comparison with the chains in use and the installation.
     */
    @Test
    public void testInterruptedRenewalChangesNothing() throws Exception {
        long now = System.currentTimeMillis();
        KeyStore startup = ca.keyStoreWithPrivateKey("startup", now - HOUR, now + HOUR);
        SslBundle same = bundle(startup, ca.trustStore());
        SslBundle renewed = bundle(ca.keyStoreWithPrivateKey("renewed", now - HOUR, now + 2 * HOUR), ca.trustStore());
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(bundle(startup, ca.trustStore()))
            .doAnswer(inv -> {
                Thread.currentThread().interrupt();

                return same;
            })
            .doAnswer(inv -> {
                Thread.currentThread().interrupt();

                return renewed;
            }).when(f).createSslBundle();

        SslBundle inUse = NODE_ACCESS.current(f);

        try {
            GridTestUtils.assertThrows(log, () -> f.renew(inUse.context()), IgniteInterruptedCheckedException.class, null);
        }
        finally {
            assertTrue("Same chains: the interrupt flag stays set", Thread.interrupted());
        }

        Assert.assertSame("Same chains", inUse, NODE_ACCESS.current(f));
        assertEquals("Same chains", 0, f.renewalFailures());
        assertEquals("Same chains", new ReloadOutcome(0, 0, null, 0), f.reloadOutcome());

        try {
            GridTestUtils.assertThrows(log, () -> f.renew(inUse.context()), IgniteInterruptedCheckedException.class, null);
        }
        finally {
            assertTrue("New chains: the interrupt flag stays set", Thread.interrupted());
        }

        Assert.assertSame("New chains", inUse, NODE_ACCESS.current(f));
        assertEquals("New chains", 0, f.renewalFailures());
        assertEquals("New chains", new ReloadOutcome(0, 0, null, 0), f.reloadOutcome());
    }

    /** Material returned by the key source after the renewal executor stops must not be installed. */
    @Test
    public void testRenewalDiscardsMaterialLoadedDuringShutdown() throws Exception {
        long now = System.currentTimeMillis();
        SslBundle inUse = bundle(ca.keyStoreWithPrivateKey("in-use", now - HOUR, now + HOUR), ca.trustStore());
        SslBundle later = bundle(ca.keyStoreWithPrivateKey("later", now - HOUR, now + 2 * HOUR), ca.trustStore());
        TestSslContextFactory factory = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(inUse).doAnswer(inv -> {
            exec.shutdownNow();

            return later;
        }).when(factory).createSslBundle();

        factory.startRenewal(exec, renewalLog, () -> "");
        exec.runNext(); // Plan renewal.
        exec.runNext(); // Load the replacement.

        assertTrue(exec.isShutdown());
        assertSame(inUse, NODE_ACCESS.current(factory));
        assertEquals(new ReloadOutcome(0, 0, null, 0), factory.reloadOutcome());
    }

    /**
     * With renewal attached, reload refuses a bundle without certificate metadata, even after the factory has turned
     * renewal off: the renewal settings are fixed until the attached node stops. renew refuses such a bundle too.
     */
    @Test
    public void testReloadCannotLoseMetadataWithRenewal() throws Exception {
        String reason = "The new SSL context must provide at least one X.509 certificate chain to renew automatically";
        SslBundle startup = bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore());
        SslBundle unknown = new SslBundle(context(ca.keyStoreWithPrivateKey("unknown"), ca.trustStore()), null);
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(startup, unknown, unknown).when(f).createSslBundle();

        f.create();

        RecordingExecutor exec = new RecordingExecutor();

        f.startRenewal(exec, renewalLog, () -> "");
        f.setRenewalEnabled(false);

        GridTestUtils.assertThrows(log, () -> NODE_ACCESS.reload(f), IgniteException.class, reason);

        assertSame(startup, NODE_ACCESS.current(f));
        assertRenewalRejected("Renewal", f, reason);
    }

    /** Renewal does not start when certificate metadata is unknown or contains no chains. */
    @Test
    public void testRenewalRefusedToStartWithoutChains() throws Exception {
        SSLContext ctx = context(ca.keyStoreWithPrivateKey(), ca.trustStore());
        TestSslContextFactory unknown = renewingFactory(QUARTER);
        TestSslContextFactory noChains = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();
        String reason = "The SSL context factory must provide at least one X.509 certificate chain to renew automatically";

        doReturn(new SslBundle(ctx, null)).when(unknown).createSslBundle();
        doReturn(bundle(ca.trustStore(), ca.trustStore())).when(noChains).createSslBundle();

        unknown.create();
        noChains.create();

        GridTestUtils.assertThrows(log, () -> unknown.startRenewal(exec, renewalLog, () -> ""), IgniteException.class, reason);
        GridTestUtils.assertThrows(log, () -> noChains.startRenewal(exec, renewalLog, () -> ""), IgniteException.class, reason);
    }

    /** A subclass can disable renewal even when configured, without requiring chains or using the executor. */
    @Test
    public void testRenewalCanBeDisabledBySubclass() throws Exception {
        SslBundle noChains = bundle(ca.trustStore(), ca.trustStore());
        AbstractSslContextFactory factory = new SslContextFactory() {
            @Override public boolean isRenewalEnabled() {
                return false;
            }

            @Override public SslBundle createSslBundle() {
                return noChains;
            }
        };
        ScheduledExecutorService exec = mock(ScheduledExecutorService.class);

        factory.setRenewalEnabled(true);
        factory.create();
        factory.startRenewal(exec, renewalLog, () -> "");

        assertSame(noChains, NODE_ACCESS.current(factory));
        assertEquals(0, factory.nextRenewalTime());
        verifyNoInteractions(exec);
    }

    /** Renewal uses the fraction supplied by a subclass when the node attaches its executor. */
    @Test
    public void testRenewalWindowUsesOverriddenFraction() throws Exception {
        long now = System.currentTimeMillis() / SECOND * SECOND;
        SslBundle startup = bundle(ca.keyStoreWithPrivateKey("startup", now - HOUR, now + 3 * HOUR), ca.trustStore());
        AbstractSslContextFactory factory = new SslContextFactory() {
            @Override public double getRenewBeforeFraction() {
                return QUARTER;
            }

            @Override public SslBundle createSslBundle() {
                return startup;
            }
        };
        RecordingExecutor exec = new RecordingExecutor();

        factory.setRenewalEnabled(true);
        factory.startRenewal(exec, renewalLog, () -> "");
        exec.runNext();

        assertEquals(now + 2 * HOUR, factory.nextRenewalTime());
    }

    /** A subclass must supply a valid window before its renewal schedule can start. */
    @Test
    public void testInvalidOverriddenFractionPreventsRenewal() throws Exception {
        TestSslContextFactory factory = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(bundle(ca.keyStoreWithPrivateKey(), ca.trustStore())).when(factory).createSslBundle();
        doReturn(Double.NaN).when(factory).getRenewBeforeFraction();

        GridTestUtils.assertThrows(log, () -> factory.startRenewal(exec, renewalLog, () -> ""),
            IgniteException.class, "renewBeforeFraction must be greater than 0 and less than 1");
        assertTrue(exec.pending().isEmpty());

        doReturn(QUARTER).when(factory).getRenewBeforeFraction();

        factory.startRenewal(exec, renewalLog, () -> "");
        assertEquals(1, exec.pending().size());
    }

    /** A factory accepts one renewal executor, and attaching it again does not duplicate its schedule. */
    @Test
    public void testRenewalAttachesOnce() throws Exception {
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();
        RecordingExecutor other = new RecordingExecutor();

        doReturn(bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore())).when(f).createSslBundle();

        f.create();
        f.startRenewal(exec, renewalLog, () -> "communication");

        Task first = exec.last();

        f.startRenewal(exec, renewalLog, () -> "communication");

        assertEquals(List.of(first), exec.pending());
        GridTestUtils.assertThrows(log, () -> f.startRenewal(other, renewalLog, () -> "discovery"),
            IgniteException.class, "another running node");
        assertTrue(other.pending().isEmpty());
        assertEquals(List.of(first), exec.pending());
    }

    /** Settings apply at attachment and stay fixed until the node stops, including when a factory was already initialized. */
    @Test
    public void testRenewalSettingsAppliedAtAttachmentAndRestart() throws Exception {
        long now = System.currentTimeMillis() / SECOND * SECOND;
        SslBundle startup = bundle(ca.keyStoreWithPrivateKey("startup", now - HOUR, now + 3 * HOUR), ca.trustStore());
        SslBundle replacement = bundle(ca.keyStoreWithPrivateKey("replacement", now - HOUR, now + 5 * HOUR), ca.trustStore());
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor first = new RecordingExecutor();
        RecordingExecutor restarted = new RecordingExecutor();

        doReturn(startup, replacement).when(f).createSslBundle();

        f.setRenewalEnabled(false);
        f.create();
        f.setRenewalEnabled(true);
        f.startRenewal(first, renewalLog, () -> "communication");
        first.runNext();

        assertEquals(now + 2 * HOUR, f.nextRenewalTime());

        first.shutdownNow();

        assertEquals(0, f.nextRenewalTime());

        f.setRenewBeforeFraction(0.5);
        f.startRenewal(restarted, renewalLog, () -> "communication");
        restarted.runNext();

        assertEquals(now + HOUR, f.nextRenewalTime());

        f.setRenewBeforeFraction(0.75);
        f.startRenewal(restarted, renewalLog, () -> "communication");
        NODE_ACCESS.reload(f);
        restarted.runNext();

        assertEquals("The active node keeps its configured fraction", now + 2 * HOUR, f.nextRenewalTime());
        assertSame(replacement, NODE_ACCESS.current(f));
    }

    /** A restarted node can disable renewal and accept unknown metadata without retaining the stopped node's schedule. */
    @Test
    public void testRenewalCanBeDisabledOnRestart() throws Exception {
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor first = new RecordingExecutor();
        RecordingExecutor restarted = new RecordingExecutor();
        SslBundle unknown = new SslBundle(context(ca.keyStoreWithPrivateKey("unknown"), ca.trustStore()), null);

        doReturn(bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()), unknown).when(f).createSslBundle();

        f.startRenewal(first, renewalLog, () -> "communication");
        first.runNext();
        first.shutdownNow();
        f.setRenewalEnabled(false);
        f.startRenewal(restarted, renewalLog, () -> "communication");
        NODE_ACCESS.reload(f);

        assertSame(unknown, NODE_ACCESS.current(f));
        assertEquals(0, f.nextRenewalTime());
        assertTrue(restarted.pending().isEmpty());

        f.setRenewalEnabled(true);
        f.startRenewal(restarted, renewalLog, () -> "communication");

        assertEquals("Enabling renewal waits for another restart", 0, f.nextRenewalTime());
        assertTrue(restarted.pending().isEmpty());
    }

    /** A subclass waits for renewal before it reloads, resets failures and schedules renewal for its replacement. */
    @Test
    public void testSubclassInitSerializesWithRenewal() throws Exception {
        long now = System.currentTimeMillis() / SECOND * SECOND;
        SslBundle startup = bundle(ca.keyStoreWithPrivateKey("startup", now - HOUR, now + HOUR), ca.trustStore());
        SslBundle replacement = bundle(ca.keyStoreWithPrivateKey("replacement", now, now + 2 * HOUR), ca.trustStore());
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();
        CountDownLatch renewing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        doReturn(startup).doAnswer(inv -> {
            renewing.countDown();
            assertTrue(release.await(10, TimeUnit.SECONDS));

            return startup;
        }).doReturn(replacement).when(f).createSslBundle();

        f.startRenewal(exec, renewalLog, () -> "communication");
        exec.runNext();

        FutureTask<SslBundle> renewal = new FutureTask<>(() -> f.renew(startup.context()));
        FutureTask<SslBundle> init = new FutureTask<>(f::reloadFromSubclass);
        Thread renewalThread = new Thread(renewal, "test-factory-renewal");
        Thread initThread = new Thread(init, "test-factory-init");

        renewalThread.start();

        try {
            assertTrue(renewing.await(10, TimeUnit.SECONDS));

            initThread.start();

            assertTrue(waitForCondition(() -> initThread.getState() == Thread.State.BLOCKED || init.isDone(), 10_000));
            assertFalse("Initialization must wait while renewal loads its candidate", init.isDone());
        }
        finally {
            release.countDown();
            renewalThread.join(10_000);
            initThread.join(10_000);
        }

        assertNull(renewal.get(10, TimeUnit.SECONDS));
        assertSame(replacement, init.get(10, TimeUnit.SECONDS));
        assertSame(replacement, NODE_ACCESS.current(f));
        assertEquals(0, f.renewalFailures());
        assertEquals(0, f.reloadOutcome().failures());

        exec.runNext();

        assertEquals(now + 2 * HOUR - 30 * MINUTE, f.nextRenewalTime());
    }

    /** Both the chains in use and a factory failure count as unsuccessful renewals, and a renewal clears the count. */
    @Test
    public void testUnsuccessfulRenewalsCounted() throws Exception {
        long now = System.currentTimeMillis();
        KeyStore inUse = ca.keyStoreWithPrivateKey("in-use", now - HOUR, now + HOUR);
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(bundle(inUse, ca.trustStore()), bundle(inUse, ca.trustStore()))
            .doThrow(new IgniteException(ISSUER_DOWN))
            .doReturn(bundle(ca.keyStoreWithPrivateKey("renewed", now - HOUR, now + 2 * HOUR), ca.trustStore())).when(f).createSslBundle();

        assertNull(f.renew(f.create()));
        assertEquals("Chains in use", 1, f.renewalFailures());

        GridTestUtils.assertThrows(log, () -> f.renew(f.create()), IgniteException.class, ISSUER_DOWN);

        assertEquals("Factory failure", 2, f.renewalFailures());

        assertNotNull(f.renew(f.create()));
        assertEquals("Renewed", 0, f.renewalFailures());
    }

    /**
     * Reload is not a renewal attempt: a failed reload does not count, and a successful one clears the count only if the earliest
     * expiry advances. Another certificate with the same expiry and a later leaf whose authority expires at the same time do not.
     */
    @Test
    public void testReloadClearsRenewalFailuresOnlyWithLaterExpiry() throws Exception {
        long now = System.currentTimeMillis();
        TestCertificateAuthority shortCa = new TestCertificateAuthority("short-ca", now - HOUR, now + HOUR);
        KeyStore inUse = ca.keyStoreWithPrivateKey("in-use", now - HOUR, now + HOUR);
        TestSslContextFactory f = renewingFactory(QUARTER);

        doReturn(bundle(inUse, ca.trustStore()), bundle(inUse, ca.trustStore()))
            .doThrow(new IgniteException(ISSUER_DOWN))
            .doReturn(
                bundle(ca.keyStoreWithPrivateKey("same-expiry", now - HOUR, now + HOUR), ca.trustStore()),
                bundle(shortCa.keyStoreWithPrivateKey("later-leaf", now - HOUR, now + 2 * HOUR), ca.trustStore()),
                bundle(ca.keyStoreWithPrivateKey("later-chain", now - HOUR, now + 2 * HOUR), ca.trustStore())).when(f).createSslBundle();

        f.renew(f.create());

        GridTestUtils.assertThrows(log, () -> NODE_ACCESS.reload(f), IgniteException.class, ISSUER_DOWN);

        assertEquals("Failed reload", 1, f.renewalFailures());

        NODE_ACCESS.reload(f);

        assertEquals("Same expiry", 1, f.renewalFailures());

        NODE_ACCESS.reload(f);

        assertEquals("Later leaf, authority expires at the same time", 1, f.renewalFailures());

        NODE_ACCESS.reload(f);

        assertEquals("Later chain expiry", 0, f.renewalFailures());
    }

    /**
     * The first attempt comes when the window of the chain that expires first opens: the window is a share of that chain's lifetime,
     * which runs from its leaf validity start to its earliest expiry. Equal expiries go to the chain with the earliest leaf validity
     * start, and an authority that expires before the leaf decides. Aliases put the expected chain second.
     */
    @Test
    public void testFirstAttemptAtWindowOfEarliestChain() throws Exception {
        // X.509 keeps whole seconds.
        long now = System.currentTimeMillis() / SECOND * SECOND;
        TestCertificateAuthority shortCa = new TestCertificateAuthority("short-ca", now - 2 * HOUR, now + HOUR);

        KeyStore earliestExpiry = merged(
            ca.keyStoreWithPrivateKey("a-later-expiry", now - HOUR, now + 3 * HOUR),
            ca.keyStoreWithPrivateKey("b-earlier-expiry", now - HOUR, now + HOUR));
        KeyStore equalExpiry = merged(
            ca.keyStoreWithPrivateKey("a-later-start", now - HOUR, now + 2 * HOUR),
            ca.keyStoreWithPrivateKey("b-earlier-start", now - 2 * HOUR, now + 2 * HOUR));

        assertEquals("Earliest expiry", now + HOUR - 30 * MINUTE, firstAttempt(QUARTER, earliestExpiry));
        assertEquals("Equal expiry, earlier start", now + 2 * HOUR - HOUR, firstAttempt(QUARTER, equalExpiry));
        assertEquals("Authority expires first", now + HOUR - 30 * MINUTE,
            firstAttempt(QUARTER, shortCa.keyStoreWithPrivateKey("leaf", now - HOUR, now + 3 * HOUR)));
    }

    /**
     * After a successful renewal or reload, the next attempt comes no sooner than a minute later, even for a certificate in its
     * window. Here every certificate is in its window: renewal plans, attempts at once and renews, then plans again, and a reload
     * then replaces the renewed certificate.
     */
    @Test
    public void testNextAttemptNotSoonerThanMinPause() throws Exception {
        long now = System.currentTimeMillis();
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("startup", now - 100 * MINUTE, now + 20 * MINUTE), ca.trustStore()),
            bundle(ca.keyStoreWithPrivateKey("renewed", now - 95 * MINUTE, now + 25 * MINUTE), ca.trustStore()),
            bundle(ca.keyStoreWithPrivateKey("reloaded", now - 90 * MINUTE, now + 30 * MINUTE), ca.trustStore())).when(f).createSslBundle();

        f.create();

        f.startRenewal(exec, renewalLog, () -> "");
        exec.runNext();
        exec.runNext();
        exec.runNext();

        assertEquals("Renewal", f.reloadOutcome().successTime() + MINUTE, f.nextRenewalTime());

        NODE_ACCESS.reload(f);
        exec.runNext();

        assertEquals("Reload", f.reloadOutcome().successTime() + MINUTE, f.nextRenewalTime());
    }

    /**
     * Failed attempts are retried after 1, 2, 4 and so on up to 60 minutes, then every 60 minutes, each plus up to half of it; the
     * executor gets the delay to the retry time, and every failure is logged, unlike a wait. The clock does not stop between the
     * attempts, so each retry time and delay is checked against the clock before and after the attempt.
     */
    @Test
    public void testFailedAttemptsBackOff() throws Exception {
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()))
            .doThrow(new IgniteException(ISSUER_DOWN)).when(f).createSslBundle();

        f.create();

        f.startRenewal(exec, renewalLog, () -> "");
        exec.runNext();

        for (long pause : new long[] {1, 2, 4, 8, 16, 32, 60, 60}) {
            long before = System.currentTimeMillis();

            exec.runNext();

            long after = System.currentTimeMillis();
            long next = f.nextRenewalTime();
            long delay = exec.last().getDelay(TimeUnit.MILLISECONDS);

            assertTrue("Pause of " + pause + " min: " + (next - before) + " ms",
                next >= before + pause * MINUTE && next <= after + pause * MINUTE * 3 / 2);
            assertTrue("Delay of the " + pause + "-minute retry: " + delay + " ms", delay >= next - after && delay <= next - before);
        }

        assertEquals(Collections.nCopies(8, "WARN " + FAILED), problems());
    }

    /**
     * Reload replans renewal: it cancels the planned retry, plans the next attempt at the window of the new certificate, resets
     * the retry pause and lets renewal warn again that the factory has nothing newer.
     */
    @Test
    public void testReloadReplans() throws Exception {
        // X.509 keeps whole seconds.
        long now = System.currentTimeMillis() / SECOND * SECOND;
        SslBundle startup = bundle(ca.keyStoreWithPrivateKey("startup", now - 100 * MINUTE, now + 20 * MINUTE), ca.trustStore());
        SslBundle reloaded = bundle(ca.keyStoreWithPrivateKey("reloaded", now, now + 2 * HOUR), ca.trustStore());
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(startup, startup, reloaded, reloaded).when(f).createSslBundle();

        f.create();

        f.startRenewal(exec, renewalLog, () -> "");
        exec.runNext();
        exec.runNext();

        ScheduledFuture<?> retry = exec.last();

        NODE_ACCESS.reload(f);
        exec.runNext();

        assertTrue(retry.isCancelled());
        assertEquals(now + 2 * HOUR - 30 * MINUTE, f.nextRenewalTime());

        long before = System.currentTimeMillis();

        exec.runNext();

        long after = System.currentTimeMillis();
        long next = f.nextRenewalTime();

        assertTrue("Retry after the reload: " + (next - before) + " ms", next >= before + MINUTE && next <= after + MINUTE * 3 / 2);
        assertEquals(List.of("WARN " + WAITING, "WARN " + WAITING), problems());
    }

    /**
     * An attempt planned for a context that a reload has replaced, here run before the plan of the reload, does not call the
     * factory, log or schedule a retry: only the plan of the reload stays pending.
     */
    @Test
    public void testStaleAttemptNotRetried() throws Exception {
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("startup"), ca.trustStore()),
            bundle(ca.keyStoreWithPrivateKey("reloaded"), ca.trustStore())).when(f).createSslBundle();

        f.create();

        f.startRenewal(exec, renewalLog, () -> "");
        exec.runNext();

        Task stale = exec.last();

        NODE_ACCESS.reload(f);

        Task plan = exec.last();

        stale.run();

        verify(f, times(2)).createSslBundle();
        assertEquals(List.of(), problems());
        assertEquals(List.of(plan), exec.pending());
    }

    /**
     * An attempt runs without the operation context of whoever started renewal, here an operator, even on the kind of executor a
     * node uses, which passes the context of a submitter to its tasks. The factory sees the operator during its first initialization.
     */
    @Test
    public void testRenewalRunsWithoutOperatorContext() throws Exception {
        long now = System.currentTimeMillis();
        SslBundle inWindow = bundle(ca.keyStoreWithPrivateKey("in-window", now - 100 * MINUTE, now + 20 * MINUTE), ca.trustStore());
        List<String> operators = new CopyOnWriteArrayList<>();
        TestSslContextFactory f = renewingFactory(QUARTER);
        ScheduledExecutorService exec = newSingleThreadScheduledExecutor("ssl-renewal", getTestIgniteInstanceName());

        doAnswer(inv -> {
            operators.add(OperationContext.get(OPERATOR));

            return inWindow;
        }).when(f).createSslBundle();

        try {
            try (Scope ignored = OperationContext.set(OPERATOR, "operator")) {
                f.create();
                f.startRenewal(exec, renewalLog, () -> "");
            }

            assertTrue(waitForCondition(() -> operators.size() == 2, 10_000));
            assertEquals(Arrays.asList("operator", null), operators);
        }
        finally {
            exec.shutdownNow();

            assertTrue(exec.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    /** The log names the certificate an attempt installed, even if a reload replaces it before the attempt logs it. */
    @Test
    public void testRenewalLogsAppliedCertificate() throws Exception {
        long now = System.currentTimeMillis();
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(
            bundle(ca.keyStoreWithPrivateKey("startup", now - HOUR, now + HOUR), ca.trustStore()),
            bundle(ca.keyStoreWithPrivateKey("renewed", now - HOUR, now + 2 * HOUR), ca.trustStore()),
            bundle(ca.keyStoreWithPrivateKey("reloaded", now - HOUR, now + 3 * HOUR), ca.trustStore())).when(f).createSslBundle();

        f.create();

        doAnswer(inv -> {
            SslBundle renewed = (SslBundle)inv.callRealMethod();

            NODE_ACCESS.reload(f);

            return renewed;
        }).when(f).renew(any(SSLContext.class));

        f.startRenewal(exec, renewalLog, () -> "");
        exec.runNext();
        exec.runNext();

        assertTrue(NODE_ACCESS.current(f).certificateChains().containsKey("reloaded"));

        List<String> reloadedLogs = appender.events().stream().map(e -> e.getMessage().getFormattedMessage())
            .filter(msg -> msg.startsWith("TLS certificates reloaded [")).toList();

        assertEquals(reloadedLogs.toString(), 1, reloadedLogs.size());
        assertContains(log, reloadedLogs.get(0), ", alias=renewed, subject=CN=renewed, ");
    }

    /**
     * A failure is a warning while more than half of the window is left and an error once less is left, the retry a minute later
     * comes after expiry or the certificates have expired. In each case the first failure is logged with its reason and stack trace.
     */
    @Test
    public void testFailureLogLevelFollowsTimeLeft() throws Exception {
        // X.509 keeps whole seconds.
        long now = System.currentTimeMillis() / SECOND * SECOND;

        assertFailureLogged("More than half of the window left",
            ca.keyStoreWithPrivateKey("early", now - 100 * MINUTE, now + 20 * MINUTE), QUARTER, "WARN " + FAILED);
        assertFailureLogged("Less than half of the window left",
            ca.keyStoreWithPrivateKey("late", now - 110 * MINUTE, now + 10 * MINUTE), QUARTER, "ERROR " + FAILED + " and expire soon");
        // 45 seconds left are more than half of the 72-second window, but less than the minute before the retry.
        assertFailureLogged("Retry after expiry",
            ca.keyStoreWithPrivateKey("short-window", now + 45 * SECOND - HOUR, now + 45 * SECOND), 0.02,
            "ERROR " + FAILED + " and expire soon");
        assertFailureLogged("Expired",
            ca.keyStoreWithPrivateKey("expired", now - 2 * HOUR, now - HOUR), QUARTER, "ERROR " + FAILED + ", though they have expired");
    }

    /**
     * While the factory returns the chains in use, renewal logs that once, as a warning without a stack trace, and again only when
     * the lateness grows: the fifth retry pause of 16 minutes takes the next attempt past the expiry 16 minutes away, which is an
     * error. The attempts run one after another, so the time left does not change.
     */
    @Test
    public void testWaitLoggedOncePerLateness() throws Exception {
        long now = System.currentTimeMillis();
        SslBundle inUse = bundle(ca.keyStoreWithPrivateKey("in-use", now - 104 * MINUTE, now + 16 * MINUTE), ca.trustStore());
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(inUse).when(f).createSslBundle();

        f.create();

        f.startRenewal(exec, renewalLog, () -> "");

        for (int i = 0; i < 8; i++)
            exec.runNext();

        assertEquals(List.of("WARN " + WAITING, "ERROR " + WAITING + " and expire soon"), problems());
        assertTrue(appender.events().stream().allMatch(e -> e.getThrown() == null));
    }

    /**
     * A wait logged as expiring soon is logged again once the certificates have expired, and not after that.
     */
    @Test
    public void testWaitLoggedAgainOnExpiry() throws Exception {
        KeyStore inUse = ca.keyStoreWithPrivateKey();
        long notAfter = chain(inUse).get(0).getNotAfter().getTime();

        // Hold the scheduler's clock before expiry until the test advances it.
        AtomicLong now = new AtomicLong(notAfter - SECOND);
        TestSslContextFactory f = renewingFactory(QUARTER);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(bundle(inUse, ca.trustStore())).when(f).createSslBundle();

        f.create();

        new SslRenewal(f, QUARTER, exec, renewalLog, () -> "", now::get).replan();
        exec.runNext();
        exec.runNext();

        now.set(notAfter);

        exec.runNext();
        exec.runNext();

        assertEquals(List.of("ERROR " + WAITING + " and expire soon", "ERROR " + WAITING + ", though they have expired"), problems());
    }

    /**
     * @param fraction Share of the lifetime left when renewal starts.
     * @return Factory with renewal enabled; tests script its fresh bundles.
     */
    private static TestSslContextFactory renewingFactory(double fraction) {
        TestSslContextFactory f = spy(new TestSslContextFactory());

        f.setRenewalEnabled(true);
        f.setRenewBeforeFraction(fraction);

        return f;
    }

    /**
     * @param fraction Share of the lifetime left when renewal starts.
     * @param keys Key store the factory starts on.
     * @return Time of the first attempt that renewal plans for it.
     */
    private long firstAttempt(double fraction, KeyStore keys) throws Exception {
        TestSslContextFactory f = renewingFactory(fraction);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(bundle(keys, ca.trustStore())).when(f).createSslBundle();

        f.create();

        f.startRenewal(exec, renewalLog, () -> "");
        exec.runNext();

        return f.nextRenewalTime();
    }

    /**
     * Checks that renew fails with the reason, keeps the bundle in use, counts one more unsuccessful renewal and records the reason
     * as the last reload failure, which status shows.
     *
     * @param desc Case, named in assertion messages.
     * @param f Factory.
     * @param reason Expected part of the failure message.
     */
    private void assertRenewalRejected(String desc, AbstractSslContextFactory f, String reason) {
        SslBundle inUse = NODE_ACCESS.current(f);
        int failures = f.renewalFailures();

        GridTestUtils.assertThrows(log, () -> f.renew(inUse.context()), IgniteException.class, reason, desc + ": not refused");

        Assert.assertSame(desc, inUse, NODE_ACCESS.current(f));
        assertEquals(desc, failures + 1, f.renewalFailures());
        assertTrue(desc, f.reloadOutcome().failureReason().contains(reason));
    }

    /**
     * Starts renewal on the key store, lets the factory fail the first attempt and checks how renewal logs the failure.
     *
     * @param desc Case, named in assertion messages.
     * @param inUse Key store the factory starts on.
     * @param fraction Share of the lifetime left when renewal starts.
     * @param expected Level and message of the failure, without the details in brackets.
     */
    private void assertFailureLogged(String desc, KeyStore inUse, double fraction, String expected) throws Exception {
        TestSslContextFactory f = renewingFactory(fraction);
        RecordingExecutor exec = new RecordingExecutor();

        doReturn(bundle(inUse, ca.trustStore())).doThrow(new IgniteException(ISSUER_DOWN)).when(f).createSslBundle();

        f.create();

        int before = problems().size();

        f.startRenewal(exec, renewalLog, () -> "");
        exec.runNext();
        exec.runNext();

        List<String> problems = problems();
        LogEvent failure = appender.events().get(appender.events().size() - 1);

        assertEquals(desc, List.of(expected), problems.subList(before, problems.size()));
        assertNotNull(desc + ": stack trace", failure.getThrown());
        assertContains(log, failure.getMessage().getFormattedMessage(), ", initiator=automatic renewal, reason=" + ISSUER_DOWN + ", ");
    }

    /** @return Warnings and errors renewal logged, in log order, each as its level and its message without the details in brackets. */
    private List<String> problems() {
        return appender.events().stream().filter(e -> e.getLevel().isMoreSpecificThan(Level.WARN))
            .map(e -> e.getLevel() + " " + e.getMessage().getFormattedMessage().replaceFirst(" \\[.*", ""))
            .toList();
    }

    /** Lets tests supply bundles and initialize the factory from a subclass. */
    private static class TestSslContextFactory extends SslContextFactory {
        /** Reloads material through the protected factory method. */
        private SslBundle reloadFromSubclass() {
            return init();
        }

        /** {@inheritDoc} */
        @Override public SslBundle createSslBundle() throws SSLException {
            return super.createSslBundle();
        }
    }

    /** Records scheduled tasks instead of running them after their delays; a test runs them one at a time in its own thread. */
    private static class RecordingExecutor extends ScheduledThreadPoolExecutor {
        /** Scheduled tasks, in the order of scheduling. */
        private final List<Task> tasks = new ArrayList<>();

        /** */
        private RecordingExecutor() {
            super(1);
        }

        /** {@inheritDoc} */
        @Override public ScheduledFuture<?> schedule(Runnable cmd, long delay, TimeUnit unit) {
            Task task = new Task(cmd, unit.toMillis(delay));

            tasks.add(task);

            return task;
        }

        /** Runs the pending task with the shortest delay, the first scheduled of equal ones. */
        private void runNext() {
            pending().stream().min(Comparator.comparingLong(t -> t.delay)).orElseThrow().run();
        }

        /** @return Tasks neither run nor cancelled, in the order of scheduling. */
        private List<Task> pending() {
            return tasks.stream().filter(t -> !t.isDone()).toList();
        }

        /** @return Last scheduled task. */
        private Task last() {
            return tasks.get(tasks.size() - 1);
        }
    }

    /** Task that runs only when a test runs it. */
    private static class Task extends FutureTask<Void> implements ScheduledFuture<Void> {
        /** Delay requested at scheduling, in milliseconds. */
        private final long delay;

        /**
         * @param cmd Command.
         * @param delay Delay requested at scheduling, in milliseconds.
         */
        private Task(Runnable cmd, long delay) {
            super(cmd, null);

            this.delay = delay;
        }

        /** {@inheritDoc} */
        @Override public long getDelay(TimeUnit unit) {
            return unit.convert(delay, TimeUnit.MILLISECONDS);
        }

        /** {@inheritDoc} */
        @Override public int compareTo(Delayed other) {
            return Long.compare(delay, other.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
