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

package org.apache.ignite.internal.processors.ssl;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import javax.cache.configuration.Factory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.TrustManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;
import org.apache.ignite.ssl.TestCertificateAuthority;
import org.apache.ignite.testframework.GridTestThread;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A manual reload is {@link SslFactoryRegistry#reload}, which the TLS reload command calls.
 * An attempt is an automatic hot reload attempt that a scheduled check makes.
 */
public class SslFactoryRegistryTest {
    /** Upper bound for waiting for an event that must happen. */
    private static final long TIMEOUT_MS = 30_000;

    /** Created in the setup, after the logger stub. */
    private SslFactoryRegistry registry;

    /** */
    private final SslContextFactory factory = mock(SslContextFactory.class);

    /** */
    private final GridKernalContext ctx = mock(GridKernalContext.class);

    /** */
    private final ScheduledExecutorService executor = mock(ScheduledExecutorService.class);

    /** */
    private final MetricRegistryImpl metrics = new MetricRegistryImpl("ssl", null, null, null, new NullLogger());

    /** */
    private MockedStatic<IgniteScheduledThreadPoolExecutor> executorFactory;

    /** Checks that the registry has scheduled and that have not run or been cancelled, with their delays, in scheduling order. */
    private final Map<Runnable, Long> scheduled = new LinkedHashMap<>();

    /** */
    private Path dir;

    /** Key store file of the factory under test. */
    private Path keys;

    /** */
    private Path trust;

    /** */
    private TestCertificateAuthority authority;

    /**
     * Each test gets a registry whose hot reload executor records the scheduled checks instead of running them.
     * Tests run the checks themselves.
     * The setup also writes the trust store of a test CA to a temporary directory and stubs the node metrics and logger.
     */
    @Before
    public void setUp() throws Exception {
        dir = Files.createTempDirectory("ssl-factory-registry");
        keys = dir.resolve("keys.jks");
        trust = dir.resolve("trust.jks");
        authority = new TestCertificateAuthority("issuer", at(-DAYS.toHours(365)), at(DAYS.toHours(365)));

        save(authority.trustStore(), trust);

        GridMetricManager metricMgr = mock(GridMetricManager.class);

        when(ctx.metric()).thenReturn(metricMgr);
        when(metricMgr.registry("ssl")).thenReturn(metrics);
        when(ctx.log(SslFactoryRegistry.class)).thenReturn(new NullLogger());

        registry = new SslFactoryRegistry(ctx);

        when(executor.schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS))).thenAnswer(call -> {
            Runnable check = call.getArgument(0);

            scheduled.put(check, call.getArgument(1));

            return mock(ScheduledFuture.class, cancel -> "cancel".equals(cancel.getMethod().getName())
                ? scheduled.remove(check) != null
                : RETURNS_DEFAULTS.answer(cancel));
        });

        executorFactory = mockStatic(IgniteScheduledThreadPoolExecutor.class);
        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(executor);
    }

    /** After each test, the registry stops, and the static executor mock and the temporary directory are released. */
    @After
    public void tearDown() throws Exception {
        try {
            registry.onKernalStop(true);
        }
        finally {
            executorFactory.close();
            U.delete(dir);
        }
    }

    /**
     * Given: communication and discovery register the same factory, with a different factory registered in between.
     * When: the registry lists its factories.
     * Then: each factory appears once under its transports, in the order of first registration.
     */
    @Test
    public void testFactoriesAreGroupedInRegistrationOrder() {
        SslContextFactory clientConnector = new SslContextFactory();

        registry.register("communication", factory);
        registry.register("client connector", clientConnector);
        registry.register("discovery", factory);

        assertEquals(List.of(Map.entry(factory, "communication, discovery"),
            Map.entry(clientConnector, "client connector")), registry.factories());
    }

    /**
     * Given: a factory with hot reload disabled and valid certificates.
     * When: the registry starts.
     * Then: no hot reload thread starts, and the failure metric reads zero.
     */
    @Test
    public void testDisabledFactoryDoesNotStartHotReload() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.setHotReloadEnabled(false);
        registerTransport("communication", communication);
        registry.onKernalStart(true);

        executorFactory.verifyNoInteractions();
        assertEquals(0, failures());
    }

    /**
     * Given: a transport registers a factory that only creates contexts and cannot reload them.
     * When: the registry starts.
     * Then: no hot reload thread starts, and the failure metric reads zero.
     */
    @Test
    public void testFactoryWithoutReloadDoesNotStartHotReload() {
        Factory<SSLContext> createOnly = () -> null;

        registry.register("communication", createOnly);
        registry.onKernalStart(true);

        executorFactory.verifyNoInteractions();
        assertEquals(0, failures());
    }

    /**
     * Given: HTTP REST registers a factory that is not an Ignite SSL context factory, with certificates due for hot reload.
     * When: the periodic check runs and loads certificates that are not due, and then a manual reload runs.
     * Then: the automatic attempt succeeds, the registry reloads the factory each time and counts one automatic
     *     and one manual attempt.
     */
    @Test
    public void testReloadableFactoryIsHotReloadedAndReloadedManually() {
        ReloadableSslFactory rest = httpRestFactory();

        when(rest.hotReloadTime()).thenReturn(due(), at(DAYS.toHours(30)));

        registry.register("HTTP REST", rest);
        registry.onKernalStart(true);
        runChecks();

        assertEquals(0, failures());

        registry.reload(rest);

        verify(rest, times(2)).init();
        assertEquals(1, metric("AutomaticHotReloadAttempts"));
        assertEquals(1, metric("ManualReloadAttempts"));
    }

    /**
     * Given: communication and discovery share a factory, and the client connector has an equal factory for the same file.
     * When: the key store is replaced and the periodic check runs.
     * Then: each factory instance loads the new certificates once.
     */
    @Test
    public void testEachFactoryInstanceReloadsOnce() throws Exception {
        SslContextFactory communication = factory(keys, new KeyStorePathFactory());
        SslContextFactory clientConnector = factory(keys, new KeyStorePathFactory());

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registerTransport("discovery", communication);
        registerTransport("client connector", clientConnector);
        registry.onKernalStart(true);

        long prevTime = communication.hotReloadTime();

        writeFreshCertificates(keys);
        runChecks();

        assertEquals(2, metric("AutomaticHotReloadAttempts"));
        assertTrue(communication.hotReloadTime() > prevTime);
        assertEquals(communication.hotReloadTime(), clientConnector.hotReloadTime());
    }

    /**
     * Given: communication and the client connector use equal but separate factories for the same key store file.
     * When: the key store is replaced, and the client connector's factory is reloaded manually.
     * Then: the client connector's factory loads the new certificates, and the communication factory keeps its own.
     */
    @Test
    public void testManualReloadLoadsOnlyItsFactoryInstance() throws Exception {
        SslContextFactory communication = factory(keys, new KeyStorePathFactory());
        SslContextFactory clientConnector = factory(keys, new KeyStorePathFactory());

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registerTransport("client connector", clientConnector);
        registry.onKernalStart(true);

        long prevTime = communication.hotReloadTime();

        writeFreshCertificates(keys);
        registry.reload(clientConnector);

        assertTrue(clientConnector.hotReloadTime() > prevTime);
        assertEquals(prevTime, communication.hotReloadTime());
    }

    /**
     * Given: the key store holds only a CA certificate.
     * When: the registry starts and the periodic check runs, then the operator puts a key store with a private key in place
     *     and reloads it.
     * Then: the registry logs that hot reload will not run, and the check makes no attempt.
     *     After the reload, the log shows when hot reload is due.
     */
    @Test
    public void testStoreWithoutPrivateKeyIsReportedUntilKeysAreReloaded() throws Exception {
        List<String> logged = captureLog();
        SslContextFactory communication = factory(keys);

        save(authority.trustStore(), keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        runChecks();

        assertEquals(1, logged.size());
        assertTrue(logged.get(0), logged.get(0).startsWith("TLS hot reload will not run"));
        assertEquals(0, metric("AutomaticHotReloadAttempts"));

        writeFreshCertificates(keys);
        registry.reload(communication);

        assertEquals("TLS hot reload due [transports=communication, hotReloadTime=" + new Date(communication.hotReloadTime()) + ']',
            logged.get(logged.size() - 1));
    }

    /**
     * Given: the loaded certificates are due for hot reload, and the key store file is deleted after start.
     * When: the scheduled check runs three times.
     * Then: the first check runs at start, each failed check schedules a retry in an hour, and each run counts one more
     *     failed attempt.
     */
    @Test
    public void testFailedCheckIsRetriedAfterCheckPeriod() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        Files.delete(keys);

        assertEquals(List.of(0L), List.copyOf(scheduled.values()));

        for (int failure = 1; failure <= 3; failure++) {
            runChecks();

            assertEquals(failure, failures());
            assertEquals(List.of(HOURS.toMillis(1)), List.copyOf(scheduled.values()));
        }
    }

    /**
     * Given: two factories, and reading the hot reload time of the first one fails.
     * When: the registry starts and its checks run.
     * Then: the start completes, the check of the first factory fails and is retried in an hour, and the second factory
     *     gets its next check at its hot reload time.
     */
    @Test
    public void testFailedReadOfHotReloadTimeIsRetriedWithoutFailingStart() throws Exception {
        ReloadableSslFactory rest = httpRestFactory();

        when(rest.hotReloadTime()).thenThrow(new IgniteException("Failed to read the certificates of the HTTPS connector"));
        writeCertificatesDueIn30Minutes(keys);
        registry.register("HTTP REST", rest);
        registerTransport("communication", factory(keys));
        registry.onKernalStart(true);
        runChecks();

        List<Long> delays = List.copyOf(scheduled.values());

        assertEquals(1, failures());
        assertEquals(HOURS.toMillis(1), (long)delays.get(0));
        assertTrue(delays.toString(), isAbout30Minutes(delays.get(1)));
    }

    /**
     * Given: communication has certificates that are due for hot reload in 30 minutes, and the client connector has
     *     certificates that are due in 23 days.
     * When: the checks at start run.
     * Then: the next check of communication comes at its hot reload time, and the next check of the client connector
     *     comes in an hour, the longest interval between checks.
     */
    @Test
    public void testNextCheckComesAtHotReloadTimeButWithinCheckPeriod() throws Exception {
        Path clientKeys = dir.resolve("client-keys.jks");

        writeCertificatesDueIn30Minutes(keys);
        writeFreshCertificates(clientKeys);
        registerTransport("communication", factory(keys));
        registerTransport("client connector", factory(clientKeys));
        registry.onKernalStart(true);
        runChecks();

        List<Long> delays = List.copyOf(scheduled.values());

        assertTrue(delays.toString(), isAbout30Minutes(delays.get(0)));
        assertEquals(HOURS.toMillis(1), (long)delays.get(1));
    }

    /**
     * Given: the check reads a hot reload time that comes in 30 minutes, and any later read finds that the time has come,
     *     as when the clock passes the hot reload time while the check runs.
     * When: the check at start runs.
     * Then: the next check comes in 30 minutes, at the hot reload time that the check read, not after the full hour.
     */
    @Test
    public void testCheckRightBeforeHotReloadTimeDoesNotDeferHotReload() {
        ReloadableSslFactory rest = httpRestFactory();

        when(rest.hotReloadTime()).thenReturn(System.currentTimeMillis() + MINUTES.toMillis(30), due());

        registry.register("HTTP REST", rest);
        registry.onKernalStart(true);
        runChecks();

        assertTrue(scheduled.toString(), isAbout30Minutes(nextCheckDelay()));
    }

    /**
     * Given: the loaded certificates are due for hot reload, and the key store gets certificates that are due in 30 minutes.
     * When: the scheduled check loads them.
     * Then: the next check of the factory comes in 30 minutes.
     */
    @Test
    public void testHotReloadSchedulesCheckAtNewHotReloadTime() throws Exception {
        writeDueCertificates(keys);
        registerTransport("communication", factory(keys));
        registry.onKernalStart(true);
        writeCertificatesDueIn30Minutes(keys);
        runChecks();

        assertEquals(0, failures());
        assertTrue(scheduled.toString(), isAbout30Minutes(nextCheckDelay()));
    }

    /**
     * Given: the registry has started, and the key store gets certificates that are due for hot reload in 30 minutes.
     * When: a manual reload loads them.
     * Then: the check that the registry scheduled at start is replaced by one in 30 minutes.
     */
    @Test
    public void testManualReloadSchedulesCheckAtHotReloadTime() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        writeCertificatesDueIn30Minutes(keys);
        registry.reload(communication);

        assertTrue(scheduled.toString(), isAbout30Minutes(nextCheckDelay()));
    }

    /**
     * Given: the HTTP REST factory fails to read its certificates once, and then reports a hot reload time that comes
     *     in 30 minutes.
     * When: a manual reload loads the factory, and the next check runs.
     * Then: the reload succeeds and schedules a check in an hour, and that check schedules the next one in 30 minutes.
     */
    @Test
    public void testManualReloadRetriesFailedReadOfHotReloadTime() {
        ReloadableSslFactory rest = httpRestFactory();

        when(rest.hotReloadTime()).thenThrow(new IgniteException("Failed to read the certificates of the HTTPS connector"))
            .thenReturn(System.currentTimeMillis() + MINUTES.toMillis(30));

        registry.register("HTTP REST", rest);
        registry.onKernalStart(true);
        registry.reload(rest);

        assertEquals(HOURS.toMillis(1), nextCheckDelay());

        runChecks();

        assertTrue(scheduled.toString(), isAbout30Minutes(nextCheckDelay()));
        verify(rest).init();
    }

    /**
     * Given: the registry has started, and the key store still holds certificates that are due for hot reload.
     * When: a manual reload loads them.
     * Then: the failure count is zero, and the next check comes in an hour rather than right away.
     */
    @Test
    public void testManualReloadOfDueCertificatesDefersNextCheck() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        registry.reload(communication);

        assertEquals(0, failures());
        assertEquals(HOURS.toMillis(1), nextCheckDelay());
    }

    /**
     * Given: the registry starts during an operation with a request ID.
     * When: its worker reloads a factory with due certificates.
     * Then: the factory sees no request ID from the operation that started the registry.
     */
    @Test
    public void testHotReloadDoesNotInheritStartupContext() throws Exception {
        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any())).thenCallRealMethod();

        OperationContextAttribute<String> reqId = OperationContextAttribute.newInstance();
        CompletableFuture<String> reloadReqId = new CompletableFuture<>();

        enableHotReload(factory, due());
        doAnswer(call -> {
            reloadReqId.complete(OperationContext.get(reqId));

            return null;
        }).when(factory).init();
        registry.register("communication", factory);

        try (Scope ignored = OperationContext.set(reqId, "startup")) {
            registry.onKernalStart(true);
        }

        assertNull(reloadReqId.get(TIMEOUT_MS, MILLISECONDS));
    }

    /**
     * Given: two factories with due certificates, and loading the first one throws an error,
     *     as a missing class of a key store provider does.
     * When: the periodic check runs.
     * Then: the check completes normally, so the executor keeps the schedule. It counts and logs the error
     *     and still reloads the second factory.
     */
    @Test
    public void testErrorFromOneFactoryDoesNotStopCheck() throws Exception {
        List<String> logged = captureLog();
        SslContextFactory clientConnector = factory(keys);

        enableHotReload(factory, due());
        doThrow(new NoClassDefFoundError("Key store provider")).when(factory).init();
        writeDueCertificates(keys);
        registry.register("communication", factory);
        registerTransport("client connector", clientConnector);
        registry.onKernalStart(true);
        writeFreshCertificates(keys);
        runChecks();

        assertEquals(1, failures());
        assertEquals(2, metric("AutomaticHotReloadAttempts"));
        assertTrue(logged.toString(), logged.contains("TLS hot reload succeeded [transports=client connector]"));
        assertTrue(logged.toString(), logged.toString().contains("TLS hot reload failed [transports=communication,"));
    }

    /**
     * Given: the loaded certificates are due for hot reload, and the key store file is deleted after start.
     * When: one automatic attempt fails, and the next one loads fresh certificates.
     * Then: the log shows when the loaded certificates are due for hot reload, and the start and result of each attempt.
     */
    @Test
    public void testLogShowsDueTimeStartAndResultOfAttempts() throws Exception {
        List<String> logged = captureLog();
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);

        long due = communication.hotReloadTime();

        Files.delete(keys);
        runChecks();
        writeFreshCertificates(keys);
        runChecks();

        List<String> evts = new ArrayList<>();

        for (String line : logged) {
            assertTrue(line, line.contains("[transports=communication"));

            evts.add(line.substring("TLS hot reload ".length(), line.indexOf(" [")));
        }

        assertEquals(List.of("due", "started", "failed", "started", "succeeded", "due"), evts);
        assertTrue(logged.get(0), logged.get(0).endsWith("hotReloadTime=" + new Date(due) + ']'));
        assertTrue(logged.get(2), logged.get(2).contains("nextAttempt="));
        assertTrue(logged.get(5), logged.get(5).endsWith("hotReloadTime=" + new Date(communication.hotReloadTime()) + ']'));
    }

    /**
     * Given: the loaded certificates are due for hot reload, and the key store file is deleted after start.
     * When: one check fails, the source then gets certificates due for hot reload in 23 days, the next check loads them,
     *     and the check runs again.
     * Then: both attempts are counted, the success resets the failure count, and the last check makes no attempt.
     */
    @Test
    public void testSuccessResetsFailuresAndLaterChecksWait() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        Files.delete(keys);
        runChecks();

        assertEquals(1, failures());
        assertEquals(1, metric("AutomaticHotReloadAttempts"));

        writeFreshCertificates(keys);
        runChecks();

        assertEquals(0, failures());
        assertEquals(2, metric("AutomaticHotReloadAttempts"));

        runChecks();

        assertEquals(2, metric("AutomaticHotReloadAttempts"));
    }

    /**
     * Given: an automatic attempt has failed because the loaded certificates are still due.
     * When: a load outside this registry installs fresh certificates, as another node sharing the factory does,
     *     and the next check runs.
     * Then: the check resets the failure count without loading the factory.
     */
    @Test
    public void testCheckResetsFailuresWhenFactoryIsNoLongerDue() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        runChecks();

        assertEquals(1, failures());

        writeFreshCertificates(keys);
        communication.init();
        runChecks();

        assertEquals(0, failures());
        assertEquals(1, metric("AutomaticHotReloadAttempts"));
    }

    /**
     * Given: a factory is registered, and the registry has not started yet.
     * When: the key store is replaced, and a manual reload runs.
     * Then: the factory loads the new certificates.
     */
    @Test
    public void testManualReloadBeforeStartLoadsFactory() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        registerTransport("communication", communication);

        long prevTime = communication.hotReloadTime();

        writeFreshCertificates(keys);
        registry.reload(communication);

        assertTrue(communication.hotReloadTime() > prevTime);
    }

    /**
     * Given: a factory with hot reload disabled.
     * When: a manual reload succeeds, and a second one fails because the key store file is deleted.
     * Then: both count as manual attempts, and none counts as automatic.
     */
    @Test
    public void testManualReloadWithoutHotReloadCountsAppliedAndFailedLoads() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.setHotReloadEnabled(false);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        registry.reload(communication);

        assertEquals(1, metric("ManualReloadAttempts"));

        Files.delete(keys);

        assertThrows(IgniteException.class, () -> registry.reload(communication));
        assertEquals(2, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticHotReloadAttempts"));
    }

    /**
     * Given: communication has hot reload disabled and due certificates, and the client connector has hot reload enabled.
     * When: communication is reloaded manually, and the scheduled checks run.
     * Then: the checks make no automatic attempt, as communication gets no scheduled check.
     */
    @Test
    public void testManualReloadDoesNotScheduleCheckOfDisabledFactory() throws Exception {
        Path clientKeys = dir.resolve("client-keys.jks");
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        writeFreshCertificates(clientKeys);
        communication.setHotReloadEnabled(false);
        registerTransport("communication", communication);
        registerTransport("client connector", factory(clientKeys));
        registry.onKernalStart(true);
        registry.reload(communication);
        runChecks();

        assertEquals(0, metric("AutomaticHotReloadAttempts"));
    }

    /**
     * Given: a custom factory has loaded a certificate that is due for hot reload.
     * When: the source supplies a new key store and password and the scheduled check runs.
     * Then: the factory installs the replacement context and schedules its next reload from the new certificate.
     */
    @Test
    public void testScheduledReloadUsesCustomFactoryKeyStoreAndPassword() throws Exception {
        SourceSslContextFactory custom = new SourceSslContextFactory();

        custom.setKeys(authority.keyStore("node", at(-1), at(10)), PASSWORD);

        SSLContext initCtx = custom.create();

        registry.register("communication", custom);
        registry.onKernalStart(true);

        char[] replacementPwd = "replacement".toCharArray();
        KeyStore replacement = authority.keyStore("node", at(-1), at(DAYS.toHours(30)), replacementPwd);

        custom.setKeys(replacement, replacementPwd);
        runChecks();

        assertNotSame(initCtx, custom.create());

        long expiry = ((X509Certificate)replacement.getCertificate("node")).getNotAfter().getTime();

        assertEquals(expiry - custom.getHotReloadBeforeExpiryMs(), custom.hotReloadTime());
    }

    /**
     * Given: a custom factory has loaded a key store with due certificates.
     * When: a manual reload creates the context without a key store, and the scheduled check runs.
     * Then: the factory has no hot reload date, and the check makes no attempt.
     */
    @Test
    public void testLoadWithoutKeyStoreClearsHotReloadDate() throws Exception {
        SourceSslContextFactory custom = new SourceSslContextFactory();

        custom.setKeys(authority.keyStore("node", at(-1), at(10)), PASSWORD);
        custom.create();

        assertTrue(custom.hotReloadTime() > 0);

        registry.register("communication", custom);
        registry.onKernalStart(true);

        custom.setKeys(null, null);
        registry.reload(custom);
        runChecks();

        assertEquals(0, custom.hotReloadTime());
        assertEquals(0, metric("AutomaticHotReloadAttempts"));
    }

    /**
     * Given: a registered factory that is not an {@link SslContextFactory}.
     * When: a manual reload runs.
     * Then: the factory is loaded.
     */
    @Test
    public void testManualReloadSupportsAbstractFactory() throws Exception {
        AbstractSslContextFactory factory = mock(AbstractSslContextFactory.class);

        registry.register("client connector", factory);
        registry.onKernalStart(true);
        registry.reload(factory);

        verify(factory).init();
    }

    /**
     * Given: an automatic attempt has failed on a missing key store file.
     * When: a manual reload runs while the file is still missing.
     * Then: the reload fails, and the failure count stays.
     */
    @Test
    public void testFailedManualReloadKeepsFailures() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        Files.delete(keys);
        runChecks();

        IgniteException err = assertThrows(IgniteException.class, () -> registry.reload(communication));

        assertTrue(X.hasCause(err, "key store file was not found", SSLException.class));
        assertEquals(1, failures());
    }

    /**
     * Given: communication only reloads manually, and the client connector is due for hot reload.
     * When: the scheduled check runs while communication is waiting for its certificate source.
     * Then: the client connector loads its replacement before the manual reload finishes.
     */
    @Test
    public void testManualOnlyFactoryDoesNotDelayHotReload() throws Exception {
        SslContextFactory clientConnector = factory(keys);
        CountDownLatch manualStarted = new CountDownLatch(1);
        CountDownLatch finishManual = new CountDownLatch(1);

        doAnswer(call -> {
            manualStarted.countDown();
            finishManual.await();

            return null;
        }).when(factory).init();

        writeDueCertificates(keys);
        registry.register("communication", factory);
        registerTransport("client connector", clientConnector);
        registry.onKernalStart(true);

        long prevTime = clientConnector.hotReloadTime();

        writeFreshCertificates(keys);

        IgniteInternalFuture<?> manual = runAsync(() -> registry.reload(factory));

        try {
            assertTrue(manualStarted.await(TIMEOUT_MS, MILLISECONDS));
            runAsync(nextCheck()).get(TIMEOUT_MS);

            assertTrue(clientConnector.hotReloadTime() > prevTime);
        }
        finally {
            finishManual.countDown();
            manual.get(TIMEOUT_MS);
        }
    }

    /**
     * Given: an automatic attempt has started, failed to load a missing key store file, and pauses at its stop check
     *     before it counts the failure.
     * When: the operator puts fresh certificates in place and reloads them during the pause.
     * Then: the manual reload waits until the attempt finishes, so the failure count ends at zero.
     */
    @Test
    public void testManualReloadWaitsForRunningAttempt() throws Exception {
        SslContextFactory communication = factory(keys);
        CountDownLatch attemptFailed = new CountDownLatch(1);
        CountDownLatch resumeAttempt = new CountDownLatch(1);

        writeDueCertificates(keys);
        registerTransport("communication", communication);
        registry.onKernalStart(true);
        Files.delete(keys);

        when(executor.isShutdown()).thenAnswer(call -> {
            if (metric("AutomaticHotReloadAttempts") > 0) {
                attemptFailed.countDown();

                assertTrue(resumeAttempt.await(TIMEOUT_MS, MILLISECONDS));
            }

            return false;
        });

        GridTestThread attempt = new GridTestThread(nextCheck());
        GridTestThread manualReload = new GridTestThread(() -> registry.reload(communication));

        attempt.start();

        try {
            assertTrue(attemptFailed.await(TIMEOUT_MS, MILLISECONDS));

            writeFreshCertificates(keys);
            manualReload.start();

            long deadline = System.nanoTime() + MILLISECONDS.toNanos(TIMEOUT_MS);

            while (manualReload.getState() != Thread.State.BLOCKED && manualReload.isAlive()) {
                assertTrue(System.nanoTime() < deadline);

                Thread.sleep(10);
            }
        }
        finally {
            resumeAttempt.countDown();
            attempt.join(TIMEOUT_MS);
            manualReload.join(TIMEOUT_MS);
        }

        attempt.checkError();
        manualReload.checkError();
        assertEquals(0, failures());
    }

    /**
     * Given: communication has fresh certificates, and the client connector has due ones, so its attempt fails.
     * When: communication loads due certificates manually, the next check fails for both factories,
     *     and the client connector then loads fresh certificates manually.
     * Then: the metric sums the failures of both factories, and each manual reload clears only its own factory's count.
     */
    @Test
    public void testManualReloadResetsOnlyItsFactoryFailures() throws Exception {
        Path clientKeys = dir.resolve("client-keys.jks");
        SslContextFactory communication = factory(keys);
        SslContextFactory clientConnector = factory(clientKeys);

        writeFreshCertificates(keys);
        writeDueCertificates(clientKeys);
        registerTransport("communication", communication);
        registerTransport("client connector", clientConnector);
        registry.onKernalStart(true);
        runChecks();

        assertEquals(1, failures());

        writeDueCertificates(keys);
        registry.reload(communication);

        assertEquals(1, failures());

        runChecks();

        assertEquals(3, failures());

        writeFreshCertificates(clientKeys);
        registry.reload(clientConnector);

        assertEquals(1, failures());
    }

    /**
     * Given: an automatic load stays blocked even after it receives an interrupt.
     * When: the registry stops.
     * Then: it interrupts the load and returns while the load is still blocked.
     */
    @Test
    public void testStopInterruptsActiveAttemptWithoutWaitingForIt() throws Exception {
        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(new IgniteScheduledThreadPoolExecutor("ssl-hot-reload", null, 1));

        CountDownLatch attemptStarted = new CountDownLatch(1);
        CountDownLatch attemptInterrupted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);

        enableHotReload(factory, due());
        doAnswer(call -> {
            attemptStarted.countDown();

            try {
                allowCompletion.await();
            }
            catch (InterruptedException e) {
                attemptInterrupted.countDown();

                U.awaitQuiet(allowCompletion);
            }

            return null;
        }).when(factory).init();

        registry.register("communication", factory);

        try {
            registry.onKernalStart(true);

            assertTrue(attemptStarted.await(TIMEOUT_MS, MILLISECONDS));

            runAsync(() -> registry.onKernalStop(true)).get(TIMEOUT_MS);

            assertTrue(attemptInterrupted.await(TIMEOUT_MS, MILLISECONDS));
        }
        finally {
            allowCompletion.countDown();
        }
    }

    /**
     * Given: the node starts stopping during an automatic attempt.
     * When: the attempt fails.
     * Then: the failure is not counted, and the debug log shows the check and the failed attempt.
     */
    @Test
    public void testAttemptFailingDuringStopIsNotCounted() {
        List<String> logged = captureLog(true);

        enableHotReload(factory, due());
        doAnswer(call -> {
            when(executor.isShutdown()).thenReturn(true);

            throw new IgniteException("Load interrupted by node stop");
        }).when(factory).init();

        registry.register("communication", factory);
        registry.onKernalStart(true);
        runChecks();

        assertEquals(0, failures());
        assertTrue(logged.toString(), logged.contains("TLS hot reload check started [transports=communication]"));
        assertTrue(logged.toString(),
            logged.toString().contains("TLS hot reload failed during node stop [transports=communication, err="));
    }

    /**
     * Given: two factories with due certificates.
     * When: the node stops while the check loads one of them.
     * Then: the check does not start loading the other one.
     */
    @Test
    public void testStopDuringCheckSkipsRemainingFactories() {
        SslContextFactory clientConnector = mock(SslContextFactory.class);
        AtomicInteger loads = new AtomicInteger();

        enableHotReload(factory, due());
        enableHotReload(clientConnector, due());

        for (SslContextFactory sslFactory : List.of(factory, clientConnector)) {
            doAnswer(call -> {
                loads.incrementAndGet();
                when(executor.isShutdown()).thenReturn(true);

                return null;
            }).when(sslFactory).init();
        }

        registry.register("communication", factory);
        registry.register("client connector", clientConnector);
        registry.onKernalStart(true);
        runChecks();

        assertEquals(1, loads.get());
    }

    /** Replaces the registry with one that logs to a listener and returns its hot reload lines at info level and above. */
    private List<String> captureLog() {
        return captureLog(false);
    }

    /**
     * Replaces the registry with one that logs to a listener and returns the hot reload lines it logs, in order.
     * Tests call it before they register factories.
     *
     * @param debug Whether the registry logs debug lines too.
     */
    private List<String> captureLog(boolean debug) {
        ListeningTestLogger log = new ListeningTestLogger(debug ? new DebugLogger() : null);
        List<String> logged = new ArrayList<>();

        log.registerListener(line -> {
            if (line.startsWith("TLS hot reload "))
                logged.add(line);
        });
        when(ctx.log(SslFactoryRegistry.class)).thenReturn(log);

        registry = new SslFactoryRegistry(ctx);

        return logged;
    }

    /** Runs the checks that the registry has scheduled, in scheduling order. The checks that they schedule stay pending. */
    private void runChecks() {
        List<Runnable> checks = List.copyOf(scheduled.keySet());

        scheduled.clear();

        for (Runnable check : checks)
            check.run();
    }

    /** @return The only check that the registry has scheduled, which no longer counts as pending. */
    private Runnable nextCheck() {
        assertEquals(1, scheduled.size());

        Runnable check = scheduled.keySet().iterator().next();

        scheduled.clear();

        return check;
    }

    /** @return Delay of the only check that the registry has scheduled. */
    private long nextCheckDelay() {
        assertEquals(1, scheduled.size());

        return scheduled.values().iterator().next();
    }

    /** Loads a file-backed factory before registration, as the TCP transports do during startup. */
    private void registerTransport(String transport, SslContextFactory factory) {
        factory.create();
        registry.register(transport, factory);
    }

    /** */
    private long failures() {
        return metric("ConsecutiveHotReloadFailures");
    }

    /** */
    private long metric(String name) {
        return metrics.<LongMetric>findMetric(name).value();
    }

    /**
     * Makes the factory mock look like a factory with hot reload enabled and loaded certificates with the given date.
     * The mock returns {@code true} from {@code isHotReloadEnabled()} and the date from {@code hotReloadTime()}.
     *
     * @param factory Factory mock.
     * @param hotReloadTime Hot reload time of the loaded certificates.
     */
    private static void enableHotReload(SslContextFactory factory, long hotReloadTime) {
        when(factory.isHotReloadEnabled()).thenReturn(true);
        when(factory.hotReloadTime()).thenReturn(hotReloadTime);
    }

    /** @return Mock of a factory with hot reload enabled that is not an Ignite SSL context factory, such as the one of HTTP REST. */
    private static ReloadableSslFactory httpRestFactory() {
        ReloadableSslFactory factory = mock(ReloadableSslFactory.class);

        when(factory.isHotReloadEnabled()).thenReturn(true);

        return factory;
    }

    /** @return Hot reload time that has already come. */
    private static long due() {
        return System.currentTimeMillis();
    }

    /**
     * Creates a factory that reads its key store from the given file.
     * The factory trusts the test CA through the trust store file from the setup.
     */
    private SslContextFactory factory(Path keyStore) {
        return factory(keyStore, new SslContextFactory());
    }

    /** Points the factory at the supplied key store and the test trust store. */
    private SslContextFactory factory(Path keyStore, SslContextFactory sslFactory) {
        sslFactory.setKeyStoreFilePath(keyStore.toString());
        sslFactory.setKeyStorePassword(PASSWORD);
        sslFactory.setTrustStoreFilePath(trust.toString());
        sslFactory.setTrustStorePassword(PASSWORD);

        return sslFactory;
    }

    /**
     * Writes a key store that is already due for hot reload with the default lead time.
     * Its certificate expires in 10 hours.
     */
    private void writeDueCertificates(Path keyStore) throws Exception {
        save(authority.keyStore("node", at(-90), at(10)), keyStore);
    }

    /**
     * Writes a key store that is due for hot reload in 30 minutes with the default lead time.
     * Its certificate became valid an hour ago and expires in 7 days and 30 minutes.
     */
    private void writeCertificatesDueIn30Minutes(Path keyStore) throws Exception {
        long now = System.currentTimeMillis();

        save(authority.keyStore("node", now - HOURS.toMillis(1), now + DAYS.toMillis(7) + MINUTES.toMillis(30)), keyStore);
    }

    /** @return Whether the delay ends within the next 30 minutes, but no more than a minute earlier. */
    private static boolean isAbout30Minutes(long delay) {
        return delay > MINUTES.toMillis(29) && delay <= MINUTES.toMillis(30);
    }

    /**
     * Writes a key store that is due for hot reload in 23 days with the default lead time.
     * Its certificate became valid an hour ago and expires in 30 days.
     */
    private void writeFreshCertificates(Path keyStore) throws Exception {
        save(authority.keyStore("node", at(-1), at(DAYS.toHours(30))), keyStore);
    }

    /** */
    private static long at(long hours) {
        return System.currentTimeMillis() + HOURS.toMillis(hours);
    }

    /** Logger with debug level enabled that drops all lines. */
    private static class DebugLogger extends NullLogger {
        /** {@inheritDoc} */
        @Override public boolean isDebugEnabled() {
            return true;
        }
    }

    /** Factories with the same key store path compare equal and cache their own SSL contexts. */
    private static class KeyStorePathFactory extends SslContextFactory {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public boolean equals(Object obj) {
            return obj instanceof KeyStorePathFactory other && getKeyStoreFilePath().equals(other.getKeyStoreFilePath());
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return getKeyStoreFilePath().hashCode();
        }
    }

    /** Creates key managers from the key store and password supplied by the test, or none without a key store. */
    private static class SourceSslContextFactory extends AbstractSslContextFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private KeyStore store;

        /** */
        private char[] pwd;

        /** */
        private void setKeys(KeyStore store, char[] pwd) {
            this.store = store;
            this.pwd = pwd;
        }

        /** {@inheritDoc} */
        @Override protected void checkParameters() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override protected KeyManager[] createKeyManagers() throws SSLException {
            if (store == null)
                return null;

            try {
                return createKeyManagers(KeyManagerFactory.getInstance("SunX509"), store, pwd);
            }
            catch (GeneralSecurityException e) {
                throw new SSLException(e);
            }
        }

        /** {@inheritDoc} */
        @Override protected TrustManager[] createTrustManagers() {
            return null;
        }
    }
}
