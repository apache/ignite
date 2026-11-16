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
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.thread.context.concurrent.OperationContextAwareScheduledExecutorService;
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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A manual reload is {@link SslFactoryRegistry#reload}, which the TLS reload command calls.
 * An attempt is an automatic hot reload attempt that the hourly check makes.
 */
public class SslFactoryRegistryTest {
    /** Upper bound for waiting for an event that must happen. */
    private static final long TIMEOUT_MS = 30_000;

    /** */
    private final SslFactoryRegistry registry = new SslFactoryRegistry();

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

    /** Hourly hot reload check that the registry scheduled. */
    private Runnable check;

    /** */
    private Path dir;

    /** Key store file of the factory under test. */
    private Path keys;

    /** */
    private Path trust;

    /** */
    private TestCertificateAuthority authority;

    /**
     * Each test gets a registry whose hot reload executor records the hourly check instead of running it.
     * Tests run the check themselves.
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

        when(executor.scheduleWithFixedDelay(any(Runnable.class), anyLong(), anyLong(), eq(MILLISECONDS))).thenAnswer(call -> {
            check = call.getArgument(0);

            return mock(ScheduledFuture.class);
        });

        executorFactory = mockStatic(IgniteScheduledThreadPoolExecutor.class);
        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(executor);
    }

    /** After each test, the registry stops, and the static executor mock and the temporary directory are released. */
    @After
    public void tearDown() throws Exception {
        try {
            registry.stop();
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
        writeFreshCertificates(keys);
        registerTransport("communication", factory(keys));
        registry.start(ctx);

        executorFactory.verifyNoInteractions();
        assertEquals(0, failures());
    }

    /**
     * Given: communication and discovery share a factory, and the client connector has an equal factory for the same file.
     * When: the key store is replaced and the hourly check runs.
     * Then: each factory instance loads the new certificates once.
     */
    @Test
    public void testEachFactoryInstanceReloadsOnce() throws Exception {
        SslContextFactory communication = factory(keys, new KeyStorePathFactory());
        SslContextFactory clientConnector = factory(keys, new KeyStorePathFactory());

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        clientConnector.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registerTransport("discovery", communication);
        registerTransport("client connector", clientConnector);
        registry.start(ctx);

        long prevTime = communication.getHotReloadTime();

        writeFreshCertificates(keys);
        check.run();

        assertEquals(2, metric("AutomaticHotReloadAttempts"));
        assertTrue(communication.getHotReloadTime() > prevTime);
        assertEquals(communication.getHotReloadTime(), clientConnector.getHotReloadTime());
    }

    /**
     * Given: the node reuses a factory that loaded its certificates with hot reload disabled.
     * When: the node starts with hot reload enabled and the registry loads the certificates again.
     * Then: neither the manual nor the automatic attempt counter grows.
     */
    @Test
    public void testStartupLoadIsNotCounted() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.create();
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);

        assertNotEquals(0, communication.getHotReloadTime());
        assertEquals(0, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticHotReloadAttempts"));
    }

    /**
     * Given: the node reuses a factory that loaded its certificates with hot reload disabled.
     * When: the node starts with hot reload enabled and the key store file is missing.
     * Then: the start fails, and the registry schedules nothing.
     */
    @Test
    public void testStartFailsWithoutSchedulingWhenFactoryLoadFails() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.create();
        communication.setHotReloadEnabled(true);
        Files.delete(keys);
        registerTransport("communication", communication);

        IgniteException err = assertThrows(IgniteException.class, () -> registry.start(ctx));

        assertTrue(X.hasCause(err, "key store file was not found", SSLException.class));
        executorFactory.verifyNoInteractions();
    }

    /**
     * Given: hot reload is enabled, and the key store holds only a CA certificate.
     * When: the registry starts and the hourly check runs, then the operator puts a key store with a private key in place
     *     and reloads it.
     * Then: the registry logs that hot reload will not run, and the check makes no attempt.
     *     After the reload, the log shows when hot reload is due.
     */
    @Test
    public void testStoreWithoutPrivateKeyIsReportedUntilKeysAreReloaded() throws Exception {
        List<String> logged = captureLog();
        SslContextFactory communication = factory(keys);

        save(authority.trustStore(), keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        check.run();

        assertEquals(1, logged.size());
        assertTrue(logged.get(0), logged.get(0).startsWith("TLS hot reload will not run"));
        assertEquals(0, metric("AutomaticHotReloadAttempts"));

        writeFreshCertificates(keys);
        registry.reload(communication);

        assertEquals("TLS hot reload due [transports=communication, hotReloadTime=" + new Date(communication.getHotReloadTime()) + ']',
            logged.get(logged.size() - 1));
    }

    /**
     * Given: hot reload is enabled for due certificates, and the key store file is deleted after start.
     * When: the hourly check runs three times.
     * Then: the check runs at start and then every hour, and each run counts one more failed attempt.
     */
    @Test
    public void testHotReloadIsCheckedEveryHourFromStart() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);

        verify(executor).scheduleWithFixedDelay(any(Runnable.class), eq(0L), eq(HOURS.toMillis(1)), eq(MILLISECONDS));

        for (int failure = 1; failure <= 3; failure++) {
            check.run();

            assertEquals(failure, failures());
        }
    }

    /**
     * Given: hot reload is enabled for due certificates, and the key store file is deleted after start.
     * When: one automatic attempt fails, and the next one loads fresh certificates.
     * Then: the log shows when the loaded certificates are due for hot reload, and the start and result of each attempt.
     */
    @Test
    public void testLogShowsDueTimeStartAndResultOfAttempts() throws Exception {
        List<String> logged = captureLog();
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);

        long due = communication.getHotReloadTime();

        Files.delete(keys);
        check.run();
        writeFreshCertificates(keys);
        check.run();

        assertEquals(List.of("due", "started", "failed", "started", "succeeded", "due"),
            logged.stream().map(e -> e.substring("TLS hot reload ".length(), e.indexOf(" ["))).collect(toList()));
        assertTrue(logged.toString(), logged.stream().allMatch(e -> e.contains("[transports=communication")));
        assertTrue(logged.get(0), logged.get(0).endsWith("hotReloadTime=" + new Date(due) + ']'));
        assertTrue(logged.get(2), logged.get(2).contains("nextAttemptNotBefore="));
        assertTrue(logged.get(5), logged.get(5).endsWith("hotReloadTime=" + new Date(communication.getHotReloadTime()) + ']'));
    }

    /**
     * Given: hot reload is enabled for due certificates, and the key store file is deleted after start.
     * When: one check fails, the source then gets certificates due for hot reload in 23 days, the next check loads them,
     *     and the check runs again.
     * Then: both attempts are counted, the success resets the failure count, and the last check makes no attempt.
     */
    @Test
    public void testSuccessResetsFailuresAndLaterChecksWait() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        check.run();

        assertEquals(1, failures());
        assertEquals(1, metric("AutomaticHotReloadAttempts"));

        writeFreshCertificates(keys);
        check.run();

        assertEquals(0, failures());
        assertEquals(2, metric("AutomaticHotReloadAttempts"));

        check.run();

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
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        check.run();

        assertEquals(1, failures());

        writeFreshCertificates(keys);
        communication.init();
        check.run();

        assertEquals(0, failures());
        assertEquals(1, metric("AutomaticHotReloadAttempts"));
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
        registerTransport("communication", communication);
        registry.start(ctx);
        registry.reload(communication);

        assertEquals(1, metric("ManualReloadAttempts"));

        Files.delete(keys);

        assertThrows(IgniteException.class, () -> registry.reload(communication));
        assertEquals(2, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticHotReloadAttempts"));
    }

    /**
     * Given: a factory with hot reload enabled and a key store deleted after startup.
     * When: a manual reload fails to read the file.
     * Then: the manual attempt counter includes the failed load.
     */
    @Test
    public void testFailedManualReloadWithHotReloadEnabledIsCounted() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);

        IgniteException err = assertThrows(IgniteException.class, () -> registry.reload(communication));

        assertTrue(X.hasCause(err, "key store file was not found", SSLException.class));
        assertEquals(1, metric("ManualReloadAttempts"));
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
        registry.start(ctx);
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
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        check.run();

        IgniteException err = assertThrows(IgniteException.class, () -> registry.reload(communication));

        assertTrue(X.hasCause(err, "key store file was not found", SSLException.class));
        assertEquals(1, failures());
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
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);

        when(executor.isShutdown()).thenAnswer(call -> {
            if (metric("AutomaticHotReloadAttempts") > 0) {
                attemptFailed.countDown();

                assertTrue(resumeAttempt.await(TIMEOUT_MS, MILLISECONDS));
            }

            return false;
        });

        GridTestThread attempt = new GridTestThread(check);
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
        communication.setHotReloadEnabled(true);
        clientConnector.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registerTransport("client connector", clientConnector);
        registry.start(ctx);
        check.run();

        assertEquals(1, failures());

        writeDueCertificates(keys);
        registry.reload(communication);

        assertEquals(1, failures());

        check.run();

        assertEquals(3, failures());

        writeFreshCertificates(clientKeys);
        registry.reload(clientConnector);

        assertEquals(1, failures());
    }

    /**
     * Given: an executor that passes the operation context to its tasks like the node executor.
     * When: the registry starts under an operator context, and the hourly check runs.
     * Then: the attempt sees no operation context.
     */
    @Test
    public void testAttemptsDoNotInheritCallerContext() throws Exception {
        OperationContextAttribute<String> caller = OperationContextAttribute.newInstance();
        AtomicReference<Optional<String>> attemptCaller = new AtomicReference<>();

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(new OperationContextAwareScheduledExecutorService(executor) { });
        enableHotReload(factory, due());
        doAnswer(call -> {
            attemptCaller.set(Optional.ofNullable(OperationContext.get(caller)));

            return null;
        }).when(factory).init();

        registry.register("communication", factory);

        try (Scope ignored = OperationContext.set(caller, "operator")) {
            registry.start(ctx);
        }

        check.run();

        assertEquals(Optional.empty(), attemptCaller.get());
    }

    /**
     * Given: an automatic attempt that blocks until it is interrupted.
     * When: the registry stops.
     * Then: the attempt is interrupted.
     */
    @Test
    public void testStopInterruptsActiveAttempt() throws Exception {
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
            }

            return null;
        }).when(factory).init();

        registry.register("communication", factory);

        try {
            registry.start(ctx);

            assertTrue(attemptStarted.await(TIMEOUT_MS, MILLISECONDS));

            registry.stop();

            assertTrue(attemptInterrupted.await(TIMEOUT_MS, MILLISECONDS));
        }
        finally {
            allowCompletion.countDown();
        }
    }

    /**
     * Given: the node starts stopping during an automatic attempt.
     * When: the attempt fails.
     * Then: the failure is not counted.
     */
    @Test
    public void testAttemptFailingDuringStopIsNotCounted() {
        enableHotReload(factory, due());
        doAnswer(call -> {
            when(executor.isShutdown()).thenReturn(true);

            throw new IgniteException("Load interrupted by node stop");
        }).when(factory).init();

        registry.register("communication", factory);
        registry.start(ctx);
        check.run();

        assertEquals(0, failures());
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
        registry.start(ctx);
        check.run();

        assertEquals(1, loads.get());
    }

    /** Makes the registry log to a listener and returns the hot reload lines it logs, in order. */
    private List<String> captureLog() {
        ListeningTestLogger log = new ListeningTestLogger();
        List<String> logged = new ArrayList<>();

        log.registerListener(line -> {
            if (line.startsWith("TLS hot reload "))
                logged.add(line);
        });
        when(ctx.log(SslFactoryRegistry.class)).thenReturn(log);

        return logged;
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
     * The mock returns {@code true} from {@code isHotReloadEnabled()} and the date from {@code getHotReloadTime()}.
     *
     * @param factory Factory mock.
     * @param hotReloadTime Hot reload time of the loaded certificates, or {@code 0} if they were loaded without hot reload.
     */
    private static void enableHotReload(SslContextFactory factory, long hotReloadTime) {
        when(factory.isHotReloadEnabled()).thenReturn(true);
        when(factory.getHotReloadTime()).thenReturn(hotReloadTime);
    }

    /** @return Hot reload time that has already come. */
    private static long due() {
        return System.currentTimeMillis();
    }

    /**
     * Creates a factory with hot reload disabled that reads its key store from the given file.
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
}
