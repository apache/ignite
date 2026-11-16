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
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
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
import org.apache.ignite.testframework.ListeningTestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.ssl.SslContextFactory.DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * A manual reload is {@link SslFactoryRegistry#reload}, which the TLS reload command calls.
 * An attempt is an automatic hot reload attempt that the registry schedules.
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

    /** */
    private Runnable scheduled;

    /** */
    private ScheduledFuture<?> pending;

    /** */
    private long delay;

    /** */
    private Path dir;

    /** Key store file of the factory under test. */
    private Path keys;

    /** */
    private Path trust;

    /** */
    private TestCertificateAuthority authority;

    /**
     * Each test gets a registry whose hot reload executor records scheduled attempts instead of running them.
     * The executor is a mock that runs other tasks at once in the calling thread and reports termination,
     * so stopping does not wait. The setup also writes the trust store of a test CA to a temporary directory
     * and stubs the node metrics and logger.
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
        when(executor.isTerminated()).thenReturn(true);

        doAnswer(call -> {
            call.<Runnable>getArgument(0).run();

            return null;
        }).when(executor).execute(any(Runnable.class));
        when(executor.submit(any(Runnable.class))).thenAnswer(call -> {
            FutureTask<?> task = new FutureTask<>(call.getArgument(0), null);

            task.run();

            return task;
        });

        when(executor.schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS))).thenAnswer(call -> {
            scheduled = call.getArgument(0);
            delay = call.getArgument(1);

            pending = mock(ScheduledFuture.class);

            return pending;
        });

        executorFactory = mockStatic(IgniteScheduledThreadPoolExecutor.class);
        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(executor);
    }

    /**
     * After each test, the registry stops, and the static executor mock and the temporary directory are released.
     * The stop runs in another thread with a timeout, so a test that leaves an attempt blocked fails instead of hanging.
     */
    @After
    public void tearDown() throws Exception {
        try {
            runAsync(registry::stop).get(TIMEOUT_MS);
        }
        finally {
            executorFactory.close();
            U.delete(dir);
        }
    }

    /**
     * Given: a factory registered for the communication transport.
     * When: another factory registers for the same transport.
     * Then: registration fails, and the first factory stays registered.
     */
    @Test
    public void testDuplicateRegistrationKeepsOriginalFactory() {
        registry.register("communication", factory);

        assertThrows(IllegalStateException.class, () -> registry.register("communication", new SslContextFactory()));
        assertSame(factory, registry.factories().get("communication"));
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
     * Given: communication and discovery share a factory with hot reload enabled and fresh certificates.
     * When: the registry starts.
     * Then: the registry schedules one attempt for the shared factory's certificates.
     */
    @Test
    public void testStartSchedulesSharedFactoryOnce() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registerTransport("discovery", communication);
        registry.start(ctx);

        verify(executor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
        assertTrue(delay > HOURS.toMillis(1));
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
     * Given: certificates from 1970 that are due for hot reload exactly at 0 ms, the value that also means no date.
     * When: the registry starts, and the source then gets new certificates.
     * Then: the registry schedules an attempt at once, and the attempt loads the new certificates.
     */
    @Test
    public void testHotReloadAtEpochLoadsReplacement() throws Exception {
        SslContextFactory communication = factory(keys);

        save(authority.keyStore("node", -DAYS.toMillis(1), DFLT_HOT_RELOAD_BEFORE_EXPIRY_MS), keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);

        assertNotNull(scheduled);
        assertEquals(0, delay);

        writeFreshCertificates(keys);
        scheduled.run();

        assertTrue(communication.getHotReloadTime() > System.currentTimeMillis());
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
     * Given: a transport is using a factory with hot reload enabled.
     * When: a manual reload runs before the registry starts.
     * Then: the reload is rejected, the transport keeps its context, and the manual attempt counter stays at zero.
     */
    @Test
    public void testManualReloadBeforeStartKeepsActiveContextAndIsNotCounted() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.setHotReloadEnabled(true);
        SSLContext active = communication.create();
        registry.register("communication", communication);

        IgniteCheckedException err = assertThrows(IgniteCheckedException.class, () -> registry.reload(communication));

        assertTrue(err.getMessage().contains("has not started"));
        assertSame(active, communication.create());

        registry.start(ctx);

        assertEquals(0, metric("ManualReloadAttempts"));
    }

    /**
     * Given: hot reload is enabled, and the key store file is deleted after start.
     * When: three attempts fail in a row.
     * Then: each retry waits one hour.
     */
    @Test
    public void testFailedAttemptsRetryEveryHour() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);

        for (int failure = 1; failure <= 3; failure++) {
            long before = System.currentTimeMillis();

            scheduled.run();

            assertEquals(failure, failures());
            assertTrue(delay >= HOURS.toMillis(1) - (System.currentTimeMillis() - before));
            assertTrue(delay <= HOURS.toMillis(1));
        }
    }

    /**
     * Given: hot reload is enabled for due certificates, and the key store file is deleted after start.
     * When: one automatic attempt fails, and the next one loads fresh certificates.
     * Then: the log shows each scheduled attempt with its time, and the start and result of each attempt.
     */
    @Test
    public void testLogShowsScheduleStartAndResultOfAttempts() throws Exception {
        ListeningTestLogger log = new ListeningTestLogger();
        List<String> logged = new ArrayList<>();

        log.registerListener(line -> {
            if (line.startsWith("TLS hot reload "))
                logged.add(line);
        });
        when(ctx.log(SslFactoryRegistry.class)).thenReturn(log);

        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);

        long due = communication.getHotReloadTime();

        Files.delete(keys);
        scheduled.run();
        writeFreshCertificates(keys);
        scheduled.run();

        assertEquals(List.of("scheduled", "started", "failed", "scheduled", "started", "succeeded", "scheduled"),
            logged.stream().map(e -> e.substring("TLS hot reload ".length(), e.indexOf(" ["))).collect(toList()));
        assertTrue(logged.toString(), logged.stream().allMatch(e -> e.contains("[transports=communication")));
        assertTrue(logged.get(0), logged.get(0).endsWith("nextAttempt=" + new Date(due) + ']'));
        assertTrue(logged.get(6), logged.get(6).endsWith("nextAttempt=" + new Date(communication.getHotReloadTime()) + ']'));
    }

    /**
     * Given: hot reload is enabled, and the key store file is deleted after start.
     * When: one automatic attempt fails and the next loads fresh certificates.
     * Then: the automatic attempt counter reads one after the failure and two after the successful retry.
     */
    @Test
    public void testAutomaticCounterIncludesFailedAndSuccessfulAttempts() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        scheduled.run();

        assertEquals(1, failures());
        assertEquals(1, metric("AutomaticHotReloadAttempts"));

        writeFreshCertificates(keys);
        scheduled.run();

        assertEquals(0, failures());
        assertEquals(2, metric("AutomaticHotReloadAttempts"));
    }

    /**
     * Given: an automatic attempt has failed on a missing key store file.
     * When: the source gets certificates due for hot reload in 23 days, and the next attempt succeeds.
     * Then: the failure count resets, and the next attempt waits for the new hot reload time.
     */
    @Test
    public void testSuccessfulAttemptResetsFailuresAndSchedulesNextHotReload() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        scheduled.run();

        assertEquals(1, failures());

        writeFreshCertificates(keys);
        scheduled.run();

        assertEquals(0, failures());
        assertTrue(delay > HOURS.toMillis(1));
    }

    /**
     * Given: an automatic attempt has failed on a missing key store file, and a retry is pending.
     * When: the operator reloads certificates that are already due and the immediate automatic attempt runs.
     *     The operator then installs fresh certificates and reloads them.
     * Then: the first reload cancels the retry and schedules an attempt at once.
     *     The second reload schedules the attempt for the new hot reload time.
     */
    @Test
    public void testManualReloadReplacesRetryWithLoadedHotReloadTime() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        scheduled.run();

        assertEquals(1, failures());

        ScheduledFuture<?> retry = pending;

        writeDueCertificates(keys);
        registry.reload(communication);

        verify(retry).cancel(false);
        assertEquals(0, delay);
        scheduled.run();

        writeFreshCertificates(keys);
        registry.reload(communication);

        assertTrue(delay > HOURS.toMillis(1));
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

        IgniteCheckedException err = assertThrows(IgniteCheckedException.class, () -> registry.reload(communication));

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
     * Given: an automatic attempt has failed on a missing key store file, and a retry is pending.
     * When: a manual reload runs while the file is still missing.
     * Then: the reload fails, and the pending retry and the failure count stay.
     */
    @Test
    public void testFailedManualReloadKeepsRetryAndFailures() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registerTransport("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        scheduled.run();

        ScheduledFuture<?> retry = pending;
        IgniteCheckedException err = assertThrows(IgniteCheckedException.class, () -> registry.reload(communication));

        assertTrue(X.hasCause(err, "key store file was not found", SSLException.class));
        assertEquals(1, failures());
        verify(retry, never()).cancel(false);
        verify(executor, times(2)).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
    }

    /**
     * Given: the node starts stopping while a manual reload loads the factory.
     * When: the executor rejects the next attempt.
     * Then: the manual reload still succeeds.
     */
    @Test
    public void testManualReloadSucceedsWhenNodeStopsAfterLoad() throws Exception {
        enableHotReload(factory, later());

        registry.register("communication", factory);
        registry.start(ctx);

        doAnswer(call -> {
            when(executor.isShutdown()).thenReturn(true);

            return null;
        }).when(factory).init();
        doThrow(new RejectedExecutionException()).when(executor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));

        registry.reload(factory);

        verify(factory).init();
    }

    /**
     * Given: the client connector has one failure because its certificates are still due.
     * When: communication loads due certificates and its automatic attempt fails too, then the client connector loads fresh certificates.
     * Then: the metric counts both failures until the client connector clears its own.
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

        ArgumentCaptor<Runnable> dueAttempt = ArgumentCaptor.forClass(Runnable.class);

        verify(executor).schedule(dueAttempt.capture(), eq(0L), eq(MILLISECONDS));
        dueAttempt.getValue().run();

        assertEquals(1, failures());

        writeDueCertificates(keys);
        registry.reload(communication);

        assertEquals(1, failures());

        scheduled.run();

        assertEquals(2, failures());

        writeFreshCertificates(clientKeys);
        registry.reload(clientConnector);

        assertEquals(1, failures());
    }

    /**
     * Given: the registry runs manual reloads on a dedicated hot reload thread.
     * When: a manual reload runs under an operator context.
     * Then: the worker sees the operator context, and the caller keeps it after the reload.
     */
    @Test
    public void testManualReloadKeepsCallerContext() throws Exception {
        OperationContextAttribute<String> caller = OperationContextAttribute.newInstance();
        AtomicReference<String> loadCaller = new AtomicReference<>();

        useRealExecutor();
        enableHotReload(factory, later());
        doAnswer(call -> {
            loadCaller.set(OperationContext.get(caller));

            return null;
        }).when(factory).init();

        registry.register("communication", factory);
        registry.start(ctx);

        try (Scope ignored = OperationContext.set(caller, "operator")) {
            registry.reload(factory);

            assertEquals("operator", OperationContext.get(caller));
        }

        assertEquals("operator", loadCaller.get());
    }

    /**
     * Given: an executor that passes the operation context to its tasks like the node executor.
     * When: a manual reload under an operator context schedules the next attempt, and the attempt runs.
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
        }).when(factory).hotReload();

        registry.register("communication", factory);
        registry.start(ctx);

        try (Scope ignored = OperationContext.set(caller, "operator")) {
            registry.reload(factory);
        }

        scheduled.run();

        assertEquals(Optional.empty(), attemptCaller.get());
    }

    /**
     * Given: an automatic attempt that blocks until it is interrupted.
     * When: the registry stops.
     * Then: the attempt is interrupted, and the stop completes.
     */
    @Test
    public void testStopInterruptsActiveAttempt() throws Exception {
        useRealExecutor();

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
        }).when(factory).hotReload();

        registry.register("communication", factory);

        try {
            registry.start(ctx);

            assertTrue(attemptStarted.await(TIMEOUT_MS, MILLISECONDS));

            runAsync(registry::stop).get(TIMEOUT_MS);

            assertEquals(0, attemptInterrupted.getCount());
        }
        finally {
            allowCompletion.countDown();
        }
    }

    /**
     * Given: an automatic attempt is loading certificates, and a manual reload waits in the queue.
     * When: the registry stops.
     * Then: the reload is cancelled, does not load the factory, and is not counted.
     */
    @Test
    public void testStopCancelsQueuedManualReload() throws Exception {
        ScheduledExecutorService hotReloadExecutor = useRealExecutor();
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch releaseLoad = new CountDownLatch(1);
        BlockingQueue<Future<?>> submissions = new LinkedBlockingQueue<>();

        enableHotReload(factory, due());
        doAnswer(call -> {
            loadStarted.countDown();
            U.awaitQuiet(releaseLoad);

            return null;
        }).when(factory).hotReload();
        doAnswer(call -> {
            Future<?> task = (Future<?>)call.callRealMethod();

            submissions.add(task);

            return task;
        }).when(hotReloadExecutor).submit(any(Runnable.class));

        registry.register("communication", factory);

        IgniteInternalFuture<IgniteCheckedException> reload = null;
        IgniteInternalFuture<?> stop = null;
        Future<?> queuedReload = null;

        try {
            registry.start(ctx);

            assertTrue(loadStarted.await(TIMEOUT_MS, MILLISECONDS));

            reload = runAsync(() -> assertThrows(IgniteCheckedException.class, () -> registry.reload(factory)));
            queuedReload = submissions.poll(TIMEOUT_MS, MILLISECONDS);

            assertNotNull(queuedReload);

            stop = runAsync(registry::stop);

            assertTrue(reload.get(TIMEOUT_MS).getCause() instanceof CancellationException);
            verify(factory, never()).init();
            assertEquals(0, metric("ManualReloadAttempts"));
        }
        finally {
            if (queuedReload != null)
                queuedReload.cancel(true);
            else if (reload != null)
                reload.cancel();

            releaseLoad.countDown();

            if (stop != null)
                stop.get(TIMEOUT_MS);

            if (reload != null)
                reload.get(TIMEOUT_MS);
        }
    }

    /**
     * Given: an automatic attempt that ignores the interrupt and blocks until the test releases it.
     * When: the registry stops.
     * Then: the stop waits until the attempt is released and the executor terminates.
     */
    @Test
    public void testStopWaitsForActiveLoad() throws Exception {
        ScheduledExecutorService hotReloadExecutor = useRealExecutor();
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        CountDownLatch terminationWaitEntered = new CountDownLatch(1);

        enableHotReload(factory, due());
        doAnswer(call -> {
            loadStarted.countDown();
            U.awaitQuiet(allowCompletion);

            return null;
        }).when(factory).hotReload();
        doAnswer(call -> {
            terminationWaitEntered.countDown();

            return call.callRealMethod();
        }).when(hotReloadExecutor).awaitTermination(anyLong(), eq(MILLISECONDS));

        registry.register("communication", factory);

        IgniteInternalFuture<?> stop = null;

        try {
            registry.start(ctx);

            assertTrue(loadStarted.await(TIMEOUT_MS, MILLISECONDS));

            stop = runAsync(registry::stop);

            assertTrue(terminationWaitEntered.await(TIMEOUT_MS, MILLISECONDS));
            assertFalse(stop.isDone());
        }
        finally {
            allowCompletion.countDown();

            if (stop != null)
                stop.get(TIMEOUT_MS);
        }

        assertTrue(hotReloadExecutor.isTerminated());
    }

    /**
     * Given: a manual reload waits in the queue.
     * When: the waiting caller is interrupted.
     * Then: the caller gets an interrupted error and keeps the interrupt flag, and the queued reload is cancelled.
     */
    @Test
    public void testInterruptedManualReloadCancelsQueuedReload() throws Exception {
        enableHotReload(factory, later());

        Future<?> queuedReload = mock(Future.class);

        when(queuedReload.get()).thenThrow(new InterruptedException());
        doReturn(queuedReload).when(executor).submit(any(Runnable.class));

        registry.register("communication", factory);
        registry.start(ctx);

        assertThrows(IgniteInterruptedCheckedException.class, () -> registry.reload(factory));
        assertTrue(Thread.interrupted());
        verify(queuedReload).cancel(true);
    }

    /**
     * Given: the node starts stopping during an automatic attempt.
     * When: the attempt fails.
     * Then: the failure is not counted, and no retry is scheduled.
     */
    @Test
    public void testAttemptFailingDuringStopIsNotCountedOrRetried() {
        enableHotReload(factory, due());
        doAnswer(call -> {
            when(executor.isShutdown()).thenReturn(true);

            throw new IgniteException("Load interrupted by node stop");
        }).when(factory).hotReload();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        assertEquals(0, failures());
        verify(executor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
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

    /** @return Hot reload time two hours ahead. */
    private static long later() {
        return System.currentTimeMillis() + HOURS.toMillis(2);
    }

    /**
     * Creates a factory with hot reload disabled that reads its key store from the given file.
     * The factory trusts the test CA through the trust store file from the setup.
     */
    private SslContextFactory factory(Path keyStore) {
        SslContextFactory sslFactory = new SslContextFactory();

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

    /**
     * Makes the registry use a real single-thread executor and returns a spy on it.
     * Tests use the spy to see when the registry submits a task or waits for termination.
     */
    private ScheduledExecutorService useRealExecutor() {
        ScheduledExecutorService hotReloadExecutor = spy(new IgniteScheduledThreadPoolExecutor("ssl-hot-reload", null, 1));

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(hotReloadExecutor);

        return hotReloadExecutor;
    }
}
