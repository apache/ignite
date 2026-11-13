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
import java.util.concurrent.atomic.AtomicLong;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.ssl.TestCertificateAuthority.PASSWORD;
import static org.apache.ignite.ssl.TestCertificateAuthority.save;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.junit.Assert.assertEquals;
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

    /** */
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

    /** */
    @After
    public void tearDown() throws Exception {
        try {
            runAsync(registry::stop).get(10_000);
        }
        finally {
            executorFactory.close();
            U.delete(dir);
        }
    }

    /** */
    @Test
    public void testDuplicateRegistrationKeepsOriginalFactory() {
        registry.register("communication", factory);

        assertThrows(IllegalStateException.class, () -> registry.register("communication", new SslContextFactory()));
        assertSame(factory, registry.factories().get("communication"));
    }

    /** */
    @Test
    public void testDisabledFactoryDoesNotStartHotReload() throws Exception {
        writeFreshCertificates(keys);
        registry.register("communication", factory(keys));
        registry.start(ctx);

        executorFactory.verifyNoInteractions();
        assertEquals(0, failures());
        assertEquals(0, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticHotReloadAttempts"));
    }

    /** */
    @Test
    public void testStartLoadsSharedFactoryAndSchedulesItOnce() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.setHotReloadEnabled(true);
        registry.register("communication", communication);
        registry.register("discovery", communication);
        registry.start(ctx);

        verify(executor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
        assertTrue(delay > HOURS.toMillis(1));
        assertEquals(0, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticHotReloadAttempts"));
    }

    /** */
    @Test
    public void testHotReloadAtEpochLoadsReplacement() throws Exception {
        SslContextFactory communication = factory(keys);

        save(authority.keyStore("node", -HOURS.toMillis(85), HOURS.toMillis(15)), keys);
        communication.setHotReloadEnabled(true);
        registry.register("communication", communication);
        registry.start(ctx);

        assertNotNull(scheduled);
        assertEquals(0, delay);

        writeFreshCertificates(keys);
        scheduled.run();

        assertTrue(communication.getHotReloadTime() > System.currentTimeMillis());
    }

    /** */
    @Test
    public void testStartFailsWithoutSchedulingWhenFactoryLoadFails() {
        SslContextFactory communication = factory(dir.resolve("missing.jks"));

        communication.setHotReloadEnabled(true);
        registry.register("communication", communication);

        IgniteException err = assertThrows(IgniteException.class, () -> registry.start(ctx));

        assertTrue(X.hasCause(err, "key store file was not found", SSLException.class));
        executorFactory.verifyNoInteractions();
    }

    /** */
    @Test
    public void testManualReloadBeforeStartIsRejectedWithoutLoadingOrCounting() throws Exception {
        SslContextFactory communication = factory(keys);

        writeFreshCertificates(keys);
        communication.setHotReloadEnabled(true);
        registry.register("communication", communication);

        IgniteCheckedException err = assertThrows(IgniteCheckedException.class, () -> registry.reload(communication));

        assertTrue(err.getMessage().contains("has not started"));
        assertEquals(0, communication.getHotReloadTime());

        registry.start(ctx);

        assertEquals(0, metric("ManualReloadAttempts"));
    }

    /** */
    @Test
    public void testFailedAttemptsRetryWithDoublingDelayUpToOneHour() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registry.register("communication", communication);
        registry.start(ctx);
        Files.delete(keys);

        long maxRandomAddition = 0;

        for (int failure = 1; failure <= 8; failure++) {
            long before = System.currentTimeMillis();

            scheduled.run();

            long base = MINUTES.toMillis(Math.min(1L << (failure - 1), 60));

            assertEquals(failure, failures());
            assertEquals(failure, metric("AutomaticHotReloadAttempts"));
            assertTrue(delay >= base - (System.currentTimeMillis() - before));
            assertTrue(delay <= base + base / 2);

            maxRandomAddition = Math.max(maxRandomAddition, delay - base);
        }

        assertTrue(maxRandomAddition > 0);
    }

    /** */
    @Test
    public void testSuccessfulAttemptResetsFailuresAndSchedulesNextHotReload() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registry.register("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        scheduled.run();

        assertEquals(1, failures());

        writeFreshCertificates(keys);
        scheduled.run();

        assertEquals(0, failures());
        assertTrue(delay > HOURS.toMillis(1));
        assertEquals(2, metric("AutomaticHotReloadAttempts"));
    }

    /** */
    @Test
    public void testManualReloadReplacesRetryWithLoadedHotReloadTime() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registry.register("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        scheduled.run();

        assertEquals(1, failures());

        ScheduledFuture<?> retry = pending;

        writeDueCertificates(keys);
        registry.reload(communication);

        verify(retry).cancel(false);
        assertEquals(0, failures());
        assertTrue(delay <= 0);

        writeFreshCertificates(keys);
        registry.reload(communication);

        assertTrue(delay > HOURS.toMillis(1));
    }

    /** */
    @Test
    public void testManualReloadWithoutHotReloadCountsAppliedAndFailedLoads() throws Exception {
        SslContextFactory communication = factory(keys);
        AbstractSslContextFactory genericFactory = mock(AbstractSslContextFactory.class);

        writeFreshCertificates(keys);
        registry.register("communication", communication);
        registry.register("client connector", genericFactory);
        registry.start(ctx);
        registry.reload(communication);
        registry.reload(genericFactory);
        Files.delete(keys);

        assertThrows(IgniteException.class, () -> registry.reload(communication));
        verify(genericFactory).init();
        assertEquals(3, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticHotReloadAttempts"));
    }

    /** */
    @Test
    public void testFailedManualReloadKeepsRetryAndFailures() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        registry.register("communication", communication);
        registry.start(ctx);
        Files.delete(keys);
        scheduled.run();

        ScheduledFuture<?> retry = pending;
        IgniteCheckedException err = assertThrows(IgniteCheckedException.class, () -> registry.reload(communication));

        assertTrue(X.hasCause(err, "key store file was not found", SSLException.class));
        assertEquals(1, failures());
        assertEquals(1, metric("ManualReloadAttempts"));
        verify(retry, never()).cancel(false);
        verify(executor, times(2)).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
    }

    /** */
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
        assertEquals(1, metric("ManualReloadAttempts"));
    }

    /** */
    @Test
    public void testManualReloadResetsOnlyItsFactoryFailures() throws Exception {
        Path clientKeys = dir.resolve("client-keys.jks");
        SslContextFactory communication = factory(keys);
        SslContextFactory clientConnector = factory(clientKeys);

        writeDueCertificates(keys);
        writeDueCertificates(clientKeys);
        communication.setHotReloadEnabled(true);
        clientConnector.setHotReloadEnabled(true);
        registry.register("communication", communication);
        registry.register("client connector", clientConnector);
        registry.start(ctx);
        Files.delete(keys);
        Files.delete(clientKeys);

        ArgumentCaptor<Runnable> attempts = ArgumentCaptor.forClass(Runnable.class);

        verify(executor, times(2)).schedule(attempts.capture(), anyLong(), eq(MILLISECONDS));
        attempts.getAllValues().forEach(Runnable::run);

        assertEquals(2, failures());

        writeFreshCertificates(keys);
        registry.reload(communication);

        assertEquals(1, failures());
    }

    /** */
    @Test
    public void testAttemptsDoNotInheritCallerContext() throws Exception {
        OperationContextAttribute<String> caller = OperationContextAttribute.newInstance();
        AtomicLong hotReloadTime = new AtomicLong(due());
        BlockingQueue<Optional<String>> attemptCallers = new LinkedBlockingQueue<>();

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(new OperationContextAwareScheduledExecutorService(executor) { });
        when(factory.isHotReloadEnabled()).thenReturn(true);
        when(factory.getHotReloadTime()).thenAnswer(call -> hotReloadTime.get());
        doAnswer(call -> {
            hotReloadTime.set(later());
            attemptCallers.add(Optional.ofNullable(OperationContext.get(caller)));

            return null;
        }).when(factory).hotReload();
        doAnswer(call -> {
            assertEquals("operator", OperationContext.get(caller));
            hotReloadTime.set(due());

            return null;
        }).when(factory).init();

        registry.register("communication", factory);
        registry.start(ctx);

        try (Scope ignored = OperationContext.set(caller, "operator")) {
            registry.reload(factory);

            assertEquals("operator", OperationContext.get(caller));
        }

        scheduled.run();

        assertEquals(Optional.empty(), attemptCallers.remove());
    }

    /** */
    @Test
    public void testStoppingNodeDoesNotStartHotReload() throws Exception {
        SslContextFactory communication = factory(keys);

        writeDueCertificates(keys);
        communication.setHotReloadEnabled(true);
        when(ctx.isStopping()).thenReturn(true);
        registry.register("communication", communication);
        registry.start(ctx);

        executorFactory.verifyNoInteractions();
    }

    /** */
    @Test
    public void testStopInterruptsAttemptCancelsQueuedReloadAndWaits() throws Exception {
        ScheduledExecutorService hotReloadExecutor = useRealExecutor();
        CountDownLatch attemptStarted = new CountDownLatch(1);
        CountDownLatch attemptInterrupted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        CountDownLatch terminationWaitEntered = new CountDownLatch(1);
        BlockingQueue<Future<?>> submissions = new LinkedBlockingQueue<>();

        enableHotReload(factory, due());
        doAnswer(call -> {
            attemptStarted.countDown();

            try {
                assertTrue(allowCompletion.await(30, SECONDS));
            }
            catch (InterruptedException e) {
                attemptInterrupted.countDown();

                assertTrue(allowCompletion.await(30, SECONDS));
            }

            return null;
        }).when(factory).hotReload();
        doAnswer(call -> {
            Future<?> fut = (Future<?>)call.callRealMethod();

            submissions.add(fut);

            return fut;
        }).when(hotReloadExecutor).submit(any(Runnable.class));
        doAnswer(call -> {
            terminationWaitEntered.countDown();

            return call.callRealMethod();
        }).when(hotReloadExecutor).awaitTermination(anyLong(), eq(MILLISECONDS));

        registry.register("communication", factory);

        IgniteInternalFuture<IgniteCheckedException> reload = null;
        IgniteInternalFuture<?> stop = null;
        Future<?> queuedReload = null;

        try {
            registry.start(ctx);

            assertTrue(attemptStarted.await(5, SECONDS));

            reload = runAsync(() -> assertThrows(IgniteCheckedException.class, () -> registry.reload(factory)));
            queuedReload = submissions.poll(5, SECONDS);

            assertNotNull(queuedReload);

            stop = runAsync(registry::stop);

            assertTrue(attemptInterrupted.await(5, SECONDS));
            assertTrue(terminationWaitEntered.await(5, SECONDS));
            assertTrue(reload.get(5_000).getCause() instanceof CancellationException);
        }
        finally {
            allowCompletion.countDown();

            if (queuedReload != null)
                queuedReload.cancel(true);

            if (stop != null)
                stop.get(5_000);

            if (reload != null)
                reload.get(5_000);
        }

        assertTrue(hotReloadExecutor.isTerminated());
        verify(factory, never()).init();
        assertEquals(0, metric("ManualReloadAttempts"));
    }

    /** */
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

    /** */
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

    /** */
    private long failures() {
        return metric("ConsecutiveHotReloadFailures");
    }

    /** */
    private long metric(String name) {
        return metrics.<LongMetric>findMetric(name).value();
    }

    /**
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

    /** @return Factory that reads its key store from {@code keyStore}. */
    private SslContextFactory factory(Path keyStore) {
        SslContextFactory sslFactory = new SslContextFactory();

        sslFactory.setKeyStoreFilePath(keyStore.toString());
        sslFactory.setKeyStorePassword(PASSWORD);
        sslFactory.setTrustStoreFilePath(trust.toString());
        sslFactory.setTrustStorePassword(PASSWORD);

        return sslFactory;
    }

    /** Writes certificates with 10 of their 100 hours left, so their hot reload window has started. */
    private void writeDueCertificates(Path keyStore) throws Exception {
        save(authority.keyStore("node", at(-90), at(10)), keyStore);
    }

    /** Writes certificates whose hot reload window starts in 84 hours. */
    private void writeFreshCertificates(Path keyStore) throws Exception {
        save(authority.keyStore("node", at(-1), at(99)), keyStore);
    }

    /** */
    private static long at(long hours) {
        return System.currentTimeMillis() + HOURS.toMillis(hours);
    }

    /** */
    private ScheduledExecutorService useRealExecutor() {
        ScheduledExecutorService hotReloadExecutor = spy(new IgniteScheduledThreadPoolExecutor("ssl-hot-reload", null, 1));

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(hotReloadExecutor);

        return hotReloadExecutor;
    }
}
