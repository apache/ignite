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
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.ssl.AbstractSslContextFactory;
import org.apache.ignite.ssl.SslContextFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
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
import static org.mockito.Mockito.doNothing;
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
 * An attempt is an automatic renewal attempt that the registry schedules.
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
    @Before
    public void setUp() {
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
    public void testDisabledFactoryDoesNotStartRenewal() {
        when(factory.isRenewalEnabled()).thenReturn(false);

        registry.register("communication", factory);
        registry.start(ctx);

        executorFactory.verifyNoInteractions();
        assertEquals(0, failures());
        assertEquals(0, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticRenewalAttempts"));
    }

    /** */
    @Test
    public void testStartLoadsAndSchedulesSharedFactoryOnce() {
        when(factory.isRenewalEnabled()).thenReturn(true);
        when(factory.getRenewalTime()).thenReturn(0L, later());

        registry.register("communication", factory);
        registry.register("discovery", factory);
        registry.start(ctx);

        verify(factory).init();
        verify(executor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
        assertTrue(delay > HOURS.toMillis(1));
        assertEquals(0, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticRenewalAttempts"));
    }

    /** */
    @Test
    public void testStartFailsWithoutSchedulingWhenFactoryLoadFails() {
        IgniteException err = new IgniteException("Certificate chain is unavailable");

        enableRenewal(factory, 0);
        doThrow(err).when(factory).init();

        registry.register("communication", factory);

        assertSame(err, assertThrows(IgniteException.class, () -> registry.start(ctx)));
        executorFactory.verifyNoInteractions();
    }

    /** */
    @Test
    public void testManualReloadBeforeStartIsRejectedWithoutLoadingOrCounting() {
        enableRenewal(factory, later());

        registry.register("communication", factory);

        IgniteCheckedException err = assertThrows(IgniteCheckedException.class, () -> registry.reload(factory));

        assertTrue(err.getMessage().contains("has not started"));
        verify(factory, never()).init();

        registry.start(ctx);

        assertEquals(0, metric("ManualReloadAttempts"));
    }

    /** */
    @Test
    public void testFailedAttemptsRetryWithDoublingDelayUpToOneHour() {
        enableRenewal(factory, due());
        doThrow(new IgniteException("Source unavailable")).when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);

        for (int failure = 1; failure <= 8; failure++) {
            long before = System.currentTimeMillis();

            scheduled.run();

            long base = MINUTES.toMillis(Math.min(1L << (failure - 1), 60));

            assertEquals(failure, failures());
            assertEquals(failure, metric("AutomaticRenewalAttempts"));
            assertTrue(delay >= base - (System.currentTimeMillis() - before));
            assertTrue(delay <= base + base / 2);
        }
    }

    /** */
    @Test
    public void testSuccessfulAttemptResetsFailuresAndSchedulesNextRenewal() {
        enableRenewal(factory, due());
        doThrow(new IgniteException("Source unavailable")).doNothing().when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        assertEquals(1, failures());

        when(factory.getRenewalTime()).thenReturn(later());

        scheduled.run();

        assertEquals(0, failures());
        assertTrue(delay > HOURS.toMillis(1));
        assertEquals(2, metric("AutomaticRenewalAttempts"));
    }

    /** */
    @Test
    public void testManualReloadReplacesRetryWithLoadedRenewalTime() throws Exception {
        enableRenewal(factory, due());
        doThrow(new IgniteException("Source unavailable")).when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        assertEquals(1, failures());

        ScheduledFuture<?> retry = pending;

        registry.reload(factory);

        verify(retry).cancel(false);
        assertEquals(0, failures());
        assertTrue(delay <= 0);

        when(factory.getRenewalTime()).thenReturn(later());

        registry.reload(factory);

        verify(factory, times(2)).init();
        assertTrue(delay > HOURS.toMillis(1));
    }

    /** */
    @Test
    public void testManualReloadWithoutRenewalCountsAppliedAndFailedLoads() throws Exception {
        AbstractSslContextFactory genericFactory = mock(AbstractSslContextFactory.class);

        when(factory.isRenewalEnabled()).thenReturn(false);
        doNothing().doThrow(new IgniteException("Source unavailable")).when(factory).init();

        registry.register("communication", factory);
        registry.register("client connector", genericFactory);
        registry.start(ctx);
        registry.reload(factory);
        registry.reload(genericFactory);

        assertThrows(IgniteException.class, () -> registry.reload(factory));
        verify(genericFactory).init();
        assertEquals(3, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticRenewalAttempts"));
    }

    /** */
    @Test
    public void testFailedManualReloadKeepsRetryAndFailures() {
        enableRenewal(factory, due());
        doThrow(new IgniteException("Source unavailable")).when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        ScheduledFuture<?> retry = pending;
        IgniteException err = new IgniteException("Invalid replacement");

        doThrow(err).when(factory).init();

        assertSame(err, assertThrows(IgniteCheckedException.class, () -> registry.reload(factory)).getCause());
        assertEquals(1, failures());
        assertEquals(1, metric("ManualReloadAttempts"));
        verify(retry, never()).cancel(false);
        verify(executor, times(2)).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
    }

    /** */
    @Test
    public void testManualReloadSucceedsWhenNodeStopsAfterLoad() throws Exception {
        enableRenewal(factory, later());

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
        SslContextFactory otherFactory = mock(SslContextFactory.class);

        enableRenewal(factory, due());
        enableRenewal(otherFactory, due());
        doThrow(new IgniteException("Source unavailable")).when(factory).renew();
        doThrow(new IgniteException("Source unavailable")).when(otherFactory).renew();

        registry.register("communication", factory);
        registry.register("client connector", otherFactory);
        registry.start(ctx);

        ArgumentCaptor<Runnable> attempts = ArgumentCaptor.forClass(Runnable.class);

        verify(executor, times(2)).schedule(attempts.capture(), anyLong(), eq(MILLISECONDS));
        attempts.getAllValues().forEach(Runnable::run);

        assertEquals(2, failures());

        registry.reload(factory);

        assertEquals(1, failures());
    }

    /** */
    @Test
    public void testAttemptsDoNotInheritCallerContext() throws Exception {
        OperationContextAttribute<String> caller = OperationContextAttribute.newInstance();
        AtomicLong renewalTime = new AtomicLong(due());
        BlockingQueue<Optional<String>> attemptCallers = new LinkedBlockingQueue<>();

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(new OperationContextAwareScheduledExecutorService(executor) { });
        when(factory.isRenewalEnabled()).thenReturn(true);
        when(factory.getRenewalTime()).thenAnswer(call -> renewalTime.get());
        doAnswer(call -> {
            renewalTime.set(later());
            attemptCallers.add(Optional.ofNullable(OperationContext.get(caller)));

            return null;
        }).when(factory).renew();
        doAnswer(call -> {
            assertEquals("operator", OperationContext.get(caller));
            renewalTime.set(due());

            return null;
        }).when(factory).init();

        registry.register("communication", factory);

        try (Scope ignored = OperationContext.set(caller, "operator")) {
            registry.start(ctx);
        }

        scheduled.run();

        assertEquals(Optional.empty(), attemptCallers.remove());

        try (Scope ignored = OperationContext.set(caller, "operator")) {
            registry.reload(factory);

            assertEquals("operator", OperationContext.get(caller));
        }

        scheduled.run();

        assertEquals(Optional.empty(), attemptCallers.remove());
    }

    /** */
    @Test
    public void testStoppingNodeDoesNotStartRenewal() {
        enableRenewal(factory, due());
        when(ctx.isStopping()).thenReturn(true);

        registry.register("communication", factory);
        registry.start(ctx);

        executorFactory.verifyNoInteractions();
    }

    /** */
    @Test
    public void testStopInterruptsAttemptCancelsQueuedReloadAndWaits() throws Exception {
        ScheduledExecutorService renewalExecutor = useRealExecutor();
        CountDownLatch attemptStarted = new CountDownLatch(1);
        CountDownLatch attemptInterrupted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        CountDownLatch terminationWaitEntered = new CountDownLatch(1);
        BlockingQueue<Future<?>> submissions = new LinkedBlockingQueue<>();

        enableRenewal(factory, due());
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
        }).when(factory).renew();
        doAnswer(call -> {
            Future<?> fut = (Future<?>)call.callRealMethod();

            submissions.add(fut);

            return fut;
        }).when(renewalExecutor).submit(any(Runnable.class));
        doAnswer(call -> {
            terminationWaitEntered.countDown();

            return call.callRealMethod();
        }).when(renewalExecutor).awaitTermination(anyLong(), eq(MILLISECONDS));

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

        assertTrue(renewalExecutor.isTerminated());
        verify(factory, never()).init();
        assertEquals(0, metric("ManualReloadAttempts"));
    }

    /** */
    @Test
    public void testInterruptedManualReloadCancelsQueuedReload() throws Exception {
        enableRenewal(factory, later());

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
        enableRenewal(factory, due());
        doAnswer(call -> {
            when(executor.isShutdown()).thenReturn(true);

            throw new IgniteException("Load interrupted by node stop");
        }).when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        assertEquals(0, failures());
        verify(executor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
    }

    /** */
    private long failures() {
        return metric("ConsecutiveRenewalFailures");
    }

    /** */
    private long metric(String name) {
        return metrics.<LongMetric>findMetric(name).value();
    }

    /**
     * @param factory Factory mock.
     * @param renewalTime Renewal time of the loaded certificates, or {@code 0} if they were loaded without renewal.
     */
    private static void enableRenewal(SslContextFactory factory, long renewalTime) {
        when(factory.isRenewalEnabled()).thenReturn(true);
        when(factory.getRenewalTime()).thenReturn(renewalTime);
    }

    /** @return Renewal time that has already come. */
    private static long due() {
        return System.currentTimeMillis();
    }

    /** @return Renewal time two hours ahead. */
    private static long later() {
        return System.currentTimeMillis() + HOURS.toMillis(2);
    }

    /** */
    private ScheduledExecutorService useRealExecutor() {
        ScheduledExecutorService renewalExecutor = spy(new IgniteScheduledThreadPoolExecutor("ssl-renewal", null, 1));

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor(any(), any()))
            .thenReturn(renewalExecutor);

        return renewalExecutor;
    }
}
