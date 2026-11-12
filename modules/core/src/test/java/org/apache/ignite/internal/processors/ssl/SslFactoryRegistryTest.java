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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
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
import org.mockito.InOrder;
import org.mockito.MockedStatic;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** */
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
        when(factory.isRenewalEnabled()).thenReturn(true);
        when(factory.getRenewalTime()).thenReturn(System.currentTimeMillis());
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
        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor("ssl-renewal", null))
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
    public void testSharedFactoryIsScheduledOnce() {
        registry.register("communication", factory);
        registry.register("discovery", factory);
        registry.start(ctx);

        verify(executor).schedule(any(Runnable.class), eq(0L), eq(MILLISECONDS));
    }

    /** */
    @Test
    public void testEnablingRenewalOnRestartInitializesSharedFactoryOnce() {
        when(factory.getRenewalTime()).thenReturn(0L, System.currentTimeMillis() + HOURS.toMillis(2));

        registry.register("communication", factory);
        registry.register("discovery", factory);
        registry.start(ctx);

        verify(factory).init();
        assertTrue(delay > HOURS.toMillis(1));
        assertEquals(0, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticRenewalAttempts"));
    }

    /** */
    @Test
    public void testEnablingRenewalRejectsInvalidMaterialAtStartup() {
        when(factory.getRenewalTime()).thenReturn(0L);
        doThrow(new IgniteException("Certificate chain is unavailable")).when(factory).init();

        registry.register("communication", factory);

        assertThrows(IgniteException.class, () -> registry.start(ctx));
        executorFactory.verifyNoInteractions();
    }

    /** */
    @Test
    public void testManualReloadBeforeRenewalStartDoesNotLoadFactory() {
        registry.register("communication", factory);

        assertThrows(IgniteCheckedException.class, () -> registry.reload(factory));
        verify(factory, never()).init();

        registry.start(ctx);

        assertEquals(0, metric("ManualReloadAttempts"));
    }

    /** */
    @Test
    public void testManualReloadFollowsInitialScheduling() throws Exception {
        ScheduledExecutorService renewalExecutor = useRealExecutor();
        AtomicReference<Thread> caller = new AtomicReference<>();
        AtomicReference<IgniteInternalFuture<?>> reload = new AtomicReference<>();

        when(factory.getRenewalTime()).thenReturn(System.currentTimeMillis() + HOURS.toMillis(2));

        registry.register("communication", factory);

        AbstractSslContextFactory registeredFactory = (AbstractSslContextFactory)registry.factories().get("communication");

        doAnswer(call -> {
            IgniteInternalFuture<?> fut = runAsync(() -> {
                caller.set(Thread.currentThread());

                registry.reload(registeredFactory);
            });

            reload.set(fut);

            long deadline = System.nanoTime() + SECONDS.toNanos(5);

            assertTrue(waitForCondition(() -> {
                if (fut.isDone())
                    return true;

                Thread thread = caller.get();
                ThreadInfo info = thread == null ? null : ManagementFactory.getThreadMXBean().getThreadInfo(thread.getId());

                return info != null && info.getThreadState() == Thread.State.BLOCKED &&
                    info.getLockInfo().getIdentityHashCode() == System.identityHashCode(registry);
            }, () -> System.nanoTime() < deadline));

            return call.callRealMethod();
        }).when(renewalExecutor).execute(any(Runnable.class));

        try {
            registry.start(ctx);
        }
        finally {
            if (reload.get() != null)
                reload.get().get(5_000);
        }

        InOrder order = inOrder(renewalExecutor, factory);

        order.verify(renewalExecutor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
        order.verify(factory).init();
        order.verify(renewalExecutor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
    }

    /** */
    @Test
    public void testFailuresIncreaseRetryDelayUntilLimit() {
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
    public void testNormalReturnResetsFailuresAndReadsNextWindow() {
        doThrow(new IgniteException("Source unavailable")).doNothing().when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        assertEquals(1, failures());

        when(factory.getRenewalTime()).thenReturn(System.currentTimeMillis() + HOURS.toMillis(2));

        scheduled.run();

        assertEquals(0, failures());
        assertTrue(delay > HOURS.toMillis(1));
        assertEquals(2, metric("AutomaticRenewalAttempts"));
    }

    /** */
    @Test
    public void testRenewalLeavingCertificatesDueIsFailure() {
        IgniteLogger log = mock(IgniteLogger.class);

        when(ctx.log(SslFactoryRegistry.class)).thenReturn(log);

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        verify(factory).renew();
        assertEquals(1, failures());
        verify(log).warning(startsWith("TLS renewal failed [transports=communication, nextAttempt="), any(IgniteException.class));
    }

    /** */
    @Test
    public void testManualReloadInCurrentWindowAttemptsImmediately() throws Exception {
        registry.register("communication", factory);
        registry.start(ctx);

        ScheduledFuture<?> previous = pending;

        registry.reload(factory);

        verify(factory).init();
        verify(previous).cancel(false);
        assertEquals(0, delay);
    }

    /** */
    @Test
    public void testManualReloadReplacesRetryAndResetsFailures() throws Exception {
        doThrow(new IgniteException("Source unavailable")).when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        assertEquals(1, failures());

        ScheduledFuture<?> retry = pending;

        when(factory.getRenewalTime()).thenReturn(System.currentTimeMillis() + HOURS.toMillis(2));

        registry.reload(factory);

        verify(retry).cancel(false);
        assertEquals(0, failures());
        assertTrue(delay > HOURS.toMillis(1));
    }

    /** */
    @Test
    public void testManualAttemptMetricCountsAppliedAndFailedLoads() throws Exception {
        when(factory.isRenewalEnabled()).thenReturn(false);
        doNothing().doThrow(new IgniteException("Source unavailable")).when(factory).init();

        registry.register("communication", factory);
        registry.start(ctx);
        registry.reload(factory);

        assertThrows(IgniteException.class, () -> registry.reload(factory));
        assertEquals(2, metric("ManualReloadAttempts"));
        assertEquals(0, metric("AutomaticRenewalAttempts"));
    }

    /** */
    @Test
    public void testManualReloadInitializesGenericFactory() throws Exception {
        AbstractSslContextFactory genericFactory = mock(AbstractSslContextFactory.class);

        registry.register("communication", genericFactory);
        registry.start(ctx);

        registry.reload(genericFactory);

        verify(genericFactory).init();
        assertEquals(1, metric("ManualReloadAttempts"));
    }

    /** */
    @Test
    public void testFailedManualReloadKeepsRetryAndFailures() {
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
    public void testAutomaticFailureAfterManualReloadRemainsCounted() throws Exception {
        doThrow(new IgniteException("Source unavailable")).when(factory).renew();
        when(executor.submit(any(Runnable.class))).thenAnswer(call -> {
            FutureTask<?> reload = new FutureTask<>(call.getArgument(0), null);

            reload.run();
            scheduled.run();

            return reload;
        });

        registry.register("communication", factory);
        registry.start(ctx);

        doAnswer(call -> {
            scheduled.run();
            call.<Runnable>getArgument(0).run();

            return null;
        }).when(executor).execute(any(Runnable.class));

        long before = System.currentTimeMillis();

        registry.reload(factory);

        verify(factory).renew();
        assertEquals(1, metric("AutomaticRenewalAttempts"));
        assertEquals(1, failures());
        assertTrue(delay >= MINUTES.toMillis(1) - (System.currentTimeMillis() - before));
    }

    /** */
    @Test
    public void testStoppingAfterManualLoadDoesNotReportFailure() throws Exception {
        registry.register("communication", factory);
        registry.start(ctx);

        doAnswer(call -> {
            when(executor.isShutdown()).thenReturn(true);

            return null;
        }).when(factory).init();
        doThrow(new RejectedExecutionException()).when(executor).execute(any(Runnable.class));
        doThrow(new RejectedExecutionException()).when(executor).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));

        registry.reload(factory);

        verify(factory).init();
        assertEquals(1, metric("ManualReloadAttempts"));
    }

    /** */
    @Test
    public void testMetricKeepsOtherFactoryFailuresAfterReload() throws Exception {
        SslContextFactory otherFactory = mock(SslContextFactory.class);

        when(otherFactory.isRenewalEnabled()).thenReturn(true);
        when(otherFactory.getRenewalTime()).thenReturn(1L);
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
    public void testRenewalDoesNotInheritCallerContext() throws Exception {
        OperationContextAttribute<String> caller = OperationContextAttribute.newInstance();
        AtomicLong renewalTime = new AtomicLong(System.currentTimeMillis());
        BlockingQueue<Optional<String>> attemptCallers = new LinkedBlockingQueue<>();

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor("ssl-renewal", null))
            .thenReturn(new OperationContextAwareScheduledExecutorService(executor) { });
        when(factory.getRenewalTime()).thenAnswer(call -> renewalTime.get());
        doAnswer(call -> {
            renewalTime.set(System.currentTimeMillis() + HOURS.toMillis(2));
            attemptCallers.add(Optional.ofNullable(OperationContext.get(caller)));

            return null;
        }).when(factory).renew();
        doAnswer(call -> {
            assertEquals("operator", OperationContext.get(caller));
            renewalTime.set(System.currentTimeMillis());

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
        when(ctx.isStopping()).thenReturn(true);

        registry.register("communication", factory);
        registry.start(ctx);

        executorFactory.verifyNoInteractions();
    }

    /** */
    @Test
    public void testStopInterruptsAndWaitsForRunningAttempt() throws Exception {
        CountDownLatch attemptStarted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        CountDownLatch terminationWaitEntered = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        ScheduledExecutorService renewalExecutor = spy(new IgniteScheduledThreadPoolExecutor("ssl-renewal", null, 1));

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor("ssl-renewal", null))
            .thenReturn(renewalExecutor);
        doAnswer(call -> {
            terminationWaitEntered.countDown();

            return call.callRealMethod();
        }).when(renewalExecutor).awaitTermination(anyLong(), eq(MILLISECONDS));
        doAnswer(call -> {
            attemptStarted.countDown();

            try {
                assertTrue(allowCompletion.await(30, SECONDS));
            }
            catch (InterruptedException e) {
                interrupted.set(true);

                assertTrue(allowCompletion.await(30, SECONDS));
            }

            if (Thread.currentThread().isInterrupted())
                interrupted.set(true);

            return null;
        }).when(factory).renew();

        registry.register("communication", factory);

        IgniteInternalFuture<?> stop = null;

        try {
            registry.start(ctx);

            assertTrue(attemptStarted.await(5, SECONDS));

            stop = runAsync(() -> {
                registry.stop();

                assertTrue(renewalExecutor.isTerminated());
            });

            assertTrue(terminationWaitEntered.await(5, SECONDS));
        }
        finally {
            allowCompletion.countDown();

            if (stop != null)
                stop.get(5_000);
        }

        assertTrue(interrupted.get());
    }

    /** */
    @Test
    public void testStopInterruptsAndWaitsForManualReload() throws Exception {
        ScheduledExecutorService renewalExecutor = useRealExecutor();
        CountDownLatch loadStarted = new CountDownLatch(1);
        CountDownLatch loadInterrupted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        CountDownLatch terminationWaitEntered = new CountDownLatch(1);

        when(factory.getRenewalTime()).thenReturn(System.currentTimeMillis() + HOURS.toMillis(2));
        doAnswer(call -> {
            terminationWaitEntered.countDown();

            return call.callRealMethod();
        }).when(renewalExecutor).awaitTermination(anyLong(), eq(MILLISECONDS));
        doAnswer(call -> {
            loadStarted.countDown();

            try {
                assertTrue(allowCompletion.await(30, SECONDS));
            }
            catch (InterruptedException e) {
                loadInterrupted.countDown();

                assertTrue(allowCompletion.await(30, SECONDS));
            }

            return null;
        }).when(factory).init();

        registry.register("communication", factory);
        registry.start(ctx);

        IgniteInternalFuture<?> stop = null;
        IgniteInternalFuture<?> reload = runAsync(() -> registry.reload(factory));

        try {
            assertTrue(loadStarted.await(5, SECONDS));

            stop = runAsync(registry::stop);

            assertTrue(loadInterrupted.await(5, SECONDS));
            assertTrue(terminationWaitEntered.await(5, SECONDS));
        }
        finally {
            allowCompletion.countDown();

            if (stop != null)
                stop.get(5_000);

            reload.get(5_000);
        }
    }

    /** */
    @Test
    public void testStopCancelsQueuedReload() throws Exception {
        ScheduledExecutorService renewalExecutor = useRealExecutor();
        CountDownLatch attemptStarted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        BlockingQueue<Future<?>> submissions = new LinkedBlockingQueue<>();

        doAnswer(call -> {
            attemptStarted.countDown();
            assertTrue(allowCompletion.await(10, SECONDS));

            return null;
        }).when(factory).renew();
        doAnswer(call -> {
            Future<?> fut = (Future<?>)call.callRealMethod();

            submissions.add(fut);

            return fut;
        }).when(renewalExecutor).submit(any(Runnable.class));

        registry.register("communication", factory);

        IgniteInternalFuture<IgniteCheckedException> reload = null;
        Future<?> queuedReload = null;

        try {
            registry.start(ctx);

            assertTrue(attemptStarted.await(5, SECONDS));

            reload = runAsync(() -> assertThrows(IgniteCheckedException.class, () -> registry.reload(factory)));
            queuedReload = submissions.poll(5, SECONDS);

            assertNotNull(queuedReload);

            registry.stop();

            assertTrue(reload.get(5_000).getCause() instanceof CancellationException);
            verify(factory, never()).init();
            assertEquals(0, metric("ManualReloadAttempts"));
        }
        finally {
            allowCompletion.countDown();

            if (queuedReload != null)
                queuedReload.cancel(true);

            if (reload != null)
                reload.get(5_000);
        }
    }

    /** */
    @Test
    public void testInterruptedCallerCancelsQueuedReload() throws Exception {
        ScheduledExecutorService renewalExecutor = useRealExecutor();
        CountDownLatch attemptStarted = new CountDownLatch(1);
        CountDownLatch allowCompletion = new CountDownLatch(1);
        CountDownLatch reloadSubmitted = new CountDownLatch(1);
        AtomicReference<Thread> caller = new AtomicReference<>();
        AtomicLong renewalTime = new AtomicLong(System.currentTimeMillis());

        when(factory.getRenewalTime()).thenAnswer(call -> renewalTime.get());
        doAnswer(call -> {
            attemptStarted.countDown();
            assertTrue(allowCompletion.await(10, SECONDS));
            renewalTime.set(System.currentTimeMillis() + HOURS.toMillis(2));

            return null;
        }).when(factory).renew();
        doAnswer(call -> {
            Object fut = call.callRealMethod();

            reloadSubmitted.countDown();

            return fut;
        }).when(renewalExecutor).submit(any(Runnable.class));

        registry.register("communication", factory);

        IgniteInternalFuture<Boolean> reload = null;

        try {
            registry.start(ctx);

            assertTrue(attemptStarted.await(5, SECONDS));

            reload = runAsync(() -> {
                caller.set(Thread.currentThread());

                assertThrows(IgniteInterruptedCheckedException.class, () -> registry.reload(factory));

                return Thread.currentThread().isInterrupted();
            });

            assertTrue(reloadSubmitted.await(5, SECONDS));

            caller.get().interrupt();

            assertTrue(reload.get(5_000));

            allowCompletion.countDown();
            renewalExecutor.submit(() -> { }).get(5, SECONDS);

            verify(factory, never()).init();
            assertEquals(0, metric("ManualReloadAttempts"));
        }
        finally {
            allowCompletion.countDown();

            if (caller.get() != null)
                caller.get().interrupt();

            if (reload != null)
                reload.get(5_000);
        }
    }

    /** */
    @Test
    public void testStoppingDuringLoadDoesNotCountFailure() {
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

    /** */
    private ScheduledExecutorService useRealExecutor() {
        ScheduledExecutorService renewalExecutor = spy(new IgniteScheduledThreadPoolExecutor("ssl-renewal", null, 1));

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor("ssl-renewal", null))
            .thenReturn(renewalExecutor);

        return renewalExecutor;
    }
}
