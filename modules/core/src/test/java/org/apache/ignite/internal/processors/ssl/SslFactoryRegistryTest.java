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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.thread.context.OperationContext;
import org.apache.ignite.internal.thread.context.OperationContextAttribute;
import org.apache.ignite.internal.thread.context.Scope;
import org.apache.ignite.internal.thread.pool.IgniteScheduledThreadPoolExecutor;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.spi.metric.LongMetric;
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
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
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
    public void tearDown() {
        registry.stop();
        executorFactory.close();
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
    public void testFailuresIncreaseRetryDelayUntilLimit() {
        doThrow(new IgniteException("Source unavailable")).when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);

        for (int failure = 1; failure <= 8; failure++) {
            long before = System.currentTimeMillis();

            scheduled.run();

            long base = MINUTES.toMillis(Math.min(1L << (failure - 1), 60));

            assertEquals(failure, failures());
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
    }

    /** */
    @Test
    public void testManualReloadReplacesRetryAndResetsFailures() {
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
    public void testFailedManualReloadKeepsRetryAndFailures() {
        doThrow(new IgniteException("Source unavailable")).when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();

        ScheduledFuture<?> retry = pending;
        IgniteException err = new IgniteException("Invalid replacement");

        doThrow(err).when(factory).init();

        assertSame(err, assertThrows(IgniteException.class, () -> registry.reload(factory)));
        assertEquals(1, failures());
        verify(retry, never()).cancel(false);
        verify(executor, times(2)).schedule(any(Runnable.class), anyLong(), eq(MILLISECONDS));
    }

    /** */
    @Test
    public void testMetricKeepsOtherFactoryFailuresAfterReload() {
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
    public void testMetricResetDoesNotCorruptFailureCount() {
        doThrow(new IgniteException("Source unavailable")).doNothing().when(factory).renew();

        registry.register("communication", factory);
        registry.start(ctx);
        scheduled.run();
        metrics.reset();

        assertEquals(1, failures());

        scheduled.run();

        assertEquals(0, failures());
    }

    /** */
    @Test
    public void testRenewalDoesNotInheritCallerContext() throws Exception {
        OperationContextAttribute<String> caller = OperationContextAttribute.newInstance();
        AtomicLong renewalTime = new AtomicLong(System.currentTimeMillis());
        BlockingQueue<Optional<String>> attemptCallers = new LinkedBlockingQueue<>();

        executorFactory.when(() -> IgniteScheduledThreadPoolExecutor.newSingleThreadScheduledExecutor("ssl-renewal", null))
            .thenCallRealMethod();
        when(factory.getRenewalTime()).thenAnswer(call -> renewalTime.get());
        doAnswer(call -> {
            renewalTime.set(System.currentTimeMillis() + HOURS.toMillis(2));
            attemptCallers.add(Optional.ofNullable(OperationContext.get(caller)));

            return null;
        }).when(factory).renew();
        doAnswer(call -> {
            renewalTime.set(System.currentTimeMillis());

            return null;
        }).when(factory).init();

        registry.register("communication", factory);

        try (Scope ignored = OperationContext.set(caller, "operator")) {
            registry.start(ctx);

            assertEquals(Optional.empty(), attemptCallers.poll(10, SECONDS));

            registry.reload(factory);

            assertEquals("operator", OperationContext.get(caller));
        }

        assertEquals(Optional.empty(), attemptCallers.poll(10, SECONDS));
    }

    /** */
    @Test
    public void testStoppingNodeDoesNotStartRenewal() {
        when(ctx.isStopping()).thenReturn(true);
        when(factory.getRenewalTime()).thenReturn(0L);

        registry.register("communication", factory);
        registry.start(ctx);

        verify(factory, never()).init();
        executorFactory.verifyNoInteractions();
    }

    /** */
    @Test
    public void testStoppingWhileWaitingForFactorySkipsRenewal() throws Exception {
        registry.register("communication", factory);
        registry.start(ctx);

        AtomicReference<Thread> renewalThread = new AtomicReference<>();
        IgniteInternalFuture<?> attempt = null;

        try {
            synchronized (factory) {
                attempt = runAsync(() -> {
                    renewalThread.set(Thread.currentThread());
                    scheduled.run();
                });

                long deadline = System.currentTimeMillis() + 5_000;

                assertTrue(waitForCondition(() -> renewalThread.get() != null && renewalThread.get().getState() == Thread.State.BLOCKED,
                    () -> System.currentTimeMillis() < deadline, 10));

                when(ctx.isStopping()).thenReturn(true);
            }
        }
        finally {
            if (attempt != null)
                attempt.get(5_000);
        }

        verify(factory, never()).renew();
        assertEquals(0, failures());
    }

    /** */
    @Test
    public void testStoppingDuringLoadDoesNotCountFailure() {
        doAnswer(call -> {
            when(ctx.isStopping()).thenReturn(true);

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
        return metrics.<LongMetric>findMetric("ConsecutiveRenewalFailures").value();
    }
}
