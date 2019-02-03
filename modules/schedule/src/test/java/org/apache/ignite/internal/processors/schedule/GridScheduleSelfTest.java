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

package org.apache.ignite.internal.processors.schedule;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test for task scheduler.
 */
@SuppressWarnings({"ProhibitedExceptionDeclared", "TooBroadScope"})
public class GridScheduleSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 2;

    /** Custom thread name. */
    private static final String CUSTOM_THREAD_NAME = "custom-async-test";

    /** */
    private static AtomicInteger execCntr = new AtomicInteger(0);

    /** Custom executor. */
    private ExecutorService exec;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        execCntr.set(0);
        exec = Executors.newSingleThreadExecutor(new ThreadFactory() {
            @Override public Thread newThread(@NotNull Runnable r) {
                Thread t = new Thread(r);

                t.setName(CUSTOM_THREAD_NAME);

                return t;
            }
        });
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        U.shutdownNow(getClass(), exec, log);

        exec = null;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRunLocal() throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteFuture<?> fut = grid(i).scheduler().runLocal(new TestRunnable());

            assert fut.get() == null;

            assertEquals(1, execCntr.getAndSet(0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCallLocal() throws Exception {
        for (int i = 0; i < NODES_CNT; i++) {
            IgniteFuture<?> fut = grid(i).scheduler().callLocal(new TestCallable());

            assertEquals(1, fut.get());

            assertEquals(1, execCntr.getAndSet(0));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScheduleRunnable() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);

        SchedulerFuture<?> fut = null;

        long freq = 60; // 1 minute frequency.
        long delay = 2; // 2 seconds delay.

        try {
            // Execute 2 times after 2 seconds delay every minute.
            fut = grid(0).scheduler().scheduleLocal(
                new Runnable() {
                    @Override public void run() {
                        latch.countDown();

                        info(">>> EXECUTING SCHEDULED RUNNABLE! <<<");
                    }
                },
                "{2, 2} * * * * *");

            assert !fut.isDone();
            assert !fut.isCancelled();
            assert fut.last() == null;

            final AtomicInteger notifyCnt = new AtomicInteger();

            fut.listen(new CI1<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> e) {
                    notifyCnt.incrementAndGet();
                }
            });

            final SchedulerFuture<?> fut0 = fut;

            //noinspection ThrowableNotThrown
            assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    fut0.listenAsync(new IgniteInClosure<IgniteFuture<?>>() {
                        @Override public void apply(IgniteFuture<?> fut) {
                            // No-op
                        }
                    }, null);

                    return null;
                }
            }, NullPointerException.class, null);

            fut.listenAsync(new IgniteInClosure<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> fut) {
                    assertEquals(Thread.currentThread().getName(), CUSTOM_THREAD_NAME);

                    notifyCnt.incrementAndGet();
                }
            }, exec);

            //noinspection ThrowableNotThrown
            assertThrows(log, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    fut0.chainAsync(new IgniteClosure<IgniteFuture<?>, String>() {
                        @Override public String apply(IgniteFuture<?> fut) {
                            // No-op

                            return null;
                        }
                    }, null);

                    return null;
                }
            }, NullPointerException.class, null);

            IgniteFuture<String> chained1 = fut.chainAsync(new IgniteClosure<IgniteFuture<?>, String>() {
                @Override public String apply(IgniteFuture<?> fut) {
                    assertEquals(Thread.currentThread().getName(), CUSTOM_THREAD_NAME);

                    fut.get();

                    return "done-custom";
                }
            }, exec);

            long timeTillRun = freq + delay;

            info("Going to wait for the first run: " + timeTillRun);

            latch.await(timeTillRun, SECONDS);

            assertEquals(0, latch.getCount());

            assert !fut.isDone();
            assert !fut.isCancelled();
            assert fut.last() == null;
            assertFalse(chained1.isDone());

            info("Going to wait for 2nd run: " + timeTillRun);

            // Wait until scheduling will be finished.
            Thread.sleep(timeTillRun * 1000);

            assert fut.isDone();
            assert notifyCnt.get() == 2 * 2;
            assert !fut.isCancelled();
            assert fut.last() == null;

            assertEquals("done-custom", chained1.get());

            assertTrue(chained1.isDone());
        }
        finally {
            assert fut != null;

            fut.cancel();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testScheduleCallable() throws Exception {
        SchedulerFuture<Integer> fut = null;

        long freq = 60; // 1 minute frequency.
        long delay = 2; // 2 seconds delay.

        try {
            fut = grid(0).scheduler().scheduleLocal(new Callable<Integer>() {
                private int cnt;

                @Override public Integer call() {
                    info(">>> EXECUTING SCHEDULED CALLABLE! <<<");

                    return ++cnt;
                }
            }, "{1, 2} * * * * *");

            final AtomicInteger notifyCnt = new AtomicInteger();

            fut.listen(new CI1<IgniteFuture<?>>() {
                @Override public void apply(IgniteFuture<?> e) {
                    notifyCnt.incrementAndGet();
                }
            });

            assert !fut.isDone();
            assert !fut.isCancelled();
            assert fut.last() == null;

            long timeTillRun = freq + delay;

            info("Going to wait for the 1st run: " + timeTillRun);

            assertEquals((Integer)1, fut.get(timeTillRun, SECONDS));
            assertEquals((Integer)1, fut.last());

            assert !fut.isDone();
            assert !fut.isCancelled();

            info("Going to wait for the 2nd run: " + timeTillRun);

            assertEquals((Integer)2, fut.get(timeTillRun, SECONDS));
            assertEquals((Integer)2, fut.last());

            assert fut.isDone();
            assert !fut.isCancelled();
        }
        finally {
            assert fut != null;

            fut.cancel();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRunnableCancel() throws Exception {
        SchedulerFuture fut = null;

        final GridTuple<Integer> tpl = new GridTuple<>(0);

        try {
            fut = grid(0).scheduler().scheduleLocal(new Runnable() {
                @Override public void run() {
                    tpl.set(tpl.get() + 1);
                }
            }, "{1, *} * * * * *");

            assertEquals(Integer.valueOf(0), tpl.get());

            fut.cancel();

            assert fut.isCancelled();
            assert fut.isDone();

            assertEquals(Integer.valueOf(0), tpl.get());

            try {
                fut.get();

                fail("IgniteException must have been thrown");
            }
            catch (IgniteException e) {
                info("Caught expected exception: " + e);
            }

            try {
                fut.get(500, SECONDS);

                fail("IgniteException must have been thrown");
            }
            catch (IgniteException e) {
                info("Caught expected exception: " + e);
            }
        }
        finally {
            assert fut != null;

            if (!fut.isCancelled())
                fut.cancel();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInvalidPatterns() throws Exception {
        Runnable run = new Runnable() {
            @Override public void run() {
                // No-op.
            }
        };

        try {
            // Invalid delay.
            grid(0).scheduler().scheduleLocal(run, "{sdf, *} * * * * *").get();

            fail("IgniteException must have been thrown");
        }
        catch (IgniteException e) {
            info("Caught expected exception: " + e);
        }

        try {
            // Invalid delay.
            grid(0).scheduler().scheduleLocal(run, "{**, *} * * * * *").get();

            fail("IgniteException must have been thrown");
        }
        catch (IgniteException e) {
            info("Caught expected exception: " + e);
        }

        try {
            // Invalid number of executions.
            grid(0).scheduler().scheduleLocal(run, "{1, ghd} * * * * *").get();

            fail("IgniteException must have been thrown");
        }
        catch (IgniteException e) {
            info("Caught expected exception: " + e);
        }

        try {
            // Number of executions in pattern must be greater than zero or equal to "*".
            grid(0).scheduler().scheduleLocal(run, "{*, 0} * * * * *").get();

            fail("IgniteException must have been thrown");
        }
        catch (IgniteException e) {
            info("Caught expected exception: " + e);
        }

        try {
            // Invalid cron expression.
            grid(0).scheduler().scheduleLocal(run, "{2, 6} * * * * * * * * * *").get();

            fail("IgniteException must have been thrown");
        }
        catch (IgniteException e) {
            info("Caught expected exception: " + e);
        }

        try {
            // Invalid both delay and number of calls.
            grid(0).scheduler().scheduleLocal(run, "{-2, -6} * * * * *").get();

            fail("IgniteException must have been thrown");
        }
        catch (IgniteException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNoNextExecutionTime() throws Exception {
        Callable<Integer> run = new Callable<Integer>() {
            @Override public Integer call() {
                return 1;
            }
        };

        SchedulerFuture<Integer> future = grid(0).scheduler().scheduleLocal(run, "{55} 53 3/5 * * *");

        try {
            future.get();

            fail("Accepted wrong cron expression");
        }
        catch (IgniteException e) {
            assertTrue(e.getMessage().startsWith("Invalid cron expression in schedule pattern"));
        }

        assertTrue(future.isDone());

        assertEquals(0, future.nextExecutionTime());

        assertEquals(0, future.nextExecutionTimes(2, System.currentTimeMillis()).length);
    }

    /**
     * Waits until method {@link org.apache.ignite.scheduler.SchedulerFuture#last()} returns not a null value. Tries to
     * call specified number of attempts with 100ms interval between them.
     *
     * @param fut Schedule future to call method on.
     * @param attempts Max number of attempts to try.
     * @return {@code true} if wait is successful, {@code false} if attempts are exhausted.
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    private boolean waitForLastResult(SchedulerFuture<Integer> fut, int attempts) throws Exception {
        assert fut != null;
        assert attempts > 0;

        boolean success = false;

        for (int i = 0; i < attempts; i++) {
            if (fut.last() != null) {

                success = true;

                break;
            }

            Thread.sleep(100);
        }

        return success;
    }

    /**
     * Test runnable job.
     */
    private static class TestRunnable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** @{inheritDoc} */
        @Override public void run() {
            log.info("Runnable job executed on node: " + ignite.cluster().localNode().id());

            assert ignite != null;

            execCntr.incrementAndGet();
        }
    }

    /**
     * Test callable job.
     */
    private static class TestCallable implements IgniteCallable<Integer> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Integer call() {
            log.info("Callable job executed on node: " + ignite.cluster().localNode().id());

            assert ignite != null;

            return execCntr.incrementAndGet();
        }
    }
}
