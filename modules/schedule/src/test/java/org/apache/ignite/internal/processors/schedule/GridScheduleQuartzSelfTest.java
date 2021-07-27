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

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.scheduler.SchedulerFuture;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** Test for Quartz Based Scheduler */
public class GridScheduleQuartzSelfTest extends GridCommonAbstractTest {
    /** Name of the job */
    private static final String JOB_NAME = "job1";
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
                    }, JOB_NAME, new Date(), 2, 2, 2);

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

            // The way we scheduled, we should have gotten one misfire
            assert notifyCnt.get() == 2 * 3;
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
    public void testRunnableCancel() throws Exception {
        SchedulerFuture fut = null;

        final GridTuple<Integer> tpl = new GridTuple<>(0);

        try {
            fut = grid(0).scheduler().scheduleLocal(new Runnable() {
                @Override public void run() {
                    tpl.set(tpl.get() + 1);
                }
            }, "job1", new Date(), 2, 2, 2);

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
}
