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

package org.apache.ignite.internal.processors.query.stat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThreadPoolExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for busy executor.
 */
public class BusyExecutorTest extends GridCommonAbstractTest {
    /** Time enough to start scheduled task in new thread. */
    private static final int TIME_TO_START_THREAD = 300;

    /** Base thread pool. */
    private IgniteThreadPoolExecutor pool;

    /**
     * Start thread pool.
     */
    @Before
    public void createPool() {
        pool = new IgniteThreadPoolExecutor("BusyExecutorPrefix",
            "BusyExecutorTest",
            0,
            2,
            IgniteConfiguration.DFLT_THREAD_KEEP_ALIVE_TIME,
            new LinkedBlockingQueue<>(),
            GridIoPolicy.UNDEFINED,
            new Thread.UncaughtExceptionHandler() {
                @Override public void uncaughtException(Thread t, Throwable e) {
                    fail(e.getMessage());
                }
            });
    }

    /**
     * Stop and check there are no unfinished task.
     */
    @After
    public void stopPool() {
        if (pool != null) {
            List<Runnable> unfinishedTasks = pool.shutdownNow();

            if (!unfinishedTasks.isEmpty())
                fail(String.format("%d BusyExecutorTest tasks cancelled.", unfinishedTasks.size()));
        }
    }

    /**
     * Create BusyExecutor and try to busyRun, execute and submit task to it without activation.
     * Check it won't start even after small amount of time to schedule.
     * Check no cancellable task was even added into the BusyExecutor internals.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testInactiveExecutor() throws Exception {
        BusyExecutor be = new BusyExecutor("testInactiveExecutor", pool, () -> false, c -> log);
        CDLTask task = new CDLTask();
        CDLCancellableTask cancellableTask = new CDLCancellableTask();

        be.submit(cancellableTask);

        assertFalse(be.busyRun(task));

        Thread.sleep(TIME_TO_START_THREAD);
        assertEquals(1, task.started.getCount());

        be.execute(task);

        Thread.sleep(TIME_TO_START_THREAD);
        assertEquals(1, task.started.getCount());

        assertFalse(be.submit(task).get());

        Thread.sleep(TIME_TO_START_THREAD);
        assertEquals(1, task.started.getCount());

        checkNoCancellableTask(be);
    }

    /**
     * Create busy executor, activate it and execute/submit two task into it.
     * Deactivate and check that both tasks were finished.
     *
     * @throws Exception In case of errors.
     */
    @Test
    public void testActivateDeactivate() throws Exception {
        BusyExecutor be = new BusyExecutor("testActivateDeactivate", pool, () -> false, c -> log);
        CDLTask taskExec = new CDLTask();
        CDLTask taskSubmit = new CDLTask();
        CDLCancellableTask cancellableTask = new CDLCancellableTask();

        be.activate();

        be.execute(taskExec);
        CompletableFuture<Boolean> submitFuture = be.submit(taskSubmit);
        be.execute(cancellableTask);

        Thread.sleep(TIME_TO_START_THREAD);
        assertEquals(0, taskExec.started.getCount());
        // Pool can await first task, so second one can be still in quieue here

        IgniteInternalFuture deactivate = GridTestUtils.runAsync(() -> be.deactivate());

        taskExec.finished.countDown();
        taskSubmit.finished.countDown();

        assertTrue(GridTestUtils.waitForCondition(() -> 0 == taskExec.finished.getCount(), TIME_TO_START_THREAD));
        assertTrue(GridTestUtils.waitForCondition(() -> 0 == taskSubmit.finished.getCount(), TIME_TO_START_THREAD));

        deactivate.get(TIME_TO_START_THREAD);

        checkNoCancellableTask(be);
    }

    /**
     * Schecule 100 task by execute + 100 task by submit and check all of them will be processed.
     *
     * @throws InterruptedException In case of errors.
     */
    @Test
    public void testNormalExecution() throws InterruptedException {
        BusyExecutor be = new BusyExecutor("testActivateDeactivate", pool, () -> false, c -> log);
        be.activate();

        CompletableFuture<Boolean> futures[] = new CompletableFuture[100];
        CountDownLatch executed = new CountDownLatch(futures.length * 2);

        for (int i = 0; i < futures.length; i++) {
            futures[i] = be.submit(() -> {
                try {
                    Thread.sleep(42);
                    executed.countDown();
                    be.execute(() -> executed.countDown());
                }
                catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            });
        }

        executed.await(10, TimeUnit.SECONDS);
        checkNoCancellableTask(be);
    }

    /**
     * Check that old tasks won't be started after reactivation.
     *
     * @throws InterruptedException In case of errrors.
     */
    @Test
    public void testReactivationWontStart() throws InterruptedException, IgniteCheckedException {
        BusyExecutor be = new BusyExecutor("testActivateDeactivate", pool, () -> false, c -> log);
        be.activate();

        CDLTask t1 = new CDLTask();
        CDLTask t2 = new CDLTask();
        CDLTask t3 = new CDLTask();

        be.execute(t1);
        be.execute(t2);
        be.execute(t3);

        Thread.sleep(TIME_TO_START_THREAD);

        assertEquals(0, t1.started.getCount());
        assertEquals(1, t3.started.getCount());

        IgniteInternalFuture reactivate = GridTestUtils.runAsync(() -> {
            be.deactivate();
            be.activate();
        });

        assertEquals(1, t3.started.getCount());

        assertTrue(GridTestUtils.waitForCondition(
            () -> !((boolean)GridTestUtils.getFieldValue(be, "active")), TIME_TO_START_THREAD));

        t1.finished.countDown();
        t2.finished.countDown();

        reactivate.get(TIME_TO_START_THREAD);

        Thread.sleep(TIME_TO_START_THREAD);

        assertEquals(1, t3.started.getCount());
        checkNoCancellableTask(be);
        Thread.sleep(TIME_TO_START_THREAD);
    }

    /**
     * Check that old cancellable tasks won't be started after reactivation.
     *
     * @throws InterruptedException In case of errrors.
     */
    @Test
    public void testReactivationCancellableWontStart() throws InterruptedException, IgniteCheckedException {
        BusyExecutor be = new BusyExecutor("testActivateDeactivate", pool, () -> false, c -> log);
        be.activate();

        CDLCancellableFinallizableTask t1 = new CDLCancellableFinallizableTask();
        CDLCancellableFinallizableTask t2 = new CDLCancellableFinallizableTask();
        CDLCancellableFinallizableTask t3 = new CDLCancellableFinallizableTask();

        be.execute(t1);
        be.execute(t2);
        be.execute(t3);

        Thread.sleep(TIME_TO_START_THREAD);

        t1.started.await();
        assertEquals(1, t3.started.getCount());

        GridConcurrentHashSet<CancellableTask> cancellableTasks =
            GridTestUtils.getFieldValue(be, "cancellableTasks");

        assertEquals(3, cancellableTasks.size());

        IgniteInternalFuture reactivte = GridTestUtils.runAsync(() -> {
            be.deactivate();
            be.activate();
        });

        assertEquals(1, t3.started.getCount());

        assertTrue(GridTestUtils.waitForCondition(
            () -> !((boolean)GridTestUtils.getFieldValue(be, "active")), TIME_TO_START_THREAD));

        t1.finished.countDown();
        t2.finished.countDown();

        reactivte.get(TIME_TO_START_THREAD);

        Thread.sleep(TIME_TO_START_THREAD);

        assertEquals(1, t3.started.getCount());

        checkNoCancellableTask(be);
    }

    /**
     * Test there is no cancellable tasks in specified BusyExecutor.
     *
     * @param be BusyExecutor to check tasks in.
     */
    private void checkNoCancellableTask(BusyExecutor be) {
        GridConcurrentHashSet<CancellableTask> cancellableTasks =
            GridTestUtils.getFieldValue(be, "cancellableTasks");

        assertTrue(cancellableTasks.isEmpty());
    }

    /**
     * Test task.
     */
    private class CDLTask implements Runnable {
        /** Task was started. */
        public CountDownLatch started = new CountDownLatch(1);

        /** Task finished. */
        public CountDownLatch finished = new CountDownLatch(1);


        /** {@inheritDoc} */
        @Override public void run() {
            started.countDown();

            try {
                finished.await();
            }
            catch (InterruptedException e) {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Test cancellable task.
     */
    private class CDLCancellableTask extends CDLTask implements CancellableTask {
        /** {@inheritDoc} */
        @Override public void cancel() {
            finished.countDown();
        }
    }

    /**
     * Test cancellable finallizable task (separate cancel and finish counters).
     */
    private class CDLCancellableFinallizableTask extends CDLTask implements CancellableTask {
        /** Task cancelled. */
        public CountDownLatch cancelled = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void cancel() {
            cancelled.countDown();
        }
    }
}
