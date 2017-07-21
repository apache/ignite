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

package org.apache.ignite.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Test for {@link IgniteThreadPoolExecutor}.
 */
@GridCommonTest(group = "Utils")
public class GridThreadPoolExecutorServiceSelfTest extends GridCommonAbstractTest {
    /** Thread count. */
    private static final int THREAD_CNT = 40;

    /**
     * @throws Exception If failed.
     */
    public void testSingleThreadExecutor() throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor();

        exec.submit(new InterruptingRunnable()).get();

        // Thread is interrupted but Thread.interrupted() is called in AbstractQueuedSynchronizer.acquireInterruptibly
        // when blockingQueue wants to get the new task (see ThreadPoolExecutor.getTask()).
        // This will reset the interrupted flag. Any subsequent calls to Thread.currentThread.isInterrupted()
        // will return false.

        Future<Boolean> fut = exec.submit(new IsInterruptedAssertionCallable());

        assert !fut.get() : "Expecting the executorService to reset the interrupted flag when reinvoking the thread";

        exec.shutdown();
        assert exec.awaitTermination(30, SECONDS);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleGridThreadExecutor() throws Exception {
        ExecutorService exec = Executors.newSingleThreadExecutor(new IgniteThreadFactory("gridName"));

        exec.submit(new InterruptingRunnable()).get();

        Future<Boolean> fut = exec.submit(new IsInterruptedAssertionCallable());

        assert !fut.get() : "Expecting the executorService to reset the interrupted flag when reinvoking the thread";

        // Thread is interrupted but Thread.interrupted() is called in AbstractQueuedSynchronizer.acquireInterruptibly
        // when blockingQueue wants to get the new task (see ThreadPoolExecutor.getTask()).
        // This will reset the interrupted flag but not the one from GridThread. Any subsequent calls to
        // Thread.currentThread.isInterrupted() will return true;

        exec.shutdown();
        assert exec.awaitTermination(30, SECONDS);
    }

    /**
     * @throws ExecutionException If failed.
     */
    public void testGridThreadPoolExecutor() throws Exception {
        IgniteThreadPoolExecutor exec = new IgniteThreadPoolExecutor(1, 1, 0, new LinkedBlockingQueue<Runnable>());

        exec.submit(new InterruptingRunnable()).get();

        Future<Boolean> fut = exec.submit(new IsInterruptedAssertionCallable());

        assert !fut.get() : "Expecting the executor to reset the interrupted flag when reinvoking the thread";

        exec.shutdown();
        assert exec.awaitTermination(30, SECONDS);
    }

    /**
     * @throws ExecutionException If failed.
     */
    public void testGridThreadPoolExecutorRejection() throws Exception {
        IgniteThreadPoolExecutor exec = new IgniteThreadPoolExecutor(1, 1, 0, new LinkedBlockingQueue<Runnable>());

        for (int i = 0; i < 10; i++)
            exec.submit(new TestRunnable());

        exec.shutdown();
        assert exec.awaitTermination(30, SECONDS);
    }

    /**
     * @throws ExecutionException If failed.
     */
    public void testGridThreadPoolExecutorPrestartCoreThreads() throws Exception {
        final AtomicInteger curPoolSize = new AtomicInteger();

        final CountDownLatch startLatch = new CountDownLatch(THREAD_CNT);
        final CountDownLatch stopLatch = new CountDownLatch(THREAD_CNT);

        IgniteThreadPoolExecutor exec = new IgniteThreadPoolExecutor(
            THREAD_CNT, THREAD_CNT, Long.MAX_VALUE,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactory() {
                @Override public Thread newThread(final Runnable r) {
                    return new Thread(new Runnable() {
                        @Override public void run() {
                            curPoolSize.incrementAndGet();

                            startLatch.countDown();

                            try {
                                r.run();
                            }
                            finally {
                                curPoolSize.decrementAndGet();

                                stopLatch.countDown();
                            }
                        }
                    });
                }
            },
            null
        );

        assert exec.prestartAllCoreThreads() == THREAD_CNT;

        startLatch.await();

        assert curPoolSize.get() == THREAD_CNT;

        exec.shutdown();

        assert exec.awaitTermination(30, SECONDS);

        stopLatch.await();

        assert curPoolSize.get() == 0;
    }

    /**
     *
     */
    private static final class IsInterruptedAssertionCallable implements Callable<Boolean> {
        @Override public Boolean call() throws Exception {
            return Thread.currentThread().isInterrupted();
        }
    }

    /**
     *
     */
    private static final class InterruptingRunnable implements Runnable {
        @Override public void run() {
            Thread.currentThread().interrupt();
        }
    }

    /**
     *
     */
    private final class TestRunnable implements Runnable {
        @Override public void run() {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                info("Got interrupted exception while sleeping: " + e);
            }
        }
    }
}