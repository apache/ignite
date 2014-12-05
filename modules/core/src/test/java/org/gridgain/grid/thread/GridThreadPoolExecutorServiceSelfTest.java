/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.thread;

import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;

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
