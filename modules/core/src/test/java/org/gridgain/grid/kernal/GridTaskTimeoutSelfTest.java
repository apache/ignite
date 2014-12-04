/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskTimeoutSelfTest extends GridCommonAbstractTest {
    /** Number of jobs each task spawns. */
    private static final int SPLIT_COUNT = 1;

    /** Timeout for task execution in milliseconds. */
    private static final long TIMEOUT = 1000;

    /** Number of worker threads. */
    private static final int N_THREADS = 16;

    /** Test execution period in milliseconds. */
    private static final int PERIOD = 10000;

    /** */
    public GridTaskTimeoutSelfTest() {
        super(true);
    }

    /**
     * @param execId Execution ID.
     */
    private void checkTimedOutEvents(final GridUuid execId) {
        Ignite ignite = G.grid(getTestGridName());

        Collection<GridEvent> evts = ignite.events().localQuery(new PE() {
            @Override public boolean apply(GridEvent evt) {
                return ((GridTaskEvent) evt).taskSessionId().equals(execId);
            }
        }, EVT_TASK_TIMEDOUT);

        assert evts.size() == 1 : "Invalid number of timed out tasks: " + evts.size();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousTimeout() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridTaskTimeoutTestTask.class, GridTaskTimeoutTestTask.class.getClassLoader());

        GridComputeTaskFuture<?> fut = executeAsync(ignite.compute().withTimeout(TIMEOUT),
            GridTaskTimeoutTestTask.class.getName(), null);

        try {
            fut.get();

            assert false : "GridComputeTaskTimeoutException was not thrown (synchronous apply)";
        }
        catch (GridComputeTaskTimeoutException e) {
            info("Received expected timeout exception (synchronous apply): " + e);
        }

        Thread.sleep(TIMEOUT + 500);

        checkTimedOutEvents(fut.getTaskSession().getId());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsynchronousTimeout() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().localDeployTask(GridTaskTimeoutTestTask.class, GridTaskTimeoutTestTask.class.getClassLoader());

        GridComputeTaskFuture<?> fut = executeAsync(ignite.compute().withTimeout(TIMEOUT),
            GridTaskTimeoutTestTask.class.getName(), null);

        // Allow timed out events to be executed.
        Thread.sleep(TIMEOUT + 500);

        checkTimedOutEvents(fut.getTaskSession().getId());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousTimeoutMultithreaded() throws Exception {
        final Ignite ignite = G.grid(getTestGridName());

        final AtomicBoolean finish = new AtomicBoolean();

        final AtomicInteger cnt = new AtomicInteger();

        final CountDownLatch finishLatch = new CountDownLatch(N_THREADS);

        new Thread(new Runnable() {
            @Override public void run() {
                try {
                    Thread.sleep(PERIOD);

                    info("Stopping test.");

                    finish.set(true);
                }
                catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        }).start();

        multithreaded(new Runnable() {
            @SuppressWarnings("InfiniteLoopStatement")
            @Override public void run() {
                while (!finish.get()) {
                    try {
                        GridComputeTaskFuture<?> fut = executeAsync(
                            ignite.compute().withTimeout(TIMEOUT), GridTaskTimeoutTestTask.class.getName(), null);

                        fut.get();

                        assert false : "Task has not been timed out. Future: " + fut;
                    }
                    catch (GridComputeTaskTimeoutException ignored) {
                        // Expected.
                    }
                    catch (GridException e) {
                        throw new IllegalStateException(e); //shouldn't happen
                    }
                    finally {
                        int cnt0 = cnt.incrementAndGet();

                        if (cnt0 % 100 == 0)
                            info("Tasks finished: " + cnt0);
                    }
                }

                info("Thread " + Thread.currentThread().getId() + " finishing.");

                finishLatch.countDown();
            }
        }, N_THREADS);

        finishLatch.await();

        //Grid will be stopped automatically on tearDown().
    }

    /**
     *
     */
    private static class GridTaskTimeoutTestTask extends GridComputeTaskSplitAdapter<Serializable, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Serializable arg) throws GridException {
            Collection<GridTaskTimeoutTestJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 0; i < SPLIT_COUNT; i++) {
                GridTaskTimeoutTestJob job = new GridTaskTimeoutTestJob();

                job.setArguments(arg);

                jobs.add(job);
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     *
     */
    private static class GridTaskTimeoutTestJob extends GridComputeJobAdapter {
        /** Injected logger. */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            try {
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (InterruptedException ignored) {
                // No-op.
            }

            return null;
        }
    }
}
