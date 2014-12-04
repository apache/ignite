/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Test for task future when grid stops.
 */
@GridCommonTest(group = "Kernal Self")
@SuppressWarnings({"UnusedDeclaration"})
public class GridTaskFutureImplStopGridSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int WAIT_TIME = 5000;

    /** */
    public static final int SPLIT_COUNT = 5;

    /** */
    private static CountDownLatch startSignal = new CountDownLatch(SPLIT_COUNT);

    /** */
    private static final Object mux = new Object();

    /** */
    @SuppressWarnings({"StaticNonFinalField"})
    private static int cnt;

    /** */
    public GridTaskFutureImplStopGridSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testGet() throws Exception {
        Ignite ignite = startGrid(getTestGridName());

        Thread futThread = null;

        try {
            final GridComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridStopTestTask.class.getName(), null);

            fut.listenAsync(new CI1<IgniteFuture>() {
                @SuppressWarnings({"NakedNotify"})
                @Override public void apply(IgniteFuture gridFut) {
                    synchronized (mux) {
                        mux.notifyAll();
                    }
                }
            });

            final CountDownLatch latch = new CountDownLatch(1);

            final AtomicBoolean failed = new AtomicBoolean(false);

            futThread = new Thread(new Runnable() {
                /** {@inheritDoc} */
                @Override public void run() {
                    try {
                        startSignal.await();

                        Object res = fut.get();

                        info("Task result: " + res);
                    }
                    catch (Throwable e) {
                        failed.set(true);

                        // Make sure that message contains info about stopping grid.
                        assert e.getMessage().startsWith("Task failed due to stopping of the grid:");
                    }
                    finally {
                        latch.countDown();
                    }
                }

            }, "test-task-future-thread");

            futThread.start();

            long delta = WAIT_TIME;
            long end = System.currentTimeMillis() + delta;

            synchronized (mux) {
                while (cnt < SPLIT_COUNT && delta > 0) {
                    mux.wait(delta);

                    delta = end - System.currentTimeMillis();
                }
            }

            // Stops grid.
            stopGrid(getTestGridName());

            boolean finished = latch.await(WAIT_TIME, TimeUnit.MILLISECONDS);

            info("Future thread [alive=" + futThread.isAlive() + ']');

            info("Test task result [failed=" + failed.get() + ", taskFuture=" + fut + ']');

            assert finished : "Future thread was not stopped.";

            assert fut.isDone();
        }
        finally {
            if (futThread != null && futThread.isAlive()) {
                info("Task future thread interruption.");

                futThread.interrupt();
            }

            if (G.state(getTestGridName()) != IgniteState.STOPPED)
                stopGrid(getTestGridName());
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass", "UnusedDeclaration"})
    public static class GridStopTestTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @GridLoggerResource private GridLogger log;

        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) throws GridException {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 0; i < SPLIT_COUNT; i++)
                jobs.add(new GridStopTestJob());

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) throws GridException {
            if (log.isInfoEnabled())
                log.info("Aggregating job [job=" + this + ", results=" + results + ']');

            int res = 0;

            for (ComputeJobResult result : results) {
                res += (Integer)result.getData();
            }

            return res;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class GridStopTestJob extends ComputeJobAdapter {
        /** */
        @GridLoggerResource private GridLogger log;

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            if (log.isInfoEnabled())
                log.info("Executing job [job=" + this + ']');

            startSignal.countDown();

            synchronized (mux) {
                cnt++;

                mux.notifyAll();
            }

            try {
                Thread.sleep(Integer.MAX_VALUE);
            }
            catch (InterruptedException ignore) {
                if (log.isInfoEnabled())
                    log.info("Job got interrupted: " + this);
            }

            if (!Thread.currentThread().isInterrupted())
                log.error("Job not interrupted: " + this);

            return !Thread.currentThread().isInterrupted() ? 0 : 1;
        }
   }
}
