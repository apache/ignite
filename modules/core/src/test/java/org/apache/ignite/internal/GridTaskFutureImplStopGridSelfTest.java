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

package org.apache.ignite.internal;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

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
            final ComputeTaskFuture<?> fut = executeAsync(ignite.compute(), GridStopTestTask.class.getName(), null);

            fut.listen(new CI1<IgniteFuture>() {
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
    public static class GridStopTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<ComputeJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 0; i < SPLIT_COUNT; i++)
                jobs.add(new GridStopTestJob());

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
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
        @LoggerResource
        private IgniteLogger log;

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