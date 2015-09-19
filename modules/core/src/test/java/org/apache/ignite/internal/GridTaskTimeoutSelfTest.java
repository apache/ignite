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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.compute.ComputeTaskTimeoutException;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PE;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

import static org.apache.ignite.events.EventType.EVT_TASK_TIMEDOUT;

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
    private void checkTimedOutEvents(final IgniteUuid execId) {
        Ignite ignite = G.ignite(getTestGridName());

        Collection<Event> evts = ignite.events().localQuery(new PE() {
            @Override public boolean apply(Event evt) {
                return ((TaskEvent) evt).taskSessionId().equals(execId);
            }
        }, EVT_TASK_TIMEDOUT);

        assert evts.size() == 1 : "Invalid number of timed out tasks: " + evts.size();
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousTimeout() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTaskTimeoutTestTask.class, GridTaskTimeoutTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute().withTimeout(TIMEOUT),
            GridTaskTimeoutTestTask.class.getName(), null);

        try {
            fut.get();

            assert false : "ComputeTaskTimeoutException was not thrown (synchronous apply)";
        }
        catch (ComputeTaskTimeoutException e) {
            info("Received expected timeout exception (synchronous apply): " + e);
        }

        Thread.sleep(TIMEOUT + 500);

        checkTimedOutEvents(fut.getTaskSession().getId());
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsynchronousTimeout() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().localDeployTask(GridTaskTimeoutTestTask.class, GridTaskTimeoutTestTask.class.getClassLoader());

        ComputeTaskFuture<?> fut = executeAsync(ignite.compute().withTimeout(TIMEOUT),
            GridTaskTimeoutTestTask.class.getName(), null);

        // Allow timed out events to be executed.
        Thread.sleep(TIMEOUT + 500);

        checkTimedOutEvents(fut.getTaskSession().getId());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSynchronousTimeoutMultithreaded() throws Exception {
        final Ignite ignite = G.ignite(getTestGridName());

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
                        ComputeTaskFuture<?> fut = executeAsync(
                            ignite.compute().withTimeout(TIMEOUT), GridTaskTimeoutTestTask.class.getName(), null);

                        fut.get();

                        assert false : "Task has not been timed out. Future: " + fut;
                    }
                    catch (ComputeTaskTimeoutException ignored) {
                        // Expected.
                    }
                    catch (IgniteCheckedException e) {
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
    private static class GridTaskTimeoutTestTask extends ComputeTaskSplitAdapter<Serializable, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Serializable arg) {
            Collection<GridTaskTimeoutTestJob> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 0; i < SPLIT_COUNT; i++) {
                GridTaskTimeoutTestJob job = new GridTaskTimeoutTestJob();

                job.setArguments(arg);

                jobs.add(job);
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     *
     */
    private static class GridTaskTimeoutTestJob extends ComputeJobAdapter {
        /** Injected logger. */
        @LoggerResource
        private IgniteLogger log;

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