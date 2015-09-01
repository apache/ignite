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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskMapAsync;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFutureCancelledException;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_JOB_CANCELLED;
import static org.apache.ignite.events.EventType.EVT_JOB_FINISHED;
import static org.apache.ignite.events.EventType.EVT_JOB_REJECTED;

/**
 * Test for task cancellation issue.
 */
public class GridTaskCancelSingleNodeSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopGrid();
    }

    /**
     * @throws Exception If failed.
     */
    public void testImmediateCancellation() throws Exception {
        checkCancellation(0L);
    }

    /**
     * @throws Exception If failed.
     */
    public void testCancellation() throws Exception {
        checkCancellation(2000L);
    }

    /**
     * @param timeoutBeforeCancel Timeout.
     * @throws Exception If failed.
     */
    @SuppressWarnings("ErrorNotRethrown")
    private void checkCancellation(long timeoutBeforeCancel) throws Exception {
        final AtomicInteger finished = new AtomicInteger();
        final AtomicInteger cancelled = new AtomicInteger();
        final AtomicInteger rejected = new AtomicInteger();

        grid().events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                info("Received event: " + evt);

                switch (evt.type()) {
                    case EVT_JOB_FINISHED:
                        finished.incrementAndGet();

                        break;

                    case EVT_JOB_CANCELLED:
                        cancelled.incrementAndGet();

                        break;

                    case EVT_JOB_REJECTED:
                        rejected.incrementAndGet();

                        break;

                    default:
                        assert false : "Unexpected event: " + evt;
                }

                return true;
            }
        }, EVT_JOB_FINISHED, EVT_JOB_CANCELLED, EVT_JOB_REJECTED);

        IgniteCompute comp = grid().compute().withAsync();

        comp.execute(TestTask.class, null);

        ComputeTaskFuture<?> fut = comp.future();

        if (timeoutBeforeCancel > 0L)
            Thread.sleep(timeoutBeforeCancel);

        assert fut.cancel();

        for (int i = 0; i < 3; i++) {
            try {
            if (timeoutBeforeCancel == 0L)
                assert (finished.get() == 0 && cancelled.get() == 0 && rejected.get() == 0) :
                    "Failed on iteration [i=" + i + ", finished=" + finished.get() +
                    ", cancelled=" + cancelled.get() + ", rejected=" + rejected.get() + ']';
            else
                assert (finished.get() == 1 && cancelled.get() == 1 && rejected.get() == 0) :
                    "Failed on iteration [i=" + i + ", finished=" + finished.get() +
                        ", cancelled=" + cancelled.get() + ", rejected=" + rejected.get() + ']';
            }
            catch (AssertionError e) {
                info("Check failed: " + e.getMessage());

                if (timeoutBeforeCancel == 0L && i == 2)
                    throw e;
            }

            if (i < 2)
                U.sleep(500);
        }

        try {
            fut.get();

            assert false;
        }
        catch (IgniteFutureCancelledException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     *
     */
    @ComputeTaskMapAsync
    private static class TestTask extends ComputeTaskSplitAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Void arg) {
            return F.asSet(new ComputeJobAdapter() {
                /** */
                @LoggerResource
                private IgniteLogger log;

                /** */
                @IgniteInstanceResource
                private Ignite g;

                /** {@inheritDoc} */
                @Override public Object execute() {
                    log.info("Executing job on node: " + g.cluster().localNode().id());

                    try {
                        Thread.sleep(5000);
                    }
                    catch (InterruptedException ignored) {
                        log.info("Job thread has been interrupted.");

                        Thread.currentThread().interrupt();
                    }

                    return null;
                }
            });
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }
}