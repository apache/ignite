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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.collision.fifoqueue.*;
import org.apache.ignite.internal.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.events.IgniteEventType.*;

/**
 * Test that rejected job is not failed over.
 */
public class GridTaskJobRejectSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid(1);
        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopGrid(1);
        stopGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        FifoQueueCollisionSpi collision = new FifoQueueCollisionSpi();

        collision.setParallelJobsNumber(1);

        cfg.setCollisionSpi(collision);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReject() throws Exception {
        grid(1).events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                X.println("Task event: " + evt);

                return true;
            }
        }, EVTS_TASK_EXECUTION);

        grid(1).events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                X.println("Job event: " + evt);

                return true;
            }
        }, EVTS_JOB_EXECUTION);

        final CountDownLatch startedLatch = new CountDownLatch(1);

        grid(1).events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                startedLatch.countDown();

                return true;
            }
        }, EVT_JOB_STARTED);

        final AtomicInteger failedOver = new AtomicInteger(0);

        grid(1).events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                failedOver.incrementAndGet();

                return true;
            }
        }, EVT_JOB_FAILED_OVER);

        final CountDownLatch finishedLatch = new CountDownLatch(1);

        grid(1).events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                finishedLatch.countDown();

                return true;
            }
        }, EVT_TASK_FINISHED, EVT_TASK_FAILED);

        final ClusterNode node = grid(1).localNode();

        IgniteCompute comp = grid(1).compute().enableAsync();

        comp.execute(new ComputeTaskAdapter<Void, Void>() {
            @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
                @Nullable Void arg) {
                return F.asMap(new SleepJob(), node, new SleepJob(), node);
            }

            /** {@inheritDoc} */
            @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
                return null;
            }
        }, null);

        ComputeTaskFuture<?> fut = comp.future();

        assert startedLatch.await(2, SECONDS);

        fut.cancel();

        assert finishedLatch.await(2, SECONDS);

        assert failedOver.get() == 0;
    }

    /**
     * Sleeping job.
     */
    private static final class SleepJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() {
            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }

            return null;
        }
    }
}
