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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.events.EventType.EVTS_JOB_EXECUTION;
import static org.apache.ignite.events.EventType.EVTS_TASK_EXECUTION;
import static org.apache.ignite.events.EventType.EVT_JOB_FAILED_OVER;
import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;
import static org.apache.ignite.events.EventType.EVT_TASK_FAILED;
import static org.apache.ignite.events.EventType.EVT_TASK_FINISHED;

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
        grid(1).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                X.println("Task event: " + evt);

                return true;
            }
        }, EVTS_TASK_EXECUTION);

        grid(1).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                X.println("Job event: " + evt);

                return true;
            }
        }, EVTS_JOB_EXECUTION);

        final CountDownLatch startedLatch = new CountDownLatch(1);

        grid(1).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                startedLatch.countDown();

                return true;
            }
        }, EVT_JOB_STARTED);

        final AtomicInteger failedOver = new AtomicInteger(0);

        grid(1).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                failedOver.incrementAndGet();

                return true;
            }
        }, EVT_JOB_FAILED_OVER);

        final CountDownLatch finishedLatch = new CountDownLatch(1);

        grid(1).events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                finishedLatch.countDown();

                return true;
            }
        }, EVT_TASK_FINISHED, EVT_TASK_FAILED);

        final ClusterNode node = grid(1).localNode();

        IgniteCompute comp = grid(1).compute().withAsync();

        comp.execute(new ComputeTaskAdapter<Void, Void>() {
            @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
                @Nullable Void arg) {
                return F.asMap(new SleepJob(), node, new SleepJob(), node);
            }

            /** {@inheritDoc} */
            @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
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