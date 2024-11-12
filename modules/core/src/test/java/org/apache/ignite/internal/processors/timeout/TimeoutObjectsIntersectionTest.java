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

package org.apache.ignite.internal.processors.timeout;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.GridTestClockTimer;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_ATOMIC_DEFERRED_ACK_BUFFER_SIZE;
import static org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicCache.DFLT_ATOMIC_DEFERRED_ACK_TIMEOUT;

/**
 * Test timeout objects intersection for different subsystems.
 */
public class TimeoutObjectsIntersectionTest extends GridCommonAbstractTest {
    /** */
    private static final int JOBS_CNT = 200;

    /** {@inheritDoc} */
    @Override protected boolean isMultiJvm() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setPublicThreadPoolSize(JOBS_CNT);
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** {@inheritDoc} */
    @Override protected List<String> additionalRemoteJvmArgs() {
        List<String> args = super.additionalRemoteJvmArgs();

        args.add("-D" + IGNITE_ATOMIC_DEFERRED_ACK_BUFFER_SIZE + "=1");

        return args;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridTestClockTimer.timeSupplier(GridTestClockTimer.DFLT_TIME_SUPPLIER);
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testComputeAndPutIntersection() throws Exception {
        startGrids(2);

        CacheConfiguration<Integer, Integer> cfg = new CacheConfiguration<Integer, Integer>(DEFAULT_CACHE_NAME)
            .setNearConfiguration(new NearCacheConfiguration<>()).setAffinity(new NodeOrderAffinityFunction()).setBackups(1);

        grid(0).getOrCreateCache(cfg);

        // Freeze the time on both nodes to get timeout timestamps equality.
        grid(0).compute().broadcast(t -> {
            GridTestClockTimer.timeSupplier(() -> t);
            GridTestClockTimer.update();

            return null;
        }, U.currentTimeMillis());

        // Synchronize IgniteUuid local and remote counters. Local node counter must be ahead of remote node counter.
        Long remoteCntr = grid(0).compute(grid(0).cluster().forRemotes()).call(IgniteUuid::lastLocalId);

        for (int i = 0; i < remoteCntr - IgniteUuid.lastLocalId() + 5_000; i++)
            IgniteUuid.randomUuid();

        // Send job with puts to remote node. Puts will be postponed until all jobs with timeout are sent to remote node.
        IgniteFuture<Void> fut = grid(0)
            .compute(grid(0).cluster().forRemotes())
            .runAsync(new PutRunnable());

        // Send batch of jobs with timeout equal to DeferredUpdateTimeout.
        // GridJobObject will be added to GridTimeoutProcessor.timeoutObjs with id:
        // grid(0).localNode().id() and local node counter.
        for (int i = 0; i < JOBS_CNT; i++) {
            grid(0)
                .compute(grid(0).cluster().forRemotes())
                .withTimeout(DFLT_ATOMIC_DEFERRED_ACK_TIMEOUT)
                .runAsync(() -> doSleep(10_000L));
        }

        // Start puts and wait for result. Here we have intersection between timeout/id of GridJobWorker
        // and DeferredUpdateTimeout timeout objects, since:
        //      - timeouts are equal
        //      - UUID in timeout object ids are equal (grid(0).localNode().id())
        //      - IgniteUuid local counter (stored in GridJobWorker) was ahead of remote counter, but during puts
        //        remote counter (stored in DeferredUpdateTimeout) has overtaken local counter.
        grid(0).countDownLatch("latch", 1, false, true).countDown();

        fut.get(10_000L);
    }

    /** */
    private static class PutRunnable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        Ignite ignite;

        /** {@inheritDoc} */
        @Override public void run() {
            ignite.countDownLatch("latch", 1, false, true).await();

            for (int i = 0; i < 10_000; i++)
                ignite.cache(DEFAULT_CACHE_NAME).put(i, i);

            assertNull(((IgniteEx)ignite).context().failure().failureContext());
        }
    }

    /** */
    private static class NodeOrderAffinityFunction extends RendezvousAffinityFunction {
        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<ClusterNode> nodes = new ArrayList<>(affCtx.currentTopologySnapshot());
            // All primaries are on local node. In this case DeferredUpdateTimeout will be created on remote node
            // with id: grid(0).localNode().id() and remote node counter.
            nodes.sort(Comparator.comparingLong(ClusterNode::order));
            List<List<ClusterNode>> assignments = new ArrayList<>(partitions());

            for (int i = 0; i < partitions(); i++)
                assignments.add(nodes);

            return assignments;
        }
    }
}
