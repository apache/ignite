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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Tests distributed queries over set of partitions on unstable topology.
 */
public class IgniteCacheDistributedPartitionQueryNodeRestartsSelfTest extends
    IgniteCacheDistributedPartitionQueryAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        System.setProperty(IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT, Long.toString(1000_000L));
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_SQL_RETRY_TIMEOUT,
            Long.toString(GridReduceQueryExecutor.DFLT_RETRY_TIMEOUT));

        super.afterTestsStopped();
    }

    /**
     * Tests join query within region on unstable topology.
     */
    public void testJoinQueryUnstableTopology() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicIntegerArray states = new AtomicIntegerArray(GRIDS_COUNT);

        final Ignite client = grid("client");

        final AtomicInteger cnt = new AtomicInteger();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Runnable() {
            @Override public void run() {
                while (!stop.get()) {
                    doTestJoinQuery(client, rnd.nextInt(PARTS_PER_REGION.length) + 1);

                    int cur = cnt.incrementAndGet();

                    if (cur % 100 == 0)
                        log().info("Queries count: " + cur);
                }
            }
        }, QUERY_THREADS_CNT);

        final AtomicIntegerArray restartStats = new AtomicIntegerArray(GRIDS_COUNT);

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                while (!stop.get()) {
                    int grid = rnd.nextInt(GRIDS_COUNT);

                    String name = getTestIgniteInstanceName(grid);

                    Integer regionId = regionForGrid(name);

                    // Restart nodes only from region with enough number of nodes.
                    if (regionId != 3 && regionId != 4)
                        continue;

                    if (states.compareAndSet(grid, 0, 1)) {
                        restartStats.incrementAndGet(grid);

                        try {
                            stopGrid(grid);

                            Thread.sleep(rnd.nextInt(NODE_RESTART_TIME));

                            startGrid(grid);

                            Thread.sleep(rnd.nextInt(NODE_RESTART_TIME));
                        } finally {
                            states.set(grid, 0);
                        }
                    }
                }

                return null;
            }
        }, RESTART_THREADS_CNT);

        // Test duration.
        U.sleep(60_000);

        stop.set(true);

        try {
            fut.get();

            fut2.get();
        } finally {
            log().info("Queries count: " + cnt.get());

            for (int i = 0; i < GRIDS_COUNT; i++)
                log().info("Grid [name = " + getTestIgniteInstanceName(i) + ", idx=" + i + " ] restarts count: " +
                        restartStats.get(i));
        }
    }
}