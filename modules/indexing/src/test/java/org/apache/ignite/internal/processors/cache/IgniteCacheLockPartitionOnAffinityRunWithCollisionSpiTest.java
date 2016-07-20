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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.collision.jobstealing.JobStealingCollisionSpi;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test to validate https://issues.apache.org/jira/browse/IGNITE-2310
 */
public class IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest extends IgniteCacheLockPartitionOnAffinityRunAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        JobStealingCollisionSpi colSpi = new JobStealingCollisionSpi();
        // One job at a time.
        colSpi.setActiveJobsThreshold(1);
        colSpi.setWaitJobsThreshold(1);
        colSpi.setMessageExpireTime(10_000);
        colSpi.setMaximumStealingAttempts(2);

        cfg.setCollisionSpi(colSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionReservation() throws Exception {
        final IgniteRunnable affRun = new IgniteRunnable() {
            IgniteEx ignite;

            @Override public void run() {
                try {
                    Thread.sleep(500);
                }
                catch (InterruptedException e) {
                    // No-op.
                }
            }
        };

        // Workaround for initial update job metadata.
        grid(0).compute().affinityRun(affRun,
            Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), 0);

        // Run restart threads: start re-balancing
        beginNodesRestart();

        IgniteInternalFuture<Long> affFut = null;
        try {
            affFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    while (System.currentTimeMillis() < endTime) {
                        try {
                            for (final int orgId : orgIds) {
                                if (System.currentTimeMillis() >= endTime)
                                    break;
                                grid(0).compute().affinityRun(affRun,
                                    Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()),
                                    orgId);
                            }
                        }
                        catch (Exception e) {
                            // No-op. Swallow exceptions on run (e.g. job canceling etc.).
                            // The test checks only correct partition release in case CollisionSpi is used.
                        }
                    }

                }
            }, AFFINITY_THREADS_CNT, "affinity-run");
        }
        finally {
            if (affFut != null)
                affFut.get();

            stopRestartThread.set(true);
            nodeRestartFut.get();

            // Should not be timed out.
            awaitPartitionMapExchange();

            // All partition must be released inspite of any exceptions during the job executions.
            for (int orgId : orgIds) {
                ClusterNode n = grid(0).context().affinity()
                    .mapKeyToNode(Organization.class.getSimpleName(), orgId);
                checkPartitionsReservations((IgniteEx)grid(n), orgId, 0);
            }
        }
    }
}