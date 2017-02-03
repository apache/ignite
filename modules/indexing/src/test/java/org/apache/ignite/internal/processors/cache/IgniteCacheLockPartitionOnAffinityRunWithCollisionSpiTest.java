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
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Test to validate https://issues.apache.org/jira/browse/IGNITE-2310
 */
public class IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest
    extends IgniteCacheLockPartitionOnAffinityRunAbstractTest {

    private static volatile boolean cancelAllJobs = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        CollisionSpi colSpi = new AlwaysCancelCollisionSpi();

        cfg.setCollisionSpi(colSpi);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionReservation() throws Exception {
        int orgId = 0;
        cancelAllJobs = true;
        // Workaround for initial update job metadata.
        try {
            grid(0).compute().affinityRun(
                Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()),
                new Integer(orgId),
                new TestRun(orgId));
        } catch (Exception ignored) {
            // No-op. Swallow exceptions on run (e.g. job canceling etc.).
            // The test checks only correct partition release in case CollisionSpi is used.
        }
        // All partition must be released in spite of any exceptions during the job executions.
        cancelAllJobs = false;
        ClusterNode n = grid(0).context().affinity()
                .mapKeyToNode(Organization.class.getSimpleName(), orgId);
        checkPartitionsReservations((IgniteEx)grid(n), orgId, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void _testJobFinishing() throws Exception {
//        fail("Affinity run / call doesn't receive response where many job rejections happen.");
        final AtomicInteger jobNum = new AtomicInteger(0);

        cancelAllJobs = true;

        IgniteInternalFuture<Long> affFut = null;
        try {
            affFut = GridTestUtils.runMultiThreadedAsync(new Runnable() {
                @Override public void run() {
                    while (System.currentTimeMillis() < endTime) {
                        int n = 0;
                        try {
                            for (final int orgId : orgIds) {
                                if (System.currentTimeMillis() >= endTime)
                                    break;

                                n = jobNum.getAndIncrement();

                                log.info("+++ Job submitted " + n);
                                grid(0).compute().affinityRun(
                                    Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()),
                                    new Integer(orgId),
                                    new TestRun(n));
                            }
                        }
                        catch (Exception e) {
                            log.info("+++ Job failed " + n + " " + e.toString());
                            // No-op. Swallow exceptions on run (e.g. job canceling etc.).
                        }
                    }

                }
            }, AFFINITY_THREADS_CNT, "affinity-run");
        }
        finally {
            if (affFut != null)
                affFut.get();

            stopRestartThread.set(true);

            cancelAllJobs = false;

            // Should not be timed out.
            awaitPartitionMapExchange();
        }
    }

    /**
     *
     */
    private static class TestRun implements IgniteRunnable {
        private int jobNum;

        /** Ignite Logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         *
         */
        public TestRun() {

        }

        /**
         * @param jobNum Job number.
         */
        public TestRun(int jobNum) {
            this.jobNum = jobNum;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    @IgniteSpiMultipleInstancesSupport(true)
    public static class AlwaysCancelCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
        /** Grid logger. */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();
            if (cancelAllJobs) {
                for (CollisionJobContext job : waitJobs)
                    job.cancel();
            } else {
                for (CollisionJobContext job : waitJobs)
                    job.activate();
            }
        }

        /** {@inheritDoc} */
        @Override public void spiStart(String gridName) throws IgniteSpiException {
            // Start SPI start stopwatch.
            startStopwatch();
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void setExternalCollisionListener(CollisionExternalListener lsnr) {
            // No-op.
        }
    }
}
