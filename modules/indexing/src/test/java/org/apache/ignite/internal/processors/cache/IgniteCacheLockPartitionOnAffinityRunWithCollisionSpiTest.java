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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;

/**
 * Test to validate https://issues.apache.org/jira/browse/IGNITE-2310
 *
 * TODO IGNITE-2310: use JobStealingCollisionSpi.
 */
public class IgniteCacheLockPartitionOnAffinityRunWithCollisionSpiTest extends IgniteCacheLockPartitionOnAffinityRunAbstractTest {
    /** Flag to use custom CollisionSpi that cancels all jobs. */
    private static boolean collisionSpiCancelAll;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCollisionSpi(new TestCollisionSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReleasePartitionJobCanceledByCollisionSpi() throws Exception {
        final int orgId = orgIds.get(0);
        final ClusterNode node = grid(0).affinity(Organization.class.getSimpleName()).mapKeyToNode(orgId);

        try {
            collisionSpiCancelAll = true;

            grid(1).compute().affinityRun(new IgniteRunnable() {
                @Override public void run() {
                    fail("Must not be executed");
                }
            }, Arrays.asList(Organization.class.getSimpleName(), Person.class.getSimpleName()), orgId);
            fail("Error must be thrown");
        }
        catch (ClusterTopologyException e) {
            checkPartitionsReservations((IgniteEx)grid(node), orgId, 0);
        }
        finally {
            collisionSpiCancelAll = false;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    @IgniteSpiMultipleInstancesSupport(true)
    public static class TestCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
        /** Grid logger. */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {

            if (!collisionSpiCancelAll) {
                Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

                for (CollisionJobContext job : waitJobs)
                    job.activate();
            }
            else {
                Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

                for (CollisionJobContext job : waitJobs)
                    job.cancel();
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