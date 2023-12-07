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

package org.apache.ignite.internal.processors.compute;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.GridTaskSessionInternal;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.priorityqueue.PriorityQueueCollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;

/**
 * Class for checking that there will be no errors when starting tasks with/without
 * {@link ComputeTaskSessionFullSupport}.
 */
public class ComputeTaskWithWithoutFullSupportTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCollisionSpi(new PriorityQueueCollisionSpiEx().setParallelJobsNumber(1))
            .setFailureHandler(new StopNodeFailureHandler())
            .setMetricsUpdateFrequency(Long.MAX_VALUE)
            .setClientFailureDetectionTimeout(Long.MAX_VALUE);
    }

    /**
     * Checking that if there is {@link PriorityQueueCollisionSpi},
     * it is possible to run tasks with and without {@link ComputeTaskSessionFullSupport}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        IgniteEx n = startGrid(0);

        n.cluster().state(ACTIVE);

        ComputeTaskFuture<Void> taskFut0 = n.compute().executeAsync(new TaskWithFullSupport(), null);
        assertTrue(((GridTaskSessionInternal)taskFut0.getTaskSession()).isFullSupport());

        ((PriorityQueueCollisionSpiEx)n.configuration().getCollisionSpi()).handleCollision = true;

        ComputeTaskFuture<Void> taskFut1 = n.compute().executeAsync(new TaskWithoutFullSupport(), null);
        assertFalse(((GridTaskSessionInternal)taskFut1.getTaskSession()).isFullSupport());

        taskFut0.get(TimeUnit.SECONDS.toMillis(1));
        taskFut1.get(TimeUnit.SECONDS.toMillis(1));
    }

    /** */
    private static class PriorityQueueCollisionSpiEx extends PriorityQueueCollisionSpi {
        /** */
        volatile boolean handleCollision;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            if (handleCollision)
                super.onCollision(ctx);
        }
    }

    /** */
    @ComputeTaskSessionFullSupport
    private static class TaskWithFullSupport extends ComputeTaskAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            Void arg
        ) throws IgniteException {
            assertFalse(subgrid.isEmpty());

            return singletonMap(new NoopJob(), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /** */
    private static class TaskWithoutFullSupport extends ComputeTaskAdapter<Void, Void> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            Void arg
        ) throws IgniteException {
            assertFalse(subgrid.isEmpty());

            return singletonMap(new NoopJob(), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
