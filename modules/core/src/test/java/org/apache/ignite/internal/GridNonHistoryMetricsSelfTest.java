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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.GridTestJob;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterMetrics;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;

/**
 *
 */
public class GridNonHistoryMetricsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMetricsHistorySize(5);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleTaskMetrics() throws Exception {
        final Ignite ignite = grid();

        ignite.compute().execute(new TestTask(), "testArg");

        // Let metrics update twice.
        final CountDownLatch latch = new CountDownLatch(2);

        ignite.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                assert evt.type() == EVT_NODE_METRICS_UPDATED;

                latch.countDown();

                return true;
            }
        }, EVT_NODE_METRICS_UPDATED);

        latch.await();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                ClusterMetrics metrics = ignite.cluster().localNode().metrics();

                return metrics.getTotalExecutedJobs() == 5;
            }
        }, 5000);

        ClusterMetrics metrics = ignite.cluster().localNode().metrics();

        info("Node metrics: " + metrics);

        assertEquals(5, metrics.getTotalExecutedJobs());
        assertEquals(0, metrics.getTotalCancelledJobs());
        assertEquals(0, metrics.getTotalRejectedJobs());
    }

    /**
     * Test task.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> refs = new ArrayList<>(gridSize*5);

            for (int i = 0; i < gridSize * 5; i++)
                refs.add(new GridTestJob(arg.toString() + i + 1));

            return refs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            return results;
        }
    }
}