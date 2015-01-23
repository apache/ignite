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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.collision.jobstealing.*;
import org.apache.ignite.spi.failover.jobstealing.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;

/**
 * Job stealing test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridJobStealingZeroActiveJobsSelfTest extends GridCommonAbstractTest {
    /** */
    private static Ignite ignite1;

    /** */
    private static Ignite ignite2;

    /** */
    public GridJobStealingZeroActiveJobsSelfTest() {
        super(false /* don't start grid*/);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite1 = startGrid(1);
        ignite2 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite1 = null;

        stopGrid(1);
        stopGrid(2);
    }

    /**
     * Test 2 jobs on 2 nodes.
     *
     * @throws IgniteCheckedException If test failed.
     */
    public void testTwoJobs() throws IgniteCheckedException {
        ignite1.compute().execute(JobStealingTask.class, null);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        JobStealingCollisionSpi colSpi = new JobStealingCollisionSpi();

        // One job at a time.
        colSpi.setActiveJobsThreshold(gridName.endsWith("1") ? 0 : 2);
        colSpi.setWaitJobsThreshold(0);

        JobStealingFailoverSpi failSpi = new JobStealingFailoverSpi();

        // Verify defaults.
        assert failSpi.getMaximumFailoverAttempts() == JobStealingFailoverSpi.DFLT_MAX_FAILOVER_ATTEMPTS;

        cfg.setCollisionSpi(colSpi);
        cfg.setFailoverSpi(failSpi);

        return cfg;
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class JobStealingTask extends ComputeTaskAdapter<Object, Object> {
        /** Grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable Object arg) throws IgniteCheckedException {
            assert subgrid.size() == 2 : "Invalid subgrid size: " + subgrid.size();

            Map<ComputeJobAdapter, ClusterNode> map = new HashMap<>(subgrid.size());

            // Put all jobs onto local node.
            for (Iterator iter = subgrid.iterator(); iter.hasNext(); iter.next())
                map.put(new GridJobStealingJob(5000L), ignite.cluster().localNode());

            return map;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assert results.size() == 2;

            for (ComputeJobResult res : results) {
                log.info("Job result: " + res.getData());
            }

            String name1 = results.get(0).getData();
            String name2 = results.get(1).getData();

            assert name1.equals(name2);

            assert !name1.equals(ignite1.name());
            assert name1.equals(ignite2.name());

            return null;
        }
    }

    /**
     *
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static final class GridJobStealingJob extends ComputeJobAdapter {
        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * @param arg Job argument.
         */
        GridJobStealingJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws IgniteCheckedException {
            try {
                Long sleep = argument(0);

                assert sleep != null;

                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                throw new IgniteCheckedException("Job got interrupted.", e);
            }

            return ignite.name();
        }
    }
}
