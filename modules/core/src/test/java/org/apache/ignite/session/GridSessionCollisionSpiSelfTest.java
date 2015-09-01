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

package org.apache.ignite.session;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.fifoqueue.FifoQueueCollisionSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Grid session collision SPI self test.
 */
public class GridSessionCollisionSpiSelfTest extends GridCommonAbstractTest {
    /**
     * Constructs a test.
     */
    public GridSessionCollisionSpiSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setCollisionSpi(new GridSessionCollisionSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollisionSessionAttribute() throws Exception {
        Ignite ignite = G.ignite(getTestGridName());

        ignite.compute().execute(GridSessionTestTask.class, null);

        info("Executed session collision test task.");
    }

    /**
     * Test task.
     */
    @ComputeTaskSessionFullSupport
    private static class GridSessionTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<ComputeJobAdapter> split(int gridSize, Object arg) {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** */
                    @TaskSessionResource
                    private ComputeTaskSession taskSes;

                    /** */
                    @JobContextResource
                    private ComputeJobContext jobCtx;

                    /** */
                    @LoggerResource
                    private IgniteLogger log;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        IgniteUuid jobId = jobCtx.getJobId();

                        String attr = (String)taskSes.getAttribute(jobId);

                        assert attr != null : "Attribute is null.";
                        assert attr.equals("test-" + jobId) : "Attribute has incorrect value: " + attr;

                        if (log.isInfoEnabled())
                            log.info("Executing job: " + jobId);

                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            // Nothing to reduce.
            return null;
        }
    }

    /**
     * Test collision spi.
     */
    private static class GridSessionCollisionSpi extends FifoQueueCollisionSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            Collection<CollisionJobContext> waitJobs = ctx.waitingJobs();

            for (CollisionJobContext job : waitJobs) {
                IgniteUuid jobId = job.getJobContext().getJobId();

                try {
                    job.getTaskSession().setAttribute(jobId, "test-" + jobId);

                    if (log.isInfoEnabled())
                        log.info("Set session attribute for job: " + jobId);
                }
                catch (IgniteException e) {
                    log.error("Failed to set session attribute: " + job, e);
                }

                job.activate();
            }
        }
    }
}