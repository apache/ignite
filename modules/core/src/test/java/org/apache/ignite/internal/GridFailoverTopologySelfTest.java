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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.failover.FailoverContext;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test failover and topology. It don't pick local node if it has been excluded from topology.
 */
@GridCommonTest(group = "Kernal Self")
public class GridFailoverTopologySelfTest extends GridCommonAbstractTest {
    /** */
    private final AtomicBoolean failed = new AtomicBoolean(false);

    /** */
    public GridFailoverTopologySelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setNodeId(null);

        cfg.setFailoverSpi(new AlwaysFailoverSpi() {
            /** Ignite instance. */
            @IgniteInstanceResource
            private Ignite ignite;

            /** {@inheritDoc} */
            @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> grid) {
                if (grid.size() != 1) {
                    failed.set(true);

                    error("Unexpected grid size [expected=1, grid=" + grid + ']');
                }

                UUID locNodeId = ignite.configuration().getNodeId();

                for (ClusterNode node : grid) {
                    if (node.id().equals(locNodeId)) {
                        failed.set(true);

                        error("Grid shouldn't contain local node [localNodeId=" + locNodeId + ", grid=" + grid + ']');
                    }
                }

                return super.failover(ctx, grid);
            }
        });

        return cfg;
    }

    /**
     * Tests that failover don't pick local node if it has been excluded from topology.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testFailoverTopology() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);

            startGrid(2);

            ignite1.compute().localDeployTask(JobFailTask.class, JobFailTask.class.getClassLoader());

            try {
                compute(ignite1.cluster().forRemotes()).execute(JobFailTask.class, null);
            }
            catch (IgniteException e) {
                info("Got expected grid exception: " + e);
            }

            assert !failed.get();
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /** */
    private static class JobFailTask implements ComputeTask<String, Object> {
        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private boolean jobFailedOver;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            assert ignite != null;

            UUID locNodeId = ignite.configuration().getNodeId();

            assert locNodeId != null;

            ClusterNode remoteNode = null;

            for (ClusterNode node : subgrid) {
                if (!node.id().equals(locNodeId))
                    remoteNode = node;
            }

            return Collections.singletonMap(new ComputeJobAdapter(arg) {
                @Override public Serializable execute() {
                    throw new IgniteException("Job exception.");
                }
            }, remoteNode);
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) {
            if (res.getException() != null && !jobFailedOver) {
                jobFailedOver = true;

                return ComputeJobResultPolicy.FAILOVER;
            }

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }
}