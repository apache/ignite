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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeExecutionRejectedException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.failover.FailoverContext;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Test failover and custom topology. Topology returns local node if remote node fails.
 */
@GridCommonTest(group = "Kernal Self")
public class GridFailoverCustomTopologySelfTest extends GridCommonAbstractTest {
    /** */
    private final AtomicInteger failCnt = new AtomicInteger(0);

    /** */
    private static final Object mux = new Object();

    /** */
    public GridFailoverCustomTopologySelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setNodeId(null);

        cfg.setFailoverSpi(new AlwaysFailoverSpi() {
            /** {@inheritDoc} */
            @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> top) {
                failCnt.incrementAndGet();

                return super.failover(ctx, top);
            }
        });

        return cfg;
    }
    /**
     * Tests that failover don't pick local node if it has been excluded from topology.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings({"WaitNotInLoop", "UnconditionalWait", "unchecked"})
    public void testFailoverTopology() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            assert ignite1 != null;
            assert ignite2 != null;

            ignite1.compute().localDeployTask(JobTask.class, JobTask.class.getClassLoader());

            try {
                ComputeTaskFuture<String> fut;

                synchronized(mux){
                    IgniteCompute comp = ignite1.compute().withAsync();

                    comp.execute(JobTask.class, null);

                    fut = comp.future();

                    mux.wait();
                }

                stopAndCancelGrid(2);

                String res = fut.get();

                info("Task result: " + res);
            }
            catch (IgniteException e) {
                info("Got unexpected grid exception: " + e);
            }

            info("Failed over: " + failCnt.get());

            assert failCnt.get() == 1 : "Invalid fail over counter [expected=1, actual=" + failCnt.get() + ']';
        }
        finally {
            stopGrid(1);

            // Stopping stopped instance just in case.
            stopGrid(2);
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class JobTask extends ComputeTaskAdapter<String, String> {
        /** */
        @LoggerResource
        private IgniteLogger log;

         /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            assert ignite != null;

            UUID locNodeId = ignite.configuration().getNodeId();

            assert locNodeId != null;

            if (log.isInfoEnabled())
                log.info("Mapping jobs [subgrid=" + subgrid + ", arg=" + arg + ']');

            ClusterNode remoteNode = null;

            for (ClusterNode node : subgrid) {
                if (!node.id().equals(locNodeId))
                    remoteNode = node;
            }

            return Collections.singletonMap(new ComputeJobAdapter(locNodeId) {
                /** */
               @IgniteInstanceResource
               private Ignite ignite;

                /** {@inheritDoc} */
                @SuppressWarnings("NakedNotify")
                @Override public Serializable execute() {
                    assert ignite != null;

                    UUID nodeId = ignite.configuration().getNodeId();

                    assert nodeId != null;

                    if (!nodeId.equals(argument(0))) {
                        try {
                            synchronized(mux) {
                                mux.notifyAll();
                            }

                            Thread.sleep(Integer.MAX_VALUE);
                        }
                        catch (InterruptedException e) {
                            throw new ComputeExecutionRejectedException("Expected interruption during execution.", e);
                        }
                    }
                    else
                        return "success";

                    throw new ComputeExecutionRejectedException("Expected exception during execution.");
                }
            }, remoteNode);
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) {
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }
}