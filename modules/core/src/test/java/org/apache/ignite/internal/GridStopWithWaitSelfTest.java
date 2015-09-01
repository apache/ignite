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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeUserUndeclaredException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Tests waiting for unfinished tasks while stopping the grid.
 */
@GridCommonTest(group = "Kernal Self")
public class GridStopWithWaitSelfTest extends GridCommonAbstractTest {
    /** Initial node that job has been mapped to. */
    private static final AtomicReference<ClusterNode> nodeRef = new AtomicReference<>(null);

    /** */
    private static CountDownLatch jobStarted;

    /**
     *
     */
    public GridStopWithWaitSelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailoverSpi(new AlwaysFailoverSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWait() throws Exception {
        jobStarted = new CountDownLatch(1);

        ComputeTaskFuture<Object> fut = null;

        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            assert ignite1 != null;
            assert ignite2 != null;

            fut = executeAsync(ignite1.compute().withTimeout(10000),
                GridWaitTask.class.getName(),
                ignite1.cluster().localNode().id());

            jobStarted.await();
        }
        finally {
            // Do not cancel but wait.
            G.stop(getTestGridName(1), false);
            G.stop(getTestGridName(2), false);
        }

        assert fut != null;

        Integer res = (Integer)fut.get();

        assert res == 1;
    }

    /**
     * @throws Exception If failed.
     */
    public void testWaitFailover() throws Exception {
        jobStarted = new CountDownLatch(1);

        ComputeTaskFuture<Object> fut = null;

        Ignite ignite1 = startGrid(1);
        Ignite ignite2 = startGrid(2);

        try {
            assert ignite1 != null;
            assert ignite2 != null;

            long timeout = 3000;

            fut = executeAsync(ignite1.compute().withTimeout(timeout), JobFailTask.class.getName(), "1");

            jobStarted.await(timeout, TimeUnit.MILLISECONDS);
        }
        finally {
            // Do not cancel but wait.
            G.stop(getTestGridName(1), false);
            G.stop(getTestGridName(2), false);
        }

        assert fut != null;

        Integer res = (Integer)fut.get();

        assert res == 1;
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    private static class GridWaitTask extends ComputeTaskAdapter<UUID, Integer> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, UUID arg) {
            ClusterNode mappedNode = null;

            for (ClusterNode node : subgrid) {
                if (node.id().equals(arg)) {
                    mappedNode = node;

                    break;
                }
            }

            assert mappedNode != null;

            return Collections.singletonMap(new ComputeJobAdapter(arg) {
                @Override public Integer execute() {
                    jobStarted.countDown();

                    return 1;
                }
            }, mappedNode);
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            return results.get(0).getData();
        }
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    private static class JobFailTask implements ComputeTask<String, Object> {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** Ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            ses.setAttribute("fail", true);

            ClusterNode node = F.view(subgrid, F.remoteNodes(ignite.configuration().getNodeId())).iterator().next();

            nodeRef.set(node);

            return Collections.singletonMap(new ComputeJobAdapter(arg) {
                /** Ignite instance. */
                @IgniteInstanceResource
                private Ignite ignite;

                /** Logger. */
                @LoggerResource
                private IgniteLogger log;

                @Override public Serializable execute() {
                    jobStarted.countDown();

                    log.info("Starting to execute job with fail attribute: " + ses.getAttribute("fail"));

                    boolean fail;

                    assert ignite != null;

                    UUID locId = ignite.configuration().getNodeId();

                    assert locId != null;

                    try {
                        fail = ses.waitForAttribute("fail", 0);
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    log.info("Failed attribute: " + fail);

                    // Should fail on local node and sent to remote one.
                    if (fail) {
                        ses.setAttribute("fail", false);

                        assert nodeRef.get().id().equals(locId);

                        log.info("Throwing grid exception from job.");

                        throw new IgniteException("Job exception.");
                    }

                    assert !nodeRef.get().id().equals(locId);

                    Integer res = Integer.parseInt(this.<String>argument(0));

                    log.info("Returning job result: " + res);

                    // This job does not return any result.
                    return res;
                }
            }, node);
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (res.getException() != null && !(res.getException() instanceof ComputeUserUndeclaredException)) {
                assert res.getNode().id().equals(nodeRef.get().id());

                return ComputeJobResultPolicy.FAILOVER;
            }

            assert !res.getNode().id().equals(nodeRef.get().id());

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> res) {
            assert res.size() == 1;

            assert nodeRef.get() != null;

            assert !res.get(0).getNode().id().equals(nodeRef.get().id()) :
                "Initial node and result one are the same (should be different).";

            return res.get(0).getData();
        }
    }
}