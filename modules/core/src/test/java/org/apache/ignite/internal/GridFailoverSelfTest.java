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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeUserUndeclaredException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Failover tests.
 */
@GridCommonTest(group = "Kernal Self")
public class GridFailoverSelfTest extends GridCommonAbstractTest {
    /** Initial node that job has been mapped to. */
    private static final AtomicReference<ClusterNode> nodeRef = new AtomicReference<>(null);

    /** */
    public GridFailoverSelfTest() {
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
    public void testJobFail() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            assert ignite1 != null;
            assert ignite2 != null;

            Integer res = ignite1.compute().withTimeout(10000).execute(JobFailTask.class.getName(), "1");

            assert res != null;
            assert res == 1;
        }
        finally {
            stopGrid(1);
            stopGrid(2);
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

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            ses.setAttribute("fail", true);

            nodeRef.set(subgrid.get(0));

            return Collections.singletonMap(new ComputeJobAdapter(arg) {
                /** Ignite instance. */
                @IgniteInstanceResource
                private Ignite ignite;

                /** {@inheritDoc} */
                @Override public Serializable execute() {
                    boolean fail;

                    UUID locId = ignite.configuration().getNodeId();

                    try {
                        fail = ses.<String, Boolean>waitForAttribute("fail", 0);
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    if (fail) {
                        ses.setAttribute("fail", false);

                        assert nodeRef.get().id().equals(locId);

                        throw new IgniteException("Job exception.");
                    }

                    assert !nodeRef.get().id().equals(locId);

                    // This job does not return any result.
                    return Integer.parseInt(this.<String>argument(0));
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> received) {
            if (res.getException() != null && !(res.getException() instanceof ComputeUserUndeclaredException)) {
                assert res.getNode().id().equals(nodeRef.get().id());

                return ComputeJobResultPolicy.FAILOVER;
            }

            assert !res.getNode().id().equals(nodeRef.get().id());

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assert results.size() == 1;

            assert nodeRef.get() != null;

            assert !results.get(0).getNode().id().equals(nodeRef.get().id()) :
                "Initial node and result one are the same (should be different).";

            return results.get(0).getData();
        }
    }
}