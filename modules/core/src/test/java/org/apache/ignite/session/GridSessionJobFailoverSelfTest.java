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
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.spi.failover.always.AlwaysFailoverSpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Job failover test.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionJobFailoverSelfTest extends GridCommonAbstractTest {
    /**
     * Default constructor.
     */
    public GridSessionJobFailoverSelfTest() {
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
    @SuppressWarnings("unchecked")
    public void testFailoverJobSession() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);

            startGrid(2);

            ignite1.compute().localDeployTask(SessionTestTask.class, SessionTestTask.class.getClassLoader());

            Object res = ignite1.compute().execute(SessionTestTask.class.getName(), "1");

            assert (Integer)res == 1;
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * Session test task implementation.
     */
    @ComputeTaskSessionFullSupport
    private static class SessionTestTask implements ComputeTask<String, Object> {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        private boolean jobFailed;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
            ses.setAttribute("fail", true);

            for (int i = 0; i < 10; i++) {
                for (int ii = 0; ii < 10; ii++)
                    ses.setAttribute("test.task.attr." + i, ii);
            }

            return Collections.singletonMap(new ComputeJobAdapter(arg) {
                /** {@inheritDoc} */
                @Override public Serializable execute() {
                    boolean fail;

                    try {
                        fail = ses.waitForAttribute("fail", 0);
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    if (fail) {
                        ses.setAttribute("fail", false);

                        for (int i = 0; i < 10; i++) {
                            for (int ii = 0; ii < 10; ii++)
                                ses.setAttribute("test.job.attr." + i, ii);
                        }

                        throw new IgniteException("Job exception.");
                    }

                    try {
                        for (int i = 0; i < 10; i++) {
                            boolean attr = ses.waitForAttribute("test.task.attr." + i, 9, 100000);

                            assert attr;
                        }

                        for (int i = 0; i < 10; i++) {
                            boolean attr = ses.waitForAttribute("test.job.attr." + i, 9, 100000);

                            assert attr;
                        }
                    }
                    catch (InterruptedException e) {
                        throw new IgniteException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    // This job does not return any result.
                    return Integer.parseInt(this.<String>argument(0));
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) {
            if (res.getException() != null) {
                assert !jobFailed;

                jobFailed = true;

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