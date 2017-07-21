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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Job context test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridJobContextSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If anything failed.
     */
    public void testJobContext() throws Exception {
        Ignite ignite = startGrid(1);

        try {
            startGrid(2);

            try {
                ignite.compute().execute(JobContextTask.class, null);
            }
            finally {
                stopGrid(2);
            }
        }
        finally{
            stopGrid(1);
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class JobContextTask extends ComputeTaskSplitAdapter<Object, Object> {
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** */
                    @JobContextResource
                    private ComputeJobContext jobCtx;

                    /** Ignite instance. */
                    @IgniteInstanceResource
                    private Ignite ignite;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        UUID locNodeId = ignite.configuration().getNodeId();

                        jobCtx.setAttribute("nodeId", locNodeId);
                        jobCtx.setAttribute("jobId", jobCtx.getJobId());

                        Map<String, String> attrs = new HashMap<>(10);

                        for (int i = 0; i < 10; i++) {
                            String s = jobCtx.getJobId().toString() + i;

                            attrs.put(s, s);
                        }

                        jobCtx.setAttributes(attrs);

                        assert jobCtx.getAttribute("nodeId").equals(locNodeId);
                        assert jobCtx.getAttributes().get("nodeId").equals(locNodeId);
                        assert jobCtx.getAttributes().keySet().containsAll(attrs.keySet());
                        assert jobCtx.getAttributes().values().containsAll(attrs.values());

                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            for (ComputeJobResult res : results) {
                ComputeJobContext jobCtx = res.getJobContext();

                assert jobCtx.getAttribute("nodeId").equals(res.getNode().id());
                assert jobCtx.getAttributes().get("nodeId").equals(res.getNode().id());

                assert jobCtx.getAttribute("jobId").equals(jobCtx.getJobId());

                for (int i = 0; i < 10; i++) {
                    String s = jobCtx.getJobId().toString() + i;

                    assert jobCtx.getAttribute(s).equals(s);
                    assert jobCtx.getAttributes().get(s).equals(s);
                }
            }

            return null;
        }
    }
}