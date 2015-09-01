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
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Grid session set job attribute self test.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionSetJobAttributeOrderSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_ATTR_KEY = "grid.task.session.test.attr";

    /** */
    private static final int SETS_ATTR_COUNT = 100;

    /** */
    private static final int TESTS_COUNT = 10;

    /**
     * @throws Exception If failed.
     */
    public void testJobSetAttribute() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ignite1.compute().localDeployTask(SessionTestTask.class, SessionTestTask.class.getClassLoader());

            IgniteCompute comp = ignite1.compute().withAsync();

            for (int i = 0; i < TESTS_COUNT; i++) {
                comp.withTimeout(100000).execute(SessionTestTask.class.getName(), ignite2.cluster().localNode().id());

                ComputeTaskFuture<?> fut = comp.future();

                fut.getTaskSession().setAttribute(TEST_ATTR_KEY, SETS_ATTR_COUNT);

                Integer res = (Integer)fut.get();

                assert res != null && res.equals(SETS_ATTR_COUNT) : "Unexpected result [res=" + res +
                    ", expected=" + SETS_ATTR_COUNT + ']';

                info("Session attribute value was correct for test [res=" + res +
                    ", expected=" + SETS_ATTR_COUNT + ']');
            }
        }
        finally {
            stopAllGrids(false);
        }
    }

    /** */
    @ComputeTaskSessionFullSupport
    private static class SessionTestTask extends ComputeTaskAdapter<UUID, Serializable> {
        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, UUID arg) {
            assert subgrid.size() == 2;
            assert arg != null;

            for (ClusterNode node : subgrid) {
                if (node.id().equals(arg))
                    return Collections.singletonMap(new SessionTestJob(), node);
            }

            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<ComputeJobResult> results) {
            try {
                if (taskSes.waitForAttribute(TEST_ATTR_KEY, SETS_ATTR_COUNT, 20000)) {
                    log.info("Successfully waited for attribute [key=" + TEST_ATTR_KEY +
                        ", val=" + SETS_ATTR_COUNT + ']');
                }
            }
            catch (InterruptedException e) {
                throw new IgniteException("Got interrupted while waiting for attribute to be set.", e);
            }

            return taskSes.getAttribute(TEST_ATTR_KEY);
        }
    }

    /** */
    private static class SessionTestJob extends ComputeJobAdapter {
        /** */
        @TaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            assert taskSes != null;

            try {
                boolean attr = taskSes.waitForAttribute(TEST_ATTR_KEY, SETS_ATTR_COUNT, 20000);

                assert attr : "Failed to wait for attribute value.";
            }
            catch (InterruptedException e) {
                throw new IgniteException("Got interrupted while waiting for attribute to be set.", e);
            }

            Integer res = taskSes.getAttribute(TEST_ATTR_KEY);

            assert res != null && res.equals(SETS_ATTR_COUNT) :
                "Unexpected result [res=" + res + ", expected=" + SETS_ATTR_COUNT + ']';

            log.info("Session attribute order was correct for job [res=" + res + ", expected=" + SETS_ATTR_COUNT + ']');

            taskSes.setAttribute(TEST_ATTR_KEY, SETS_ATTR_COUNT);

            return null;
        }
    }
}