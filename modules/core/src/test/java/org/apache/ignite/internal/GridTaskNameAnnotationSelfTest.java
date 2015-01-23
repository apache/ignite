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
import org.apache.ignite.resources.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.compute.ComputeJobResultPolicy.*;

/**
 * Tests for {@link org.apache.ignite.compute.ComputeTaskName} annotation.
 */
public class GridTaskNameAnnotationSelfTest extends GridCommonAbstractTest {
    /** Task name. */
    private static final String TASK_NAME = "test-task";

    /** Peer deploy aware task name. */
    private static final String PEER_DEPLOY_AWARE_TASK_NAME = "peer-deploy-aware-test-task";

    /**
     * Starts grid.
     */
    public GridTaskNameAnnotationSelfTest() {
        super(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClass() throws Exception {
        assert grid().compute().execute(TestTask.class, null).equals(TASK_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testClassPeerDeployAware() throws Exception {
        assert grid().compute().execute(PeerDeployAwareTestTask.class, null).equals(PEER_DEPLOY_AWARE_TASK_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInstance() throws Exception {
        assert grid().compute().execute(new TestTask(), null).equals(TASK_NAME);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInstancePeerDeployAware() throws Exception {
        assert grid().compute().execute(new PeerDeployAwareTestTask(), null).
            equals(PEER_DEPLOY_AWARE_TASK_NAME);
    }

    /**
     * Test task.
     */
    @ComputeTaskName(TASK_NAME)
    private static class TestTask implements ComputeTask<Void, String> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Void arg) throws IgniteCheckedException {
            return F.asMap(new ComputeJobAdapter() {
                @IgniteTaskSessionResource
                private ComputeTaskSession ses;

                @Override public Object execute() {
                    return ses.getTaskName();
                }
            }, F.rand(subgrid));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
            throws IgniteCheckedException {
            return WAIT;
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return F.first(results).getData();
        }
    }

    /**
     * Test task that implements {@link org.apache.ignite.internal.util.lang.GridPeerDeployAware}.
     */
    @ComputeTaskName(PEER_DEPLOY_AWARE_TASK_NAME)
    private static class PeerDeployAwareTestTask extends TestTask implements GridPeerDeployAware {
        /** {@inheritDoc} */
        @Override public Class<?> deployClass() {
            return getClass();
        }

        /** {@inheritDoc} */
        @Override public ClassLoader classLoader() {
            return getClass().getClassLoader();
        }
    }
}
