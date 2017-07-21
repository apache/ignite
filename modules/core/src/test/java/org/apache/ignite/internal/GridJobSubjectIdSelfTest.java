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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.events.JobEvent;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 * Test job subject ID propagation.
 */
public class GridJobSubjectIdSelfTest extends GridCommonAbstractTest {
    /** Job subject ID. */
    private static volatile UUID taskSubjId;

    /** Job subject ID. */
    private static volatile UUID jobSubjId;

    /** Event subject ID. */
    private static volatile UUID evtSubjId;

    /** First node. */
    private Ignite node1;

    /** Second node. */
    private Ignite node2;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        node1 = startGrid(1);
        node2 = startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        node1 = null;
        node2 = null;
    }

    /**
     * Test job subject ID propagation.
     *
     * @throws Exception If failed.
     */
    public void testJobSubjectId() throws Exception {
        node2.events().localListen(new IgnitePredicate<Event>() {
            @Override public boolean apply(Event evt) {
                JobEvent evt0 = (JobEvent)evt;

                assert evtSubjId == null;

                evtSubjId = evt0.taskSubjectId();

                return false;
            }
        }, EventType.EVT_JOB_STARTED);

        node1.compute().execute(new Task(node2.cluster().localNode().id()), null);

        assertEquals(taskSubjId, jobSubjId);
        assertEquals(taskSubjId, evtSubjId);
    }

    /**
     * Task class.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Task extends ComputeTaskAdapter<Object, Object> {
        /** Target node ID. */
        private UUID targetNodeId;

        /** Session. */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /**
         * Constructor.
         *
         * @param targetNodeId Target node ID.
         */
        public Task(UUID targetNodeId) {
            this.targetNodeId = targetNodeId;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) {
            taskSubjId = ((GridTaskSessionInternal)ses).subjectId();

            ClusterNode node = null;

            for (ClusterNode subgridNode : subgrid) {
                if (F.eq(targetNodeId, subgridNode.id())) {
                    node = subgridNode;

                    break;
                }
            }

            assert node != null;

            return Collections.singletonMap(new Job(), node);
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Job class.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class Job extends ComputeJobAdapter {
        /** Session. */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
            jobSubjId = ((GridTaskSessionInternal)ses).subjectId();

            return null;
        }
    }
}