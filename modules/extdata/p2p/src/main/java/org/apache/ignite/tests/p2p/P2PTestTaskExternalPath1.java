/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.tests.p2p;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskSessionResource;

/**
 * Test P2P task.
 */
public class P2PTestTaskExternalPath1 extends ComputeTaskAdapter<Object, Integer> {
    /** */
    @LoggerResource
    private IgniteLogger log;

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings({"unchecked"})
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
        if (log.isInfoEnabled()) {
            log.info("Mapping [task=" + this + ", subgrid=" + F.viewReadOnly(subgrid, F.node2id()) +
                ", arg=" + arg + ']');
        }

        Set<UUID> nodeIds;

        boolean sleep;

        if (arg instanceof Object[]) {
            nodeIds = Collections.singleton((UUID)(((Object[])arg)[0]));

            sleep = (Boolean)((Object[])arg)[1];
        }
        else if (arg instanceof List) {
            nodeIds = new HashSet<>((Collection<UUID>)arg);

            sleep = false;
        }
        else {
            nodeIds = Collections.singleton((UUID)arg);

            sleep = false;
        }

        Map<TestJob1, ClusterNode> jobs = U.newHashMap(subgrid.size());

        for (ClusterNode node : subgrid) {
            if (nodeIds.contains(node.id()))
                jobs.put(new TestJob1(node.id(), sleep), node);
        }

        if (!jobs.isEmpty())
            return jobs;

        throw new IgniteException("Failed to find target node: " + arg);
    }

    /**
     * {@inheritDoc}
     */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        return results.get(0).getData();
    }

    /**
     * Simple job class
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJob1 extends ComputeJobAdapter {
        /** Task session. */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @IgniteInstanceResource
        private Ignite g;

        /** */
        private boolean sleep;

        /**
         *
         */
        public TestJob1() {
            // No-op.
        }

        /**
         * @param nodeId Node ID for node this job is supposed to execute on.
         * @param sleep Sleep flag.
         */
        public TestJob1(UUID nodeId, boolean sleep) {
            super(nodeId);

            this.sleep = sleep;
        }

        /** {@inheritDoc} */
        @Override public Integer execute() {
            assert g.configuration().getNodeId().equals(argument(0));

            log.info("Running job on node: " + g.cluster().localNode().id());

            if (sleep) {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                }
                catch (InterruptedException e) {
                    log.info("Job has been cancelled. Caught exception: " + e);

                    Thread.currentThread().interrupt();
                }
            }

            return System.identityHashCode(ses.getClassLoader());
        }
    }
}