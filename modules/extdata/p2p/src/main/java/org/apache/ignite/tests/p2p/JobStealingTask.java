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

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.jetbrains.annotations.Nullable;

/**
 * Task for testing job stealing.
 */
public class JobStealingTask extends ComputeTaskAdapter<Object, Map<UUID, Integer>> {
    /** Number of jobs to spawn from task. */
    private static final int N_JOBS = 4;

    /** Grid. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Logger. */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) {
        assert !subgrid.isEmpty();

        Map<ComputeJobAdapter, ClusterNode> map = U.newHashMap(subgrid.size());

        // Put all jobs onto one node.
        for (int i = 0; i < N_JOBS; i++)
            map.put(new GridJobStealingJob(5000L), subgrid.get(0));

        return map;
    }

    /** {@inheritDoc} */
    @Override public Map<UUID, Integer> reduce(List<ComputeJobResult> results) {
        Map<UUID, Integer> ret = U.newHashMap(results.size());

        for (ComputeJobResult res : results) {
            if (log.isInfoEnabled())
                log.info("Job result: " + res.getData());

            UUID resUuid = (UUID)res.getData();

            ret.put(resUuid,
                ret.containsKey(resUuid) ? ret.get(resUuid) + 1 : 1);
        }

        return ret;
    }

    /**
     * Job stealing job.
     */
    private static final class GridJobStealingJob extends ComputeJobAdapter {
        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Logger. */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param arg Job argument.
         */
        GridJobStealingJob(Long arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            if (log.isInfoEnabled())
                log.info("Started job on node: " + ignite.cluster().localNode().id());

            try {
                Long sleep = argument(0);

                assert sleep != null;

                Thread.sleep(sleep);
            }
            catch (InterruptedException e) {
                if (log.isInfoEnabled())
                    log.info("Job got interrupted on node: " + ignite.cluster().localNode().id());

                throw new IgniteException("Job got interrupted.", e);
            }
            finally {
                if (log.isInfoEnabled())
                    log.info("Job finished on node: " + ignite.cluster().localNode().id());
            }

            return ignite.cluster().localNode().id();
        }
    }
}