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

package org.apache.ignite.internal.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.compute.ComputeJobResultPolicy.FAILOVER;
import static org.apache.ignite.compute.ComputeJobResultPolicy.WAIT;

/**
 * Stop node task, applicable arguments:
 * <ul>
 *     <li>node id (as string) to stop or</li>
 *     <li>node type (see start nodes task).</li>
 * </ul>
 */
public class ClientStopNodeTask extends ComputeTaskSplitAdapter<String, Integer> {
    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** {@inheritDoc} */
    @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
        Collection<ComputeJob> jobs = new ArrayList<>();

        for (int i = 0; i < gridSize; i++)
            jobs.add(new StopJob(arg));

        return jobs;
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        ComputeJobResultPolicy superRes = super.result(res, rcvd);

        // Deny failover.
        if (superRes == FAILOVER)
            superRes = WAIT;

        return superRes;
    }

    /** {@inheritDoc} */
    @Override public Integer reduce(List<ComputeJobResult> results) {
        int stoppedCnt = 0;

        for (ComputeJobResult res : results)
            if (!res.isCancelled())
                stoppedCnt+=(Integer)res.getData();

        return stoppedCnt;
    }

    /**
     * Stop node job it is executed on.
     */
    private static class StopJob extends ComputeJobAdapter {
        /** */
        private final String gridType;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private StopJob(String gridType) {
            this.gridType = gridType;
        }

        /** {@inheritDoc} */
        @Override public Object execute() {
            log.info(">>> Stop node [nodeId=" + ignite.cluster().localNode().id() + ", name='" + ignite.name() + "']");

            String prefix = ClientStartNodeTask.getConfig(gridType).getIgniteInstanceName() + " (";

            if (!ignite.name().startsWith(prefix)) {
                int stoppedCnt = 0;

                for (Ignite g : G.allGrids())
                    if (g.name().startsWith(prefix)) {
                        try {
                            log.info(">>> Grid stopping [nodeId=" + g.cluster().localNode().id() +
                                ", name='" + g.name() + "']");

                            G.stop(g.name(), true);

                            stoppedCnt++;
                        }
                        catch (IllegalStateException e) {
                            log.warning("Failed to stop grid.", e);
                        }
                    }

                return stoppedCnt;
            }

            return 0;
        }
    }
}