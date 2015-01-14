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

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Special kill task that never fails over jobs.
 */
@GridInternal
class GridKillTask extends ComputeTaskAdapter<Boolean, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Restart flag. */
    private boolean restart;

    /** {@inheritDoc} */
    @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Boolean restart)
        throws IgniteCheckedException {
        assert restart != null;

        this.restart = restart;

        Map<ComputeJob, ClusterNode> jobs = U.newHashMap(subgrid.size());

        for (ClusterNode n : subgrid)
            if (!daemon(n))
                jobs.put(new GridKillJob(), n);

        return jobs;
    }

    /**
     * Checks if given node is a daemon node.
     *
     * @param n Node.
     * @return Whether node is daemon.
     */
    private boolean daemon(ClusterNode n) {
        return "true".equalsIgnoreCase(n.<String>attribute(ATTR_DAEMON));
    }

    /** {@inheritDoc} */
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        return ComputeJobResultPolicy.WAIT;
    }

    /** {@inheritDoc} */
    @Override public Void reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
        return null;
    }

    /**
     * Kill job.
     */
    private class GridKillJob extends ComputeJobAdapter {
        /** */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteCheckedException {
            if (restart)
                new Thread(new Runnable() {
                    @Override public void run() {
                        G.restart(true);
                    }
                },
                "grid-restarter").start();
            else
                new Thread(new Runnable() {
                    @Override public void run() {
                        G.kill(true);
                    }
                },
                "grid-stopper").start();

            return null;
        }
    }
}
