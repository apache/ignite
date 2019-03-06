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

package org.apache.ignite.internal.processors.platform.compute;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskNoResultCache;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Interop multi-closure task with broadcast semantics.
 */
@ComputeTaskNoResultCache
public class PlatformBroadcastingMultiClosureTask extends PlatformAbstractTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** Jobs. */
    private Collection<PlatformJob> jobs;

    /**
     * Constructor.
     *
     * @param ctx Platform context.
     * @param taskPtr Task pointer.
     */
    public PlatformBroadcastingMultiClosureTask(PlatformContext ctx, long taskPtr) {
        super(ctx, taskPtr);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) {
        assert !F.isEmpty(jobs) : "Jobs emptiness must be checked in native platform.";

        if (!F.isEmpty(subgrid)) {
            Map<ComputeJob, ClusterNode> map = new HashMap<>(jobs.size() * subgrid.size(), 1);

            for (PlatformJob job : jobs) {
                boolean first = true;

                for (ClusterNode node : subgrid) {
                    if (first) {
                        map.put(job, node);

                        first = false;
                    }
                    else
                        map.put(ctx.createClosureJob(this, job.pointer(), job.job()), node);
                }
            }

            return map;
        }
        else
            return Collections.emptyMap();
    }

    /**
     * @param jobs Jobs.
     */
    public void jobs(Collection<PlatformJob> jobs) {
        this.jobs = jobs;
    }
}