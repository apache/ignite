/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.jetbrains.annotations.NotNull;
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
    @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
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