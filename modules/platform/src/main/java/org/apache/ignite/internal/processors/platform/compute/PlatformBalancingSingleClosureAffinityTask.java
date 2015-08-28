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

package org.apache.ignite.internal.processors.platform.compute;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.platform.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Interop single-closure task with node balancing.
 */
@ComputeTaskNoResultCache
public class PlatformBalancingSingleClosureAffinityTask extends PlatformAbstractTask {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job. */
    private PlatformJob job;

    /** Node, according to affinity. */
    private ClusterNode node;

    /**
     * Constructor.
     *
     * @param ctx Platform context.
     * @param taskPtr Task pointer.
     */
    public PlatformBalancingSingleClosureAffinityTask(PlatformContext ctx, long taskPtr) {
        super(ctx, taskPtr);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
        @Nullable Object arg) {
        assert job != null : "Job null-check must be performed in native platform.";

        return Collections.singletonMap(job, node);
    }

    /**
     * @param job Job.
     */
    public void job(PlatformJob job) {
        this.job = job;
    }

    /**
     * Init affinity.
     *
     * @param cacheName Cache name.
     * @param affKey Affinity key.
     * @param ctx Kernal context.
     */
    public void affinity(String cacheName, Object affKey, GridKernalContext ctx) {
        try {
            final Object affKey0 = ctx.affinity().affinityKey(cacheName, affKey);

            node = ctx.affinity().mapKeyToNode(cacheName, affKey0);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }
}
