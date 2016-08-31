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

package org.apache.ignite.internal.managers.failover;

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTaskSessionImpl;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.spi.failover.FailoverSpi;
import org.jetbrains.annotations.Nullable;

/**
 * Grid failover spi manager.
 */
public class GridFailoverManager extends GridManagerAdapter<FailoverSpi> {
    /**
     * @param ctx Kernal context.
     */
    public GridFailoverManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getFailoverSpi());
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        startSpi();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @param taskSes Task session.
     * @param jobRes Job result.
     * @param top Collection of all topology nodes.
     * @param affPartId Partition number.
     * @param affKey Affinity key.
     * @param affCacheName Affinity cache name.
     * @param topVer Affinity topology version.
     * @return New node to route this job to.
     */
    public ClusterNode failover(GridTaskSessionImpl taskSes,
        ComputeJobResult jobRes,
        List<ClusterNode> top,
        int affPartId,
        @Nullable Object affKey,
        @Nullable String affCacheName,
        @Nullable AffinityTopologyVersion topVer) {
        return getSpi(taskSes.getFailoverSpi()).failover(new GridFailoverContextImpl(taskSes,
            jobRes,
            ctx.loadBalancing(),
            affPartId,
            affKey,
            affCacheName,
            topVer),
            top);
    }
}