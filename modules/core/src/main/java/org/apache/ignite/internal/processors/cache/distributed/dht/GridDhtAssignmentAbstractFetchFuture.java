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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 */
public abstract class GridDhtAssignmentAbstractFetchFuture<R> extends GridFutureAdapter<R> {

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** */
    protected final GridCacheSharedContext ctx;

    /** List of available nodes this future can fetch data from. */
    @GridToStringInclude
    protected Queue<ClusterNode> availableNodes;

    /** Pending node from which response is being awaited. */
    protected ClusterNode pendingNode;

    /** */
    @GridToStringInclude
    protected final T2<Integer, AffinityTopologyVersion> key;

    /**
     * @param ctx
     * @param topVer
     * @param cacheId
     */
    public GridDhtAssignmentAbstractFetchFuture(GridCacheSharedContext ctx,
        AffinityTopologyVersion topVer, int cacheId) {
        this.ctx = ctx;
        this.key = new T2<>(cacheId, topVer);

        if (log == null)
            log = U.logger(ctx.kernalContext(), logRef, GridDhtAssignmentAbstractFetchFuture.class);
    }

    /**
     * Initializes fetch future.
     */
    public void init() {
        ctx.affinity().addDhtAssignmentFetchFuture(this);

        requestFromNextNode();
    }

    /**
     * @return Future key.
     */
    public T2<Integer, AffinityTopologyVersion> key() {
        return key;
    }

    /**
     * Requests affinity from next node in the list.
     */
    protected abstract void requestFromNextNode();

    /**
     * @param leftNodeId Left node ID.
     */
    public void onNodeLeft(UUID leftNodeId) {
        synchronized (this) {
            if (pendingNode != null && pendingNode.id().equals(leftNodeId)) {
                availableNodes.remove(pendingNode);

                pendingNode = null;
            }
        }

        requestFromNextNode();
    }

    /**
     * @param nodeId Node ID.
     * @param res Response.
     */
    public abstract void onResponse(UUID nodeId, R res);

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable R res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            ctx.affinity().removeDhtAssignmentFetchFuture(this);

            return true;
        }

        return false;
    }

}
