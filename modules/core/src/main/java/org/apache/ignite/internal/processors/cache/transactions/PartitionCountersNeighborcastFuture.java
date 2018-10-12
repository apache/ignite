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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheCompoundIdentityFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.PartitionUpdateCountersMessage;
import org.apache.ignite.internal.processors.cache.mvcc.msg.PartitionCountersNeighborcastRequest;
import org.apache.ignite.internal.processors.cache.mvcc.msg.PartitionCountersNeighborcastResponse;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

public class PartitionCountersNeighborcastFuture extends GridCacheCompoundIdentityFuture<Void> {
    /** */
    private final IgniteUuid futId = IgniteUuid.randomUuid();
    /** */
    private boolean trackable = true;
    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    public PartitionCountersNeighborcastFuture(GridCacheSharedContext<?, ?> cctx) {
        super(null);
        this.cctx = cctx;
    }

    public void init(Iterable<UUID> peers, Collection<PartitionUpdateCountersMessage> cntrs) {
        assert cntrs != null;

        for (UUID peer : peers) {
            MiniFuture miniFut = new MiniFuture(peer);
            try {
                cctx.io().send(peer, new PartitionCountersNeighborcastRequest(cntrs, futId, miniFut.miniId), SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                miniFut.onDone();
                // t0d0
            }
        }

        cctx.mvcc().addFuture(this, futId);

        markInitialized();
    }

    @Override public boolean onDone(@Nullable Void res, @Nullable Throwable err) {
        boolean comp = super.onDone(res, err);
        if (comp)
            cctx.mvcc().removeFuture(futId);
        return comp;
    }

    @Override public IgniteUuid futureId() {
        return futId;
    }

    public void onResult(UUID id, PartitionCountersNeighborcastResponse res) {
        for (IgniteInternalFuture<?> fut : futures()) {
            if (fut instanceof MiniFuture) {
                final MiniFuture f = (MiniFuture)fut;

                if (f.miniId.equals(res.miniId()))
                    cctx.kernalContext().closure().runLocalSafe(f::onDone);
            }
        }
    }

    @Override public boolean onNodeLeft(UUID nodeId) {
        for (IgniteInternalFuture<?> fut : futures()) {
            if (fut instanceof MiniFuture) {
                final MiniFuture f = (MiniFuture)fut;

                if (f.nodeId.equals(nodeId))
                    cctx.kernalContext().closure().runLocalSafe(f::onDone);
            }
        }

        return true;
    }

    @Override public boolean trackable() {
        return trackable;
    }

    @Override public void markNotTrackable() {
        trackable = false;
    }

    private class MiniFuture extends GridFutureAdapter<Void> {
        /** */
        private final IgniteUuid miniId = IgniteUuid.randomUuid();
        /** */
        private final UUID nodeId;

        private MiniFuture(UUID nodeId) {
            this.nodeId = nodeId;
        }
    }
}
