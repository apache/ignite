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

package org.apache.ignite.internal.processors.cache;

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityAssignment;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheEntry;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopologyImpl;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.AFFINITY_POOL;

/**
 *
 */
public class CacheGroupInfrastructure {
    /** */
    private GridAffinityAssignmentCache aff;

    /** */
    private final int grpId;

    /** */
    private final CacheConfiguration ccfg;

    /** */
    private final GridCacheSharedContext ctx;

    /** */
    private final IgniteLogger log;

    /** */
    private GridDhtPartitionTopologyImpl top;

    /** */
    private AffinityTopologyVersion grpStartVer;

    /** */
    private AffinityTopologyVersion locStartVer;

    /**
     * @param grpId Group ID.
     * @param ctx Context.
     * @param ccfg Cache configuration.
     */
    CacheGroupInfrastructure(GridCacheSharedContext ctx,
        int grpId,
        CacheConfiguration ccfg,
        AffinityTopologyVersion grpStartVer,
        AffinityTopologyVersion locStartVer) {
        assert grpId != 0 : "Invalid group ID [cache=" + ccfg.getName() + ", grpName=" + ccfg.getGroupName() + ']';
        assert ccfg != null;

        this.grpId = grpId;
        this.ctx = ctx;
        this.ccfg = ccfg;
        this.grpStartVer = grpStartVer;
        this.locStartVer = locStartVer;

        log = ctx.kernalContext().log(getClass());
    }

    public AffinityTopologyVersion groupStartVersion() {
        return grpStartVer;
    }

    public AffinityTopologyVersion localStartVersion() {
        return locStartVer;
    }

    /**
     * @return {@code True} if cache is local.
     */
    public boolean isLocal() {
        return ccfg.getCacheMode() == LOCAL;
    }

    public CacheConfiguration config() {
        return ccfg;
    }

    public GridAffinityAssignmentCache affinity() {
        return aff;
    }

    @Nullable public String name() {
        return ccfg.getGroupName();
    }

    public int groupId() {
        return grpId;
    }

    public boolean sharedGroup() {
        return ccfg.getGroupName() != null;
    }

    public void start() throws IgniteCheckedException {
        aff = new GridAffinityAssignmentCache(ctx.kernalContext(),
            name(),
            grpId,
            ccfg.getAffinity(),
            ccfg.getNodeFilter(),
            ccfg.getBackups(),
            ccfg.getCacheMode() == LOCAL);

        if (ccfg.getCacheMode() != LOCAL) {
            GridCacheMapEntryFactory entryFactory = new GridCacheMapEntryFactory() {
                @Override public GridCacheMapEntry create(
                    GridCacheContext ctx,
                    AffinityTopologyVersion topVer,
                    KeyCacheObject key,
                    int hash,
                    CacheObject val
                ) {
                    return new GridDhtCacheEntry(ctx, topVer, key, hash, val);
                }
            };

            top = new GridDhtPartitionTopologyImpl(ctx, entryFactory);

            if (!ctx.kernalContext().clientNode()) {
                ctx.io().addHandler(groupId(), GridDhtAffinityAssignmentRequest.class,
                    new IgniteBiInClosure<UUID, GridDhtAffinityAssignmentRequest>() {
                        @Override public void apply(UUID nodeId, GridDhtAffinityAssignmentRequest msg) {
                            processAffinityAssignmentRequest(nodeId, msg);
                        }
                    });
            }
        }

        ctx.affinity().onCacheGroupCreated(this);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void processAffinityAssignmentRequest(final UUID nodeId,
        final GridDhtAffinityAssignmentRequest req) {
        if (log.isDebugEnabled())
            log.debug("Processing affinity assignment request [node=" + nodeId + ", req=" + req + ']');

        IgniteInternalFuture<AffinityTopologyVersion> fut = aff.readyFuture(req.topologyVersion());

        if (fut != null) {
            fut.listen(new CI1<IgniteInternalFuture<AffinityTopologyVersion>>() {
                @Override public void apply(IgniteInternalFuture<AffinityTopologyVersion> fut) {
                    processAffinityAssignmentRequest0(nodeId, req);
                }
            });
        }
        else
            processAffinityAssignmentRequest0(nodeId, req);
    }

    /**
     * @param nodeId Node ID.
     * @param req Request.
     */
    private void processAffinityAssignmentRequest0(UUID nodeId, final GridDhtAffinityAssignmentRequest req) {
        AffinityTopologyVersion topVer = req.topologyVersion();

        if (log.isDebugEnabled())
            log.debug("Affinity is ready for topology version, will send response [topVer=" + topVer +
                ", node=" + nodeId + ']');

        AffinityAssignment assignment = aff.cachedAffinity(topVer);

        GridDhtAffinityAssignmentResponse res = new GridDhtAffinityAssignmentResponse(grpId,
            topVer,
            assignment.assignment());

        if (aff.centralizedAffinityFunction()) {
            assert assignment.idealAssignment() != null;

            res.idealAffinityAssignment(assignment.idealAssignment());
        }

        try {
            ctx.io().send(nodeId, res, AFFINITY_POOL);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send affinity assignment response to remote node [node=" + nodeId + ']', e);
        }
    }

    /**
     * @param reconnectFut
     */
    public void onDisconnected(IgniteFuture reconnectFut) {
        // TODO IGNITE-5075.
        IgniteCheckedException err = new IgniteClientDisconnectedCheckedException(reconnectFut,
            "Failed to wait for topology update, client disconnected.");

        if (aff != null)
            aff.cancelFutures(err);
    }

    /**
     * @return {@code True} if rebalance is enabled.
     */
    public boolean rebalanceEnabled() {
        return ccfg.getRebalanceMode() != NONE;
    }

    /**
     *
     */
    public void onReconnected() {
        // TODO IGNITE-5075.
        aff.onReconnected();

        if (top != null)
            top.onReconnected();
    }

    public GridDhtPartitionTopology topology() {
        if (top == null)
            throw new IllegalStateException("Topology is not initialized: " + groupName());

        return top;
    }
}
