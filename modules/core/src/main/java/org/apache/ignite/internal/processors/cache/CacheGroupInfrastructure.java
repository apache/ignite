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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.lang.IgniteFuture;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheRebalanceMode.NONE;

/**
 *
 */
public class CacheGroupInfrastructure {
    /** */
    private GridAffinityAssignmentCache aff;

    /** */
    private final int id;

    /** */
    private final CacheConfiguration ccfg;

    /** */
    private final GridCacheSharedContext ctx;

    /** */
    private GridDhtPartitionTopology top;

    /**
     * @param id Group ID.
     * @param ctx Context.
     * @param ccfg Cache configuration.
     */
    CacheGroupInfrastructure(int id, GridCacheSharedContext ctx, CacheConfiguration ccfg) {
        assert id != 0 : "Invalid group ID [cache=" + ccfg.getName() + ", grpName=" + ccfg.getGroupName() + ']';
        assert ccfg != null;

        this.id = id;
        this.ctx = ctx;
        this.ccfg = ccfg;
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

    @Nullable public String groupName() {
        return ccfg.getGroupName();
    }

    public int groupId() {
        return id;
    }

    public boolean sharedGroup() {
        return ccfg.getGroupName() != null;
    }

    public void start() throws IgniteCheckedException {
        aff = new GridAffinityAssignmentCache(ctx.kernalContext(),
            groupName(),
            id,
            ccfg.getAffinity(),
            ccfg.getNodeFilter(),
            ccfg.getBackups(),
            ccfg.getCacheMode() == LOCAL);
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
    }

    public GridDhtPartitionTopology topology() {
        return top;
    }
}
