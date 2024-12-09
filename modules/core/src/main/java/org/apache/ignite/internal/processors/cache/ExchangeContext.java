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

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteFeatures.PME_FREE_SWITCH;
import static org.apache.ignite.internal.IgniteFeatures.allNodesSupports;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.isSnapshotOperation;

/**
 *
 */
public class ExchangeContext {
    /** Logger. */
    private final IgniteLogger log;

    /** Cache groups to request affinity for during local join exchange. */
    private Set<Integer> requestGrpsAffOnJoin;

    /** Per-group affinity fetch on join (old protocol). */
    private boolean fetchAffOnJoin;

    /** PME is not required. */
    private boolean exchangeFreeSwitch;

    /**
     * Whether cache requests that were mapped to an outdated topology version should be forcibly remapped to
     * the up-to-date one. {@code True} forces all cache requests (including those that were mapped to the stale topology
     * version with the same key affinity distribution) to wait current PME completion on all nodes.
     * This is critical in situations when you want to ensure that no cache requests can be executed until PME is
     * finished on all nodes (e.g. snapshot operation).
     *
     * @see GridCacheAffinityManager#isCompatibleWithCurrentTopologyVersion(AffinityTopologyVersion)
     */
    private final boolean remapStaleCacheReq;

    /** Merges allowed flag. */
    private final boolean merge;

    /** */
    private final ExchangeDiscoveryEvents evts;

    /**
     * @param cctx Context.
     * @param crd Coordinator flag.
     * @param fut Exchange future.
     */
    public ExchangeContext(GridCacheSharedContext<?, ?> cctx, boolean crd, GridDhtPartitionsExchangeFuture fut) {
        log = cctx.logger(getClass());

        boolean allNodesSupportsPmeFreeSwitch = allNodesSupports(fut.firstEventCache().allNodes(), PME_FREE_SWITCH);

        if (!allNodesSupportsPmeFreeSwitch)
            log.warning("Current topology does not support the PME-free switch. Please check all nodes support" +
                " this feature and it was not explicitly disabled by IGNITE_PME_FREE_SWITCH_DISABLED JVM option.");

        boolean pmeFreeAvailable = (fut.wasRebalanced() && fut.isBaselineNodeFailed()) || isSnapshotOperation(fut.firstEvent());

        if (pmeFreeAvailable &&
            allNodesSupportsPmeFreeSwitch) {
            exchangeFreeSwitch = true;
            merge = false;
        }
        else if (crd && fut.localJoinExchange()) {
            fetchAffOnJoin = true;
            merge = false;
        }
        else {
            boolean startCaches = fut.exchangeId().isJoined() &&
                fut.sharedContext().cache().hasCachesReceivedFromJoin(fut.exchangeId().eventNode());

            fetchAffOnJoin = false;

            merge = !startCaches &&
                fut.firstEvent().type() != EVT_DISCOVERY_CUSTOM_EVT;
        }

        evts = new ExchangeDiscoveryEvents(fut);

        remapStaleCacheReq = isSnapshotOperation(fut.firstEvent());
    }

    /**
     * @return Discovery events.
     */
    public ExchangeDiscoveryEvents events() {
        return evts;
    }

    /**
     * @return {@code True} if on local join need fetch affinity per-group (old protocol),
     *      otherwise affinity is sent in {@link GridDhtPartitionsFullMessage}.
     */
    public boolean fetchAffinityOnJoin() {
        return fetchAffOnJoin;
    }

    /**
     * @return {@code True} if it's safe to perform PME-free switch.
     */
    public boolean exchangeFreeSwitch() {
        return exchangeFreeSwitch;
    }

    /**
     * @param grpId Cache group ID.
     */
    synchronized void addGroupAffinityRequestOnJoin(Integer grpId) {
        if (requestGrpsAffOnJoin == null)
            requestGrpsAffOnJoin = new HashSet<>();

        requestGrpsAffOnJoin.add(grpId);
    }

    /**
     * @return Groups to request affinity for.
     */
    @Nullable public synchronized Set<Integer> groupsAffinityRequestOnJoin() {
        return requestGrpsAffOnJoin;
    }

    /**
     * @return {@code True} if exchanges merge is allowed during current exchange.
     */
    public boolean mergeExchanges() {
        return merge;
    }

   /**
     * @return Whether cache requests that were mapped to an outdated topology version should be forcibly remapped to
     * the up-to-date one.
     */
    public boolean remapStaleCacheRequests() {
        return remapStaleCacheReq;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExchangeContext.class, this);
    }
}
