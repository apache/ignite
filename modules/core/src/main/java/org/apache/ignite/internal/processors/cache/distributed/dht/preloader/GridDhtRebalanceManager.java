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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteCacheSnapshotManager;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.LoggerResource;

import static org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion.NONE;

/**
 *
 */
public class GridDhtRebalanceManager {
    /** Context. */
    protected GridCacheSharedContext cctx;

    /**
     * Latest started rebalance topology version but possibly not finished yet. Value {@code NONE} means that previous
     * rebalance is undefined and the new one should be initiated.
     *
     * Should not be used to determine latest rebalanced topology.
     */
    private volatile AffinityTopologyVersion rebTopVer = NONE;

    /** Auto-injected logger instance. */
    @SuppressWarnings("unused")
    @LoggerResource
    private IgniteLogger log;

    /**
     *
     */
    public GridDhtRebalanceManager(GridCacheSharedContext cctx) {
        this.cctx = cctx;
    }

    /**
     *
     */
    @Deprecated
    public AffinityTopologyVersion rebalanceTopologyVersion() {
        return rebTopVer;
    }

    /**
     *
     */
    public void rebalance(
        GridDhtPartitionExchangeId exchId,
        GridDhtPartitionsExchangeFuture exchFut,
        long cnt,
        GridCompoundFuture<Boolean, Boolean> forcedRebFut) {
        boolean forcePreload = forcedRebFut != null;

        if (exchFut != null)
            for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                if (grp.isLocal())
                    continue;

                if (grp.preloader().rebalanceRequired(rebTopVer, exchFut))
                    rebTopVer = NONE;
            }

        // Schedule rebalance if force rebalance or force reassign occurs.
        if (exchFut == null)
            rebTopVer = NONE;

        Map<Integer, GridDhtPreloaderAssignments> assignsMap = null;

        AffinityTopologyVersion resVer = null;

        if (!cctx.kernalContext().clientNode() && rebTopVer.equals(NONE)) {
            assignsMap = new HashMap<>();

            IgniteCacheSnapshotManager snp = cctx.snapshot();

            for (final CacheGroupContext grp : cctx.cache().cacheGroups()) {
                long delay = grp.config().getRebalanceDelay();

                boolean disableRebalance = snp.partitionsAreFrozen(grp);

                GridDhtPreloaderAssignments assigns = null;

                // Don't delay for dummy reassigns to avoid infinite recursion.
                if ((delay == 0 || forcePreload) && !disableRebalance)
                    assigns = grp.preloader().generateAssignments(exchId, exchFut);

                assignsMap.put(grp.groupId(), assigns);

                if (resVer == null && !grp.isLocal())
                    resVer = grp.topology().readyTopologyVersion();
            }
        }

        if (resVer == null)
            resVer = exchId.topologyVersion();

        if (assignsMap != null && rebTopVer.equals(NONE)) {
            int size = assignsMap.size();

            NavigableMap<Integer, List<Integer>> orderMap = new TreeMap<>();

            for (Map.Entry<Integer, GridDhtPreloaderAssignments> e : assignsMap.entrySet()) {
                int grpId = e.getKey();

                CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                int order = grp.config().getRebalanceOrder();

                orderMap.computeIfAbsent(order, k -> new ArrayList<>(size));

                orderMap.get(order).add(grpId);
            }

            Runnable r = null;

            List<String> rebList = new LinkedList<>();

            boolean assignsCancelled = false;

            for (Integer order : orderMap.descendingKeySet()) {
                for (Integer grpId : orderMap.get(order)) {
                    CacheGroupContext grp = cctx.cache().cacheGroup(grpId);

                    GridDhtPreloaderAssignments assigns = assignsMap.get(grpId);

                    if (assigns != null)
                        assignsCancelled |= assigns.cancelled();

                    Runnable cur = grp.preloader().addAssignments(assigns,
                        forcePreload,
                        cnt,
                        r,
                        forcedRebFut);

                    if (cur != null) {
                        rebList.add(grp.cacheOrGroupName());

                        r = cur;
                    }
                }
            }

            if (forcedRebFut != null)
                forcedRebFut.markInitialized();

            if (assignsCancelled) {
                U.log(log, "Skipping rebalancing (obsolete exchange ID) " +
                    "[top=" + resVer + ", evt=" + exchId.discoveryEventName() +
                    ", node=" + exchId.nodeId() + ']');
            }
            else if (r != null) {
                Collections.reverse(rebList);

                U.log(log, "Rebalancing scheduled [order=" + rebList +
                    ", top=" + resVer + ", force=" + (exchFut == null) +
                    ", evt=" + exchId.discoveryEventName() +
                    ", node=" + exchId.nodeId() + ']');

                rebTopVer = resVer;

                r.run();
            }
            else
                U.log(log, "Skipping rebalancing (nothing scheduled) " +
                    "[top=" + resVer + ", force=" + (exchFut == null) +
                    ", evt=" + exchId.discoveryEventName() +
                    ", node=" + exchId.nodeId() + ']');
        }
        else
            U.log(log, "Skipping rebalancing (no affinity changes) " +
                "[top=" + resVer +
                ", rebTopVer=" + rebTopVer +
                ", evt=" + exchId.discoveryEventName() +
                ", evtNode=" + exchId.nodeId() +
                ", client=" + cctx.kernalContext().clientNode() + ']');
    }
}
