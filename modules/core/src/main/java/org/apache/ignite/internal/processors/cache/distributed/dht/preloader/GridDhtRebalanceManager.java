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

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
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
        assert !cctx.kernalContext().clientNode();

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

        AffinityTopologyVersion resVer = null;

        if (rebTopVer.equals(NONE)) {
            if (resVer == null)
                resVer = exchId.topologyVersion();

            Runnable r = null;

            boolean assignsCancelled = false;

            IgniteCacheSnapshotManager snp = cctx.snapshot();

            List<CacheGroupContext> rebList = cctx.cache().cacheGroups()
                .stream()
                .sorted(Comparator.comparingInt(grp -> grp.config().getRebalanceOrder()))
                .collect(Collectors.toList());

            for (CacheGroupContext grp : rebList) {
                long delay = grp.config().getRebalanceDelay();

                boolean disableRebalance = snp.partitionsAreFrozen(grp);

                GridDhtPreloaderAssignments assigns = null;

                // Don't delay for dummy reassigns to avoid infinite recursion.
                if ((delay == 0 || forcePreload) && !disableRebalance)
                    assigns = grp.preloader().generateAssignments(exchId, exchFut);

                if (assigns != null)
                    assignsCancelled |= assigns.cancelled();

                Runnable cur = grp.preloader().addAssignments(assigns,
                    forcePreload,
                    cnt,
                    r,
                    forcedRebFut);

                if (cur != null)
                    r = cur;
            }

            if (forcedRebFut != null)
                forcedRebFut.markInitialized();

            if (assignsCancelled) {
                U.log(log, "Skipping rebalancing (obsolete exchange ID) " +
                    "[top=" + resVer + ", evt=" + exchId.discoveryEventName() +
                    ", node=" + exchId.nodeId() + ']');
            }
            else if (r != null) {
                U.log(log, "Rebalancing scheduled [top=" + resVer + ", force=" + (exchFut == null) +
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
