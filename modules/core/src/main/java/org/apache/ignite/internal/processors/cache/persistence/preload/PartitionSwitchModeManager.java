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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Predicate;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheDataStoreEx;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.persistence.DbCheckpointListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class PartitionSwitchModeManager implements DbCheckpointListener {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final ConcurrentLinkedQueue<SwitchModeRequest> switchReqs = new ConcurrentLinkedQueue<>();

    /**
     * @param cctx Shared context.
     */
    public PartitionSwitchModeManager(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;
        this.log = cctx.logger(PartitionSwitchModeManager.class);
    }

    /**
     * @param p The condition to check.
     * @return The number of pending switch request satisfyed by given condition.
     */
    public int pendingRequests(Predicate<CacheDataStoreEx.StorageMode> p) {
        int cnt = 0;

        for (SwitchModeRequest rq : switchReqs) {
            if (p.test(rq.nextMode))
                cnt++;
        }

        return cnt;
    }

    /** {@inheritDoc} */
    @Override public void onMarkCheckpointBegin(Context ctx) throws IgniteCheckedException {
        SwitchModeRequest rq;

        while ((rq = switchReqs.poll()) != null) {
            for (Map.Entry<Integer, Set<Integer>> e : rq.parts.entrySet()) {
                CacheGroupContext grp = cctx.cache().cacheGroup(e.getKey());

                for (Integer partId : e.getValue()) {
                    GridDhtLocalPartition locPart = grp.topology().localPartition(partId);

                    if (locPart.dataStoreMode() == rq.nextMode)
                        continue;

                    //TODO invalidate partition

                    IgniteCacheOffheapManager.CacheDataStore currStore = locPart.dataStore(locPart.dataStoreMode());

                    // Pre-init the new storage.
                    locPart.dataStore(rq.nextMode)
                        .init(currStore.fullSize(), currStore.updateCounter(), currStore.cacheSizes());

                    // Switching mode under the write lock.
                    locPart.dataStoreMode(rq.nextMode);
                }
            }

            rq.rqFut.onDone(true);
        }
    }

    /** {@inheritDoc} */
    @Override public void onCheckpointBegin(Context ctx) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void beforeCheckpointBegin(Context ctx) throws IgniteCheckedException {
        // No-op.
    }

    /**
     * @param mode The storage mode to switch to.
     * @param parts The set of partitions to change storage mode.
     * @return The future which will be completed when request is done.
     */
    public GridFutureAdapter<Boolean> offerSwitchRequest(
        CacheDataStoreEx.StorageMode mode,
        Map<Integer, Set<Integer>> parts
    ) {
        SwitchModeRequest req = new SwitchModeRequest(mode, parts);

        boolean offered = switchReqs.offer(req);

        assert offered;

        U.log(log, "Change partition mode request registered [mode=" + mode + ", parts=" + parts + ']');

        return req.rqFut;
    }

    /**
     *
     */
    private static class SwitchModeRequest {
        /** The storage mode to switch to. */
        private final CacheDataStoreEx.StorageMode nextMode;

        /** The map of cache groups and corresponding partition to switch mode to. */
        private final Map<Integer, Set<Integer>> parts;

        /** The future will be completed when the request has been processed. */
        private final GridFutureAdapter<Boolean> rqFut = new GridFutureAdapter<>();

        /**
         * @param nextMode The mode to set to.
         * @param parts The partitions to switch mode to.
         */
        public SwitchModeRequest(
            CacheDataStoreEx.StorageMode nextMode,
            Map<Integer, Set<Integer>> parts
        ) {
            this.nextMode = nextMode;
            this.parts = parts;
        }
    }
}