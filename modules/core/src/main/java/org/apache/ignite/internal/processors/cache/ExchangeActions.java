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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.jetbrains.annotations.Nullable;

/**
 * Cache change requests to execute when receive {@link DynamicCacheChangeBatch} event.
 */
public class ExchangeActions {
    /** */
    private List<CacheGroupActionData> cacheGrpsToStart;

    /** */
    private List<CacheGroupActionData> cacheGrpsToStop;

    /** */
    private Map<String, ActionData> cachesToStart;

    /** */
    private Map<String, ActionData> cachesToStop;

    /** */
    private Map<String, ActionData> cachesToResetLostParts;

    /** */
    private ClusterState newState;

    /**
     * @param grpId Group ID.
     * @return Always {@code true}, fails with assert error if inconsistent.
     */
    boolean checkStopRequestConsistency(int grpId) {
        Boolean destroy = null;

        // Check that caches associated with that group will be all stopped only or all destroyed.
        for (ExchangeActions.ActionData action : cacheStopRequests()) {
            if (action.descriptor().groupId() == grpId) {
                if (destroy == null)
                    destroy = action.request().destroy();
                else {
                    assert action.request().destroy() == destroy
                        : "Both cache stop only and cache destroy request associated with one group in batch "
                        + cacheStopRequests();
                }
            }
        }

        return true;
    }

    /**
     * @return {@code True} if server nodes should not participate in exchange.
     */
    public boolean clientOnlyExchange() {
        return F.isEmpty(cachesToStart) &&
            F.isEmpty(cachesToStop) &&
            F.isEmpty(cacheGrpsToStart) &&
            F.isEmpty(cacheGrpsToStop) &&
            F.isEmpty(cachesToResetLostParts);
    }

    /**
     * @return New caches start requests.
     */
    Collection<ActionData> cacheStartRequests() {
        return cachesToStart != null ? cachesToStart.values() : Collections.<ActionData>emptyList();
    }

    /**
     * @return Stop cache requests.
     */
    Collection<ActionData> cacheStopRequests() {
        return cachesToStop != null ? cachesToStop.values() : Collections.<ActionData>emptyList();
    }

    /**
     * @param ctx Context.
     */
    public void completeRequestFutures(GridCacheSharedContext ctx) {
        completeRequestFutures(cachesToStart, ctx);
        completeRequestFutures(cachesToStop, ctx);
        completeRequestFutures(cachesToResetLostParts, ctx);
    }

    /**
     * @return {@code True} if starting system caches.
     */
    public boolean systemCachesStarting() {
        if (cachesToStart != null) {
            for (ActionData data : cachesToStart.values()) {
                if (CU.isSystemCache(data.request().cacheName()))
                    return true;
            }
        }

        return false;
    }

    /**
     * @param map Actions map.
     * @param ctx Context.
     */
    private void completeRequestFutures(Map<String, ActionData> map, GridCacheSharedContext ctx) {
        if (map != null) {
            for (ActionData req : map.values())
                ctx.cache().completeCacheStartFuture(req.req, true, null);
        }
    }

    /**
     * @return {@code True} if have cache stop requests.
     */
    public boolean hasStop() {
        return !F.isEmpty(cachesToStop);
    }

    /**
     * @return Caches to reset lost partitions for.
     */
    public Set<String> cachesToResetLostPartitions() {
        Set<String> caches = null;
        
        if (cachesToResetLostParts != null)
            caches = new HashSet<>(cachesToResetLostParts.keySet());

        return caches != null ? caches : Collections.<String>emptySet();
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code True} if cache stop was requested.
     */
    public boolean cacheStopped(int cacheId) {
        if (cachesToStop != null) {
            for (ActionData cache : cachesToStop.values()) {
                if (cache.desc.cacheId() == cacheId)
                    return true;
            }
        }

        return false;
    }

    /**
     * @param cacheId Cache ID.
     * @return {@code True} if cache start was requested.
     */
    public boolean cacheStarted(int cacheId) {
        if (cachesToStart != null) {
            for (ActionData cache : cachesToStart.values()) {
                if (cache.desc.cacheId() == cacheId)
                    return true;
            }
        }

        return false;
    }

    /**
     * @param state New cluster state.
     */
    void newClusterState(ClusterState state) {
        assert state != null;

        newState = state;
    }

    /**
     * @return New cluster state if state change was requested.
     */
    @Nullable public ClusterState newClusterState() {
        return newState;
    }

    /**
     * @param map Actions map.
     * @param req Request.
     * @param desc Cache descriptor.
     * @return Actions map.
     */
    private Map<String, ActionData> add(Map<String, ActionData> map,
        DynamicCacheChangeRequest req,
        DynamicCacheDescriptor desc) {
        assert req != null;
        assert desc != null;

        if (map == null)
            map = new HashMap<>();

        ActionData old = map.put(req.cacheName(), new ActionData(req, desc));

        assert old == null : old;

        return map;
    }

    /**
     * @param req Request.
     * @param desc Cache descriptor.
     */
    void addCacheToStart(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
        assert req.start() : req;

        cachesToStart = add(cachesToStart, req, desc);
    }

    /**
     * @param req Request.
     * @param desc Cache descriptor.
     */
    public void addCacheToStop(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
        assert req.stop() : req;

        cachesToStop = add(cachesToStop, req, desc);
    }

    /**
     * @param req Request.
     * @param desc Cache descriptor.
     */
    void addCacheToResetLostPartitions(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
        assert req.resetLostPartitions() : req;

        cachesToResetLostParts = add(cachesToResetLostParts, req, desc);
    }

    /**
     * @param grpDesc Group descriptor.
     */
    void addCacheGroupToStart(CacheGroupDescriptor grpDesc) {
        assert grpDesc != null;

        if (cacheGrpsToStart == null)
            cacheGrpsToStart = new ArrayList<>();

        cacheGrpsToStart.add(new CacheGroupActionData(grpDesc));
    }

    /**
     * @return Cache groups to start.
     */
    public List<CacheGroupActionData> cacheGroupsToStart() {
        return cacheGrpsToStart != null ? cacheGrpsToStart : Collections.<CacheGroupActionData>emptyList();
    }

    /**
     * @param grpId Group ID.
     * @return {@code True} if given cache group starting.
     */
    public boolean cacheGroupStarting(int grpId) {
        if (cacheGrpsToStart != null) {
            for (CacheGroupActionData grp : cacheGrpsToStart) {
                if (grp.desc.groupId() == grpId)
                    return true;
            }
        }

        return false;
    }

    /**
     * @param grpDesc Group descriptor.
     * @param destroy Destroy flag.
     */
    public void addCacheGroupToStop(CacheGroupDescriptor grpDesc, boolean destroy) {
        assert grpDesc != null;

        if (cacheGrpsToStop == null)
            cacheGrpsToStop = new ArrayList<>();

        cacheGrpsToStop.add(new CacheGroupActionData(grpDesc, destroy));
    }

    /**
     * @return Cache groups to start.
     */
    public List<CacheGroupActionData> cacheGroupsToStop() {
        return cacheGrpsToStop != null ? cacheGrpsToStop : Collections.<CacheGroupActionData>emptyList();
    }

    /**
     * @param grpId Group ID.
     * @return {@code True} if given cache group stopping.
     */
    public boolean cacheGroupStopping(int grpId) {
        if (cacheGrpsToStop != null) {
            for (CacheGroupActionData grp : cacheGrpsToStop) {
                if (grp.desc.groupId() == grpId)
                    return true;
            }
        }

        return false;
    }

    /**
     * @return {@code True} if there are no cache change actions.
     */
    public boolean empty() {
        return F.isEmpty(cachesToStart) &&
            F.isEmpty(cachesToStop) &&
            F.isEmpty(cacheGrpsToStart) &&
            F.isEmpty(cacheGrpsToStop) &&
            F.isEmpty(cachesToResetLostParts);
    }

    /**
     *
     */
    static class ActionData {
        /** */
        private final DynamicCacheChangeRequest req;

        /** */
        private final DynamicCacheDescriptor desc;

        /**
         * @param req Request.
         * @param desc Cache descriptor.
         */
        ActionData(DynamicCacheChangeRequest req, DynamicCacheDescriptor desc) {
            assert req != null;
            assert desc != null;

            this.req = req;
            this.desc = desc;
        }

        /**
         * @return Request.
         */
        public DynamicCacheChangeRequest request() {
            return req;
        }

        /**
         * @return Cache descriptor.
         */
        public DynamicCacheDescriptor descriptor() {
            return desc;
        }
    }

    /**
     *
     */
    static class CacheGroupActionData {
        /** */
        private final CacheGroupDescriptor desc;

        /** */
        private final boolean destroy;

        /**
         * @param desc Group descriptor
         * @param destroy Destroy flag
         */
        CacheGroupActionData(CacheGroupDescriptor desc, boolean destroy) {
            assert desc != null;

            this.desc = desc;
            this.destroy = destroy;
        }

        /**
         * @param desc Group descriptor
         */
        CacheGroupActionData(CacheGroupDescriptor desc) {
            this(desc, false);
        }

        /**
         * @return Group descriptor
         */
        public CacheGroupDescriptor descriptor() {
            return desc;
        }

        /**
         * @return Destroy flag
         */
        public boolean destroy() {
            return destroy;
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        Object startGrps = F.viewReadOnly(cacheGrpsToStart, new C1<CacheGroupActionData, String>() {
            @Override public String apply(CacheGroupActionData data) {
                return data.desc.cacheOrGroupName();
            }
        });
        Object stopGrps = F.viewReadOnly(cacheGrpsToStop, new C1<CacheGroupActionData, String>() {
            @Override public String apply(CacheGroupActionData data) {
                return data.desc.cacheOrGroupName() + ", destroy=" + data.destroy;
            }
        });

        return "ExchangeActions [startCaches=" + (cachesToStart != null ? cachesToStart.keySet() : null) +
            ", stopCaches=" + (cachesToStop != null ? cachesToStop.keySet() : null) +
            ", startGrps=" + startGrps +
            ", stopGrps=" + stopGrps +
            ", resetParts=" + (cachesToResetLostParts != null ? cachesToResetLostParts.keySet() : null) +
            ", newState=" + newState + ']';
    }
}
