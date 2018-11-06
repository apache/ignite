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
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 * Discovery events processed in single exchange (contain multiple events if exchanges for multiple
 * discovery events are merged into single exchange).
 */
public class ExchangeDiscoveryEvents {
    /** Last event version. */
    private AffinityTopologyVersion topVer;

    /** Last server join/fail event version. */
    private AffinityTopologyVersion srvEvtTopVer;

    /** Discovery data cache for last event. */
    private DiscoCache discoCache;

    /** Last event. */
    private DiscoveryEvent lastEvt;

    /** Last server join/fail event. */
    private DiscoveryEvent lastSrvEvt;

    /** All events. */
    private List<DiscoveryEvent> evts = new ArrayList<>();

    /** Server join flag. */
    private boolean srvJoin;

    /** Sever left flag. */
    private boolean srvLeft;

    /**
     * @param fut Current exchange future.
     */
    ExchangeDiscoveryEvents(GridDhtPartitionsExchangeFuture fut) {
        addEvent(fut.initialVersion(), fut.firstEvent(), fut.firstEventCache());
    }

    /**
     * @param fut Current exchange future.
     */
    public void processEvents(GridDhtPartitionsExchangeFuture fut) {
        for (DiscoveryEvent evt : evts) {
            if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED)
                fut.sharedContext().mvcc().removeExplicitNodeLocks(evt.eventNode().id(), fut.initialVersion());
        }

        if (hasServerLeft())
            warnNoAffinityNodes(fut.sharedContext());
    }

    /**
     * @param nodeId Node ID.
     * @return {@code True} if has join event for give node.
     */
    public boolean nodeJoined(UUID nodeId) {
        for (int i = 0; i < evts.size(); i++) {
            DiscoveryEvent evt = evts.get(i);

            if (evt.type() == EVT_NODE_JOINED && nodeId.equals(evt.eventNode().id()))
                return true;
        }

        return false;
    }

    /**
     * @return Last server join/fail event version.
     */
    AffinityTopologyVersion lastServerEventVersion() {
        assert srvEvtTopVer != null : this;

        return srvEvtTopVer;
    }

    /**
     * @param topVer Event version.
     * @param evt Event.
     * @param cache Discovery data cache for given topology version.
     */
    void addEvent(AffinityTopologyVersion topVer, DiscoveryEvent evt, DiscoCache cache) {
        assert evts.isEmpty() || topVer.compareTo(this.topVer) > 0 : topVer;

        evts.add(evt);

        this.topVer = topVer;
        this.lastEvt = evt;
        this.discoCache = cache;

        ClusterNode node = evt.eventNode();

        if (!node.isClient()) {
            lastSrvEvt = evt;

            srvEvtTopVer = new AffinityTopologyVersion(evt.topologyVersion(), 0);

            if (evt.type()== EVT_NODE_JOINED)
                srvJoin = true;
            else if (evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED)
                srvLeft = !node.isClient();
        }
    }

    /**
     * @return All events.
     */
    public List<DiscoveryEvent> events() {
        return evts;
    }

    /**
     * @param evt Event.
     * @return {@code True} if given event is {@link EventType#EVT_NODE_FAILED} or {@link EventType#EVT_NODE_LEFT}.
     */
    public static boolean serverLeftEvent(DiscoveryEvent evt) {
        return  ((evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT) && !evt.eventNode().isClient());
    }

    /**
     * @param evt Event.
     * @return {@code True} if given event is {@link EventType#EVT_NODE_JOINED}.
     */
    public static boolean serverJoinEvent(DiscoveryEvent evt) {
        return  (evt.type() == EVT_NODE_JOINED && !evt.eventNode().isClient());
    }

    /**
     * @return Discovery data cache for last event.
     */
    public DiscoCache discoveryCache() {
        return discoCache;
    }

    /**
     * @return Last event.
     */
    public DiscoveryEvent lastEvent() {
        return lastSrvEvt != null ? lastSrvEvt : lastEvt;
    }

    /**
     * @return Last event version.
     */
    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @return {@code True} if has event for server join.
     */
    public boolean hasServerJoin() {
        return srvJoin;
    }

    /**
     * @return {@code True} if has event for server leave.
     */
    public boolean hasServerLeft() {
        return srvLeft;
    }

    /**
     * @param cctx Context.
     */
    public void warnNoAffinityNodes(GridCacheSharedContext<?, ?> cctx) {
        List<String> cachesWithoutNodes = null;

        for (DynamicCacheDescriptor cacheDesc : cctx.cache().cacheDescriptors().values()) {
            if (discoCache.cacheGroupAffinityNodes(cacheDesc.groupId()).isEmpty()) {
                if (cachesWithoutNodes == null)
                    cachesWithoutNodes = new ArrayList<>();

                cachesWithoutNodes.add(cacheDesc.cacheName());

                // Fire event even if there is no client cache started.
                if (cctx.gridEvents().isRecordable(EventType.EVT_CACHE_NODES_LEFT)) {
                    Event evt = new CacheEvent(
                        cacheDesc.cacheName(),
                        cctx.localNode(),
                        cctx.localNode(),
                        "All server nodes have left the cluster.",
                        EventType.EVT_CACHE_NODES_LEFT,
                        0,
                        false,
                        null,
                        null,
                        null,
                        null,
                        false,
                        null,
                        false,
                        null,
                        null,
                        null
                    );

                    cctx.gridEvents().record(evt);
                }
            }
        }

        if (cachesWithoutNodes != null) {
            StringBuilder sb =
                new StringBuilder("All server nodes for the following caches have left the cluster: ");

            for (int i = 0; i < cachesWithoutNodes.size(); i++) {
                String cache = cachesWithoutNodes.get(i);

                sb.append('\'').append(cache).append('\'');

                if (i != cachesWithoutNodes.size() - 1)
                    sb.append(", ");
            }

            IgniteLogger log = cctx.logger(getClass());

            U.quietAndWarn(log, sb.toString());

            U.quietAndWarn(log, "Must have server nodes for caches to operate.");
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExchangeDiscoveryEvents.class, this);
    }
}
