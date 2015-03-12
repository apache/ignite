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

import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.events.EventType.*;

/**
 * Cache event manager.
 */
public class GridCacheEventManager extends GridCacheManagerAdapter {
    /** Local node ID. */
    private UUID locNodeId;

    /** {@inheritDoc} */
    @Override public void start0() {
        locNodeId = cctx.localNodeId();
    }

    /**
     * Adds local event listener.
     *
     * @param lsnr Listener.
     * @param evts Types of events.
     */
    public void addListener(GridLocalEventListener lsnr, int... evts) {
        cctx.gridEvents().addLocalEventListener(lsnr, evts);
    }

    /**
     * Removes local event listener.
     *
     * @param lsnr Local event listener.
     */
    public void removeListener(GridLocalEventListener lsnr) {
        cctx.gridEvents().removeLocalEventListener(lsnr);
    }

    /**
     * @param part Partition.
     * @param key Key for the event.
     * @param tx Possible surrounding transaction.
     * @param owner Possible surrounding lock.
     * @param type Event type.
     * @param newVal New value.
     * @param hasNewVal Whether new value is present or not.
     * @param oldVal Old value.
     * @param hasOldVal Whether old value is present or not.
     * @param subjId Subject ID.
     * @param cloClsName Closure class name.
     * @param taskName Task name.
     */
    public void addEvent(int part,
        KeyCacheObject key,
        IgniteInternalTx tx,
        @Nullable GridCacheMvccCandidate owner,
        int type,
        @Nullable CacheObject newVal,
        boolean hasNewVal,
        @Nullable CacheObject oldVal,
        boolean hasOldVal,
        UUID subjId,
        String cloClsName,
        String taskName)
    {
        addEvent(part,
            key,
            locNodeId,
            tx,
            owner,
            type,
            newVal,
            hasNewVal,
            oldVal,
            hasOldVal,
            subjId,
            cloClsName,
            taskName);
    }

    /**
     * @param part Partition.
     * @param key Key for the event.
     * @param nodeId Node ID.
     * @param tx Possible surrounding transaction.
     * @param owner Possible surrounding lock.
     * @param type Event type.
     * @param newVal New value.
     * @param hasNewVal Whether new value is present or not.
     * @param oldVal Old value.
     * @param hasOldVal Whether old value is present or not.
     * @param subjId Subject ID.
     * @param cloClsName Closure class name.
     * @param taskName Task name.
     */
    public void addEvent(int part,
        KeyCacheObject key,
        UUID nodeId,
        IgniteInternalTx tx,
        GridCacheMvccCandidate owner,
        int type,
        CacheObject newVal,
        boolean hasNewVal,
        CacheObject oldVal,
        boolean hasOldVal,
        UUID subjId,
        String cloClsName,
        String taskName)
    {
        addEvent(part,
            key,
            nodeId, tx == null ? null : tx.xid(),
            owner == null ? null : owner.version(),
            type,
            newVal,
            hasNewVal,
            oldVal,
            hasOldVal,
            subjId,
            cloClsName,
            taskName);
    }

    /**
     * @param part Partition.
     * @param key Key for the event.
     * @param evtNodeId Node ID.
     * @param owner Possible surrounding lock.
     * @param type Event type.
     * @param newVal New value.
     * @param hasNewVal Whether new value is present or not.
     * @param oldVal Old value.
     * @param hasOldVal Whether old value is present or not.
     * @param subjId Subject ID.
     * @param cloClsName Closure class name.
     * @param taskName Task name.
     */
    public void addEvent(int part,
        KeyCacheObject key,
        UUID evtNodeId,
        @Nullable GridCacheMvccCandidate owner,
        int type,
        @Nullable CacheObject newVal,
        boolean hasNewVal,
        CacheObject oldVal,
        boolean hasOldVal,
        UUID subjId,
        String cloClsName,
        String taskName)
    {
        IgniteInternalTx tx = owner == null ? null : cctx.tm().tx(owner.version());

        addEvent(part,
            key,
            evtNodeId,
            tx == null ? null : tx.xid(),
            owner == null ? null : owner.version(),
            type,
            newVal,
            hasNewVal,
            oldVal,
            hasOldVal,
            subjId,
            cloClsName,
            taskName);
    }

    /**
     * @param part Partition.
     * @param key Key for the event.
     * @param evtNodeId Event node ID.
     * @param xid Transaction ID.
     * @param lockId Lock ID.
     * @param type Event type.
     * @param newVal New value.
     * @param hasNewVal Whether new value is present or not.
     * @param oldVal Old value.
     * @param hasOldVal Whether old value is present or not.
     * @param subjId Subject ID.
     * @param cloClsName Closure class name.
     * @param taskName Task class name.
     */
    public void addEvent(
        int part,
        KeyCacheObject key,
        UUID evtNodeId,
        @Nullable IgniteUuid xid,
        @Nullable Object lockId,
        int type,
        @Nullable CacheObject newVal,
        boolean hasNewVal,
        @Nullable CacheObject oldVal,
        boolean hasOldVal,
        UUID subjId,
        @Nullable String cloClsName,
        @Nullable String taskName
    ) {
        assert key != null;

        if (!cctx.events().isRecordable(type))
            LT.warn(log, null, "Added event without checking if event is recordable: " + U.gridEventName(type));

        // Events are not fired for internal entry.
        if (!key.internal()) {
            ClusterNode evtNode = cctx.discovery().node(evtNodeId);

            if (evtNode == null)
                evtNode = findNodeInHistory(evtNodeId);

            if (evtNode == null)
                LT.warn(log, null, "Failed to find event node in grid topology history " +
                    "(try to increase topology history size configuration property of configured " +
                    "discovery SPI): " + evtNodeId);

            cctx.gridEvents().record(new CacheEvent(cctx.name(),
                cctx.localNode(),
                evtNode,
                "Cache event.",
                type,
                part,
                cctx.isNear(),
                key.value(cctx.cacheObjectContext(), false),
                xid,
                lockId,
                CU.value(newVal, cctx, false),
                hasNewVal,
                CU.value(oldVal, cctx, false),
                hasOldVal,
                subjId,
                cloClsName,
                taskName));
        }
    }

    /**
     * Tries to find node in history by specified ID.
     *
     * @param nodeId Node ID.
     * @return Found node or {@code null} if history doesn't contain this node.
     */
    @Nullable private ClusterNode findNodeInHistory(UUID nodeId) {
        for (long topVer = cctx.discovery().topologyVersion() - 1; topVer > 0; topVer--) {
            Collection<ClusterNode> top = cctx.discovery().topology(topVer);

            if (top == null)
                break;

            for (ClusterNode node : top)
                if (F.eq(node.id(), nodeId))
                    return node;
        }

        return null;
    }

    /**
     * Adds preloading event.
     *
     * @param part Partition.
     * @param type Event type.
     * @param discoNode Discovery node.
     * @param discoType Discovery event type.
     * @param discoTs Discovery event timestamp.
     */
    public void addPreloadEvent(int part, int type, ClusterNode discoNode, int discoType, long discoTs) {
        assert discoNode != null;
        assert type > 0;
        assert discoType > 0;
        assert discoTs > 0;

        if (!cctx.events().isRecordable(type))
            LT.warn(log, null, "Added event without checking if event is recordable: " + U.gridEventName(type));

        cctx.gridEvents().record(new CacheRebalancingEvent(cctx.name(), cctx.localNode(),
            "Cache rebalancing event.", type, part, discoNode, discoType, discoTs));
    }

    /**
     * Adds partition unload event.
     *
     * @param part Partition.
     */
    public void addUnloadEvent(int part) {
        if (!cctx.events().isRecordable(EVT_CACHE_REBALANCE_PART_UNLOADED))
            LT.warn(log, null, "Added event without checking if event is recordable: " +
                U.gridEventName(EVT_CACHE_REBALANCE_PART_UNLOADED));

        cctx.gridEvents().record(new CacheRebalancingEvent(cctx.name(), cctx.localNode(),
            "Cache unloading event.", EVT_CACHE_REBALANCE_PART_UNLOADED, part, null, 0, 0));
    }

    /**
     * @param type Event type.
     * @return {@code True} if event is recordable.
     */
    public boolean isRecordable(int type) {
        return !cctx.system() &&
            !CU.isAtomicsCache(cctx.name()) &&
            cctx.gridEvents().isRecordable(type);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache event manager memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() +
            ", stats=" + "N/A" + ']');
    }
}
