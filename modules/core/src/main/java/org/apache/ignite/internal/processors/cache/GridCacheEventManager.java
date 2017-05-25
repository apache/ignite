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

import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
import static org.apache.ignite.events.EventType.EVT_CACHE_REBALANCE_PART_UNLOADED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STARTED;
import static org.apache.ignite.events.EventType.EVT_CACHE_STOPPED;

/**
 * Cache event manager.
 */
public class GridCacheEventManager extends GridCacheManagerAdapter {
    /** Force keep binary flag. Will be set if event notification encountered exception during unmarshalling. */
    private boolean forceKeepBinary;

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
     * @param key Key for event.
     * @param tx Possible surrounding transaction.
     * @param val Read value.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param keepBinary Keep binary flag.
     */
    public void readEvent(KeyCacheObject key,
        @Nullable IgniteInternalTx tx,
        @Nullable CacheObject val,
        @Nullable UUID subjId,
        @Nullable String taskName,
        boolean keepBinary) {
        if (isRecordable(EVT_CACHE_OBJECT_READ)) {
            addEvent(cctx.affinity().partition(key),
                key,
                tx,
                null,
                EVT_CACHE_OBJECT_READ,
                val,
                val != null,
                val,
                val != null,
                subjId,
                null,
                taskName,
                keepBinary);
        }
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
        String taskName,
        boolean keepBinary)
    {
        addEvent(part,
            key,
            cctx.localNodeId(),
            tx,
            owner,
            type,
            newVal,
            hasNewVal,
            oldVal,
            hasOldVal,
            subjId,
            cloClsName,
            taskName,
            keepBinary);
    }

    /**
     * @param type Event type (start or stop).
     */
    public void addEvent(int type) {
        addEvent(
            0,
            null,
            cctx.localNodeId(),
            (IgniteUuid)null,
            null,
            type,
            null,
            false,
            null,
            false,
            null,
            null,
            null,
            false);
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
        String taskName,
        boolean keepBinary)
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
            taskName,
            keepBinary);
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
        String taskName,
        boolean keepBinary)
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
            taskName,
            keepBinary);
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
        @Nullable String taskName,
        boolean keepBinary
    ) {
        assert key != null || type == EVT_CACHE_STARTED || type == EVT_CACHE_STOPPED;

        if (!cctx.events().isRecordable(type))
            LT.warn(log, "Added event without checking if event is recordable: " + U.gridEventName(type));

        // Events are not fired for internal entry.
        if (key == null || !key.internal()) {
            ClusterNode evtNode = cctx.discovery().node(evtNodeId);

            if (evtNode == null)
                evtNode = findNodeInHistory(evtNodeId);

            if (evtNode == null)
                LT.warn(log, "Failed to find event node in grid topology history " +
                    "(try to increase topology history size configuration property of configured " +
                    "discovery SPI): " + evtNodeId);

            keepBinary = keepBinary || forceKeepBinary;

            Object key0;
            Object val0;
            Object oldVal0;

            try {
                key0 = cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false);
                val0 = cctx.cacheObjectContext().unwrapBinaryIfNeeded(newVal, keepBinary, false);
                oldVal0 = cctx.cacheObjectContext().unwrapBinaryIfNeeded(oldVal, keepBinary, false);
            }
            catch (Exception e) {
                if (!cctx.cacheObjectContext().processor().isBinaryEnabled(cctx.config()))
                    throw e;

                if (log.isDebugEnabled())
                    log.debug("Failed to unmarshall cache object value for the event notification: " + e);

                if (!forceKeepBinary)
                    LT.warn(log, "Failed to unmarshall cache object value for the event notification " +
                        "(all further notifications will keep binary object format).");

                forceKeepBinary = true;

                key0 = cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, true, false);
                val0 = cctx.cacheObjectContext().unwrapBinaryIfNeeded(newVal, true, false);
                oldVal0 = cctx.cacheObjectContext().unwrapBinaryIfNeeded(oldVal, true, false);
            }

            cctx.gridEvents().record(new CacheEvent(cctx.name(),
                cctx.localNode(),
                evtNode,
                "Cache event.",
                type,
                part,
                cctx.isNear(),
                key0,
                xid,
                lockId,
                val0,
                hasNewVal,
                oldVal0,
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
            LT.warn(log, "Added event without checking if event is recordable: " + U.gridEventName(type));

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
            LT.warn(log, "Added event without checking if event is recordable: " +
                U.gridEventName(EVT_CACHE_REBALANCE_PART_UNLOADED));

        cctx.gridEvents().record(new CacheRebalancingEvent(cctx.name(), cctx.localNode(),
            "Cache unloading event.", EVT_CACHE_REBALANCE_PART_UNLOADED, part, null, 0, 0));
    }

    /**
     * @param type Event type.
     * @return {@code True} if event is recordable.
     */
    public boolean isRecordable(int type) {
        GridCacheContext cctx0 = cctx;

        return cctx0 != null && cctx0.userCache() && cctx0.gridEvents().isRecordable(type);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache event manager memory stats [igniteInstanceName=" + cctx.igniteInstanceName() +
            ", cache=" + cctx.name() + ", stats=" + "N/A" + ']');
    }
}
