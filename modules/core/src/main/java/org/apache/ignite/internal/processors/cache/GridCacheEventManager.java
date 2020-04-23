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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_CACHE_OBJECT_READ;
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
     * @param txLbl Possible lable of possible surrounding transaction.
     * @param val Read value.
     * @param subjId Subject ID.
     * @param taskName Task name.
     * @param keepBinary Keep binary flag.
     */
    public void readEvent(KeyCacheObject key,
        @Nullable IgniteInternalTx tx,
        @Nullable String txLbl,
        @Nullable CacheObject val,
        @Nullable UUID subjId,
        @Nullable String taskName,
        boolean keepBinary) {
        if (isRecordable(EVT_CACHE_OBJECT_READ)) {
            addEvent(cctx.affinity().partition(key),
                key,
                cctx.localNodeId(),
                tx,
                txLbl,
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
        @Nullable IgniteInternalTx tx,
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
            null,
            null,
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
        @Nullable IgniteInternalTx tx,
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
            nodeId,
            tx,
            null,
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
            tx,
            null,
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
     * @param tx Possible surrounding transaction.
     * @param txLbl Possible label of possible surrounding transaction.
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
        @Nullable IgniteInternalTx tx,
        @Nullable String txLbl,
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
                if (!cctx.cacheObjectContext().kernalContext().cacheObjects().isBinaryEnabled(cctx.config()))
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

            IgniteUuid xid = tx == null ? null : tx.xid();

            String finalTxLbl = (tx == null || tx.label() == null) ? txLbl : tx.label();

            cctx.gridEvents().record(new CacheEvent(cctx.name(),
                cctx.localNode(),
                evtNode,
                "Cache event.",
                type,
                part,
                cctx.isNear(),
                key0,
                xid,
                finalTxLbl,
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
     * @param type Event type.
     * @return {@code True} if event is recordable.
     */
    public boolean isRecordable(int type) {
        GridCacheContext cctx0 = cctx;

        // Event recording is impossible in recovery mode.
        if (cctx0 == null || cctx0.kernalContext().recoveryMode())
            return false;

        try {
            CacheConfiguration cfg = cctx0.config();

            return cctx0.userCache() && cctx0.gridEvents().isRecordable(type) && !cfg.isEventsDisabled();
        }
        catch (IllegalStateException e) {
            // Cache context was cleaned up.
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache event manager memory stats [igniteInstanceName=" + cctx.igniteInstanceName() +
            ", cache=" + cctx.name() + ", stats=" + "N/A" + ']');
    }
}
