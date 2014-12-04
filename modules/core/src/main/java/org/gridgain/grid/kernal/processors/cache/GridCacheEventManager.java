/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Cache event manager.
 */
public class GridCacheEventManager<K, V> extends GridCacheManagerAdapter<K, V> {
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
    public void addEvent(int part, K key, GridCacheTx tx, @Nullable GridCacheMvccCandidate<K> owner,
        int type, @Nullable V newVal, boolean hasNewVal, @Nullable V oldVal, boolean hasOldVal, UUID subjId,
        String cloClsName, String taskName) {
        addEvent(part, key, locNodeId, tx, owner, type, newVal, hasNewVal, oldVal, hasOldVal, subjId, cloClsName,
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
    public void addEvent(int part, K key, UUID nodeId, GridCacheTx tx, GridCacheMvccCandidate<K> owner,
        int type, V newVal, boolean hasNewVal, V oldVal, boolean hasOldVal, UUID subjId, String cloClsName,
        String taskName) {
        addEvent(part, key, nodeId, tx == null ? null : tx.xid(), owner == null ? null : owner.version(), type,
            newVal, hasNewVal, oldVal, hasOldVal, subjId, cloClsName, taskName);
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
    public void addEvent(int part, K key, UUID evtNodeId, @Nullable GridCacheMvccCandidate<K> owner,
        int type, @Nullable V newVal, boolean hasNewVal, V oldVal, boolean hasOldVal, UUID subjId, String cloClsName,
        String taskName) {
        GridCacheTx tx = owner == null ? null : cctx.tm().tx(owner.version());

        addEvent(part, key, evtNodeId, tx == null ? null : tx.xid(), owner == null ? null : owner.version(), type,
            newVal, hasNewVal, oldVal, hasOldVal, subjId, cloClsName, taskName);
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
        K key,
        UUID evtNodeId,
        @Nullable GridUuid xid,
        @Nullable Object lockId,
        int type,
        @Nullable V newVal,
        boolean hasNewVal,
        @Nullable V oldVal,
        boolean hasOldVal,
        UUID subjId,
        @Nullable String cloClsName,
        @Nullable String taskName
    ) {
        assert key != null;

        if (!cctx.events().isRecordable(type))
            LT.warn(log, null, "Added event without checking if event is recordable: " + U.gridEventName(type));

        // Events are not fired for internal entry.
        if (!(key instanceof GridCacheInternal)) {
            GridNode evtNode = cctx.discovery().node(evtNodeId);

            if (evtNode == null)
                evtNode = findNodeInHistory(evtNodeId);

            if (evtNode == null)
                LT.warn(log, null, "Failed to find event node in grid topology history " +
                    "(try to increase topology history size configuration property of configured " +
                    "discovery SPI): " + evtNodeId);

            cctx.gridEvents().record(new GridCacheEvent(cctx.name(), cctx.localNode(), evtNode,
                "Cache event.", type, part, cctx.isNear(), key, xid, lockId, newVal, hasNewVal, oldVal, hasOldVal,
                subjId, cloClsName, taskName));
        }
    }

    /**
     * Tries to find node in history by specified ID.
     *
     * @param nodeId Node ID.
     * @return Found node or {@code null} if history doesn't contain this node.
     */
    @Nullable private GridNode findNodeInHistory(UUID nodeId) {
        for (long topVer = cctx.discovery().topologyVersion() - 1; topVer > 0; topVer--) {
            Collection<GridNode> top = cctx.discovery().topology(topVer);

            if (top == null)
                break;

            for (GridNode node : top)
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
    public void addPreloadEvent(int part, int type, GridNode discoNode, int discoType, long discoTs) {
        assert discoNode != null;
        assert type > 0;
        assert discoType > 0;
        assert discoTs > 0;

        if (!cctx.events().isRecordable(type))
            LT.warn(log, null, "Added event without checking if event is recordable: " + U.gridEventName(type));

        cctx.gridEvents().record(new GridCachePreloadingEvent(cctx.name(), cctx.localNode(),
            "Cache preloading event.", type, part, discoNode, discoType, discoTs));
    }

    /**
     * Adds partition unload event.
     *
     * @param part Partition.
     */
    public void addUnloadEvent(int part) {
        if (!cctx.events().isRecordable(EVT_CACHE_PRELOAD_PART_UNLOADED))
            LT.warn(log, null, "Added event without checking if event is recordable: " +
                U.gridEventName(EVT_CACHE_PRELOAD_PART_UNLOADED));

        cctx.gridEvents().record(new GridCachePreloadingEvent(cctx.name(), cctx.localNode(),
            "Cache unloading event.", EVT_CACHE_PRELOAD_PART_UNLOADED, part, null, 0, 0));
    }

    /**
     * @param type Event type.
     * @return {@code True} if event is recordable.
     */
    public boolean isRecordable(int type) {
        return !CU.isUtilityCache(cctx.name()) && cctx.gridEvents().isRecordable(type);
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache event manager memory stats [grid=" + cctx.gridName() + ", cache=" + cctx.name() +
            ", stats=" + "N/A" + ']');
    }
}
