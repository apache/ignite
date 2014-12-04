/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;

/**
 * Continuous queries manager.
 */
public class GridCacheContinuousQueryManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Ordered topic prefix. */
    private String topicPrefix;

    /** Listeners. */
    private final ConcurrentMap<UUID, ListenerInfo<K, V>> lsnrs = new ConcurrentHashMap8<>();

    /** Listeners count. */
    private final AtomicInteger lsnrCnt = new AtomicInteger();

    /** Internal entries listeners. */
    private final ConcurrentMap<UUID, ListenerInfo<K, V>> intLsnrs = new ConcurrentHashMap8<>();

    /** Internal listeners count. */
    private final AtomicInteger intLsnrCnt = new AtomicInteger();

    /** Query sequence number for message topic. */
    private final AtomicLong seq = new AtomicLong();

    /** {@inheritDoc} */
    @Override protected void start0() throws GridException {
        // Append cache name to the topic.
        topicPrefix = "CONTINUOUS_QUERY" + (cctx.name() == null ? "" : "_" + cctx.name());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws GridException {
        if (intLsnrCnt.get() > 0 || lsnrCnt.get() > 0) {
            Collection<ClusterNode> nodes = cctx.discovery().cacheNodes(cctx.name(), -1);

            for (ClusterNode n : nodes) {
                if (!n.version().greaterThanEqual(6, 2, 0))
                    throw new GridException("Rolling update is not supported for continuous queries " +
                        "for versions below 6.2.0");
            }
        }
    }

    /**
     * @param prjPred Projection predicate.
     * @return New continuous query.
     */
    public GridCacheContinuousQuery<K, V> createQuery(@Nullable GridPredicate<GridCacheEntry<K, V>> prjPred) {
        Object topic = TOPIC_CACHE.topic(topicPrefix, cctx.localNodeId(), seq.getAndIncrement());

        return new GridCacheContinuousQueryAdapter<>(cctx, topic, prjPred);
    }

    /**
     * @param e Cache entry.
     * @param key Key.
     * @param newVal New value.
     * @param newBytes New value bytes.
     * @param oldVal Old value.
     * @param oldBytes Old value bytes.
     * @throws GridException In case of error.
     */
    public void onEntryUpdate(GridCacheEntryEx<K, V> e, K key, @Nullable V newVal,
        @Nullable GridCacheValueBytes newBytes, V oldVal, @Nullable GridCacheValueBytes oldBytes) throws GridException {
        assert e != null;
        assert key != null;

        ConcurrentMap<UUID, ListenerInfo<K, V>> lsnrCol;

        if (e.isInternal())
            lsnrCol = intLsnrCnt.get() > 0 ? intLsnrs : null;
        else
            lsnrCol = lsnrCnt.get() > 0 ? lsnrs : null;

        if (F.isEmpty(lsnrCol))
            return;

        oldVal = cctx.unwrapTemporary(oldVal);

        GridCacheContinuousQueryEntry<K, V> e0 = new GridCacheContinuousQueryEntry<>(
            cctx, e.wrap(false), key, newVal, newBytes, oldVal, oldBytes);

        e0.initValue(cctx.marshaller(), cctx.deploy().globalLoader());

        boolean recordEvt = !e.isInternal() && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ);

        for (ListenerInfo<K, V> lsnr : lsnrCol.values())
            lsnr.onEntryUpdate(e0, recordEvt);
    }

    /**
     * @param nodeId Node ID.
     * @param lsnrId Listener ID.
     * @param lsnr Listener.
     * @param internal Internal flag.
     * @return Whether listener was actually registered.
     */
    boolean registerListener(UUID nodeId, UUID lsnrId, GridCacheContinuousQueryListener<K, V> lsnr, boolean internal) {
        ListenerInfo<K, V> info = new ListenerInfo<>(lsnr);

        boolean added;

        if (internal) {
            added = intLsnrs.putIfAbsent(lsnrId, info) == null;

            if (added)
                intLsnrCnt.incrementAndGet();
        }
        else {
            added = lsnrs.putIfAbsent(lsnrId, info) == null;

            if (added) {
                lsnrCnt.incrementAndGet();

                lsnr.onExecution();
            }
        }

        return added;
    }

    /**
     * @param internal Internal flag.
     * @param id Listener ID.
     */
    void unregisterListener(boolean internal, UUID id) {
        if (internal) {
            if (intLsnrs.remove(id) != null)
                intLsnrCnt.decrementAndGet();
        }
        else {
            if (lsnrs.remove(id) != null)
                lsnrCnt.decrementAndGet();
        }
    }

    /**
     * Iterates through existing data.
     *
     * @param internal Internal flag.
     * @param id Listener ID.
     */
    void iterate(boolean internal, UUID id) {
        ListenerInfo<K, V> info = internal ? intLsnrs.get(id) : lsnrs.get(id);

        assert info != null;

        for (GridCacheEntry<K, V> e : internal ? cctx.cache().primaryEntrySetx() : cctx.cache().primaryEntrySet()) {
            info.onIterate(new GridCacheContinuousQueryEntry<>(cctx, e, e.getKey(), e.getValue(), null, null, null),
                !internal && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ));
        }

        info.flushPending();
    }

    /**
     * Listener info.
     */
    private static class ListenerInfo<K, V> {
        /** Listener. */
        private final GridCacheContinuousQueryListener<K, V> lsnr;

        /** Pending entries. */
        private Collection<PendingEntry<K, V>> pending = new LinkedList<>();

        /**
         * @param lsnr Listener.
         */
        private ListenerInfo(GridCacheContinuousQueryListener<K, V> lsnr) {
            this.lsnr = lsnr;
        }

        /**
         * @param e Entry update callback.
         * @param recordEvt Whether to record event.
         */
        void onEntryUpdate(GridCacheContinuousQueryEntry<K, V> e, boolean recordEvt) {
            boolean notifyLsnr = true;

            synchronized (this) {
                if (pending != null) {
                    pending.add(new PendingEntry<>(e, recordEvt));

                    notifyLsnr = false;
                }
            }

            if (notifyLsnr)
                lsnr.onEntryUpdate(e, recordEvt);
        }

        /**
         * @param e Entry iteration callback.
         * @param recordEvt Whether to record event.
         */
        void onIterate(GridCacheContinuousQueryEntry<K, V> e, boolean recordEvt) {
            lsnr.onEntryUpdate(e, recordEvt);
        }

        /**
         * Flushes pending entries to listener.
         */
        void flushPending() {
            Collection<PendingEntry<K, V>> pending0;

            synchronized (this) {
                pending0 = pending;

                pending = null;
            }

            for (PendingEntry<K, V> e : pending0)
                lsnr.onEntryUpdate(e.entry, e.recordEvt);
        }
    }

    /**
     * Pending entry.
     */
    private static class PendingEntry<K, V> {
        /** Entry. */
        private final GridCacheContinuousQueryEntry<K, V> entry;

        /** Whether to record event. */
        private final boolean recordEvt;

        /**
         * @param entry Entry.
         * @param recordEvt Whether to record event.
         */
        private PendingEntry(GridCacheContinuousQueryEntry<K, V> entry, boolean recordEvt) {
            this.entry = entry;
            this.recordEvt = recordEvt;
        }
    }
}
