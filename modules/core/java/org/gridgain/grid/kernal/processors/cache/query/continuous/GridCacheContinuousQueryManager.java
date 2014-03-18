// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.kernal.GridTopic.*;

/**
 * Continuous queries manager.
 */
public class GridCacheContinuousQueryManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Ordered topic prefix. */
    private static final String TOPIC_PREFIX = "CONTINUOUS_QUERY";

    /** Listeners. */
    private final ConcurrentMap<UUID, ListenerInfo<K, V>> lsnrs = new ConcurrentHashMap8<>();

    /** Listeners count. */
    private final AtomicInteger lsnrCnt = new AtomicInteger();

    /** Query sequence number for message topic. */
    private final AtomicLong seq = new AtomicLong();

    /**
     * @param prjPred Projection predicate.
     * @return New continuous query.
     */
    public GridCacheContinuousQuery<K, V> createQuery(@Nullable GridPredicate<GridCacheEntry<K, V>> prjPred) {
        Object topic = TOPIC_CACHE.topic(TOPIC_PREFIX, cctx.localNodeId(), seq.getAndIncrement());

        return new GridCacheContinuousQueryAdapter<>(cctx, topic, prjPred);
    }

    /**
     * @param e Cache entry.
     * @param key Key.
     * @param val Value.
     * @param valBytes Value bytes.
     * @param unwind Whether to unwind after entry is added to buffer.
     * @throws GridException In case of error.
     */
    public void onEntryUpdate(GridCacheEntryEx<K, V> e, K key, @Nullable V val, @Nullable GridCacheValueBytes valBytes,
        boolean unwind) throws GridException {
        assert e != null;
        assert key != null;

        if (lsnrCnt.get() > 0) {
            if (e.isInternal())
                return;

            GridCacheContinuousQueryEntry<K, V> e0 = new GridCacheContinuousQueryEntry<>(
                cctx, e.wrap(false), key, val, valBytes);

            e0.initValue(cctx.marshaller(), cctx.deploy().globalLoader());

            for (ListenerInfo<K, V> lsnr : lsnrs.values())
                lsnr.onEntryUpdate(e0);
        }
    }

    /**
     * @param id Listener ID.
     * @param lsnr Listener.
     * @return Whether listener was actually registered.
     */
    boolean registerListener(UUID id, GridCacheContinuousQueryListener<K, V> lsnr) {
        ListenerInfo<K, V> info = new ListenerInfo<>(lsnr);

        boolean added = lsnrs.putIfAbsent(id, info) == null;

        if (added)
            lsnrCnt.incrementAndGet();

        return added;
    }

    /**
     * @param id Listener ID.
     */
    void unregisterListener(UUID id) {
        if (lsnrs.remove(id) != null)
            lsnrCnt.decrementAndGet();
    }

    /**
     * Iterates through existing data.
     *
     * @param id Listener ID.
     */
    void iterate(UUID id) {
        ListenerInfo<K, V> info = lsnrs.get(id);

        assert info != null;

        for (GridCacheEntry<K, V> e : cctx.cache().primaryEntrySet())
            info.onIterate(new GridCacheContinuousQueryEntry<>(cctx, e, e.getKey(), e.getValue(), null));

        info.flushPending();
    }

    /**
     * Listener info.
     */
    private static class ListenerInfo<K, V> {
        /** Listener. */
        private final GridCacheContinuousQueryListener<K, V> lsnr;

        /** Pending entries. */
        private Collection<GridCacheContinuousQueryEntry<K, V>> pending = new LinkedList<>();

        /**
         * @param lsnr Listener.
         */
        private ListenerInfo(GridCacheContinuousQueryListener<K, V> lsnr) {
            this.lsnr = lsnr;
        }

        /**
         * @param e Entry update callback.
         */
        void onEntryUpdate(GridCacheContinuousQueryEntry<K, V> e) {
            boolean notifyLsnr = true;

            synchronized (this) {
                if (pending != null) {
                    pending.add(e);

                    notifyLsnr = false;
                }
            }

            if (notifyLsnr)
                lsnr.onEntryUpdate(e);
        }

        /**
         * @param e Entry iteration callback.
         */
        @SuppressWarnings("TypeMayBeWeakened")
        void onIterate(GridCacheContinuousQueryEntry<K, V> e) {
            lsnr.onEntryUpdate(e);
        }

        /**
         * Flushes pending entries to listener.
         */
        void flushPending() {
            Collection<GridCacheContinuousQueryEntry<K, V>> pending0;

            synchronized (this) {
                pending0 = pending;

                pending = null;
            }

            for (GridCacheContinuousQueryEntry<K, V> e : pending0)
                lsnr.onEntryUpdate(e);
        }
    }
}
