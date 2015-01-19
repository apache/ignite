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

package org.gridgain.grid.kernal.processors.cache.query.continuous;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.events.IgniteEventType.*;
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
    @Override protected void start0() throws IgniteCheckedException {
        // Append cache name to the topic.
        topicPrefix = "CONTINUOUS_QUERY" + (cctx.name() == null ? "" : "_" + cctx.name());
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        if (intLsnrCnt.get() > 0 || lsnrCnt.get() > 0) {
            Collection<ClusterNode> nodes = cctx.discovery().cacheNodes(cctx.name(), -1);

            for (ClusterNode n : nodes) {
                if (!n.version().greaterThanEqual(6, 2, 0))
                    throw new IgniteCheckedException("Rolling update is not supported for continuous queries " +
                        "for versions below 6.2.0");
            }
        }
    }

    /**
     * @param prjPred Projection predicate.
     * @return New continuous query.
     */
    public GridCacheContinuousQuery<K, V> createQuery(@Nullable IgnitePredicate<GridCacheEntry<K, V>> prjPred) {
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
     * @throws IgniteCheckedException In case of error.
     */
    public void onEntryUpdate(GridCacheEntryEx<K, V> e, K key, @Nullable V newVal,
        @Nullable GridCacheValueBytes newBytes, V oldVal, @Nullable GridCacheValueBytes oldBytes) throws IgniteCheckedException {
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
    @SuppressWarnings("UnusedParameters")
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
        ListenerInfo info;

        if (internal) {
            if ((info = intLsnrs.remove(id)) != null) {
                intLsnrCnt.decrementAndGet();

                info.lsnr.onUnregister();
            }
        }
        else {
            if ((info = lsnrs.remove(id)) != null) {
                lsnrCnt.decrementAndGet();

                info.lsnr.onUnregister();
            }
        }
    }

    /**
     * Iterates through existing data.
     *
     * @param internal Internal flag.
     * @param id Listener ID.
     * @param keepPortable Keep portable flag.
     */
    @SuppressWarnings("unchecked")
    void iterate(boolean internal, UUID id, boolean keepPortable) {
        ListenerInfo<K, V> info = internal ? intLsnrs.get(id) : lsnrs.get(id);

        assert info != null;

        GridCacheProjectionImpl<K, V> oldPrj = null;

        try {
            if (keepPortable) {
                oldPrj = cctx.projectionPerCall();

                cctx.projectionPerCall(cctx.cache().<K, V>keepPortable0());
            }

            Set<GridCacheEntry<K, V>> entries;

            if (cctx.isReplicated())
                entries = internal ? cctx.cache().entrySetx() :
                    cctx.cache().entrySet();
            else
                entries = internal ? cctx.cache().primaryEntrySetx() :
                    cctx.cache().primaryEntrySet();

            for (GridCacheEntry<K, V> e : entries) {
                info.onIterate(new GridCacheContinuousQueryEntry<>(cctx, e, e.getKey(), e.getValue(), null, null, null),
                    !internal && cctx.gridEvents().isRecordable(EVT_CACHE_QUERY_OBJECT_READ));
            }

            info.flushPending();
        }
        finally {
            if (keepPortable)
                cctx.projectionPerCall(oldPrj);
        }
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
