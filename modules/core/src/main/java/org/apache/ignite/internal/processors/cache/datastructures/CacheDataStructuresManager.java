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

package org.apache.ignite.internal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.continuous.*;
import org.apache.ignite.internal.processors.datastructures.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class CacheDataStructuresManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Set keys used for set iteration. */
    private ConcurrentMap<IgniteUuid, GridConcurrentHashSet<GridCacheSetItemKey>> setDataMap =
        new ConcurrentHashMap8<>();

    /** Queue header view.  */
    private CacheProjection<GridCacheQueueHeaderKey, GridCacheQueueHeader> queueHdrView;

    /** Query notifying about queue update. */
    private GridCacheContinuousQueryAdapter queueQry;

    /** Queue query creation guard. */
    private final AtomicBoolean queueQryGuard = new AtomicBoolean();

    /** Busy lock. */
    private final GridSpinBusyLock busyLock = new GridSpinBusyLock();

    /** Init latch. */
    private final CountDownLatch initLatch = new CountDownLatch(1);

    /** Init flag. */
    private boolean initFlag;

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        try {
            queueHdrView = cctx.cache().projection(GridCacheQueueHeaderKey.class, GridCacheQueueHeader.class);

            initFlag = true;
        }
        finally {
            initLatch.countDown();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        busyLock.block();

        if (queueQry != null) {
            try {
                queueQry.close();
            }
            catch (IgniteCheckedException e) {
                U.warn(log, "Failed to cancel queue header query.", e);
            }
        }
    }

    /**
     * @throws IgniteCheckedException If thread is interrupted or manager
     *     was not successfully initialized.
     */
    private void waitInitialization() throws IgniteCheckedException {
        if (initLatch.getCount() > 0)
            U.await(initLatch);

        if (!initFlag)
            throw new IgniteCheckedException("DataStructures processor was not properly initialized.");
    }

    /**
     * @param name Queue name.
     * @param cap Capacity.
     * @param colloc Collocated flag.
     * @param create Create flag.
     * @return Queue header.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Nullable public GridCacheQueueHeader queue(final String name,
        final int cap,
        boolean colloc,
        final boolean create)
        throws IgniteCheckedException
    {
        waitInitialization();

        cctx.gate().enter();

        try {
            GridCacheQueueHeaderKey key = new GridCacheQueueHeaderKey(name);

            GridCacheQueueHeader hdr;

            if (create) {
                hdr = new GridCacheQueueHeader(IgniteUuid.randomUuid(), cap, colloc, 0, 0, null);

                GridCacheQueueHeader old = queueHdrView.putIfAbsent(key, hdr);

                if (old != null) {
                    if (old.capacity() != cap || old.collocated() != colloc)
                        throw new IgniteCheckedException("Failed to create queue, queue with the same name but different " +
                            "configuration already exists [name=" + name + ']');

                    hdr = old;
                }
            }
            else
                hdr = queueHdrView.get(key);

            if (hdr == null)
                return null;

            if (queueQryGuard.compareAndSet(false, true)) {
                queueQry = (GridCacheContinuousQueryAdapter)cctx.cache().queries().createContinuousQuery();

                queueQry.filter(new QueueHeaderPredicate());

                queueQry.localCallback(new IgniteBiPredicate<UUID, Collection<GridCacheContinuousQueryEntry>>() {
                    @Override public boolean apply(UUID id, Collection<GridCacheContinuousQueryEntry> entries) {
                        if (!busyLock.enterBusy())
                            return false;

                        try {
                            for (GridCacheContinuousQueryEntry e : entries) {
                                GridCacheQueueHeaderKey key = (GridCacheQueueHeaderKey)e.getKey();
                                GridCacheQueueHeader hdr = (GridCacheQueueHeader)e.getValue();
                                GridCacheQueueHeader oldHdr = (GridCacheQueueHeader)e.getOldValue();

                                cctx.kernalContext().dataStructures().onQueueUpdated(key, hdr, oldHdr);
                            }

                            return true;
                        }
                        finally {
                            busyLock.leaveBusy();
                        }
                    }
                });

                queueQry.execute(cctx.isLocal() || cctx.isReplicated() ? cctx.grid().forLocal() : null,
                    true,
                    false,
                    false,
                    true);
            }

            return hdr;
        }
        finally {
            cctx.gate().leave();
        }
    }

    /**
     * Entry update callback.
     *
     * @param key Key.
     * @param rmv {@code True} if entry was removed.
     */
    public void onEntryUpdated(K key, boolean rmv) {
        if (key instanceof GridCacheSetItemKey)
            onSetItemUpdated((GridCacheSetItemKey)key, rmv);
    }

    /**
     * Partition evicted callback.
     *
     * @param part Partition number.
     */
    public void onPartitionEvicted(int part) {
        GridCacheAffinityManager aff = cctx.affinity();

        for (GridConcurrentHashSet<GridCacheSetItemKey> set : setDataMap.values()) {
            Iterator<GridCacheSetItemKey> iter = set.iterator();

            while (iter.hasNext()) {
                GridCacheSetItemKey key = iter.next();

                if (aff.partition(key) == part)
                    iter.remove();
            }
        }
    }

    /**
     * @param id Set ID.
     * @return Data for given set.
     */
    @Nullable public GridConcurrentHashSet<GridCacheSetItemKey> setData(IgniteUuid id) {
        return setDataMap.get(id);
    }

    /**
     * @param setId Set ID.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public void removeSetData(IgniteUuid setId, long topVer) throws IgniteCheckedException {
        boolean loc = cctx.isLocal();

        GridCacheAffinityManager aff = cctx.affinity();

        if (!loc) {
            aff.affinityReadyFuture(topVer).get();

            cctx.preloader().syncFuture().get();
        }

        GridConcurrentHashSet<GridCacheSetItemKey> set = setDataMap.get(setId);

        if (set == null)
            return;

        GridCache cache = cctx.cache();

        final int BATCH_SIZE = 100;

        Collection<GridCacheSetItemKey> keys = new ArrayList<>(BATCH_SIZE);

        for (GridCacheSetItemKey key : set) {
            if (!loc && !aff.primary(cctx.localNode(), key, topVer))
                continue;

            keys.add(key);

            if (keys.size() == BATCH_SIZE) {
                retryRemoveAll(cache, keys);

                keys.clear();
            }
        }

        if (!keys.isEmpty())
            retryRemoveAll(cache, keys);

        setDataMap.remove(setId);
    }

    /**
     * @param key Set item key.
     * @param rmv {@code True} if item was removed.
     */
    private void onSetItemUpdated(GridCacheSetItemKey key, boolean rmv) {
        GridConcurrentHashSet<GridCacheSetItemKey> set = setDataMap.get(key.setId());

        if (set == null) {
            if (rmv)
                return;

            GridConcurrentHashSet<GridCacheSetItemKey> old = setDataMap.putIfAbsent(key.setId(),
                set = new GridConcurrentHashSet<>());

            if (old != null)
                set = old;
        }

        if (rmv)
            set.remove(key);
        else
            set.add(key);
    }

    /**
     * @param cache Cache.
     * @param keys Keys to remove.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void retryRemoveAll(final GridCache cache, final Collection<GridCacheSetItemKey> keys)
        throws IgniteCheckedException {
        CacheDataStructuresProcessor.retry(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.removeAll(keys);

                return null;
            }
        });
    }

    /**
     * Predicate for queue continuous query.
     */
    private static class QueueHeaderPredicate implements IgniteBiPredicate, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Required by {@link Externalizable}.
         */
        public QueueHeaderPredicate() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Object key, Object val) {
            return key instanceof GridCacheQueueHeaderKey;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) {
            // No-op.
        }
    }
}
