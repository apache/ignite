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
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.query.continuous.*;
import org.apache.ignite.internal.processors.datastructures.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.internal.GridClosureCallMode.*;

/**
 *
 */
public class CacheDataStructuresManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Sets map. */
    private final ConcurrentMap<IgniteUuid, GridCacheSetProxy> setsMap;

    /** Set keys used for set iteration. */
    private ConcurrentMap<IgniteUuid, GridConcurrentHashSet<GridCacheSetItemKey>> setDataMap =
        new ConcurrentHashMap8<>();

    /** Queues map. */
    private final ConcurrentMap<IgniteUuid, GridCacheQueueProxy> queuesMap;

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

    /**
     *
     */
    public CacheDataStructuresManager() {
        queuesMap = new ConcurrentHashMap8<>(10);

        setsMap = new ConcurrentHashMap8<>(10);
    }

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

        for (GridCacheQueueProxy q : queuesMap.values())
            q.delegate().onKernalStop();
    }

    /**
     * @throws IgniteCheckedException If thread is interrupted or manager
     *     was not successfully initialized.
     */
    private void waitInitialization() throws IgniteCheckedException {
        if (initLatch.getCount() > 0)
            U.await(initLatch);

        if (!initFlag)
            throw new IgniteCheckedException("DataStructures manager was not properly initialized.");
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
    @Nullable public <T> GridCacheQueueProxy<T> queue(final String name,
        final int cap,
        boolean colloc,
        final boolean create)
        throws IgniteCheckedException
    {
        waitInitialization();

        // Non collocated mode enabled only for PARTITIONED cache.
        final boolean colloc0 = create && (cctx.cache().configuration().getCacheMode() != PARTITIONED || colloc);

        return queue0(name, cap, colloc0, create);
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
    @Nullable public <T> GridCacheQueueProxy<T> queue0(final String name,
        final int cap,
        boolean colloc,
        final boolean create)
        throws IgniteCheckedException
    {
        cctx.gate().enter();

        try {
            GridCacheQueueHeaderKey key = new GridCacheQueueHeaderKey(name);

            GridCacheQueueHeader hdr;

            if (create) {
                hdr = new GridCacheQueueHeader(IgniteUuid.randomUuid(), cap, colloc, 0, 0, null);

                GridCacheQueueHeader old = queueHdrView.putIfAbsent(key, hdr);

                if (old != null) {
                    if (old.capacity() != cap || old.collocated() != colloc)
                        throw new IgniteCheckedException("Failed to create queue, queue with the same name but " +
                            "different configuration already exists [name=" + name + ']');

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

                                for (final GridCacheQueueProxy queue : queuesMap.values()) {
                                    if (queue.name().equals(key.queueName())) {
                                        if (hdr == null) {
                                            GridCacheQueueHeader oldHdr = (GridCacheQueueHeader)e.getOldValue();

                                            assert oldHdr != null;

                                            if (oldHdr.id().equals(queue.delegate().id())) {
                                                queue.delegate().onRemoved(false);

                                                queuesMap.remove(queue.delegate().id());
                                            }
                                        }
                                        else
                                            queue.delegate().onHeaderChanged(hdr);
                                    }
                                }
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

            GridCacheQueueProxy queue = queuesMap.get(hdr.id());

            if (queue == null) {
                queue = new GridCacheQueueProxy(cctx, cctx.atomic() ? new GridAtomicCacheQueueImpl<>(name, hdr, cctx) :
                    new GridTransactionalCacheQueueImpl<>(name, hdr, cctx));

                GridCacheQueueProxy old = queuesMap.putIfAbsent(hdr.id(), queue);

                if (old != null)
                    queue = old;
            }

            return queue;
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
     * @param name Set name.
     * @param colloc Collocated flag.
     * @param create Create flag.
     * @return Set.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public <T> IgniteSet<T> set(final String name,
        boolean colloc,
        final boolean create)
        throws IgniteCheckedException
    {
        // Non collocated mode enabled only for PARTITIONED cache.
        final boolean colloc0 =
            create && (cctx.cache().configuration().getCacheMode() != PARTITIONED || colloc);

        return set0(name, colloc0, create);
    }

    /**
     * @param name Name of set.
     * @param collocated Collocation flag.
     * @param create If {@code true} set will be created in case it is not in cache.
     * @return Set.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <T> IgniteSet<T> set0(String name,
        boolean collocated,
        boolean create)
        throws IgniteCheckedException
    {
        cctx.gate().enter();

        try {
            GridCacheSetHeaderKey key = new GridCacheSetHeaderKey(name);

            GridCacheSetHeader hdr;

            GridCacheAdapter cache = cctx.cache();

            if (create) {
                hdr = new GridCacheSetHeader(IgniteUuid.randomUuid(), collocated);

                GridCacheSetHeader old = retryPutIfAbsent(cache, key, hdr);

                if (old != null)
                    hdr = old;
            }
            else
                hdr = (GridCacheSetHeader)cache.get(key);

            if (hdr == null)
                return null;

            GridCacheSetProxy<T> set = setsMap.get(hdr.id());

            if (set == null) {
                GridCacheSetProxy<T> old = setsMap.putIfAbsent(hdr.id(),
                    set = new GridCacheSetProxy<>(cctx, new GridCacheSetImpl<T>(cctx, name, hdr)));

                if (old != null)
                    set = old;
            }

            return set;
        }
        finally {
            cctx.gate().leave();
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
    private void removeSetData(IgniteUuid setId, long topVer) throws IgniteCheckedException {
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
     * @param id Set ID.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    public void removeSetData(IgniteUuid id) throws IgniteCheckedException {
        assert id != null;

        if (!cctx.isLocal()) {
            while (true) {
                long topVer = cctx.topologyVersionFuture().get();

                Collection<ClusterNode> nodes = CU.affinityNodes(cctx, topVer);

                try {
                    cctx.closures().callAsyncNoFailover(BROADCAST,
                        new BlockSetCallable(cctx.name(), id),
                        nodes,
                        true).get();
                }
                catch (ClusterTopologyException | ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("BlockSet job failed, will retry: " + e);

                    continue;
                }

                try {
                    cctx.closures().callAsyncNoFailover(BROADCAST,
                        new RemoveSetDataCallable(cctx.name(), id, topVer),
                        nodes,
                        true).get();
                }
                catch (ClusterTopologyException | ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("RemoveSetData job failed, will retry: " + e);

                    continue;
                }

                if (cctx.topologyVersionFuture().get() == topVer)
                    break;
            }
        }
        else {
            blockSet(id);

            cctx.dataStructures().removeSetData(id, 0);
        }
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
     * @param setId Set ID.
     */
    @SuppressWarnings("unchecked")
    private void blockSet(IgniteUuid setId) {
        GridCacheSetProxy set = setsMap.remove(setId);

        if (set != null)
            set.blockOnRemove();
    }

    /**
     * @param cache Cache.
     * @param key Key.
     * @param val Value.
     * @throws IgniteCheckedException If failed.
     * @return Previous value.
     */
    @SuppressWarnings("unchecked")
    @Nullable private <T> T retryPutIfAbsent(final GridCache cache, final Object key, final T val)
        throws IgniteCheckedException {
        return CacheDataStructuresProcessor.retry(log, new Callable<T>() {
            @Nullable @Override public T call() throws Exception {
                return (T)cache.putIfAbsent(key, val);
            }
        });
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

    /**
     * Waits for completion of all started set operations and blocks all subsequent operations.
     */
    @GridInternal
    private static class BlockSetCallable implements Callable<Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 0;

        /** Injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private String cacheName;

        /** */
        private IgniteUuid setId;

        /**
         * Required by {@link Externalizable}.
         */
        public BlockSetCallable() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param setId Set ID.
         */
        private BlockSetCallable(String cacheName, IgniteUuid setId) {
            this.cacheName = cacheName;
            this.setId = setId;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws IgniteCheckedException {
            assert ignite != null;

            GridCacheAdapter cache = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

            assert cache != null : cacheName;

            cache.context().dataStructures().blockSet(setId);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeGridUuid(out, setId);
            U.writeString(out, cacheName);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            setId = U.readGridUuid(in);
            cacheName = U.readString(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "BlockSetCallable [setId=" + setId + ']';
        }
    }

    /**
     * Removes set items.
     */
    @GridInternal
    private static class RemoveSetDataCallable implements Callable<Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 5053205121218843148L;

        /** Injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        private String cacheName;

        /** */
        private IgniteUuid setId;

        /** */
        private long topVer;

        /**
         * Required by {@link Externalizable}.
         */
        public RemoveSetDataCallable() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param setId Set ID.
         * @param topVer Topology version.
         */
        private RemoveSetDataCallable(String cacheName, IgniteUuid setId, long topVer) {
            this.cacheName = cacheName;
            this.setId = setId;
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public Void call() throws IgniteCheckedException {
            assert ignite != null;

            GridCacheAdapter cache = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

            assert cache != null;

            GridCacheGateway gate = cache.context().gate();

            gate.enter();

            try {
                cache.context().dataStructures().removeSetData(setId, topVer);
            }
            finally {
                gate.leave();
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
            U.writeGridUuid(out, setId);
            out.writeLong(topVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
            setId = U.readGridUuid(in);
            topVer = in.readLong();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "RemoveSetCallable [setId=" + setId + ']';
        }
    }
}
