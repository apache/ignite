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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheGateway;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor;
import org.apache.ignite.internal.processors.datastructures.GridAtomicCacheQueueImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheQueueHeader;
import org.apache.ignite.internal.processors.datastructures.GridCacheQueueHeaderKey;
import org.apache.ignite.internal.processors.datastructures.GridCacheQueueProxy;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetHeader;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetHeaderKey;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetImpl;
import org.apache.ignite.internal.processors.datastructures.GridCacheSetProxy;
import org.apache.ignite.internal.processors.datastructures.GridTransactionalCacheQueueImpl;
import org.apache.ignite.internal.processors.datastructures.SetItemKey;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridSpinBusyLock;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.internal.GridClosureCallMode.BROADCAST;

/**
 *
 */
public class CacheDataStructuresManager extends GridCacheManagerAdapter {
    /** Sets map. */
    private final ConcurrentMap<IgniteUuid, GridCacheSetProxy> setsMap;

    /** Set keys used for set iteration. */
    private ConcurrentMap<IgniteUuid, GridConcurrentHashSet<SetItemKey>> setDataMap =
        new ConcurrentHashMap8<>();

    /** Queues map. */
    private final ConcurrentMap<IgniteUuid, GridCacheQueueProxy> queuesMap;

    /** Queue header view.  */
    private IgniteInternalCache<GridCacheQueueHeaderKey, GridCacheQueueHeader> queueHdrView;

    /** Query notifying about queue update. */
    private UUID queueQryId;

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
            queueHdrView = cctx.cache();

            initFlag = true;
        }
        finally {
            initLatch.countDown();
        }
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        busyLock.block();

        if (queueQryId != null)
            cctx.continuousQueries().cancelInternalQuery(queueQryId);

        for (GridCacheQueueProxy q : queuesMap.values())
            q.delegate().onKernalStop();
    }

    /**
     * @param set Set.
     */
    public void onRemoved(GridCacheSetProxy set) {
        setsMap.remove(set.delegate().id(), set);
    }

    /**
     * @param clusterRestarted Cluster restarted flag.
     * @throws IgniteCheckedException If failed.
     */
    public void onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        for (Map.Entry<IgniteUuid, GridCacheSetProxy> e : setsMap.entrySet()) {
            GridCacheSetProxy set = e.getValue();

            if (clusterRestarted) {
                set.blockOnRemove();

                setsMap.remove(e.getKey(), set);
            }
            else
                set.needCheckNotRemoved();
        }

        for (Map.Entry<IgniteUuid, GridCacheQueueProxy> e : queuesMap.entrySet()) {
            GridCacheQueueProxy queue = e.getValue();

            if (clusterRestarted) {
                queue.delegate().onRemoved(false);

                queuesMap.remove(e.getKey(), queue);
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

                GridCacheQueueHeader old = queueHdrView.withNoRetries().getAndPutIfAbsent(key, hdr);

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
                queueQryId = cctx.continuousQueries().executeInternalQuery(
                    new CacheEntryUpdatedListener<Object, Object>() {
                        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
                            if (!busyLock.enterBusy())
                                return;

                            try {
                                for (CacheEntryEvent<?, ?> e : evts) {
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
                            }
                            finally {
                                busyLock.leaveBusy();
                            }
                        }
                    },
                    new QueueHeaderPredicate(),
                    cctx.isLocal() || (cctx.isReplicated() && cctx.affinityNode()),
                    true,
                    false);
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
     * @param keepBinary Keep binary flag.
     */
    public void onEntryUpdated(KeyCacheObject key, boolean rmv, boolean keepBinary) {
        // No need to notify data structures manager for a user cache since all DS objects are stored
        // in system caches.
        if (cctx.userCache())
            return;

        Object key0 = cctx.cacheObjectContext().unwrapBinaryIfNeeded(key, keepBinary, false);

        if (key0 instanceof SetItemKey)
            onSetItemUpdated((SetItemKey)key0, rmv);
    }

    /**
     * Partition evicted callback.
     *
     * @param part Partition number.
     */
    public void onPartitionEvicted(int part) {
        GridCacheAffinityManager aff = cctx.affinity();

        for (GridConcurrentHashSet<SetItemKey> set : setDataMap.values()) {
            Iterator<SetItemKey> iter = set.iterator();

            while (iter.hasNext()) {
                SetItemKey key = iter.next();

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

            IgniteInternalCache cache = cctx.cache().withNoRetries();

            if (create) {
                hdr = new GridCacheSetHeader(IgniteUuid.randomUuid(), collocated);

                GridCacheSetHeader old = (GridCacheSetHeader)cache.getAndPutIfAbsent(key, hdr);

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
    @Nullable public GridConcurrentHashSet<SetItemKey> setData(IgniteUuid id) {
        return setDataMap.get(id);
    }

    /**
     * @param setId Set ID.
     * @param topVer Topology version.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void removeSetData(IgniteUuid setId, AffinityTopologyVersion topVer) throws IgniteCheckedException {
        boolean loc = cctx.isLocal();

        GridCacheAffinityManager aff = cctx.affinity();

        if (!loc) {
            aff.affinityReadyFuture(topVer).get();

            cctx.preloader().syncFuture().get();
        }

        GridConcurrentHashSet<SetItemKey> set = setDataMap.get(setId);

        if (set == null)
            return;

        IgniteInternalCache cache = cctx.cache();

        final int BATCH_SIZE = 100;

        Collection<SetItemKey> keys = new ArrayList<>(BATCH_SIZE);

        for (SetItemKey key : set) {
            if (!loc && !aff.primaryByKey(cctx.localNode(), key, topVer))
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
                AffinityTopologyVersion topVer = cctx.topologyVersionFuture().get();

                Collection<ClusterNode> nodes = CU.affinityNodes(cctx, topVer);

                try {
                    cctx.closures().callAsyncNoFailover(BROADCAST,
                        new BlockSetCallable(cctx.name(), id),
                        nodes,
                        true,
                        0).get();
                }
                catch (IgniteCheckedException e) {
                    if (e.hasCause(ClusterTopologyCheckedException.class)) {
                        if (log.isDebugEnabled())
                            log.debug("RemoveSetData job failed, will retry: " + e);

                        continue;
                    }
                    else if (!pingNodes(nodes)) {
                        if (log.isDebugEnabled())
                            log.debug("RemoveSetData job failed and set data node left, will retry: " + e);

                        continue;
                    }
                    else
                        throw e;
                }

                try {
                    cctx.closures().callAsyncNoFailover(BROADCAST,
                        new RemoveSetDataCallable(cctx.name(), id, topVer),
                        nodes,
                        true,
                        0).get();
                }
                catch (IgniteCheckedException e) {
                    if (e.hasCause(ClusterTopologyCheckedException.class)) {
                        if (log.isDebugEnabled())
                            log.debug("RemoveSetData job failed, will retry: " + e);

                        continue;
                    }
                    else if (!pingNodes(nodes)) {
                        if (log.isDebugEnabled())
                            log.debug("RemoveSetData job failed and set data node left, will retry: " + e);

                        continue;
                    }
                    else
                        throw e;
                }

                if (topVer.equals(cctx.topologyVersionFuture().get()))
                    break;
            }
        }
        else {
            blockSet(id);

            cctx.dataStructures().removeSetData(id, AffinityTopologyVersion.ZERO);
        }
    }

    /**
     * @param nodes Nodes to ping.
     * @return {@code True} if was able to ping all nodes.
     * @throws IgniteCheckedException If failed/
     */
    private boolean pingNodes(Collection<ClusterNode> nodes) throws IgniteCheckedException {
        for (ClusterNode node : nodes) {
            if (!cctx.discovery().pingNode(node.id()))
                return false;
        }

        return true;
    }

    /**
     * @param key Set item key.
     * @param rmv {@code True} if item was removed.
     */
    private void onSetItemUpdated(SetItemKey key, boolean rmv) {
        GridConcurrentHashSet<SetItemKey> set = setDataMap.get(key.setId());

        if (set == null) {
            if (rmv)
                return;

            GridConcurrentHashSet<SetItemKey> old = setDataMap.putIfAbsent(key.setId(),
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
     * @param keys Keys to remove.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("unchecked")
    private void retryRemoveAll(final IgniteInternalCache cache, final Collection<SetItemKey> keys)
        throws IgniteCheckedException {
        DataStructuresProcessor.retry(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                cache.removeAll(keys);

                return null;
            }
        });
    }

    /**
     * Predicate for queue continuous query.
     */
    private static class QueueHeaderPredicate<K, V> implements CacheEntryEventSerializableFilter<K, V>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Required by {@link Externalizable}.
         */
        public QueueHeaderPredicate() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<? extends K, ? extends V> e) {
            return e.getKey() instanceof GridCacheQueueHeaderKey;
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
        private AffinityTopologyVersion topVer;

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
        private RemoveSetDataCallable(String cacheName, IgniteUuid setId, @NotNull AffinityTopologyVersion topVer) {
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
            out.writeObject(topVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
            setId = U.readGridUuid(in);
            topVer = (AffinityTopologyVersion)in.readObject();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "RemoveSetCallable [setId=" + setId + ']';
        }
    }
}
