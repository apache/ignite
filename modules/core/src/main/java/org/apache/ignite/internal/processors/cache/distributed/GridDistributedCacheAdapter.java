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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.datastreamer.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.internal.GridClosureCallMode.*;

/**
 * Distributed cache implementation.
 */
public abstract class GridDistributedCacheAdapter<K, V> extends GridCacheAdapter<K, V> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int MAX_REMOVE_ALL_ATTEMPTS = 50;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    protected GridDistributedCacheAdapter() {
        // No-op.
    }

    /**
     * @param ctx Cache registry.
     * @param startSize Start size.
     */
    protected GridDistributedCacheAdapter(GridCacheContext<K, V> ctx, int startSize) {
        super(ctx, startSize);
    }

    /**
     * @param ctx Cache context.
     * @param map Cache map.
     */
    protected GridDistributedCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> txLockAsync(
        Collection<KeyCacheObject> keys,
        long timeout,
        IgniteTxLocalEx tx,
        boolean isRead,
        boolean retval,
        TransactionIsolation isolation,
        boolean isInvalidate,
        long accessTtl,
        CacheEntryPredicate[] filter
    ) {
        assert tx != null;

        return lockAllAsync(keys, timeout, tx, isInvalidate, isRead, retval, isolation, accessTtl, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout,
        CacheEntryPredicate... filter) {
        IgniteTxLocalEx tx = ctx.tm().userTxx();

        // Return value flag is true because we choose to bring values for explicit locks.
        return lockAllAsync(ctx.cacheKeysView(keys),
            timeout,
            tx,
            false,
            false,
            /*retval*/true,
            null,
            -1L,
            filter);
    }

    /**
     * @param keys Keys to lock.
     * @param timeout Timeout.
     * @param tx Transaction
     * @param isInvalidate Invalidation flag.
     * @param isRead Indicates whether value is read or written.
     * @param retval Flag to return value.
     * @param isolation Transaction isolation.
     * @param accessTtl TTL for read operation.
     * @param filter Optional filter.
     * @return Future for locks.
     */
    protected abstract IgniteInternalFuture<Boolean> lockAllAsync(Collection<KeyCacheObject> keys,
        long timeout,
        @Nullable IgniteTxLocalEx tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        @Nullable TransactionIsolation isolation,
        long accessTtl,
        CacheEntryPredicate[] filter);

    /**
     * @param key Key to remove.
     * @param ver Version to remove.
     */
    public void removeVersionedEntry(KeyCacheObject key, GridCacheVersion ver) {
        GridCacheEntryEx entry = peekEx(key);

        if (entry == null)
            return;

        if (entry.markObsoleteVersion(ver))
            removeEntry(entry);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws IgniteCheckedException {
        int attemptCnt = 0;

        while (true) {
            long topVer = ctx.discovery().topologyVersion();

            IgniteInternalFuture<Long> fut = ctx.affinity().affinityReadyFuturex(topVer);
            if (fut != null)
                fut.get();

            // Send job to all data nodes.
            ClusterGroup cluster = ctx.grid().cluster().forDataNodes(name());

            if (cluster.nodes().isEmpty())
                break;

            try {
                Collection<Long> res = ctx.grid().compute(cluster).withNoFailover().broadcast(
                    new GlobalRemoveAllCallable<>(name(), topVer));

                Long max = Collections.max(res);

                if (max > 0) {
                    assert max > topVer;

                    ctx.affinity().affinityReadyFuture(max).get();

                    continue;
                }

                if (res.contains(-1L)) {
                    if (++attemptCnt > MAX_REMOVE_ALL_ATTEMPTS)
                        throw new IgniteCheckedException("Failed to remove all entries.");

                    continue;
                }
            }
            catch (ClusterGroupEmptyException ignore) {
                if (log.isDebugEnabled())
                    log.debug("All remote nodes left while cache remove [cacheName=" + name() + "]");

                break;
            }
            catch (ClusterTopologyException e) {
                // GlobalRemoveAllCallable was sent to node that has left.
                if (topVer == ctx.discovery().topologyVersion()) {
                    // Node was not left, some other error has occurs.
                    throw e;
                }
            }

            if (topVer == ctx.discovery().topologyVersion())
                break;
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        GridFutureAdapter<Void> opFut = new GridFutureAdapter<>();

        removeAllAsync(opFut, 0);

        return opFut;
    }

    /**
     * @param opFut Future.
     * @param attemptCnt Attempts count.
     */
    private void removeAllAsync(final GridFutureAdapter<Void> opFut, final int attemptCnt) {
        final long topVer = ctx.affinity().affinityTopologyVersion();

        ClusterGroup cluster = ctx.grid().cluster().forDataNodes(name());

        if (cluster.nodes().isEmpty())
            opFut.onDone();
        else {
            IgniteCompute computeAsync = ctx.grid().compute(cluster).withNoFailover().withAsync();

            computeAsync.broadcast(new GlobalRemoveAllCallable<>(name(), topVer));

            ComputeTaskFuture<Collection<Long>> fut = computeAsync.future();

            fut.listen(new IgniteInClosure<IgniteFuture<Collection<Long>>>() {
                @Override public void apply(IgniteFuture<Collection<Long>> fut) {
                    try {
                        Collection<Long> res = fut.get();

                        Long max = Collections.max(res);

                        if (max > 0) {
                            assert max > topVer;

                            try {
                                ctx.affinity().affinityReadyFuture(max).get();

                                removeAllAsync(opFut, attemptCnt);
                            }
                            catch (IgniteCheckedException e) {
                                opFut.onDone(e);
                            }

                            return;
                        }

                        if (res.contains(-1L)) {
                            if (attemptCnt >= MAX_REMOVE_ALL_ATTEMPTS)
                                opFut.onDone(new IgniteCheckedException("Failed to remove all entries."));
                            else
                                removeAllAsync(opFut, attemptCnt + 1);

                            return;
                        }

                        if (topVer != ctx.affinity().affinityTopologyVersion())
                            removeAllAsync(opFut, attemptCnt);
                    }
                    catch (ClusterGroupEmptyException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("All remote nodes left while cache remove [cacheName=" + name() + "]");

                        opFut.onDone();
                    }
                    catch (ClusterTopologyException e) {
                        // GlobalRemoveAllCallable was sent to node that has left.
                        if (topVer == ctx.discovery().topologyVersion()) {
                            // Node was not left, some other error has occurs.
                            opFut.onDone(e);

                            return;
                        }

                        removeAllAsync(opFut, attemptCnt + 1);
                    }
                    catch (Error e) {
                        opFut.onDone(e);

                        throw e;
                    }
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDistributedCacheAdapter.class, this, "super", super.toString());
    }

    /**
     * Internal callable which performs remove all primary key mappings
     * operation on a cache with the given name.
     */
    @GridInternal
    private static class GlobalRemoveAllCallable<K,V> implements IgniteCallable<Long>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private String cacheName;

        /** Topology version. */
        private long topVer;

        /** Injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Empty constructor for serialization.
         */
        public GlobalRemoveAllCallable() {
            // No-op.
        }

        /**
         * @param cacheName Cache name.
         * @param topVer Topology version.
         */
        private GlobalRemoveAllCallable(String cacheName, long topVer) {
            this.cacheName = cacheName;
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public Long call() throws Exception {
            GridCacheAdapter<K, V> cacheAdapter = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

            final GridCacheContext<K, V> ctx = cacheAdapter.context();

            IgniteInternalFuture<Long> topVerFut = ctx.affinity().affinityReadyFuture(topVer);

            if (topVerFut != null)
                topVerFut.get();

            ctx.gate().enter();

            try {
                if (ctx.affinity().affinityTopologyVersion() > topVer)
                    return ctx.affinity().affinityTopologyVersion();

                GridDhtCacheAdapter<K, V> dht;

                if (cacheAdapter instanceof GridNearCacheAdapter)
                    dht = ((GridNearCacheAdapter<K, V>)cacheAdapter).dht();
                else
                    dht = (GridDhtCacheAdapter<K, V>)cacheAdapter;

                try (DataStreamerImpl<KeyCacheObject, Object> dataLdr =
                         (DataStreamerImpl)ignite.dataStreamer(cacheName)) {
                    ((DataStreamerImpl)dataLdr).maxRemapCount(0);

                    dataLdr.updater(DataStreamerCacheUpdaters.<KeyCacheObject, Object>batched());

                    for (GridDhtLocalPartition locPart : dht.topology().currentLocalPartitions()) {
                        if (!locPart.isEmpty() && locPart.primary(topVer)) {
                            for (GridDhtCacheEntry o : locPart.entries()) {
                                if (!o.obsoleteOrDeleted())
                                    dataLdr.removeDataInternal(o.key());
                            }
                        }
                    }

                    Iterator<KeyCacheObject> it = dht.context().swap().offHeapKeyIterator(true, false, topVer);

                    while (it.hasNext())
                        dataLdr.removeDataInternal(it.next());

                    it = dht.context().swap().swapKeyIterator(true, false, topVer);

                    while (it.hasNext())
                        dataLdr.removeDataInternal(it.next());

                    return 0L; // 0 means remove completer successfully.
                }
                catch (IgniteException e) {
                    if (e instanceof ClusterTopologyException
                        || e.hasCause(ClusterTopologyCheckedException.class, ClusterTopologyException.class))
                        return -1L;

                    throw e;
                }
                catch (IllegalStateException ignored) {
                    // Looks like node is about stop.
                    return -1L; // -1 means request should be resend.
                }
            }
            finally {
                ctx.gate().leave();
            }
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeString(out, cacheName);
            out.writeLong(topVer);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            cacheName = U.readString(in);
            topVer = in.readLong();
        }
    }
}
