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
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.distributed.dht.*;
import org.apache.ignite.internal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.dataload.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.transactions.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
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
    protected GridDistributedCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        super(ctx, map);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> txLockAsync(
        Collection<? extends K> keys,
        long timeout,
        IgniteTxLocalEx<K, V> tx,
        boolean isRead,
        boolean retval,
        IgniteTxIsolation isolation,
        boolean isInvalidate,
        long accessTtl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter
    ) {
        assert tx != null;

        return lockAllAsync(keys, timeout, tx, isInvalidate, isRead, retval, isolation, accessTtl, filter);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout,
        IgnitePredicate<Cache.Entry<K, V>>... filter) {
        IgniteTxLocalEx<K, V> tx = ctx.tm().userTxx();

        // Return value flag is true because we choose to bring values for explicit locks.
        return lockAllAsync(keys, timeout, tx, false, false, /*retval*/true, null, -1L, filter);
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
    protected abstract IgniteInternalFuture<Boolean> lockAllAsync(Collection<? extends K> keys,
        long timeout,
        @Nullable IgniteTxLocalEx<K, V> tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        @Nullable IgniteTxIsolation isolation,
        long accessTtl,
        IgnitePredicate<Cache.Entry<K, V>>[] filter);

    /**
     * @param key Key to remove.
     * @param ver Version to remove.
     */
    public void removeVersionedEntry(K key, GridCacheVersion ver) {
        GridCacheEntryEx<K, V> entry = peekEx(key);

        if (entry == null)
            return;

        if (entry.markObsoleteVersion(ver))
            removeEntry(entry);
    }

    /** {@inheritDoc} */
    @Override public void removeAll() throws IgniteCheckedException {
        try {
            long topVer;

            do {
                topVer = ctx.affinity().affinityTopologyVersion();

                // Send job to all data nodes.
                Collection<ClusterNode> nodes = ctx.grid().forDataNodes(name()).nodes();

                if (!nodes.isEmpty()) {
                    ctx.closures().callAsyncNoFailover(BROADCAST,
                        new GlobalRemoveAllCallable<>(name(), topVer), nodes, true).get();
                }
            }
            while (ctx.affinity().affinityTopologyVersion() > topVer);
        }
        catch (ClusterGroupEmptyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("All remote nodes left while cache remove [cacheName=" + name() + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        GridFutureAdapter<Void> opFut = new GridFutureAdapter<>(ctx.kernalContext());

        long topVer = ctx.affinity().affinityTopologyVersion();

        removeAllAsync(opFut, topVer);

        return opFut;
    }

    /**
     * @param opFut Future.
     * @param topVer Topology version.
     */
    private void removeAllAsync(final GridFutureAdapter<Void> opFut, final long topVer) {
        Collection<ClusterNode> nodes = ctx.grid().forDataNodes(name()).nodes();

        if (!nodes.isEmpty()) {
            IgniteInternalFuture<?> rmvFut = ctx.closures().callAsyncNoFailover(BROADCAST,
                    new GlobalRemoveAllCallable<>(name(), topVer), nodes, true);

            rmvFut.listenAsync(new IgniteInClosure<IgniteInternalFuture<?>>() {
                @Override public void apply(IgniteInternalFuture<?> fut) {
                    try {
                        fut.get();

                        long topVer0 = ctx.affinity().affinityTopologyVersion();

                        if (topVer0 == topVer)
                            opFut.onDone();
                        else
                            removeAllAsync(opFut, topVer0);
                    }
                    catch (ClusterGroupEmptyCheckedException ignore) {
                        if (log.isDebugEnabled())
                            log.debug("All remote nodes left while cache remove [cacheName=" + name() + "]");

                        opFut.onDone();
                    }
                    catch (IgniteCheckedException e) {
                        opFut.onDone(e);
                    }
                    catch (Error e) {
                        opFut.onDone(e);

                        throw e;
                    }
                }
            });
        }
        else
            opFut.onDone();
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
    private static class GlobalRemoveAllCallable<K,V> implements Callable<Object>, Externalizable {
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

        /**
         * {@inheritDoc}
         */
        @Override public Object call() throws Exception {
            GridCacheAdapter<K, V> cacheAdapter = ((IgniteKernal)ignite).context().cache().internalCache(cacheName);

            final GridCacheContext<K, V> ctx = cacheAdapter.context();

            ctx.affinity().affinityReadyFuture(topVer).get();

            ctx.gate().enter();

            try {
                if (ctx.affinity().affinityTopologyVersion() != topVer)
                    return null; // Ignore this remove request because remove request will be sent again.

                GridDhtCacheAdapter<K, V> dht;

                if (cacheAdapter instanceof GridNearCacheAdapter)
                    dht = ((GridNearCacheAdapter<K, V>)cacheAdapter).dht();
                else
                    dht = (GridDhtCacheAdapter<K, V>)cacheAdapter;

                try (IgniteDataLoader<K, V> dataLdr = ignite.dataLoader(cacheName)) {
                    dataLdr.updater(GridDataLoadCacheUpdaters.<K, V>batched());

                    for (GridDhtLocalPartition<K, V> locPart : dht.topology().currentLocalPartitions()) {
                        if (!locPart.isEmpty() && locPart.primary(topVer)) {
                            for (GridDhtCacheEntry<K, V> o : locPart.entries()) {
                                if (!o.obsoleteOrDeleted())
                                    dataLdr.removeData(o.key());
                            }
                        }
                    }

                    Iterator<Cache.Entry<K, V>> it = dht.context().swap().offheapIterator(true, false, topVer);

                    while (it.hasNext())
                        dataLdr.removeData(it.next().getKey());

                    it = dht.context().swap().swapIterator(true, false, topVer);

                    while (it.hasNext())
                        dataLdr.removeData(it.next().getKey());
                }
            }
            finally {
                ctx.gate().leave();
            }

            return null;
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
