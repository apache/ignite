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

import java.io.Externalizable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterTopologyException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.cluster.ClusterGroupEmptyCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheOperationContext;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheConcurrentMap;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheSwapEntry;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtCacheAdapter;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalEx;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerImpl;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.transactions.TransactionIsolation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBGRID;

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
        long createTtl,
        long accessTtl
    ) {
        assert tx != null;

        return lockAllAsync(keys, timeout, tx, isInvalidate, isRead, retval, isolation, createTtl, accessTtl);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<Boolean> lockAllAsync(Collection<? extends K> keys, long timeout) {
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
            -1L);
    }

    /**
     * @param keys Keys to lock.
     * @param timeout Timeout.
     * @param tx Transaction
     * @param isInvalidate Invalidation flag.
     * @param isRead Indicates whether value is read or written.
     * @param retval Flag to return value.
     * @param isolation Transaction isolation.
     * @param createTtl TTL for create operation.
     * @param accessTtl TTL for read operation.
     * @return Future for locks.
     */
    protected abstract IgniteInternalFuture<Boolean> lockAllAsync(Collection<KeyCacheObject> keys,
        long timeout,
        @Nullable IgniteTxLocalEx tx,
        boolean isInvalidate,
        boolean isRead,
        boolean retval,
        @Nullable TransactionIsolation isolation,
        long createTtl,
        long accessTtl);

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
        try {
            AffinityTopologyVersion topVer;

            boolean retry;

            CacheOperationContext opCtx = ctx.operationContextPerCall();

            boolean skipStore = opCtx != null && opCtx.skipStore();

            boolean keepBinary = opCtx != null && opCtx.isKeepBinary();

            do {
                retry = false;

                topVer = ctx.affinity().affinityTopologyVersion();

                // Send job to all data nodes.
                Collection<ClusterNode> nodes = ctx.grid().cluster().forDataNodes(name()).nodes();

                if (!nodes.isEmpty()) {
                    ctx.kernalContext().task().setThreadContext(TC_SUBGRID, nodes);

                    retry = !ctx.kernalContext().task().execute(
                        new RemoveAllTask(ctx.name(), topVer, skipStore, keepBinary), null).get();
                }
            }
            while (ctx.affinity().affinityTopologyVersion().compareTo(topVer) != 0 || retry);
        }
        catch (ClusterGroupEmptyCheckedException ignore) {
            if (log.isDebugEnabled())
                log.debug("All remote nodes left while cache remove [cacheName=" + name() + "]");
        }
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> removeAllAsync() {
        GridFutureAdapter<Void> opFut = new GridFutureAdapter<>();

        AffinityTopologyVersion topVer = ctx.affinity().affinityTopologyVersion();

        CacheOperationContext opCtx = ctx.operationContextPerCall();

        removeAllAsync(opFut, topVer, opCtx != null && opCtx.skipStore(), opCtx != null && opCtx.isKeepBinary());

        return opFut;
    }

    /**
     * @param opFut Future.
     * @param topVer Topology version.
     * @param skipStore Skip store flag.
     */
    private void removeAllAsync(
        final GridFutureAdapter<Void> opFut,
        final AffinityTopologyVersion topVer,
        final boolean skipStore,
        final boolean keepBinary
    ) {
        Collection<ClusterNode> nodes = ctx.grid().cluster().forDataNodes(name()).nodes();

        if (!nodes.isEmpty()) {
            ctx.kernalContext().task().setThreadContext(TC_SUBGRID, nodes);

            IgniteInternalFuture<Boolean> rmvAll = ctx.kernalContext().task().execute(
                new RemoveAllTask(ctx.name(), topVer, skipStore, keepBinary), null);

            rmvAll.listen(new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                    try {
                        boolean retry = !fut.get();

                        AffinityTopologyVersion topVer0 = ctx.affinity().affinityTopologyVersion();

                        if (topVer0.equals(topVer) && !retry)
                            opFut.onDone();
                        else
                            removeAllAsync(opFut, topVer0, skipStore, keepBinary);
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
     * Remove task.
     */
    @GridInternal
    private static class RemoveAllTask extends ComputeTaskAdapter<Object, Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Cache name. */
        private final String cacheName;

        /** Affinity topology version. */
        private final AffinityTopologyVersion topVer;

        /** Skip store flag. */
        private final boolean skipStore;

        /** Keep binary flag. */
        private final boolean keepBinary;

        /**
         * @param cacheName Cache name.
         * @param topVer Affinity topology version.
         * @param skipStore Skip store flag.
         */
        public RemoveAllTask(String cacheName, AffinityTopologyVersion topVer, boolean skipStore, boolean keepBinary) {
            this.cacheName = cacheName;
            this.topVer = topVer;
            this.skipStore = skipStore;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Object arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid)
                jobs.put(new GlobalRemoveAllJob(cacheName, topVer, skipStore, keepBinary), node);

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            IgniteException e = res.getException();

            if (e != null) {
                if (e instanceof ClusterTopologyException)
                    return ComputeJobResultPolicy.WAIT;

                throw new IgniteException("Remote job threw exception.", e);
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
            for (ComputeJobResult locRes : results) {
                if (locRes != null && (locRes.getException() != null || !locRes.<Boolean>getData()))
                    return false;
            }

            return true;
        }
    }
    /**
     * Internal job which performs remove all primary key mappings
     * operation on a cache with the given name.
     */
    @GridInternal
    private static class GlobalRemoveAllJob<K,V>  extends TopologyVersionAwareJob {
        /** */
        private static final long serialVersionUID = 0L;

        /** Skip store flag. */
        private final boolean skipStore;

        /** Keep binary flag. */
        private final boolean keepBinary;

        /**
         * @param cacheName Cache name.
         * @param topVer Topology version.
         * @param skipStore Skip store flag.
         */
        private GlobalRemoveAllJob(
            String cacheName,
            @NotNull AffinityTopologyVersion topVer,
            boolean skipStore,
            boolean keepBinary
        ) {
            super(cacheName, topVer);

            this.skipStore = skipStore;
            this.keepBinary = keepBinary;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object localExecute(@Nullable IgniteInternalCache cache0) {
            GridCacheAdapter cache = ((IgniteKernal) ignite).context().cache().internalCache(cacheName);

            if (cache == null)
                return true;

            final GridCacheContext<K, V> ctx = cache.context();

            ctx.gate().enter();

            try {
                if (!ctx.affinity().affinityTopologyVersion().equals(topVer))
                    return false; // Ignore this remove request because remove request will be sent again.

                GridDhtCacheAdapter<K, V> dht;
                GridNearCacheAdapter<K, V> near = null;

                if (cache instanceof GridNearCacheAdapter) {
                    near = ((GridNearCacheAdapter<K, V>) cache);
                    dht = near.dht();
                }
                else
                    dht = (GridDhtCacheAdapter<K, V>) cache;

                try (DataStreamerImpl<KeyCacheObject, Object> dataLdr =
                         (DataStreamerImpl) ignite.dataStreamer(cacheName)) {
                    ((DataStreamerImpl) dataLdr).maxRemapCount(0);

                    dataLdr.skipStore(skipStore);
                    dataLdr.keepBinary(keepBinary);

                    dataLdr.receiver(DataStreamerCacheUpdaters.<KeyCacheObject, Object>batched());

                    for (int part : ctx.affinity().primaryPartitions(ctx.localNodeId(), topVer)) {
                        GridDhtLocalPartition locPart = dht.topology().localPartition(part, topVer, false);

                        if (locPart == null || (ctx.rebalanceEnabled() && locPart.state() != OWNING) || !locPart.reserve())
                            return false;

                        try {
                            if (!locPart.isEmpty()) {
                                for (GridCacheEntryEx o : locPart.allEntries()) {
                                    if (!o.obsoleteOrDeleted())
                                        dataLdr.removeDataInternal(o.key());
                                }
                            }

                            GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry>> iter =
                                dht.context().swap().iterator(part);

                            if (iter != null) {
                                for (Map.Entry<byte[], GridCacheSwapEntry> e : iter)
                                    dataLdr.removeDataInternal(ctx.toCacheKeyObject(e.getKey()));
                            }
                        }
                        finally {
                            locPart.release();
                        }
                    }
                }

                if (near != null) {
                    GridCacheVersion obsoleteVer = ctx.versions().next();

                    for (GridCacheEntryEx e : near.allEntries()) {
                        if (!e.valid(topVer) && e.markObsolete(obsoleteVer))
                            near.removeEntry(e);
                    }
                }
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
            finally {
                ctx.gate().leave();
            }

            return true;
        }
    }
}
