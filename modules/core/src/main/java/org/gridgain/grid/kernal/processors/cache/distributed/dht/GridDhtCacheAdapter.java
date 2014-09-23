/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.colocated.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.preloader.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.kernal.processors.version.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.product.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;
import static org.gridgain.grid.kernal.processors.dr.GridDrType.*;
import static org.gridgain.grid.util.direct.GridTcpCommunicationMessageAdapter.*;

/**
 * DHT cache adapter.
 */
public abstract class GridDhtCacheAdapter<K, V> extends GridDistributedCacheAdapter<K, V> {
    /** */
    public static final GridProductVersion SUBJECT_ID_EVENTS_SINCE_VER = GridProductVersion.fromString("6.1.7");

    /** */
    public static final GridProductVersion TASK_NAME_HASH_SINCE_VER = GridProductVersion.fromString("6.2.1");

    /** */
    public static final GridProductVersion PRELOAD_WITH_LOCK_SINCE_VER = GridProductVersion.fromString("6.5.0");

    /** */
    private static final long serialVersionUID = 0L;

    /** Topology. */
    private GridDhtPartitionTopology<K, V> top;

    /** Preloader. */
    protected GridCachePreloader<K, V> preldr;

    /** Multi tx future holder. */
    private ThreadLocal<GridBiTuple<GridUuid, GridDhtTopologyFuture>> multiTxHolder = new ThreadLocal<>();

    /** Multi tx futures. */
    private ConcurrentMap<GridUuid, MultiUpdateFuture> multiTxFuts = new ConcurrentHashMap8<>();

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    protected GridDhtCacheAdapter() {
        // No-op.
    }

    /**
     * @param ctx Context.
     */
    protected GridDhtCacheAdapter(GridCacheContext<K, V> ctx) {
        super(ctx, ctx.config().getStartSize());

        top = new GridDhtPartitionTopologyImpl<>(ctx);
    }

    /**
     * Constructor used for near-only cache.
     *
     * @param ctx Cache context.
     * @param map Cache map.
     */
    protected GridDhtCacheAdapter(GridCacheContext<K, V> ctx, GridCacheConcurrentMap<K, V> map) {
        super(ctx, map);

        top = new GridDhtPartitionTopologyImpl<>(ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init() {
        map.setEntryFactory(new GridCacheMapEntryFactory<K, V>() {
            /** {@inheritDoc} */
            @Override public GridCacheMapEntry<K, V> create(GridCacheContext<K, V> ctx, long topVer, K key, int hash,
                V val, GridCacheMapEntry<K, V> next, long ttl, int hdrId) {
                return new GridDhtCacheEntry<>(ctx, topVer, key, hash, val, next, ttl, hdrId);
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void stop() {
        super.stop();

        if (preldr != null)
            preldr.stop();

        // Clean up to help GC.
        preldr = null;
        top = null;
    }

    /** {@inheritDoc} */
    @Override public void onKernalStart() throws GridException {
        super.onKernalStart();

        preldr.onKernalStart();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop() {
        super.onKernalStop();

        if (preldr != null)
            preldr.onKernalStop();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        super.printMemoryStats();

        top.printMemoryStats(1024);
    }

    /**
     * @return Near cache.
     */
    public abstract GridNearCacheAdapter<K, V> near();

    /**
     * @return Partition topology.
     */
    public GridDhtPartitionTopology<K, V> topology() {
        return top;
    }

    /** {@inheritDoc} */
    @Override public GridCachePreloader<K, V> preloader() {
        return preldr;
    }

    /**
     * @return DHT preloader.
     */
    public GridDhtPreloader<K, V> dhtPreloader() {
        assert preldr instanceof GridDhtPreloader;

        return (GridDhtPreloader<K, V>)preldr;
    }

    /**
     * @return Topology version future registered for multi-update.
     */
    @Nullable public GridDhtTopologyFuture multiUpdateTopologyFuture() {
        GridBiTuple<GridUuid, GridDhtTopologyFuture> tup = multiTxHolder.get();

        return tup == null ? null : tup.get2();
    }

    /**
     * Starts multi-update lock. Will wait for topology future is ready.
     *
     * @return Topology version.
     * @throws GridException If failed.
     */
    public long beginMultiUpdate() throws GridException {
        GridBiTuple<GridUuid, GridDhtTopologyFuture> tup = multiTxHolder.get();

        if (tup != null)
            throw new GridException("Nested multi-update locks are not supported");

        top.readLock();

        GridDhtTopologyFuture topFut;

        long topVer;

        try {
            // While we are holding read lock, register lock future for partition release future.
            GridUuid lockId = GridUuid.fromUuid(ctx.localNodeId());

            topVer = top.topologyVersion();

            MultiUpdateFuture fut = new MultiUpdateFuture(ctx.kernalContext(), topVer);

            MultiUpdateFuture old = multiTxFuts.putIfAbsent(lockId, fut);

            assert old == null;

            topFut = top.topologyVersionFuture();

            multiTxHolder.set(F.t(lockId, topFut));
        }
        finally {
            top.readUnlock();
        }

        topFut.get();

        return topVer;
    }

    /**
     * Ends multi-update lock.
     *
     * @throws GridException If failed.
     */
    public void endMultiUpdate() throws GridException {
        GridBiTuple<GridUuid, GridDhtTopologyFuture> tup = multiTxHolder.get();

        if (tup == null)
            throw new GridException("Multi-update was not started or released twice.");

        top.readLock();

        try {
            GridUuid lockId = tup.get1();

            MultiUpdateFuture multiFut = multiTxFuts.remove(lockId);

            multiTxHolder.set(null);

            // Finish future.
            multiFut.onDone(lockId);
        }
        finally {
            top.readUnlock();
        }
    }

    /**
     * Creates multi update finish future. Will return {@code null} if no multi-update locks are found.
     *
     * @param topVer Topology version.
     * @return Finish future.
     */
    @Nullable public GridFuture<?> multiUpdateFinishFuture(long topVer) {
        GridCompoundFuture<GridUuid, Object> fut = null;

        for (MultiUpdateFuture multiFut : multiTxFuts.values()) {
            if (multiFut.topologyVersion() <= topVer) {
                if (fut == null)
                    fut = new GridCompoundFuture<>(ctx.kernalContext());

                fut.add(multiFut);
            }
        }

        if (fut != null)
            fut.markInitialized();

        return fut;
    }

    /**
     * @param key Key.
     * @return DHT entry.
     */
    @Nullable public GridDhtCacheEntry<K, V> peekExx(K key) {
        return (GridDhtCacheEntry<K, V>)peekEx(key);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    @Override public GridCacheEntry<K, V> entry(K key) throws GridDhtInvalidPartitionException {
        return super.entry(key);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    @Override public GridCacheEntryEx<K, V> entryEx(K key, boolean touch) throws GridDhtInvalidPartitionException {
        return super.entryEx(key, touch);
    }

    /**
     * {@inheritDoc}
     *
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    @Override public GridCacheEntryEx<K, V> entryEx(K key, long topVer) throws GridDhtInvalidPartitionException {
        return super.entryEx(key, topVer);
    }

    /**
     * @param key Key.
     * @return DHT entry.
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    public GridDhtCacheEntry<K, V> entryExx(K key) throws GridDhtInvalidPartitionException {
        return (GridDhtCacheEntry<K, V>)entryEx(key);
    }

    /**
     * @param key Key.
     * @param topVer Topology version.
     * @return DHT entry.
     * @throws GridDhtInvalidPartitionException If partition for the key is no longer valid.
     */
    public GridDhtCacheEntry<K, V> entryExx(K key, long topVer) throws GridDhtInvalidPartitionException {
        return (GridDhtCacheEntry<K, V>)entryEx(key, topVer);
    }

    /**
     * Gets or creates entry for given key. If key belongs to local node, dht entry will be returned, otherwise
     * if {@code allowDetached} is {@code true}, detached entry will be returned, otherwise exception will be
     * thrown.
     *
     * @param key Key for which entry should be returned.
     * @param allowDetached Whether to allow detached entries.
     * @param touch {@code True} if entry should be passed to eviction policy.
     * @return Cache entry.
     * @throws GridDhtInvalidPartitionException if entry does not belong to this node and
     *      {@code allowDetached} is {@code false}.
     */
    public GridCacheEntryEx<K, V> entryExx(K key, long topVer, boolean allowDetached, boolean touch) {
        try {
            return allowDetached && !ctx.affinity().localNode(key, topVer) ?
                new GridDhtDetachedCacheEntry<>(ctx, key, key.hashCode(), null, null, 0, 0) :
                entryEx(key, touch);
        }
        catch (GridDhtInvalidPartitionException e) {
            if (!allowDetached)
                throw e;

            return new GridDhtDetachedCacheEntry<>(ctx, key, key.hashCode(), null, null, 0, 0);
        }
    }

    /** {@inheritDoc} */
    @Override public void loadCache(final GridBiPredicate<K, V> p, final long ttl, Object[] args) throws GridException {
        if (ctx.store().isLocalStore()) {
            super.loadCache(p, ttl, args);

            return;
        }

        // Version for all loaded entries.
        final GridCacheVersion ver0 = ctx.versions().nextForLoad(topology().topologyVersion());

        final boolean replicate = ctx.isDrEnabled();

        final long topVer = ctx.affinity().affinityTopologyVersion();

        ctx.store().loadCache(new CI3<K, V, GridCacheVersion>() {
            @Override public void apply(K key, V val, @Nullable GridCacheVersion ver) {
                assert ver == null;

                if (p != null && !p.apply(key, val))
                    return;

                try {
                    GridDhtLocalPartition<K, V> part = top.localPartition(ctx.affinity().partition(key), -1, true);

                    // Reserve to make sure that partition does not get unloaded.
                    if (part.reserve()) {
                        GridCacheEntryEx<K, V> entry = null;

                        try {
                            if (ctx.portableEnabled()) {
                                key = (K)ctx.marshalToPortable(key);
                                val = (V)ctx.marshalToPortable(val);
                            }

                            entry = entryEx(key, false);

                            entry.initialValue(val, null, ver0, ttl, -1, false, topVer, replicate ? DR_LOAD : DR_NONE);
                        }
                        catch (GridException e) {
                            throw new GridRuntimeException("Failed to put cache value: " + entry, e);
                        }
                        catch (GridCacheEntryRemovedException ignore) {
                            if (log.isDebugEnabled())
                                log.debug("Got removed entry during loadCache (will ignore): " + entry);
                        }
                        finally {
                            if (entry != null)
                                entry.context().evicts().touch(entry, topVer);

                            part.release();
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Will node load entry into cache (partition is invalid): " + part);
                }
                catch (GridDhtInvalidPartitionException e) {
                    if (log.isDebugEnabled())
                        log.debug("Ignoring entry for partition that does not belong [key=" + key + ", val=" + val +
                            ", err=" + e + ']');
                }
            }
        }, args);
    }

    /** {@inheritDoc} */
    @Override public int primarySize() {
        int sum = 0;

        long topVer = ctx.affinity().affinityTopologyVersion();

        for (GridDhtLocalPartition<K, V> p : topology().currentLocalPartitions()) {
            if (p.primary(topVer))
                sum += p.publicSize();
        }

        return sum;
    }

    /**
     * This method is used internally. Use
     * {@link #getDhtAsync(UUID, long, LinkedHashMap, boolean, long, UUID, int, boolean, GridPredicate[])}
     * method instead to retrieve DHT value.
     *
     * @param keys {@inheritDoc}
     * @param forcePrimary {@inheritDoc}
     * @param skipTx {@inheritDoc}
     * @param filter {@inheritDoc}
     * @return {@inheritDoc}
     */
    @Override public GridFuture<Map<K, V>> getAllAsync(
        @Nullable Collection<? extends K> keys,
        boolean forcePrimary,
        boolean skipTx,
        @Nullable GridCacheEntryEx<K, V> entry,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter
    ) {
        return getAllAsync(keys, null, /*don't check local tx. */false, subjId, taskName, deserializePortable,
            forcePrimary, filter);
    }

    /** {@inheritDoc} */
    @Override public V reload(K key, @Nullable GridPredicate<GridCacheEntry<K, V>>... filter)
        throws GridException {
        try {
            return super.reload(key, filter);
        }
        catch (GridDhtInvalidPartitionException ignored) {
            return null;
        }
    }

    /**
     * @param keys Keys to get
     * @param filter {@inheritDoc}
     * @return {@inheritDoc}
     */
    GridFuture<Map<K, V>> getDhtAllAsync(@Nullable Collection<? extends K> keys, @Nullable UUID subjId,
        String taskName, boolean deserializePortable, @Nullable GridPredicate<GridCacheEntry<K, V>>[] filter) {
        return getAllAsync(keys, null, /*don't check local tx. */false, subjId, taskName, deserializePortable, false,
            filter);
    }

    /**
     * @param reader Reader node ID.
     * @param msgId Message ID.
     * @param keys Keys to get.
     * @param reload Reload flag.
     * @param topVer Topology version.
     * @param filter Optional filter.
     * @return DHT future.
     */
    public GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> getDhtAsync(UUID reader, long msgId,
        LinkedHashMap<? extends K, Boolean> keys, boolean reload, long topVer, @Nullable UUID subjId,
        int taskNameHash, boolean deserializePortable, GridPredicate<GridCacheEntry<K, V>>[] filter) {
        GridDhtGetFuture<K, V> fut = new GridDhtGetFuture<>(ctx, msgId, reader, keys, reload, /*tx*/null,
            topVer, filter, subjId, taskNameHash, deserializePortable);

        fut.init();

        return fut;
    }

    /**
     * @param nodeId Node ID.
     * @param req Get request.
     */
    protected void processNearGetRequest(final UUID nodeId, final GridNearGetRequest<K, V> req) {
        assert isAffinityNode(cacheCfg);

        GridFuture<Collection<GridCacheEntryInfo<K, V>>> fut =
            getDhtAsync(nodeId, req.messageId(), req.keys(), req.reload(), req.topologyVersion(), req.subjectId(),
                req.taskNameHash(), false, req.filter());

        fut.listenAsync(new CI1<GridFuture<Collection<GridCacheEntryInfo<K, V>>>>() {
            @Override public void apply(GridFuture<Collection<GridCacheEntryInfo<K, V>>> f) {
                GridNearGetResponse<K, V> res = new GridNearGetResponse<>(
                    req.futureId(), req.miniId(), req.version());

                GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> fut =
                    (GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>>)f;

                try {
                    Collection<GridCacheEntryInfo<K, V>> entries = fut.get();

                    res.entries(entries);
                }
                catch (GridException e) {
                    U.error(log, "Failed processing get request: " + req, e);

                    res.error(e);
                }

                res.invalidPartitions(fut.invalidPartitions(), ctx.discovery().topologyVersion());

                try {
                    ctx.io().send(nodeId, res);
                }
                catch (GridException e) {
                    U.error(log, "Failed to send get response to node (is node still alive?) [nodeId=" + nodeId +
                        ",req=" + req + ", res=" + res + ']', e);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public GridCacheTxLocalAdapter<K, V> newTx(boolean implicit, boolean implicitSingle,
        GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation, long timeout, boolean invalidate,
        boolean syncCommit, boolean syncRollback, boolean swapOrOffheapEnabled, boolean storeEnabled, int txSize,
        @Nullable Object grpLockKey, boolean partLock) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void unlockAll(Collection<? extends K> keys,
        GridPredicate<GridCacheEntry<K, V>>[] filter) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public Set<GridCacheEntry<K, V>> entrySet(int part) {
        return new PartitionEntrySet(part);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtCacheAdapter.class, this);
    }

    /**
     *
     */
    private class PartitionEntrySet extends AbstractSet<GridCacheEntry<K, V>> {
        /** */
        private int partId;

        /**
         * @param partId Partition id.
         */
        private PartitionEntrySet(int partId) {
            this.partId = partId;
        }

        /** {@inheritDoc} */
        @NotNull @Override public Iterator<GridCacheEntry<K, V>> iterator() {
            final GridDhtLocalPartition<K, V> part = ctx.topology().localPartition(partId,
                ctx.discovery().topologyVersion(), false);

            Iterator<GridDhtCacheEntry<K, V>> partIt = part == null ? null : part.entries().iterator();

            return new PartitionEntryIterator<>(partIt);
        }

        /** {@inheritDoc} */
        @Override public boolean remove(Object o) {
            if (!(o instanceof GridCacheEntry))
                return false;

            GridCacheEntry<K, V> entry = (GridCacheEntry<K, V>)o;

            K key = entry.getKey();
            V val = entry.peek();

            if (val == null)
                return false;

            try {
                // Cannot use remove(key, val) since we may be in DHT cache and should go through near.
                return entry(key).remove(val);
            }
            catch (GridException e) {
                throw new GridRuntimeException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public boolean removeAll(Collection<?> c) {
            boolean rmv = false;

            for (Object o : c)
                rmv |= remove(o);

            return rmv;
        }

        /** {@inheritDoc} */
        @Override public boolean contains(Object o) {
            if (!(o instanceof GridCacheEntry))
                return false;

            GridCacheEntry<K, V> entry = (GridCacheEntry<K, V>)o;

            return partId == entry.partition() && F.eq(entry.peek(), peek(entry.getKey()));
        }

        /** {@inheritDoc} */
        @Override public int size() {
            GridDhtLocalPartition<K, V> part = ctx.topology().localPartition(partId,
                ctx.discovery().topologyVersion(), false);

            return part != null ? part.publicSize() : 0;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(PartitionEntrySet.class, this, "super", super.toString());
        }
    }

    /** {@inheritDoc} */
    @Override public List<GridCacheClearAllRunnable<K, V>> splitClearAll() {
        GridCacheDistributionMode mode = configuration().getDistributionMode();

        return (mode == PARTITIONED_ONLY || mode == NEAR_PARTITIONED) ? super.splitClearAll() :
            Collections.<GridCacheClearAllRunnable<K, V>>emptyList();
    }

    /** {@inheritDoc} */
    @Override public void onDeferredDelete(GridCacheEntryEx<K, V> entry, GridCacheVersion ver) {
        assert entry.isDht();

        GridDhtLocalPartition<K, V> part = topology().localPartition(entry.partition(), -1, false);

        // Do not remove entry on replica topology. Instead, add entry to removal queue.
        // It will be cleared eventually.
        if (part != null) {
            try {
                part.onDeferredDelete(entry.key(), ver);
            }
            catch (GridException e) {
                U.error(log, "Failed to enqueue deleted entry [key=" + entry.key() + ", ver=" + ver + ']', e);
            }
        }
    }

    /**
     * Complex partition iterator for both partition and swap iteration.
     */
    private static class PartitionEntryIterator<K, V> extends GridIteratorAdapter<GridCacheEntry<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Next entry. */
        private GridCacheEntry<K, V> entry;

        /** Last seen entry to support remove. */
        private GridCacheEntry<K, V> last;

        /** Partition iterator. */
        private final Iterator<GridDhtCacheEntry<K, V>> partIt;

        /**
         * @param partIt Partition iterator.
         */
        private PartitionEntryIterator(@Nullable Iterator<GridDhtCacheEntry<K, V>> partIt) {
            this.partIt = partIt;

            advance();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNextX() {
            return entry != null;
        }

        /** {@inheritDoc} */
        @Override public GridCacheEntry<K, V> nextX() throws GridException {
            if (!hasNext())
                throw new NoSuchElementException();

            last = entry;

            advance();

            return last;
        }

        /** {@inheritDoc} */
        @Override public void removeX() throws GridException {
            if (last == null)
                throw new IllegalStateException();

            last.remove();
        }

        /**
         *
         */
        private void advance() {
            if (partIt != null) {
                while (partIt.hasNext()) {
                    GridDhtCacheEntry<K, V> next = partIt.next();

                    if (next.isInternal() || !next.visitable(CU.<K, V>empty()))
                        continue;

                    entry = next.wrap(true);

                    return;
                }
            }

            entry = null;
        }
    }

    /**
     * Multi update future.
     */
    private static class MultiUpdateFuture extends GridFutureAdapter<GridUuid> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Topology version. */
        private long topVer;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public MultiUpdateFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         * @param topVer Topology version.
         */
        private MultiUpdateFuture(GridKernalContext ctx, long topVer) {
            super(ctx);

            this.topVer = topVer;
        }

        /**
         * @return Topology version.
         */
        private long topologyVersion() {
            return topVer;
        }
    }


    /**
     * GridDhtAtomicUpdateRequest converter for version 6.1.2
     */
    @SuppressWarnings("PublicInnerClass")
    public static class GridSubjectIdAddedMessageConverter616 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0: {
                    if (!commState.putUuid(GridTcpCommunicationMessageAdapter.UUID_NOT_READ))
                        return false;

                    commState.idx++;
                }
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0: {
                    UUID subjId0 = commState.getUuid();

                    if (subjId0 == GridTcpCommunicationMessageAdapter.UUID_NOT_READ)
                        return false;

                    commState.idx++;
                }
            }

            return true;
        }
    }

    /**
     * GridDhtAtomicUpdateRequest converter for version 6.1.2
     */
    @SuppressWarnings("PublicInnerClass")
    public static class GridTaskNameHashAddedMessageConverter621 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0: {
                    if (!commState.putInt(0))
                        return false;

                    commState.idx++;
                }
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0: {
                    if (buf.remaining() < 4)
                        return false;

                    commState.getInt();

                    commState.idx++;
                }
            }

            return true;
        }
    }

    /**
     * GridDht{Prepare|Lock}Request message converter.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class PreloadKeysAddedMessageConverter650 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (!commState.putBitSet(null))
                        return false;

                    commState.idx++;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    BitSet preloadKeys0 = commState.getBitSet();

                    if (preloadKeys0 == BIT_SET_NOT_READ)
                        return false;

                    commState.idx++;
            }

            return true;
        }
    }

    /**
     * GridDht{Prepare|Lock}Response message converter.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class PreloadEntriesAddedMessageConverter650 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (!commState.putInt(-1))
                        return false;

                    commState.idx++;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (commState.readSize == -1) {
                        if (buf.remaining() < 4)
                            return false;

                        commState.readSize = commState.getInt();
                    }

                    if (commState.readSize >= 0) {
                        for (int i = commState.readItems; i < commState.readSize; i++) {
                            byte[] _val = commState.getByteArray();

                            if (_val == BYTE_ARR_NOT_READ)
                                return false;

                            commState.readItems++;
                        }
                    }

                    commState.readSize = -1;
                    commState.readItems = 0;

                    commState.idx++;
            }

            return true;
        }
    }

    /**
     * GridCachePessimisticCheckCommittedTxRequest message converter.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class BooleanFlagAddedMessageConverter650 extends GridVersionConverter {
        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (!commState.putBoolean(false))
                        return false;

                    commState.idx++;
            }

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean readFrom(ByteBuffer buf) {
            commState.setBuffer(buf);

            switch (commState.idx) {
                case 0:
                    if (buf.remaining() < 1)
                        return false;

                    commState.getBoolean();

                    commState.idx++;
            }

            return true;
        }
    }
}
