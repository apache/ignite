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
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.portables.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.GridSystemProperties.*;

/**
 * Colocated get future.
 */
public class GridPartitionedGetFuture<K, V> extends GridCompoundIdentityFuture<Map<K, V>>
    implements GridCacheFuture<Map<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default max remap count value. */
    public static final int DFLT_MAX_REMAP_CNT = 3;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Maximum number of attempts to remap key to the same primary node. */
    private static final int MAX_REMAP_CNT = GridSystemProperties.getInteger(GG_NEAR_GET_MAX_REMAPS,
        DFLT_MAX_REMAP_CNT);

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /** Keys. */
    private Collection<? extends K> keys;

    /** Topology version. */
    private long topVer;

    /** Reload flag. */
    private boolean reload;

    /** Force primary flag. */
    private boolean forcePrimary;

    /** Future ID. */
    private GridUuid futId;

    /** Version. */
    private GridCacheVersion ver;

    /** Filters. */
    private GridPredicate<GridCacheEntry<K, V>>[] filters;

    /** Logger. */
    private GridLogger log;

    /** Trackable flag. */
    private volatile boolean trackable;

    /** Remap count. */
    private AtomicInteger remapCnt = new AtomicInteger();

    /** Subject ID. */
    private UUID subjId;

    /** Task name. */
    private String taskName;

    /** Whether to deserialize portable objects. */
    private boolean deserializePortable;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridPartitionedGetFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param topVer Topology version.
     * @param reload Reload flag.
     * @param forcePrimary If {@code true} then will force network trip to primary node even
     *          if called on backup node.
     * @param filters Filters.
     */
    public GridPartitionedGetFuture(
        GridCacheContext<K, V> cctx,
        Collection<? extends K> keys,
        long topVer,
        boolean reload,
        boolean forcePrimary,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filters,
        @Nullable UUID subjId,
        String taskName,
        boolean deserializePortable
    ) {
        super(cctx.kernalContext(), CU.<K, V>mapsReducer(keys.size()));

        assert cctx != null;
        assert !F.isEmpty(keys);

        this.cctx = cctx;
        this.keys = keys;
        this.topVer = topVer;
        this.reload = reload;
        this.forcePrimary = forcePrimary;
        this.filters = filters;
        this.subjId = subjId;
        this.deserializePortable = deserializePortable;
        this.taskName = taskName;

        futId = GridUuid.randomUuid();

        ver = cctx.versions().next();

        log = U.logger(ctx, logRef, GridPartitionedGetFuture.class);
    }

    /**
     * Initializes future.
     */
    public void init() {
        long topVer = this.topVer > 0 ? this.topVer : cctx.affinity().affinityTopologyVersion();

        map(keys, Collections.<GridNode, LinkedHashMap<K, Boolean>>emptyMap(), topVer);

        markInitialized();
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // Should not flip trackable flag from true to false since get future can be remapped.
    }

    /**
     * @return Keys.
     */
    Collection<? extends K> keys() {
        return keys;
    }

    /** {@inheritDoc} */
    @Override public GridUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Collection<? extends GridNode> nodes() {
        return
            F.viewReadOnly(futures(), new GridClosure<GridFuture<Map<K, V>>, GridNode>() {
                @Nullable @Override public GridNode apply(GridFuture<Map<K, V>> f) {
                    if (isMini(f))
                        return ((MiniFuture)f).node();

                    return cctx.discovery().localNode();
                }
            });
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        for (GridFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.node().id().equals(nodeId)) {
                    f.onResult(new GridTopologyException("Remote node left grid (will retry): " + nodeId));

                    return true;
                }
            }

        return false;
    }

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public void onResult(UUID nodeId, GridNearGetResponse<K, V> res) {
        for (GridFuture<Map<K, V>> fut : futures())
            if (isMini(fut)) {
                MiniFuture f = (MiniFuture)fut;

                if (f.futureId().equals(res.miniId())) {
                    assert f.node().id().equals(nodeId);

                    f.onResult(res);
                }
            }
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Map<K, V> res, Throwable err) {
        if (super.onDone(res, err)) {
            // Don't forget to clean up.
            if (trackable)
                cctx.mvcc().removeFuture(this);

            return true;
        }

        return false;
    }

    /**
     * @param f Future.
     * @return {@code True} if mini-future.
     */
    private boolean isMini(GridFuture<Map<K, V>> f) {
        return f.getClass().equals(MiniFuture.class);
    }

    /**
     * @param keys Keys.
     * @param mapped Mappings to check for duplicates.
     * @param topVer Topology version on which keys should be mapped.
     */
    private void map(Collection<? extends K> keys, Map<GridNode, LinkedHashMap<K, Boolean>> mapped, long topVer) {
        if (CU.affinityNodes(cctx, topVer).isEmpty()) {
            onDone(new GridTopologyException("Failed to map keys for cache (all partition nodes left the grid)."));

            return;
        }

        Map<GridNode, LinkedHashMap<K, Boolean>> mappings = new HashMap<>(CU.affinityNodes(cctx, topVer).size());

        final int keysSize = keys.size();

        Map<K, V> locVals = new HashMap<>(keysSize);

        boolean hasRmtNodes = false;

        // Assign keys to primary nodes.
        for (K key : keys) {
            if (key != null)
                hasRmtNodes |= map(key, mappings, locVals, topVer, mapped);
        }

        if (isDone())
            return;

        if (!locVals.isEmpty())
            add(new GridFinishedFuture<>(cctx.kernalContext(), locVals));

        if (hasRmtNodes) {
            trackable = true;

            cctx.mvcc().addFuture(this);
        }

        // Create mini futures.
        for (Map.Entry<GridNode, LinkedHashMap<K, Boolean>> entry : mappings.entrySet()) {
            final GridNode n = entry.getKey();

            final LinkedHashMap<K, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                final GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> fut =
                    cache().getDhtAsync(n.id(), -1, mappedKeys, reload, topVer, subjId,
                        taskName == null ? 0 : taskName.hashCode(), deserializePortable, filters);

                final Collection<Integer> invalidParts = fut.invalidPartitions();

                if (!F.isEmpty(invalidParts)) {
                    Collection<K> remapKeys = new ArrayList<>(keysSize);

                    for (K key : keys) {
                        if (key != null && invalidParts.contains(cctx.affinity().partition(key)))
                            remapKeys.add(key);
                    }

                    long updTopVer = ctx.discovery().topologyVersion();

                    assert updTopVer > topVer : "Got invalid partitions for local node but topology version did " +
                        "not change [topVer=" + topVer + ", updTopVer=" + updTopVer +
                        ", invalidParts=" + invalidParts + ']';

                    // Remap recursively.
                    map(remapKeys, mappings, updTopVer);
                }

                // Add new future.
                add(fut.chain(new C1<GridFuture<Collection<GridCacheEntryInfo<K, V>>>, Map<K, V>>() {
                    @Override public Map<K, V> apply(GridFuture<Collection<GridCacheEntryInfo<K, V>>> fut) {
                        try {
                            return createResultMap(fut.get());
                        }
                        catch (Exception e) {
                            U.error(log, "Failed to get values from dht cache [fut=" + fut + "]", e);

                            onDone(e);

                            return Collections.emptyMap();
                        }
                    }
                }));
            }
            else {
                MiniFuture fut = new MiniFuture(n, mappedKeys, topVer);

                GridCacheMessage<K, V> req = new GridNearGetRequest<>(futId, fut.futureId(), ver, mappedKeys,
                    reload, topVer, filters, subjId, taskName == null ? 0 : taskName.hashCode());

                add(fut); // Append new future.

                try {
                    cctx.io().send(n, req);
                }
                catch (GridException e) {
                    // Fail the whole thing.
                    if (e instanceof GridTopologyException)
                        fut.onResult((GridTopologyException)e);
                    else
                        fut.onResult(e);
                }
            }
        }
    }

    /**
     * @param mappings Mappings.
     * @param key Key to map.
     * @param locVals Local values.
     * @param topVer Topology version.
     * @param mapped Previously mapped.
     * @return {@code True} if has remote nodes.
     */
    @SuppressWarnings("ConstantConditions")
    private boolean map(K key, Map<GridNode, LinkedHashMap<K, Boolean>> mappings, Map<K, V> locVals,
        long topVer, Map<GridNode, LinkedHashMap<K, Boolean>> mapped) {
        GridDhtCacheAdapter<K, V> colocated = cache();

        boolean remote = false;

        // Allow to get cached value from the local node.
        boolean allowLocRead = !forcePrimary || cctx.affinity().primary(cctx.localNode(), key, topVer);

        while (true) {
            GridCacheEntryEx<K, V> entry = null;

            try {
                if (!reload && allowLocRead) {
                    try {
                        entry = colocated.context().isSwapOrOffheapEnabled() ? colocated.entryEx(key) :
                            colocated.peekEx(key);

                        // If our DHT cache do has value, then we peek it.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked();

                            V v = entry.innerGet(null,
                                /*swap*/true,
                                /*read-through*/false,
                                /*fail-fast*/true,
                                /*unmarshal*/true,
                                /**update-metrics*/true,
                                /*event*/true,
                                /*temporary*/false,
                                subjId,
                                null,
                                taskName,
                                filters);

                            colocated.context().evicts().touch(entry, topVer);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null) {
                                if (isNew && entry.markObsoleteIfEmpty(ver))
                                    colocated.removeIfObsolete(key);
                            }
                            else {
                                if (cctx.portableEnabled() && deserializePortable && v instanceof GridPortableObject)
                                    v = ((GridPortableObject)v).deserialize();

                                locVals.put(key, v);

                                return false;
                            }
                        }
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        // No-op.
                    }
                }

                GridNode node = cctx.affinity().primary(key, topVer);

                remote = !node.isLocal();

                LinkedHashMap<K, Boolean> keys = mapped.get(node);

                if (keys != null && keys.containsKey(key)) {
                    if (remapCnt.incrementAndGet() > MAX_REMAP_CNT) {
                        onDone(new GridTopologyException("Failed to remap key to a new node after " + MAX_REMAP_CNT
                            + " attempts (key got remapped to the same node) [key=" + key + ", node=" +
                            U.toShortString(node) + ", mappings=" + mapped + ']'));

                        return false;
                    }
                }

                LinkedHashMap<K, Boolean> old = mappings.get(node);

                if (old == null)
                    mappings.put(node, old = new LinkedHashMap<>(3, 1f));

                old.put(key, false);

                break;
            }
            catch (GridException e) {
                onDone(e);

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                // No-op, will retry.
            }
            catch (GridCacheFilterFailedException e) {
                if (log.isDebugEnabled())
                    log.debug("Filter validation failed for entry: " + e);

                colocated.context().evicts().touch(entry, topVer);

                break;
            }
        }

        return remote;
    }

    /**
     * @return Near cache.
     */
    private GridDhtCacheAdapter<K, V> cache() {
        return cctx.dht();
    }

    /**
     * @param infos Entry infos.
     * @return Result map.
     */
    private Map<K, V> createResultMap(Collection<GridCacheEntryInfo<K, V>> infos) {
        int keysSize = infos.size();

        try {
            if (keysSize != 0) {
                Map<K, V> map = new GridLeanMap<>(keysSize);

                for (GridCacheEntryInfo<K, V> info : infos) {
                    info.unmarshalValue(cctx, cctx.deploy().globalLoader());

                    V val = info.value();

                    if (cctx.portableEnabled() && deserializePortable && val instanceof GridPortableObject)
                        val = ((GridPortableObject)val).deserialize();

                    map.put(info.key(), val);
                }

                return map;
            }
        }
        catch (GridException e) {
            // Fail.
            onDone(e);
        }

        return Collections.emptyMap();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPartitionedGetFuture.class, this, super.toString());
    }

    /**
     * Mini-future for get operations. Mini-futures are only waiting on a single
     * node as opposed to multiple nodes.
     */
    private class MiniFuture extends GridFutureAdapter<Map<K, V>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridUuid futId = GridUuid.randomUuid();

        /** Node ID. */
        private GridNode node;

        /** Keys. */
        @GridToStringInclude
        private LinkedHashMap<K, Boolean> keys;

        /** Topology version on which this future was mapped. */
        private long topVer;

        /**
         * Empty constructor required for {@link Externalizable}.
         */
        public MiniFuture() {
            // No-op.
        }

        /**
         * @param node Node.
         * @param keys Keys.
         * @param topVer Topology version.
         */
        MiniFuture(GridNode node, LinkedHashMap<K, Boolean> keys, long topVer) {
            super(cctx.kernalContext());

            this.node = node;
            this.keys = keys;
            this.topVer = topVer;
        }

        /**
         * @return Future ID.
         */
        GridUuid futureId() {
            return futId;
        }

        /**
         * @return Node ID.
         */
        public GridNode node() {
            return node;
        }

        /**
         * @return Keys.
         */
        public Collection<K> keys() {
            return keys.keySet();
        }

        /**
         * @param e Error.
         */
        void onResult(Throwable e) {
            if (log.isDebugEnabled())
                log.debug("Failed to get future result [fut=" + this + ", err=" + e + ']');

            // Fail.
            onDone(e);
        }

        /**
         * @param e Failure exception.
         */
        @SuppressWarnings("UnusedParameters")
        void onResult(GridTopologyException e) {
            if (log.isDebugEnabled())
                log.debug("Remote node left grid while sending or waiting for reply (will retry): " + this);

            long updTopVer = ctx.discovery().topologyVersion();

            assert updTopVer > topVer : "Got topology exception but topology version did " +
                "not change [topVer=" + topVer + ", updTopVer=" + updTopVer +
                ", nodeId=" + node.id() + ']';

            // Remap.
            map(keys.keySet(), F.t(node, keys), updTopVer);

            onDone(Collections.<K, V>emptyMap());
        }

        /**
         * @param res Result callback.
         */
        @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
        void onResult(final GridNearGetResponse<K, V> res) {
            final Collection<Integer> invalidParts = res.invalidPartitions();

            // If error happened on remote node, fail the whole future.
            if (res.error() != null) {
                onDone(res.error());

                return;
            }

            // Remap invalid partitions.
            if (!F.isEmpty(invalidParts)) {
                long rmtTopVer = res.topologyVersion();

                assert rmtTopVer != 0;

                if (rmtTopVer <= topVer) {
                    // Fail the whole get future.
                    onDone(new GridException("Failed to process invalid partitions response (remote node reported " +
                        "invalid partitions but remote topology version does not differ from local) " +
                        "[topVer=" + topVer + ", rmtTopVer=" + rmtTopVer + ", invalidParts=" + invalidParts +
                        ", nodeId=" + node.id() + ']'));

                    return;
                }

                if (log.isDebugEnabled())
                    log.debug("Remapping mini get future [invalidParts=" + invalidParts + ", fut=" + this + ']');

                // Need to wait for next topology version to remap.
                GridFuture<Long> topFut = ctx.discovery().topologyFuture(rmtTopVer);

                topFut.listenAsync(new CIX1<GridFuture<Long>>() {
                    @SuppressWarnings("unchecked")
                    @Override public void applyx(GridFuture<Long> fut) throws GridException {
                        long topVer = fut.get();

                        // This will append new futures to compound list.
                        map(F.view(keys.keySet(),  new P1<K>() {
                            @Override public boolean apply(K key) {
                                return invalidParts.contains(cctx.affinity().partition(key));
                            }
                        }), F.t(node, keys), topVer);

                        onDone(createResultMap(res.entries()));
                    }
                });
            }
            else
                onDone(createResultMap(res.entries()));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }
    }
}
