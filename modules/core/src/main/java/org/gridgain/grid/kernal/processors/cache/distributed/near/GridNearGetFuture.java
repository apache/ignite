/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.distributed.dht.*;
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
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.kernal.processors.cache.GridCacheUtils.*;

/**
 *
 */
public final class GridNearGetFuture<K, V> extends GridCompoundIdentityFuture<Map<K, V>>
    implements GridCacheFuture<Map<K, V>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default max remap count value. */
    public static final int DFLT_MAX_REMAP_CNT = 3;

    /** Logger reference. */
    private static final AtomicReference<GridLogger> logRef = new AtomicReference<>();

    /** Maximum number of attempts to remap key to the same primary node. */
    private static final int MAX_REMAP_CNT;

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /** Keys. */
    private Collection<? extends K> keys;

    /** Reload flag. */
    private boolean reload;

    /** Force primary flag. */
    private boolean forcePrimary;

    /** Future ID. */
    private GridUuid futId;

    /** Version. */
    private GridCacheVersion ver;

    /** Transaction. */
    private GridCacheTxLocalEx<K, V> tx;

    /** Filters. */
    private GridPredicate<GridCacheEntry<K, V>>[] filters;

    /** Logger. */
    private GridLogger log;

    /** Trackable flag. */
    private boolean trackable;

    /** Remap count. */
    private AtomicInteger remapCnt = new AtomicInteger();

    /** Subject ID. */
    private UUID subjId;

    /** Whether to deserialize portable objects. */
    private boolean deserializePortable;

    /**
     * Initializes max remap count.
     */
    static {
        int dfltRemapCnt = DFLT_MAX_REMAP_CNT;

        String s = X.getSystemOrEnv(GG_NEAR_GET_MAX_REMAPS, Integer.toString(dfltRemapCnt));

        int cnt;

        try {
            cnt = Integer.parseInt(s);
        }
        catch (NumberFormatException ignore) {
            cnt = dfltRemapCnt;
        }

        MAX_REMAP_CNT = cnt;
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridNearGetFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param keys Keys.
     * @param reload Reload flag.
     * @param forcePrimary If {@code true} get will be performed on primary node even if
     *      called on backup node.
     * @param tx Transaction.
     * @param filters Filters.
     */
    public GridNearGetFuture(
        GridCacheContext<K, V> cctx,
        Collection<? extends K> keys,
        boolean reload,
        boolean forcePrimary,
        @Nullable GridCacheTxLocalEx<K, V> tx,
        @Nullable GridPredicate<GridCacheEntry<K, V>>[] filters,
        @Nullable UUID subjId,
        boolean deserializePortable
    ) {
        super(cctx.kernalContext(), CU.<K, V>mapsReducer(keys.size()));

        assert cctx != null;
        assert !F.isEmpty(keys);

        this.cctx = cctx;
        this.keys = keys;
        this.reload = reload;
        this.forcePrimary = forcePrimary;
        this.filters = filters;
        this.tx = tx;
        this.subjId = subjId;
        this.deserializePortable = deserializePortable;

        futId = GridUuid.randomUuid();

        ver = tx == null ? cctx.versions().next() : tx.xidVersion();

        log = U.logger(ctx, logRef, GridNearGetFuture.class);
    }

    /**
     * Initializes future.
     */
    public void init() {
        long topVer = tx == null ? ctx.discovery().topologyVersion() : tx.topologyVersion();

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
    void onResult(UUID nodeId, GridNearGetResponse<K, V> res) {
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
     * @param topVer Topology version to map on.
     */
    private void map(Collection<? extends K> keys, Map<GridNode, LinkedHashMap<K, Boolean>> mapped, final long topVer) {
        Collection<GridNode> affNodes = CU.affinityNodes(cctx, topVer);

        if (affNodes.isEmpty()) {
            assert !isAffinityNode(cctx.config());

            onDone(new GridTopologyException("Failed to map keys for near-only cache (all partition " +
                "nodes left the grid)."));

            return;
        }

        Map<GridNode, LinkedHashMap<K, Boolean>> mappings =
            new HashMap<>(affNodes.size());

        Map<K, GridCacheVersion> savedVers = null;

        // Assign keys to primary nodes.
        for (K key : keys) {
            if (key != null)
                savedVers = map(key, mappings, topVer, mapped, savedVers);
        }

        if (isDone())
            return;

        final Map<K, GridCacheVersion> saved = savedVers;

        final int keysSize = keys.size();

        // Create mini futures.
        for (Map.Entry<GridNode, LinkedHashMap<K, Boolean>> entry : mappings.entrySet()) {
            final GridNode n = entry.getKey();

            final LinkedHashMap<K, Boolean> mappedKeys = entry.getValue();

            assert !mappedKeys.isEmpty();

            // If this is the primary or backup node for the keys.
            if (n.isLocal()) {
                final GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> fut =
                    dht().getDhtAsync(n.id(), -1, mappedKeys, reload, topVer, subjId, deserializePortable, filters);

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
                            return loadEntries(n.id(), mappedKeys.keySet(), fut.get(), saved, topVer);
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
                if (!trackable) {
                    trackable = true;

                    cctx.mvcc().addFuture(this);
                }

                MiniFuture fut = new MiniFuture(n, mappedKeys, saved, topVer);

                GridCacheMessage<K, V> req = new GridNearGetRequest<>(futId, fut.futureId(), ver, mappedKeys,
                    reload, topVer, filters, subjId);

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
     * @param topVer Topology version
     * @param mapped Previously mapped.
     * @param savedVers Saved versions.
     * @return Map.
     */
    private Map<K, GridCacheVersion> map(K key, Map<GridNode, LinkedHashMap<K, Boolean>> mappings,
        long topVer, Map<GridNode, LinkedHashMap<K, Boolean>> mapped, Map<K, GridCacheVersion> savedVers) {
        final GridNearCacheAdapter<K, V> near = cache();

        // Allow to get cached value from the local node.
        boolean allowLocRead = !forcePrimary || cctx.affinity().primary(cctx.localNode(), key, topVer);

        GridCacheEntryEx<K, V> entry = allowLocRead ? near.peekEx(key) : null;

        while (true) {
            try {
                V v = null;

                boolean isNear = entry != null;

                // First we peek into near cache.
                if (isNear)
                    v = entry.innerGet(tx, /*swap*/false, /*read-through*/false, /*fail-fast*/true, /*unmarshal*/true,
                        true/*metrics*/, true/*events*/, subjId, filters);

                GridNode primary = null;

                if (v == null && allowLocRead) {
                    GridDhtCacheAdapter<K, V> dht = cache().dht();

                    try {
                        entry = dht.context().isSwapOrOffheapEnabled() ? dht.entryEx(key) : dht.peekEx(key);

                        // If near cache does not have value, then we peek DHT cache.
                        if (entry != null) {
                            boolean isNew = entry.isNewLocked() || !entry.valid(topVer);

                            v = entry.innerGet(tx, /*swap*/true, /*read-through*/false, /*fail-fast*/true,
                                /*unmarshal*/true, /*update-metrics*/false, !isNear, subjId, filters);

                            // Entry was not in memory or in swap, so we remove it from cache.
                            if (v == null && isNew && entry.markObsoleteIfEmpty(ver))
                                dht.removeIfObsolete(key);
                        }

                        if (v != null)
                            near.metrics0().onRead(true);
                        else {
                            primary = cctx.affinity().primary(key, topVer);

                            if (!primary.isLocal())
                                near.metrics0().onRead(false);
                        }
                    }
                    catch (GridDhtInvalidPartitionException ignored) {
                        // No-op.
                    }
                    finally {
                        if (entry != null && (tx == null || (!tx.implicit() && tx.isolation() == READ_COMMITTED))) {
                            dht.context().evicts().touch(entry, topVer);

                            entry = null;
                        }
                    }
                }

                if (v != null && !reload) {
                    if (cctx.portableEnabled() && deserializePortable && v instanceof GridPortableObject)
                        v = ((GridPortableObject)v).deserialize();

                    add(new GridFinishedFuture<>(cctx.kernalContext(), Collections.singletonMap(key, v)));
                }
                else {
                    if (primary == null)
                        primary = cctx.affinity().primary(key, topVer);

                    GridNearCacheEntry<K, V> nearEntry = allowLocRead ? near.peekExx(key) : null;

                    entry = nearEntry;

                    if (savedVers == null)
                        savedVers = new HashMap<>(3);

                    savedVers.put(key, nearEntry == null ? null : nearEntry.dhtVersion());

                    LinkedHashMap<K, Boolean> keys = mapped.get(primary);

                    if (keys != null && keys.containsKey(key)) {
                        if (remapCnt.incrementAndGet() > MAX_REMAP_CNT) {
                            onDone(new GridTopologyException("Failed to remap key to a new node after " + MAX_REMAP_CNT
                                + " attempts (key got remapped to the same node) [key=" + key + ", node=" +
                                U.toShortString(primary) + ", mappings=" + mapped + ']'));

                            return savedVers;
                        }
                    }

                    // Don't add reader if transaction acquires lock anyway to avoid deadlock.
                    boolean addRdr = tx == null || tx.optimistic();

                    if (!addRdr && tx.readCommitted() && !tx.writeSet().contains(key))
                        addRdr = true;

                    LinkedHashMap<K, Boolean> old = mappings.get(primary);

                    if (old == null)
                        mappings.put(primary, old = new LinkedHashMap<>(3, 1f));

                    old.put(key, addRdr);
                }

                break;
            }
            catch (GridException e) {
                onDone(e);

                break;
            }
            catch (GridCacheEntryRemovedException ignored) {
                entry = allowLocRead ? near.peekEx(key) : null;
            }
            catch (GridCacheFilterFailedException e) {
                if (log.isDebugEnabled())
                    log.debug("Filter validation failed for entry: " + e);

                break;
            }
            finally {
                if (entry != null && !reload && tx == null)
                    cctx.evicts().touch(entry, topVer);
            }
        }

        return savedVers;
    }

    /**
     * @return Near cache.
     */
    private GridNearCacheAdapter<K, V> cache() {
        return (GridNearCacheAdapter<K, V>)cctx.cache();
    }

    /**
     * @return DHT cache.
     */
    private GridDhtCacheAdapter<K, V> dht() {
        return cache().dht();
    }

    /**
     * @param nodeId Node id.
     * @param keys Keys.
     * @param infos Entry infos.
     * @param savedVers Saved versions.
     * @return Result map.
     */
    private Map<K, V> loadEntries(UUID nodeId, Collection<K> keys, Collection<GridCacheEntryInfo<K, V>> infos,
        Map<K, GridCacheVersion> savedVers, long topVer) {
        boolean empty = F.isEmpty(keys);

        Map<K, V> map = empty ? Collections.<K, V>emptyMap() : new GridLeanMap<K, V>(keys.size());

        if (!empty) {
            boolean atomic = cctx.atomic();

            GridCacheVersion ver = atomic ? null : F.isEmpty(infos) ? null : cctx.versions().next();

            for (GridCacheEntryInfo<K, V> info : infos) {
                try {
                    info.unmarshalValue(cctx, cctx.deploy().globalLoader());

                    // Entries available locally in DHT should not be loaded into near cache for reading.
                    if (!cctx.cache().affinity().isPrimaryOrBackup(cctx.localNode(), info.key())) {
                        GridNearCacheEntry<K, V> entry = cache().entryExx(info.key(), topVer);

                        GridCacheVersion saved = savedVers.get(info.key());

                        // Load entry into cache.
                        entry.loadedValue(tx,
                            nodeId,
                            info.value(),
                            info.valueBytes(),
                            atomic ? info.version() : ver,
                            info.version(),
                            saved,
                            info.ttl(),
                            info.expireTime(),
                            true,
                            topVer,
                            subjId);
                    }

                    V val = info.value();

                    if (cctx.portableEnabled() && deserializePortable && val instanceof GridPortableObject)
                        val = ((GridPortableObject)val).deserialize();

                    map.put(info.key(), val);
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry while processing get response (will not retry).");
                }
                catch (GridException e) {
                    // Fail.
                    onDone(e);

                    return Collections.emptyMap();
                }
            }
        }

        return map;
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

        /** Saved entry versions. */
        private Map<K, GridCacheVersion> savedVers;

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
         * @param savedVers Saved entry versions.
         * @param topVer Topology version.
         */
        MiniFuture(GridNode node, LinkedHashMap<K, Boolean> keys, Map<K, GridCacheVersion> savedVers, long topVer) {
            super(cctx.kernalContext());

            this.node = node;
            this.keys = keys;
            this.savedVers = savedVers;
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
         * @param e Topology exception.
         */
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
                    @Override public void applyx(GridFuture<Long> fut) throws GridException {
                        long readyTopVer = fut.get();

                        // This will append new futures to compound list.
                        map(F.view(keys.keySet(), new P1<K>() {
                            @Override public boolean apply(K key) {
                                return invalidParts.contains(cctx.affinity().partition(key));
                            }
                        }), F.t(node, keys), readyTopVer);

                        // It is critical to call onDone after adding futures to compound list.
                        onDone(loadEntries(node.id(), keys.keySet(), res.entries(), savedVers, topVer));
                    }
                });
            }
            else
                onDone(loadEntries(node.id(), keys.keySet(), res.entries(), savedVers, topVer));
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(MiniFuture.class, this);
        }
    }
}
