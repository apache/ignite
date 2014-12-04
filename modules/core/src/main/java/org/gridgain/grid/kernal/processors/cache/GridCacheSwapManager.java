/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.managers.swapspace.*;
import org.gridgain.grid.kernal.processors.cache.query.*;
import org.gridgain.grid.kernal.processors.license.*;
import org.gridgain.grid.kernal.processors.offheap.*;
import org.gridgain.grid.spi.swapspace.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.offheap.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.lang.ref.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMemoryMode.*;
import static org.gridgain.grid.events.GridEventType.*;
import static org.gridgain.grid.kernal.processors.license.GridLicenseSubsystem.*;

/**
 * Handles all swap operations.
 */
public class GridCacheSwapManager<K, V> extends GridCacheManagerAdapter<K, V> {
    /** Swap manager. */
    private GridSwapSpaceManager swapMgr;

    /** */
    private String spaceName;

    /** Flag to indicate if manager is enabled. */
    private final boolean enabled;

    /** Flag to indicate if swap is enabled. */
    private boolean swapEnabled;

    /** Flag to indicate if offheap is enabled. */
    private boolean offheapEnabled;

    /** Swap listeners. */
    private final ConcurrentMap<Integer, Collection<GridCacheSwapListener<K, V>>>
        swapLsnrs = new ConcurrentHashMap8<>();


    /** Swap listeners. */
    private final ConcurrentMap<Integer, Collection<GridCacheSwapListener<K, V>>>
        offheapLsnrs = new ConcurrentHashMap8<>();

    /** Offheap. */
    private GridOffHeapProcessor offheap;

    /** Soft iterator queue. */
    private final ReferenceQueue<Iterator<Map.Entry<K, V>>> itQ = new ReferenceQueue<>();

    /** Soft iterator set. */
    private final Collection<GridWeakIterator<Map.Entry<K, V>>> itSet =
        new GridConcurrentHashSet<>();

    /**
     * @param enabled Flag to indicate if swap is enabled.
     */
    public GridCacheSwapManager(boolean enabled) {
        this.enabled = enabled;
    }

    /** {@inheritDoc} */
    @Override public void start0() throws GridException {
        spaceName = CU.swapSpaceName(cctx);

        swapMgr = cctx.gridSwap();
        offheap = cctx.offheap();

        swapEnabled = enabled && cctx.config().isSwapEnabled() && cctx.kernalContext().swap().enabled();
        offheapEnabled = enabled && cctx.config().getOffHeapMaxMemory() >= 0 &&
            (cctx.config().getMemoryMode() == ONHEAP_TIERED || cctx.config().getMemoryMode() == OFFHEAP_TIERED);

        if (offheapEnabled)
            initOffHeap();
    }

    /**
     * Initializes off-heap space.
     */
    private void initOffHeap() {
        // Register big data usage.
        GridLicenseUseRegistry.onUsage(DATA_GRID, GridOffHeapMapFactory.class);

        long max = cctx.config().getOffHeapMaxMemory();

        long init = max > 0 ? max / 1024 : 8L * 1024L * 1024L;

        int parts = cctx.config().getAffinity().partitions();

        GridOffHeapEvictListener lsnr = !swapEnabled && !offheapEnabled ? null : new GridOffHeapEvictListener() {
            private volatile boolean firstEvictWarn;

            @Override public void onEvict(int part, int hash, byte[] kb, byte[] vb) {
                try {
                    if (!firstEvictWarn)
                        warnFirstEvict();

                    writeToSwap(part, null, kb, vb);
                }
                catch (GridException e) {
                    log.error("Failed to unmarshal off-heap entry [part=" + part + ", hash=" + hash + ']', e);
                }
            }

            private void warnFirstEvict() {
                synchronized (this) {
                    if (firstEvictWarn)
                        return;

                    firstEvictWarn = true;
                }

                U.warn(log, "Off-heap evictions started. You may wish to increase 'offHeapMaxMemory' in " +
                    "cache configuration [cache=" + cctx.name() +
                    ", offHeapMaxMemory=" + cctx.config().getOffHeapMaxMemory() + ']',
                    "Off-heap evictions started: " + cctx.name());
            }
        };

        offheap.create(spaceName, parts, init, max, lsnr);
    }

    /**
     * @return {@code True} if swap store is enabled.
     */
    public boolean swapEnabled() {
        return swapEnabled;
    }

    /**
     * @return {@code True} if off-heap cache is enabled.
     */
    public boolean offHeapEnabled() {
        return offheapEnabled;
    }

    /**
     * @return Swap size.
     * @throws GridException If failed.
     */
    public long swapSize() throws GridException {
        return enabled ? swapMgr.swapSize(spaceName) : -1;
    }

    /**
     * Gets number of swap entries (keys).
     *
     * @return Swap keys count.
     * @throws GridException If failed.
     */
    public long swapKeys() throws GridException {
        return enabled ? swapMgr.swapKeys(spaceName) : -1;
    }

    /**
     * @param part Partition.
     * @param key Cache key.
     * @param keyBytes Key bytes.
     * @param e Entry.
     */
    private void onUnswapped(int part, K key, byte[] keyBytes, GridCacheSwapEntry<V> e) {
        onEntryUnswapped(swapLsnrs, part, key, keyBytes, e);
    }

    /**
     * @param part Partition.
     * @param key Cache key.
     * @param keyBytes Key bytes.
     * @param e Entry.
     */
    private void onOffHeaped(int part, K key, byte[] keyBytes, GridCacheSwapEntry<V> e) {
        onEntryUnswapped(offheapLsnrs, part, key, keyBytes, e);
    }

    /**
     * @param map Listeners.
     * @param part Partition.
     * @param key Cache key.
     * @param keyBytes Key bytes.
     * @param e Entry.
     */
    private void onEntryUnswapped(ConcurrentMap<Integer, Collection<GridCacheSwapListener<K, V>>> map,
        int part, K key, byte[] keyBytes, GridCacheSwapEntry<V> e) {
        Collection<GridCacheSwapListener<K, V>> lsnrs = map.get(part);

        if (lsnrs == null) {
            if (log.isDebugEnabled())
                log.debug("Skipping unswapped notification [key=" + key + ", part=" + part + ']');

            return;
        }

        for (GridCacheSwapListener<K, V> lsnr : lsnrs)
            lsnr.onEntryUnswapped(part, key, keyBytes, e);
    }

    /**
     * @param part Partition.
     * @param lsnr Listener.
     */
    public void addSwapListener(int part, GridCacheSwapListener<K, V> lsnr) {
        addListener(part, swapLsnrs, lsnr);
    }

    /**
     * @param part Partition.
     * @param lsnr Listener.
     */
    public void removeSwapListener(int part, GridCacheSwapListener<K, V> lsnr) {
        removeListener(part, swapLsnrs, lsnr);
    }

    /**
     * @param part Partition.
     * @param lsnr Listener.
     */
    public void addOffHeapListener(int part, GridCacheSwapListener<K, V> lsnr) {
        addListener(part, offheapLsnrs, lsnr);
    }

    /**
     * @param part Partition.
     * @param lsnr Listener.
     */
    public void removeOffHeapListener(int part, GridCacheSwapListener<K, V> lsnr) {
        removeListener(part, offheapLsnrs, lsnr);
    }

    /**
     * @param part Partition.
     * @param map Listeners.
     * @param lsnr Listener.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void addListener(int part, ConcurrentMap<Integer, Collection<GridCacheSwapListener<K, V>>> map,
        GridCacheSwapListener<K, V> lsnr) {
        Collection<GridCacheSwapListener<K, V>> lsnrs = map.get(part);

        while (true) {
            if (lsnrs != null) {
                synchronized (lsnrs) {
                    if (!lsnrs.isEmpty()) {
                        lsnrs.add(lsnr);

                        break;
                    }
                }

                lsnrs = swapLsnrs.remove(part, lsnrs) ? null : swapLsnrs.get(part);
            }
            else {
                lsnrs = new GridConcurrentHashSet<GridCacheSwapListener<K, V>>() {
                    @Override public boolean equals(Object o) {
                        return o == this;
                    }
                };

                lsnrs.add(lsnr);

                Collection<GridCacheSwapListener<K, V>> old = swapLsnrs.putIfAbsent(part, lsnrs);

                if (old == null)
                    break;
                else
                    lsnrs = old;
            }
        }
    }

    /**
     * @param part Partition.
     * @param map Listeners.
     * @param lsnr Listener.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void removeListener(int part, ConcurrentMap<Integer, Collection<GridCacheSwapListener<K, V>>> map,
        GridCacheSwapListener<K, V> lsnr) {
        Collection<GridCacheSwapListener<K, V>> lsnrs = map.get(part);

        if (lsnrs != null) {
            boolean empty;

            synchronized (lsnrs) {
                lsnrs.remove(lsnr);

                empty = lsnrs.isEmpty();
            }

            if (empty)
                map.remove(part, lsnrs);
        }
    }

    /**
     * Checks iterator queue.
     */
    @SuppressWarnings("RedundantCast")
    private void checkIteratorQueue() {
        GridWeakIterator<Map.Entry<K, V>> it;

        do {
            // NOTE: don't remove redundant cast - otherwise build fails.
            it = (GridWeakIterator<Map.Entry<K,V>>)(Reference<Iterator<Map.Entry<K, V>>>)itQ.poll();

            try {
                if (it != null)
                    it.close();
            }
            catch (GridException e) {
                log.error("Failed to close iterator.", e);
            }
            finally {
                if (it != null)
                    itSet.remove(it);
            }
        }
        while (it != null);
    }

    /**
     * @param e Swap entry to reconstitute.
     * @return Reconstituted swap entry or {@code null} if entry is obsolete.
     * @throws GridException If failed.
     */
    @Nullable private GridCacheSwapEntry<V> swapEntry(GridCacheSwapEntry<V> e) throws GridException {
        return swapEntry(e, true);
    }

    /**
     * Recreates raw swap entry (that just has been received from swap storage).
     *
     * @param e Swap entry to reconstitute.
     * @param unmarshal If {@code true} then value is unmarshalled.
     * @return Reconstituted swap entry or {@code null} if entry is obsolete.
     * @throws GridException If failed.
     */
    @Nullable private GridCacheSwapEntry<V> swapEntry(GridCacheSwapEntry<V> e, boolean unmarshal) throws GridException {
        assert e != null;

        checkIteratorQueue();

        if (e.valueIsByteArray())
            e.value((V)e.valueBytes());
        else if (unmarshal) {
            V val;

            if (cctx.portableEnabled() && cctx.offheapTiered())
                val = (V)cctx.portable().unmarshal(e.valueBytes(), 0);
            else {
                ClassLoader ldr = e.valueClassLoaderId() != null ? cctx.deploy().getClassLoader(e.valueClassLoaderId()) :
                    cctx.deploy().localLoader();

                if (ldr == null)
                    return null;

                val = cctx.marshaller().unmarshal(e.valueBytes(), ldr);
            }

            e.value(val);
        }

        return e;
    }

    /**
     * @param key Key to check.
     * @param keyBytes Key bytes to check.
     * @return {@code True} if key is contained.
     * @throws GridException If failed.
     */
    public boolean containsKey(K key, byte[] keyBytes) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return false;

        checkIteratorQueue();

        int part = cctx.affinity().partition(key);

        // First check off-heap store.
        if (offheapEnabled)
            if (offheap.contains(spaceName, part, key, keyBytes))
                return true;

        if (swapEnabled) {
            assert key != null;

            byte[] valBytes = swapMgr.read(spaceName, new GridSwapKey(key, part, keyBytes),
                cctx.deploy().globalLoader());

            return valBytes != null;
        }

        return false;
    }

    /**
     * @param key Key to read.
     * @param keyBytes Key bytes.
     * @param entryLocked {@code True} if cache entry is locked.
     * @return Value from swap or {@code null}.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable GridCacheSwapEntry<V> read(K key, byte[] keyBytes, boolean entryLocked) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return null;

        checkIteratorQueue();

        int part = cctx.affinity().partition(key);

        KeySwapListener<K, V> lsnr = null;

        try {
            if (offheapEnabled && swapEnabled && !entryLocked) {
                lsnr = new KeySwapListener(key);

                addSwapListener(part, lsnr);
            }

            // First check off-heap store.
            if (offheapEnabled) {
                byte[] bytes = offheap.get(spaceName, part, key, keyBytes);

                if (bytes != null)
                    return swapEntry(unmarshalSwapEntry(bytes));
            }

            if (!swapEnabled)
                return null;

            assert key != null;

            byte[] bytes = swapMgr.read(spaceName, new GridSwapKey(key, part, keyBytes), cctx.deploy().globalLoader());

            if (bytes == null && lsnr != null)
                return lsnr.entry;

            return bytes != null ? swapEntry(unmarshalSwapEntry(bytes)) : null;
        }
        finally {
            if (lsnr != null)
                removeSwapListener(part, lsnr);
        }
    }

    /**
     * @param key Key to remove.
     * @param keyBytes Key bytes.
     * @return Value from swap or {@code null}.
     * @throws GridException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable GridCacheSwapEntry<V> readAndRemove(final K key, final byte[] keyBytes) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return null;

        checkIteratorQueue();

        final int part = cctx.affinity().partition(key);

        // First try removing from offheap.
        if (offheapEnabled) {
            byte[] entryBytes = offheap.remove(spaceName, part, key, keyBytes);

            if (entryBytes != null) {
                GridCacheSwapEntry<V> entry = swapEntry(unmarshalSwapEntry(entryBytes));

                if (entry == null)
                    return null;

                // Always fire this event, since preloading depends on it.
                onOffHeaped(part, key, keyBytes, entry);

                if (cctx.events().isRecordable(EVT_CACHE_OBJECT_FROM_OFFHEAP))
                    cctx.events().addEvent(
                        part,
                        key,
                        cctx.nodeId(),
                        (IgniteUuid)null,
                        null,
                        EVT_CACHE_OBJECT_FROM_OFFHEAP,
                        null,
                        false,
                        null,
                        true,
                        null,
                        null,
                        null);

                GridCacheQueryManager<K, V> qryMgr = cctx.queries();

                if (qryMgr != null)
                    qryMgr.onUnswap(key, entry.value(), entry.valueBytes());

                return entry;
            }
        }

        return readAndRemoveSwap(key, part, keyBytes);
    }

    /**
     * @param key Key.
     * @param part Partition.
     * @param keyBytes Key bytes.
     * @return Value from swap or {@code null}.
     * @throws GridException If failed.
     */
    @Nullable private GridCacheSwapEntry<V> readAndRemoveSwap(final K key, final int part, final byte[] keyBytes)
        throws GridException {
        if (!swapEnabled)
            return null;

        final GridTuple<GridCacheSwapEntry<V>> t = F.t1();
        final GridTuple<GridException> err = F.t1();

        swapMgr.remove(spaceName, new GridSwapKey(key, part, keyBytes), new CI1<byte[]>() {
            @Override public void apply(byte[] rmv) {
                if (rmv != null) {
                    try {
                        GridCacheSwapEntry<V> entry = swapEntry(unmarshalSwapEntry(rmv));

                        if (entry == null)
                            return;

                        t.set(entry);

                        V v = entry.value();
                        byte[] valBytes = entry.valueBytes();

                        // Event notification.
                        if (cctx.events().isRecordable(EVT_CACHE_OBJECT_UNSWAPPED))
                            cctx.events().addEvent(part, key, cctx.nodeId(), (IgniteUuid)null, null,
                                EVT_CACHE_OBJECT_UNSWAPPED, null, false, v, true, null, null, null);

                        // Always fire this event, since preloading depends on it.
                        onUnswapped(part, key, keyBytes, entry);

                        GridCacheQueryManager<K, V> qryMgr = cctx.queries();

                        if (qryMgr != null)
                            qryMgr.onUnswap(key, v, valBytes);
                    }
                    catch (GridException e) {
                        err.set(e);
                    }
                }
            }
        }, cctx.deploy().globalLoader());

        if (err.get() != null)
            throw err.get();

        return t.get();
    }

    /**
     * @param entry Entry to read.
     * @param locked {@code True} if cache entry is locked.
     * @return Read value.
     * @throws GridException If read failed.
     */
    @Nullable GridCacheSwapEntry<V> read(GridCacheMapEntry<K, V> entry, boolean locked) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return null;

        return read(entry.key(), entry.getOrMarshalKeyBytes(), locked);
    }

    /**
     * @param entry Entry to read.
     * @return Read value address.
     * @throws GridException If read failed.
     */
    @Nullable GridCacheSwapEntry<V> readOffheapPointer(GridCacheMapEntry<K, V> entry) throws GridException {
        if (!offheapEnabled)
            return null;

        K key = entry.key();

        int part = cctx.affinity().partition(key);

        byte[] keyBytes = entry.getOrMarshalKeyBytes();

        IgniteBiTuple<Long, Integer> ptr = offheap.valuePointer(spaceName, part, key, keyBytes);

        if (ptr != null) {
            assert ptr.get1() != null;
            assert ptr.get2() != null;

            return new GridCacheOffheapSwapEntry<>(ptr.get1(), ptr.get2());
        }

        return readAndRemoveSwap(key, part, keyBytes);
    }

    /**
     * @param key Key to read swap entry for.
     * @return Read value.
     * @throws GridException If read failed.
     */
    @Nullable public GridCacheSwapEntry<V> read(K key) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return null;

        return read(key, CU.marshal(cctx.shared(), key), false);
    }

    /**
     * @param entry Entry to read.
     * @return Read value.
     * @throws GridException If read failed.
     */
    @Nullable GridCacheSwapEntry<V> readAndRemove(GridCacheMapEntry<K, V> entry) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return null;

        return readAndRemove(entry.key(), entry.getOrMarshalKeyBytes());
    }

    /**
     * @param keys Collection of keys to remove from swap.
     * @return Collection of swap entries.
     * @throws GridException If failed,
     */
    public Collection<GridCacheBatchSwapEntry<K, V>> readAndRemove(Collection<? extends K> keys) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return Collections.emptyList();

        checkIteratorQueue();

        final GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        ArrayList<K> keysList = new ArrayList<>(keys);
        final Collection<GridCacheBatchSwapEntry<K, V>> res = new ArrayList<>(keys.size());

        // First try removing from offheap.
        if (offheapEnabled) {
            Iterator<K> iter = keysList.iterator();

            while (iter.hasNext()) {
                K key = iter.next();

                int part = cctx.affinity().partition(key);

                byte[] keyBytes = CU.marshal(cctx.shared(), key);

                byte[] entryBytes = offheap.remove(spaceName, part, key, keyBytes);

                if (entryBytes != null) {
                    GridCacheSwapEntry<V> entry = swapEntry(unmarshalSwapEntry(entryBytes));

                    if (entry == null)
                        continue;

                    iter.remove();

                    // Always fire this event, since preloading depends on it.
                    onOffHeaped(part, key, keyBytes, entry);

                    if (cctx.events().isRecordable(EVT_CACHE_OBJECT_FROM_OFFHEAP))
                        cctx.events().addEvent(part, key, cctx.nodeId(), (IgniteUuid)null, null,
                            EVT_CACHE_OBJECT_FROM_OFFHEAP, null, false, null, true, null, null, null);

                    if (qryMgr != null)
                        qryMgr.onUnswap(key, entry.value(), entry.valueBytes());

                    GridCacheBatchSwapEntry<K, V> unswapped = new GridCacheBatchSwapEntry<>(key,
                        keyBytes,
                        part,
                        ByteBuffer.wrap(entry.valueBytes()),
                        entry.valueIsByteArray(),
                        entry.version(), entry.ttl(),
                        entry.expireTime(),
                        entry.keyClassLoaderId(),
                        entry.valueClassLoaderId());

                    unswapped.value(entry.value());

                    res.add(unswapped);
                }
            }

            if (!swapEnabled || keysList.isEmpty())
                return res;
        }

        // Swap is enabled.
        final GridTuple<GridException> err = F.t1();

        Collection<GridSwapKey> converted = new ArrayList<>(F.viewReadOnly(keysList, new C1<K, GridSwapKey>() {
            @Override public GridSwapKey apply(K key) {
                try {
                    return new GridSwapKey(key, cctx.affinity().partition(key), CU.marshal(cctx.shared(), key));
                }
                catch (GridException e) {
                    throw new GridRuntimeException(e);
                }
            }
        }));

        swapMgr.removeAll(spaceName,
            converted,
            new IgniteBiInClosure<GridSwapKey, byte[]>() {
                @Override public void apply(GridSwapKey swapKey, byte[] rmv) {
                    if (rmv != null) {
                        try {
                            GridCacheSwapEntry<V> entry = swapEntry(unmarshalSwapEntry(rmv));

                            if (entry == null)
                                return;

                            K key = (K)swapKey.key();

                            GridCacheBatchSwapEntry<K, V> unswapped = new GridCacheBatchSwapEntry<>(key,
                                swapKey.keyBytes(),
                                swapKey.partition(),
                                ByteBuffer.wrap(entry.valueBytes()),
                                entry.valueIsByteArray(),
                                entry.version(),
                                entry.ttl(),
                                entry.expireTime(),
                                entry.keyClassLoaderId(),
                                entry.valueClassLoaderId());

                            unswapped.value(entry.value());

                            res.add(unswapped);

                            // Event notification.
                            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_UNSWAPPED))
                                cctx.events().addEvent(swapKey.partition(), key, cctx.nodeId(),
                                    (IgniteUuid)null, null, EVT_CACHE_OBJECT_UNSWAPPED, null, false, entry.value(),
                                    true, null, null, null);

                            // Always fire this event, since preloading depends on it.
                            onUnswapped(swapKey.partition(), key, swapKey.keyBytes(), entry);

                            if (qryMgr != null)
                                qryMgr.onUnswap(key, entry.value(), entry.valueBytes());
                        }
                        catch (GridException e) {
                            err.set(e);
                        }
                    }
                }
            },
            cctx.deploy().globalLoader());

        if (err.get() != null)
            throw err.get();

        return res;
    }

    /**
     * @param key Key to read swap entry for.
     * @return Read value.
     * @throws GridException If read failed.
     */
    @Nullable GridCacheSwapEntry<V> readAndRemove(K key) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return null;

        return readAndRemove(key, CU.marshal(cctx.shared(), key));
    }

    /**
     * @param key Key to remove.
     * @param keyBytes Key bytes.
     * @return {@code True} If succeeded.
     * @throws GridException If failed.
     */
    boolean removeOffheap(final K key, byte[] keyBytes) throws GridException {
        assert offheapEnabled;

        checkIteratorQueue();

        int part = cctx.affinity().partition(key);

        return offheap.removex(spaceName, part, key, keyBytes);
    }

    /**
     * @return {@code True} if offheap eviction is enabled.
     */
    boolean offheapEvictionEnabled() {
        return offheapEnabled && cctx.config().getOffHeapMaxMemory() > 0;
    }

    /**
     * Enables eviction for offheap entry after {@link #readOffheapPointer} was called.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @throws GridException If failed.
     */
    void enableOffheapEviction(final K key, byte[] keyBytes) throws GridException {
        if (!offheapEnabled)
            return;

        checkIteratorQueue();

        int part = cctx.affinity().partition(key);

        offheap.enableEviction(spaceName, part, key, keyBytes);
    }

    /**
     * @param key Key to remove.
     * @param keyBytes Key bytes.
     * @throws GridException If failed.
     */
    void remove(final K key, byte[] keyBytes) throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return;

        checkIteratorQueue();

        final GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        CI1<byte[]> c = qryMgr == null ? null : new CI1<byte[]>() {
            @Override public void apply(byte[] rmv) {
                if (rmv == null)
                    return;

                try {
                    GridCacheSwapEntry<V> entry = swapEntry(unmarshalSwapEntry(rmv));

                    if (entry == null)
                        return;

                    qryMgr.onUnswap(key, entry.value(), entry.valueBytes());
                }
                catch (GridException e) {
                    throw new GridRuntimeException(e);
                }
            }
        };

        int part = cctx.affinity().partition(key);

        // First try offheap.
        if (offheapEnabled) {
            byte[] val = offheap.remove(spaceName, part, key, keyBytes);

            if (val != null) {
                if (c != null)
                    c.apply(val); // Probably we should read value and apply closure before removing...

                return;
            }
        }

        if (swapEnabled)
            swapMgr.remove(spaceName, new GridSwapKey(key, part, keyBytes), c,
                cctx.deploy().globalLoader());
    }

    /**
     * Writes a versioned value to swap.
     *
     * @param key Key.
     * @param keyBytes Key bytes.
     * @param val Value.
     * @param valIsByteArr Whether value is byte array.
     * @param ver Version.
     * @param ttl Entry time to live.
     * @param expireTime Swap entry expiration time.
     * @param keyClsLdrId Class loader ID for entry key.
     * @param valClsLdrId Class loader ID for entry value.
     * @throws GridException If failed.
     */
    void write(K key,
        byte[] keyBytes,
        ByteBuffer val,
        boolean valIsByteArr,
        GridCacheVersion ver,
        long ttl,
        long expireTime,
        @Nullable IgniteUuid keyClsLdrId,
        @Nullable IgniteUuid valClsLdrId)
        throws GridException {
        if (!offheapEnabled && !swapEnabled)
            return;

        checkIteratorQueue();

        int part = cctx.affinity().partition(key);

        GridCacheSwapEntryImpl<V> entry = new GridCacheSwapEntryImpl<>(val, valIsByteArr, ver, ttl, expireTime,
            keyClsLdrId, valClsLdrId);

        if (offheapEnabled) {
            offheap.put(spaceName, part, key, keyBytes, entry.marshal());

            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_TO_OFFHEAP))
                cctx.events().addEvent(part, key, cctx.nodeId(), (IgniteUuid)null, null,
                    EVT_CACHE_OBJECT_TO_OFFHEAP, null, false, null, true, null, null, null);
        }
        else if (swapEnabled)
            writeToSwap(part, key, keyBytes, entry.marshal());

        GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        if (qryMgr != null)
            qryMgr.onSwap(spaceName, key);
    }

    /**
     * Performs batch write of swapped entries.
     *
     * @param swapped Collection of swapped entries.
     * @throws GridException If failed.
     */
    void writeAll(Iterable<GridCacheBatchSwapEntry<K, V>> swapped) throws GridException {
        assert offheapEnabled || swapEnabled;

        checkIteratorQueue();

        GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        if (offheapEnabled) {
            for (GridCacheBatchSwapEntry<K, V> swapEntry : swapped) {
                offheap.put(spaceName,
                    swapEntry.partition(),
                    swapEntry.key(),
                    swapEntry.keyBytes(),
                    swapEntry.marshal());

                if (cctx.events().isRecordable(EVT_CACHE_OBJECT_TO_OFFHEAP))
                    cctx.events().addEvent(swapEntry.partition(), swapEntry.key(), cctx.nodeId(),
                        (IgniteUuid)null, null, EVT_CACHE_OBJECT_TO_OFFHEAP, null, false, null, true, null, null, null);

                if (qryMgr != null)
                    qryMgr.onSwap(spaceName, swapEntry.key());
            }
        }
        else {
            Map<GridSwapKey, byte[]> batch = new LinkedHashMap<>();

            for (GridCacheBatchSwapEntry entry : swapped)
                batch.put(new GridSwapKey(entry.key(), entry.partition(), entry.keyBytes()), entry.marshal());

            swapMgr.writeAll(spaceName, batch, cctx.deploy().globalLoader());

            if (cctx.events().isRecordable(EVT_CACHE_OBJECT_SWAPPED)) {
                for (GridCacheBatchSwapEntry<K, V> batchSwapEntry : swapped) {
                    cctx.events().addEvent(batchSwapEntry.partition(), batchSwapEntry.key(), cctx.nodeId(),
                        (IgniteUuid)null, null, EVT_CACHE_OBJECT_SWAPPED, null, false, null, true, null, null, null);

                    if (qryMgr != null)
                        qryMgr.onSwap(spaceName, batchSwapEntry.key());
                }
            }
        }
    }

    /**
     * Writes given bytes to swap.
     *
     * @param part Partition.
     * @param key Key. If {@code null} then it will be deserialized from {@code keyBytes}.
     * @param keyBytes Key bytes.
     * @param entry Entry bytes.
     * @throws GridException If failed.
     */
    private void writeToSwap(int part, @Nullable K key, byte[] keyBytes, byte[] entry) throws GridException{
        checkIteratorQueue();

        if (key == null)
            key = unmarshalKey(keyBytes, cctx.deploy().globalLoader());

        swapMgr.write(spaceName, new GridSwapKey(key, part, keyBytes), entry, cctx.deploy().globalLoader());

        if (cctx.events().isRecordable(EVT_CACHE_OBJECT_SWAPPED))
            cctx.events().addEvent(part, key, cctx.nodeId(), (IgniteUuid)null, null,
                EVT_CACHE_OBJECT_SWAPPED, null, false, null, true, null, null, null);
    }

    /**
     * Clears off-heap.
     */
    public void clearOffHeap() {
        if (offheapEnabled)
            initOffHeap();
    }

    /**
     * Clears swap.
     *
     * @throws GridException If failed.
     */
    public void clearSwap() throws GridException {
        if (swapEnabled)
            swapMgr.clear(spaceName);
    }

    /**
     * Gets offheap and swap iterator over partition.
     *
     * @param part Partition to iterate over.
     * @param unmarshal Unmarshal value flag.
     * @return Iterator over partition.
     * @throws GridException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> iterator(
        final int part,
        final boolean unmarshal)
        throws GridException {
        if (!swapEnabled() && !offHeapEnabled())
            return null;

        checkIteratorQueue();

        if (offHeapEnabled() && !swapEnabled())
            return offHeapIterator(part, unmarshal);

        if (swapEnabled() && !offHeapEnabled())
            return swapIterator(part, unmarshal);

        // Both, swap and off-heap are enabled.
        return new GridCloseableIteratorAdapter<Map.Entry<byte[], GridCacheSwapEntry<V>>>() {
            private GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> it;

            private boolean offheap = true;

            private boolean done;

            {
                it = offHeapIterator(part, unmarshal);

                advance();
            }

            private void advance() throws GridException {
                if (it.hasNext())
                    return;

                it.close();

                if (offheap) {
                    offheap = false;

                    it = swapIterator(part, unmarshal);

                    assert it != null;

                    if (!it.hasNext()) {
                        it.close();

                        done = true;
                    }
                }
                else
                    done = true;
            }

            @Override protected Map.Entry<byte[], GridCacheSwapEntry<V>> onNext() throws GridException {
                if (done)
                    throw new NoSuchElementException();

                Map.Entry<byte[], GridCacheSwapEntry<V>> e = it.next();

                advance();

                return e;
            }

            @Override protected boolean onHasNext() {
                return !done;
            }

            @Override protected void onRemove() {
                throw new UnsupportedOperationException();
            }

            @Override protected void onClose() throws GridException {
                if (it != null)
                    it.close();
            }
        };
    }

    /**
     * Gets offheap and swap iterator over partition.
     *
     * @return Iterator over partition.
     * @throws GridException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator()
        throws GridException {
        if (!swapEnabled() && !offHeapEnabled())
            return new GridEmptyCloseableIterator<>();

        checkIteratorQueue();

        if (offHeapEnabled() && !swapEnabled())
            return rawOffHeapIterator();

        if (swapEnabled() && !offHeapEnabled())
            return rawSwapIterator();

        // Both, swap and off-heap are enabled.
        return new GridCloseableIteratorAdapter<Map.Entry<byte[], byte[]>>() {
            private GridCloseableIterator<Map.Entry<byte[], byte[]>> it;

            private boolean offheapFlag = true;

            private boolean done;

            private Map.Entry<byte[], byte[]> cur;

            {
                it = rawOffHeapIterator();

                advance();
            }

            private void advance() throws GridException {
                if (it.hasNext())
                    return;

                it.close();

                if (offheapFlag) {
                    offheapFlag = false;

                    it = rawSwapIterator();

                    if (!it.hasNext()) {
                        it.close();

                        done = true;
                    }
                }
                else
                    done = true;
            }

            @Override protected Map.Entry<byte[], byte[]> onNext() throws GridException {
                if (done)
                    throw new NoSuchElementException();

                cur = it.next();

                advance();

                return cur;
            }

            @Override protected boolean onHasNext() {
                return !done;
            }

            @Override protected void onRemove() throws GridException {
                if (offheapFlag) {
                    K key = unmarshalKey(cur.getKey(), cctx.deploy().globalLoader());

                    int part = cctx.affinity().partition(key);

                    offheap.removex(spaceName, part, key, cur.getKey());
                }
                else
                    it.removeX();
            }

            @Override protected void onClose() throws GridException {
                if (it != null)
                    it.close();
            }
        };
    }

    /**
     * @return Lazy swap iterator.
     * @throws GridException If failed.
     */
    public Iterator<Map.Entry<K, V>> lazySwapIterator() throws GridException {
        if (!swapEnabled)
            return new GridEmptyIterator<>();

        return lazyIterator(cctx.gridSwap().rawIterator(spaceName));
    }

    /**
     * @return Lazy off-heap iterator.
     */
    public Iterator<Map.Entry<K, V>> lazyOffHeapIterator() {
        if (!offheapEnabled)
            return new GridEmptyCloseableIterator<>();

        return lazyIterator(offheap.iterator(spaceName));
    }

    /**
     * Gets number of elements in off-heap
     *
     * @return Number of elements or {@code 0} if off-heap is disabled.
     */
    public long offHeapEntriesCount() {
        return offheapEnabled ? offheap.entriesCount(spaceName) : 0;
    }

    /**
     * Gets memory size allocated in off-heap.
     *
     * @return Allocated memory size or {@code 0} if off-heap is disabled.
     */
    public long offHeapAllocatedSize() {
        return offheapEnabled ? offheap.allocatedSize(spaceName) : 0;
    }

    /**
     * Gets lazy iterator for which key and value are lazily deserialized.
     *
     * @param it Closeable iterator.
     * @return Lazy iterator.
     */
    private Iterator<Map.Entry<K, V>> lazyIterator(
        final GridCloseableIterator<? extends Map.Entry<byte[], byte[]>> it) {
        if (it == null)
            return new GridEmptyIterator<>();

        checkIteratorQueue();

        // Weak reference will hold hard reference to this iterator, so it can properly be closed.
        final GridCloseableIteratorAdapter<Map.Entry<K, V>> iter = new GridCloseableIteratorAdapter<Map.Entry<K, V>>() {
            private Map.Entry<K, V> cur;

            @Override protected Map.Entry<K, V> onNext() {
                final Map.Entry<byte[], byte[]> cur0 = it.next();

                cur = new Map.Entry<K, V>() {
                    @Override public K getKey() {
                        try {
                            return unmarshalKey(cur0.getKey(), cctx.deploy().globalLoader());
                        }
                        catch (GridException e) {
                            throw new GridRuntimeException(e);
                        }
                    }

                    @Override public V getValue() {
                        try {
                            GridCacheSwapEntry<V> e = unmarshalSwapEntry(cur0.getValue());

                            swapEntry(e);

                            return e.value();
                        }
                        catch (GridException ex) {
                            throw new GridRuntimeException(ex);
                        }
                    }

                    @Override public V setValue(V val) {
                        throw new UnsupportedOperationException();
                    }
                };

                return cur;
            }

            @Override protected boolean onHasNext() {
                return it.hasNext();
            }

            @Override protected void onRemove() throws GridException {
                if (cur == null)
                    throw new IllegalStateException("Method next() has not yet been called, or the remove() method " +
                        "has already been called after the last call to the next() method.");

                try {
                    if (cctx.isDht())
                        cctx.dht().near().removex(cur.getKey(), CU.<K, V>empty());
                    else
                        cctx.cache().removex(cur.getKey(), CU.<K, V>empty());
                }
                finally {
                    cur = null;
                }
            }

            @Override protected void onClose() throws GridException {
                it.close();
            }
        };

        // Don't hold hard reference to this iterator - only weak one.
        Iterator<Map.Entry<K, V>> ret = new Iterator<Map.Entry<K, V>>() {
            @Override public boolean hasNext() {
                return iter.hasNext();
            }

            @Override public Map.Entry<K, V> next() {
                return iter.next();
            }

            @Override public void remove() {
                iter.remove();
            }
        };

        itSet.add(new GridWeakIterator<>(ret, iter, itQ));

        return ret;
    }

    /**
     * Gets offheap iterator over partition.
     *
     * @param part Partition to iterate over.
     * @param unmarshal Unmarshal value flag.
     * @return Iterator over partition.
     * @throws GridException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> offHeapIterator(
        int part,
        boolean unmarshal)
        throws GridException {
        if (!offheapEnabled)
            return null;

        checkIteratorQueue();

        return new IteratorWrapper(offheap.iterator(spaceName, part), unmarshal);
    }

    /**
     * @param c Key/value closure.
     * @return Off-heap iterator.
     */
    public <T> GridCloseableIterator<T> rawOffHeapIterator(CX2<T2<Long, Integer>, T2<Long, Integer>, T> c) {
        assert c != null;

        if (!offheapEnabled)
            return new GridEmptyCloseableIterator<>();

        checkIteratorQueue();

        return offheap.iterator(spaceName, c);
    }

    /**
     * @return Raw off-heap iterator.
     */
    public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawOffHeapIterator() {
        if (!offheapEnabled)
            return new GridEmptyCloseableIterator<>();

        return new GridCloseableIteratorAdapter<Map.Entry<byte[], byte[]>>() {
            private GridCloseableIterator<IgniteBiTuple<byte[], byte[]>> it = offheap.iterator(spaceName);

            private Map.Entry<byte[], byte[]> cur;

            @Override protected Map.Entry<byte[], byte[]> onNext() {
                return cur = it.next();
            }

            @Override protected boolean onHasNext() {
                return it.hasNext();
            }

            @Override protected void onRemove() throws GridException {
                K key = unmarshalKey(cur.getKey(), cctx.deploy().globalLoader());

                int part = cctx.affinity().partition(key);

                offheap.removex(spaceName, part, key, cur.getKey());
            }

            @Override protected void onClose() throws GridException {
                it.close();
            }
        };
    }

    /**
     * Gets swap space iterator over partition.
     *
     * @param part Partition to iterate over.
     * @param unmarshal Unmarshal value flag.
     * @return Iterator over partition.
     * @throws GridException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> swapIterator(
        int part,
        boolean unmarshal)
        throws GridException {
        if (!swapEnabled)
            return null;

        checkIteratorQueue();

        return new IteratorWrapper(swapMgr.rawIterator(spaceName, part), unmarshal);
    }

    /**
     * @return Raw off-heap iterator.
     * @throws GridException If failed.
     */
    public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawSwapIterator() throws GridException {
        if (!swapEnabled)
            return new GridEmptyCloseableIterator<>();

        checkIteratorQueue();

        return swapMgr.rawIterator(spaceName);
    }

    /**
     * @param leftNodeId Left Node ID.
     * @param ldr Undeployed class loader.
     * @return Undeploy count.
     */
    public int onUndeploy(UUID leftNodeId, ClassLoader ldr) {
        if (cctx.portableEnabled())
            return 0;

        IgniteUuid ldrId = cctx.deploy().getClassLoaderId(ldr);

        assert ldrId != null;

        checkIteratorQueue();

        try {
            GridCloseableIterator<Map.Entry<byte[], byte[]>> iter = rawIterator();

            if (iter != null) {
                int undeployCnt = 0;

                try {
                    for (Map.Entry<byte[], byte[]> e : iter) {
                        try {
                            GridCacheSwapEntry<V> swapEntry = unmarshalSwapEntry(e.getValue());

                            IgniteUuid valLdrId = swapEntry.valueClassLoaderId();

                            if (ldrId.equals(swapEntry.keyClassLoaderId())) {
                                iter.removeX();

                                undeployCnt++;
                            }
                            else {
                                if (valLdrId == null && swapEntry.value() == null && !swapEntry.valueIsByteArray()) {
                                    // We need value here only for classloading purposes.
                                    V val =  cctx.marshaller().unmarshal(swapEntry.valueBytes(),
                                        cctx.deploy().globalLoader());

                                    if (val != null)
                                        valLdrId = cctx.deploy().getClassLoaderId(val.getClass().getClassLoader());
                                }

                                if (ldrId.equals(valLdrId)) {
                                    iter.removeX();

                                    undeployCnt++;
                                }
                            }
                        }
                        catch (GridException ex) {
                            U.error(log, "Failed to process swap entry.", ex);
                        }
                    }
                }
                finally {
                    iter.close();
                }

                return undeployCnt;
            }
        }
        catch (GridException e) {
            U.error(log, "Failed to clear cache swap space on undeploy.", e);
        }

        return 0;
    }

    /**
     * @return Swap space name.
     */
    public String spaceName() {
        return spaceName;
    }

    /**
     * @param bytes Bytes to unmarshal.
     * @return Unmarshalled entry.
     */
    private GridCacheSwapEntry<V> unmarshalSwapEntry(byte[] bytes) {
        return GridCacheSwapEntryImpl.unmarshal(bytes);
    }

    /**
     * @param bytes Bytes to unmarshal.
     * @param ldr Class loader.
     * @return Unmarshalled value.
     * @throws GridException If unmarshal failed.
     */
    private <T> T unmarshalKey(byte[] bytes, ClassLoader ldr) throws GridException {
        return (T)cctx.marshaller().unmarshal(bytes, ldr);
    }

    /**
     * @return Size of internal weak iterator set.
     */
    int iteratorSetSize() {
        return itSet.size();
    }

    /**
     *
     */
    private class IteratorWrapper extends GridCloseableIteratorAdapter<Map.Entry<byte[], GridCacheSwapEntry<V>>> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final GridCloseableIterator<? extends Map.Entry<byte[], byte[]>> iter;

        /** */
        private final boolean unmarshal;

        /**
         * @param iter Iterator.
         * @param unmarshal Unmarshal value flag.
         */
        private IteratorWrapper(GridCloseableIterator<? extends Map.Entry<byte[], byte[]>> iter, boolean unmarshal) {
            assert iter != null;

            this.iter = iter;
            this.unmarshal = unmarshal;
        }

        /** {@inheritDoc} */
        @Override protected Map.Entry<byte[], GridCacheSwapEntry<V>> onNext() throws GridException {
            Map.Entry<byte[], byte[]> e = iter.nextX();

            GridCacheSwapEntry<V> unmarshalled = unmarshalSwapEntry(e.getValue());

            return F.t(e.getKey(), swapEntry(unmarshalled, unmarshal));
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws GridException {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws GridException {
            iter.close();
        }

        /** {@inheritDoc} */
        @Override protected void onRemove() {
            iter.remove();
        }
    }

    /**
     *
     */
    private static class KeySwapListener<K1, V1> implements GridCacheSwapListener<K1, V1> {
        /** */
        private final K1 key;

        /** */
        private volatile GridCacheSwapEntry entry;

        /**
         * @param key Key.
         */
        KeySwapListener(K1 key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public void onEntryUnswapped(int part, K1 key, byte[] keyBytes, GridCacheSwapEntry<V1> e) {
            if (this.key.equals(key))
                entry = new GridCacheSwapEntryImpl(ByteBuffer.wrap(e.valueBytes()),
                    e.valueIsByteArray(),
                    e.version(),
                    e.ttl(),
                    e.expireTime(),
                    e.keyClassLoaderId(),
                    e.valueClassLoaderId());
        }
    }
}
