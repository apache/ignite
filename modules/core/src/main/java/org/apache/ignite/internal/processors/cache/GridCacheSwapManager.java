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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.internal.managers.swapspace.*;
import org.apache.ignite.internal.processors.cache.query.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.processors.offheap.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.offheap.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.spi.swapspace.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.*;
import java.lang.ref.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.cache.CacheMemoryMode.*;
import static org.apache.ignite.events.EventType.*;

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
    @Override public void start0() throws IgniteCheckedException {
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
                catch (IgniteCheckedException e) {
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
     * @throws IgniteCheckedException If failed.
     */
    public long swapSize() throws IgniteCheckedException {
        return enabled ? swapMgr.swapSize(spaceName) : -1;
    }

    /**
     * @param primary If {@code true} includes primary entries.
     * @param backup If {@code true} includes backup entries.
     * @param topVer Topology version.
     * @return Number of swap entries.
     * @throws IgniteCheckedException If failed.
     */
    public int swapEntriesCount(boolean primary, boolean backup, long topVer) throws IgniteCheckedException {
        assert primary || backup;

        if (!swapEnabled)
            return 0;

        if (!(primary && backup)) {
            Set<Integer> parts = primary ? cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer) :
                cctx.affinity().backupPartitions(cctx.localNodeId(), topVer);

            return (int)swapMgr.swapKeys(spaceName, parts);
        }
        else
            return (int)swapMgr.swapKeys(spaceName);
    }

    /**
     * @param primary If {@code true} includes primary entries.
     * @param backup If {@code true} includes backup entries.
     * @param topVer Topology version.
     * @return Number of offheap entries.
     * @throws IgniteCheckedException If failed.
     */
    public int offheapEntriesCount(boolean primary, boolean backup, long topVer) throws IgniteCheckedException {
        assert primary || backup;

        if (!offheapEnabled)
            return 0;

        if (!(primary && backup)) {
            Set<Integer> parts = primary ? cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer) :
                cctx.affinity().backupPartitions(cctx.localNodeId(), topVer);

            return (int)offheap.entriesCount(spaceName, parts);
        }
        else
            return (int)offheap.entriesCount(spaceName);
    }

    /**
     * Gets number of swap entries (keys).
     *
     * @return Swap keys count.
     * @throws IgniteCheckedException If failed.
     */
    public long swapKeys() throws IgniteCheckedException {
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
            catch (IgniteCheckedException e) {
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
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private GridCacheSwapEntry<V> swapEntry(GridCacheSwapEntry<V> e) throws IgniteCheckedException {
        return swapEntry(e, true);
    }

    /**
     * Recreates raw swap entry (that just has been received from swap storage).
     *
     * @param e Swap entry to reconstitute.
     * @param unmarshal If {@code true} then value is unmarshalled.
     * @return Reconstituted swap entry or {@code null} if entry is obsolete.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private GridCacheSwapEntry<V> swapEntry(GridCacheSwapEntry<V> e, boolean unmarshal) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    public boolean containsKey(K key, byte[] keyBytes) throws IgniteCheckedException {
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

            byte[] valBytes = swapMgr.read(spaceName, new SwapKey(key, part, keyBytes),
                cctx.deploy().globalLoader());

            return valBytes != null;
        }

        return false;
    }

    /**
     * @param key Key to read.
     * @param keyBytes Key bytes.
     * @param part Key partition.
     * @param entryLocked {@code True} if cache entry is locked.
     * @param readOffheap Read offheap flag.
     * @param readSwap Read swap flag.
     * @return Value from swap or {@code null}.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable private GridCacheSwapEntry<V> read(K key,
        byte[] keyBytes,
        int part,
        boolean entryLocked,
        boolean readOffheap,
        boolean readSwap)
        throws IgniteCheckedException
    {
        assert readOffheap || readSwap;

        if (!offheapEnabled && !swapEnabled)
            return null;

        checkIteratorQueue();

        KeySwapListener<K, V> lsnr = null;

        try {
            if (offheapEnabled && swapEnabled && !entryLocked) {
                lsnr = new KeySwapListener(key);

                addSwapListener(part, lsnr);
            }

            // First check off-heap store.
            if (readOffheap && offheapEnabled) {
                byte[] bytes = offheap.get(spaceName, part, key, keyBytes);

                if (bytes != null)
                    return swapEntry(unmarshalSwapEntry(bytes));
            }

            if (!swapEnabled || !readSwap)
                return null;

            assert key != null;

            byte[] bytes = swapMgr.read(spaceName, new SwapKey(key, part, keyBytes), cctx.deploy().globalLoader());

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
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable GridCacheSwapEntry<V> readAndRemove(final K key, final byte[] keyBytes) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    @Nullable private GridCacheSwapEntry<V> readAndRemoveSwap(final K key, final int part, final byte[] keyBytes)
        throws IgniteCheckedException {
        if (!swapEnabled)
            return null;

        final GridTuple<GridCacheSwapEntry<V>> t = F.t1();
        final GridTuple<IgniteCheckedException> err = F.t1();

        swapMgr.remove(spaceName, new SwapKey(key, part, keyBytes), new CI1<byte[]>() {
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
                    catch (IgniteCheckedException e) {
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
     * @param readOffheap Read offheap flag.
     * @param readSwap Read swap flag.
     * @return Read value.
     * @throws IgniteCheckedException If read failed.
     */
    @Nullable GridCacheSwapEntry<V> read(GridCacheEntryEx<K, V> entry,
        boolean locked,
        boolean readOffheap,
        boolean readSwap)
        throws IgniteCheckedException
    {
        if (!offheapEnabled && !swapEnabled)
            return null;

        return read(entry.key(),
            entry.getOrMarshalKeyBytes(),
            entry.partition(),
            locked,
            readOffheap,
            readSwap);
    }

    /**
     * @param entry Entry to read.
     * @return Read value address.
     * @throws IgniteCheckedException If read failed.
     */
    @Nullable GridCacheSwapEntry<V> readOffheapPointer(GridCacheEntryEx<K, V> entry) throws IgniteCheckedException {
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
     * @param readOffheap Read offheap flag.
     * @param readSwap Read swap flag.
     * @return Read value.
     * @throws IgniteCheckedException If read failed.
     */
    @Nullable public GridCacheSwapEntry<V> read(K key,
        boolean readOffheap,
        boolean readSwap)
        throws IgniteCheckedException
    {
        if (!offheapEnabled && !swapEnabled)
            return null;

        int part = cctx.affinity().partition(key);

        return read(key, CU.marshal(cctx.shared(), key), part, false, readOffheap, readSwap);
    }

    /**
     * @param entry Entry to read.
     * @return Read value.
     * @throws IgniteCheckedException If read failed.
     */
    @Nullable GridCacheSwapEntry<V> readAndRemove(GridCacheEntryEx<K, V> entry) throws IgniteCheckedException {
        if (!offheapEnabled && !swapEnabled)
            return null;

        return readAndRemove(entry.key(), entry.getOrMarshalKeyBytes());
    }

    /**
     * @param keys Collection of keys to remove from swap.
     * @return Collection of swap entries.
     * @throws IgniteCheckedException If failed,
     */
    public Collection<GridCacheBatchSwapEntry<K, V>> readAndRemove(Collection<? extends K> keys) throws IgniteCheckedException {
        if (!offheapEnabled && !swapEnabled)
            return Collections.emptyList();

        checkIteratorQueue();

        final GridCacheQueryManager<K, V> qryMgr = cctx.queries();

        Collection<K> keysList = new ArrayList<>(keys);
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
                        entry.valueIsByteArray() ? null : ByteBuffer.wrap(entry.valueBytes()),
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
        final GridTuple<IgniteCheckedException> err = F.t1();

        Collection<SwapKey> converted = new ArrayList<>(F.viewReadOnly(keysList, new C1<K, SwapKey>() {
            @Override public SwapKey apply(K key) {
                try {
                    return new SwapKey(key, cctx.affinity().partition(key), CU.marshal(cctx.shared(), key));
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        }));

        swapMgr.removeAll(spaceName,
            converted,
            new IgniteBiInClosure<SwapKey, byte[]>() {
                @Override public void apply(SwapKey swapKey, byte[] rmv) {
                    if (rmv != null) {
                        try {
                            GridCacheSwapEntry<V> entry = swapEntry(unmarshalSwapEntry(rmv));

                            if (entry == null)
                                return;

                            K key = (K)swapKey.key();

                            GridCacheBatchSwapEntry<K, V> unswapped = new GridCacheBatchSwapEntry<>(key,
                                swapKey.keyBytes(),
                                swapKey.partition(),
                                entry.valueIsByteArray() ? null : ByteBuffer.wrap(entry.valueBytes()),
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
                        catch (IgniteCheckedException e) {
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
     * @throws IgniteCheckedException If read failed.
     */
    @Nullable GridCacheSwapEntry<V> readAndRemove(K key) throws IgniteCheckedException {
        if (!offheapEnabled && !swapEnabled)
            return null;

        return readAndRemove(key, CU.marshal(cctx.shared(), key));
    }

    /**
     * @param key Key to remove.
     * @param keyBytes Key bytes.
     * @return {@code True} If succeeded.
     * @throws IgniteCheckedException If failed.
     */
    boolean removeOffheap(final K key, byte[] keyBytes) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    void enableOffheapEviction(final K key, byte[] keyBytes) throws IgniteCheckedException {
        if (!offheapEnabled)
            return;

        checkIteratorQueue();

        int part = cctx.affinity().partition(key);

        offheap.enableEviction(spaceName, part, key, keyBytes);
    }

    /**
     * @param key Key to remove.
     * @param keyBytes Key bytes.
     * @throws IgniteCheckedException If failed.
     */
    public void remove(final K key, byte[] keyBytes) throws IgniteCheckedException {
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
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
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
            swapMgr.remove(spaceName, new SwapKey(key, part, keyBytes), c,
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
     * @throws IgniteCheckedException If failed.
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
        throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    void writeAll(Iterable<GridCacheBatchSwapEntry<K, V>> swapped) throws IgniteCheckedException {
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
            Map<SwapKey, byte[]> batch = new LinkedHashMap<>();

            for (GridCacheBatchSwapEntry entry : swapped)
                batch.put(new SwapKey(entry.key(), entry.partition(), entry.keyBytes()), entry.marshal());

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
     * @throws IgniteCheckedException If failed.
     */
    private void writeToSwap(int part, @Nullable K key, byte[] keyBytes, byte[] entry) throws IgniteCheckedException{
        checkIteratorQueue();

        if (key == null)
            key = unmarshalKey(keyBytes, cctx.deploy().globalLoader());

        swapMgr.write(spaceName, new SwapKey(key, part, keyBytes), entry, cctx.deploy().globalLoader());

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
     * @throws IgniteCheckedException If failed.
     */
    public void clearSwap() throws IgniteCheckedException {
        if (swapEnabled)
            swapMgr.clear(spaceName);
    }

    /**
     * Gets offheap and swap iterator over partition.
     *
     * @param part Partition to iterate over.
     * @param unmarshal Unmarshal value flag.
     * @return Iterator over partition.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> iterator(
        final int part,
        final boolean unmarshal)
        throws IgniteCheckedException {
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

            private void advance() throws IgniteCheckedException {
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

            @Override protected Map.Entry<byte[], GridCacheSwapEntry<V>> onNext() throws IgniteCheckedException {
                if (done)
                    throw new NoSuchElementException();

                Map.Entry<byte[], GridCacheSwapEntry<V>> e = it.next();

                advance();

                return e;
            }

            @Override protected boolean onHasNext() {
                return !done;
            }

            @Override protected void onClose() throws IgniteCheckedException {
                if (it != null)
                    it.close();
            }
        };
    }

    /**
     * Gets offheap and swap iterator over partition.
     *
     * @return Iterator over partition.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawIterator()
        throws IgniteCheckedException {
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

            private void advance() throws IgniteCheckedException {
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

            @Override protected Map.Entry<byte[], byte[]> onNext() throws IgniteCheckedException {
                if (done)
                    throw new NoSuchElementException();

                cur = it.next();

                advance();

                return cur;
            }

            @Override protected boolean onHasNext() {
                return !done;
            }

            @Override protected void onRemove() throws IgniteCheckedException {
                if (offheapFlag) {
                    K key = unmarshalKey(cur.getKey(), cctx.deploy().globalLoader());

                    int part = cctx.affinity().partition(key);

                    offheap.removex(spaceName, part, key, cur.getKey());
                }
                else
                    it.removeX();
            }

            @Override protected void onClose() throws IgniteCheckedException {
                if (it != null)
                    it.close();
            }
        };
    }

    /**
     * @return Lazy swap iterator.
     * @throws IgniteCheckedException If failed.
     */
    public Iterator<Map.Entry<K, V>> lazySwapIterator() throws IgniteCheckedException {
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
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }

                    @Override public V getValue() {
                        try {
                            GridCacheSwapEntry<V> e = unmarshalSwapEntry(cur0.getValue());

                            swapEntry(e);

                            return e.value();
                        }
                        catch (IgniteCheckedException ex) {
                            throw new IgniteException(ex);
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

            @Override protected void onRemove() throws IgniteCheckedException {
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

            @Override protected void onClose() throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> offHeapIterator(
        int part,
        boolean unmarshal)
        throws IgniteCheckedException {
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

            @Override protected void onRemove() throws IgniteCheckedException {
                K key = unmarshalKey(cur.getKey(), cctx.deploy().globalLoader());

                int part = cctx.affinity().partition(key);

                offheap.removex(spaceName, part, key, cur.getKey());
            }

            @Override protected void onClose() throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public GridCloseableIterator<Map.Entry<byte[], GridCacheSwapEntry<V>>> swapIterator(
        int part,
        boolean unmarshal)
        throws IgniteCheckedException {
        if (!swapEnabled)
            return null;

        checkIteratorQueue();

        return new IteratorWrapper(swapMgr.rawIterator(spaceName, part), unmarshal);
    }

    /**
     * @return Raw off-heap iterator.
     * @throws IgniteCheckedException If failed.
     */
    public GridCloseableIterator<Map.Entry<byte[], byte[]>> rawSwapIterator() throws IgniteCheckedException {
        if (!swapEnabled)
            return new GridEmptyCloseableIterator<>();

        checkIteratorQueue();

        return swapMgr.rawIterator(spaceName);
    }

    /**
     * @param primary If {@code true} includes primary entries.
     * @param backup If {@code true} includes backup entries.
     * @param topVer Topology version.
     * @return Swap entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    public Iterator<Cache.Entry<K, V>> swapIterator(boolean primary, boolean backup, long topVer)
        throws IgniteCheckedException
    {
        assert primary || backup;

        if (!swapEnabled)
            return F.emptyIterator();

        if (primary && backup)
            return cacheEntryIterator(lazySwapIterator());

        Set<Integer> parts = primary ? cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer) :
            cctx.affinity().backupPartitions(cctx.localNodeId(), topVer);

        return new PartitionsIterator(parts) {
            @Override protected GridCloseableIterator<? extends Map.Entry<byte[], byte[]>> partitionIterator(int part)
                throws IgniteCheckedException
            {
                return swapMgr.rawIterator(spaceName, part);
            }
        };
    }

    /**
     * @param primary If {@code true} includes primary entries.
     * @param backup If {@code true} includes backup entries.
     * @param topVer Topology version.
     * @return Offheap entries iterator.
     * @throws IgniteCheckedException If failed.
     */
    public Iterator<Cache.Entry<K, V>> offheapIterator(boolean primary, boolean backup, long topVer)
        throws IgniteCheckedException
    {
        assert primary || backup;

        if (!offheapEnabled)
            return F.emptyIterator();

        if (primary && backup)
            return cacheEntryIterator(lazyOffHeapIterator());

        Set<Integer> parts = primary ? cctx.affinity().primaryPartitions(cctx.localNodeId(), topVer) :
            cctx.affinity().backupPartitions(cctx.localNodeId(), topVer);

        return new PartitionsIterator(parts) {
            @Override protected GridCloseableIterator<? extends Map.Entry<byte[], byte[]>> partitionIterator(int part)
                throws IgniteCheckedException
            {
                return offheap.iterator(spaceName, part);
            }
        };
    }

    /**
     * @param ldr Undeployed class loader.
     * @return Undeploy count.
     */
    public int onUndeploy(ClassLoader ldr) {
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
                        catch (IgniteCheckedException ex) {
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
        catch (IgniteCheckedException e) {
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
     * @throws IgniteCheckedException If unmarshal failed.
     */
    private <T> T unmarshalKey(byte[] bytes, ClassLoader ldr) throws IgniteCheckedException {
        return (T)cctx.marshaller().unmarshal(bytes, ldr);
    }

    /**
     * @return Size of internal weak iterator set.
     */
    int iteratorSetSize() {
        return itSet.size();
    }

    /**
     * @param it Map.Entry iterator.
     * @return Cache.Entry iterator.
     */
    private static <K, V> Iterator<Cache.Entry<K, V>> cacheEntryIterator(Iterator<Map.Entry<K, V>> it) {
        return F.iterator(it, new C1<Map.Entry<K, V>, Cache.Entry<K, V>>() {
            @Override public Cache.Entry<K, V> apply(Map.Entry<K, V> e) {
                // Create Cache.Entry over Map.Entry to do not deserialize keys/values if not needed.
                return new CacheEntryImpl0<>(e);
            }
        }, true);
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
        @Override protected Map.Entry<byte[], GridCacheSwapEntry<V>> onNext() throws IgniteCheckedException {
            Map.Entry<byte[], byte[]> e = iter.nextX();

            GridCacheSwapEntry<V> unmarshalled = unmarshalSwapEntry(e.getValue());

            return F.t(e.getKey(), swapEntry(unmarshalled, unmarshal));
        }

        /** {@inheritDoc} */
        @Override protected boolean onHasNext() throws IgniteCheckedException {
            return iter.hasNext();
        }

        /** {@inheritDoc} */
        @Override protected void onClose() throws IgniteCheckedException {
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

    /**
     *
     */
    private abstract class PartitionsIterator implements Iterator<Cache.Entry<K, V>> {
        /** */
        private Iterator<Integer> partIt;

        /** */
        private Iterator<Cache.Entry<K, V>> curIt;

        /** */
        private Cache.Entry<K, V> next;

        /**
         * @param parts Partitions
         */
        public PartitionsIterator(Collection<Integer> parts) {
            this.partIt = parts.iterator();

            advance();
        }

        /** {@inheritDoc} */
        @Override public boolean hasNext() {
            return next != null;
        }

        /** {@inheritDoc} */
        @Override public Cache.Entry<K, V> next() {
            if (next == null)
                throw new NoSuchElementException();

            Cache.Entry<K, V> e = next;

            advance();

            return e;
        }

        /** {@inheritDoc} */
        @Override public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * Switches to next element.
         */
        private void advance() {
            next = null;

            do {
                if (curIt == null) {
                    if (partIt.hasNext()) {
                        int part = partIt.next();

                        try {
                            curIt = cacheEntryIterator(lazyIterator(partitionIterator(part)));
                        }
                        catch (IgniteCheckedException e) {
                            throw new IgniteException(e);
                        }
                    }
                }

                if (curIt != null) {
                    if (curIt.hasNext()) {
                        next = curIt.next();

                        break;
                    }
                    else
                        curIt = null;
                }
            }
            while (partIt.hasNext());
        }

        /**
         * @param part Partition.
         * @return Iterator for given partition.
         * @throws IgniteCheckedException If failed.
         */
        abstract protected GridCloseableIterator<? extends Map.Entry<byte[], byte[]>> partitionIterator(int part)
            throws IgniteCheckedException;
    }
}
