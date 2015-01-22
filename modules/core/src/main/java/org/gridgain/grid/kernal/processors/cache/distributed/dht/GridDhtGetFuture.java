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

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.kernal.processors.cache.transactions.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.apache.ignite.internal.util.future.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public final class GridDhtGetFuture<K, V> extends GridCompoundIdentityFuture<Collection<GridCacheEntryInfo<K, V>>>
    implements GridDhtFuture<Collection<GridCacheEntryInfo<K, V>>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Message ID. */
    private long msgId;

    /** */
    private UUID reader;

    /** Reload flag. */
    private boolean reload;

    /** Read through flag. */
    private boolean readThrough;

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /** Keys. */
    private LinkedHashMap<? extends K, Boolean> keys;

    /** Reserved partitions. */
    private Collection<GridDhtLocalPartition> parts = new GridLeanSet<>(5);

    /** Future ID. */
    private IgniteUuid futId;

    /** Version. */
    private GridCacheVersion ver;

    /** Topology version .*/
    private long topVer;

    /** Transaction. */
    private IgniteTxLocalEx<K, V> tx;

    /** Filters. */
    private IgnitePredicate<GridCacheEntry<K, V>>[] filters;

    /** Logger. */
    private IgniteLogger log;

    /** Retries because ownership changed. */
    private Collection<Integer> retries = new GridLeanSet<>();

    /** Subject ID. */
    private UUID subjId;

    /** Task name. */
    private int taskNameHash;

    /** Whether to deserialize portable objects. */
    private boolean deserializePortable;

    /** Expiry policy. */
    private IgniteCacheExpiryPolicy expiryPlc;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridDhtGetFuture() {
        // No-op.
    }

    /**
     * @param cctx Context.
     * @param msgId Message ID.
     * @param reader Reader.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param reload Reload flag.
     * @param tx Transaction.
     * @param topVer Topology version.
     * @param filters Filters.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param deserializePortable Deserialize portable flag.
     * @param expiryPlc Expiry policy.
     */
    public GridDhtGetFuture(
        GridCacheContext<K, V> cctx,
        long msgId,
        UUID reader,
        LinkedHashMap<? extends K, Boolean> keys,
        boolean readThrough,
        boolean reload,
        @Nullable IgniteTxLocalEx<K, V> tx,
        long topVer,
        @Nullable IgnitePredicate<GridCacheEntry<K, V>>[] filters,
        @Nullable UUID subjId,
        int taskNameHash,
        boolean deserializePortable,
        @Nullable IgniteCacheExpiryPolicy expiryPlc) {
        super(cctx.kernalContext(), CU.<GridCacheEntryInfo<K, V>>collectionsReducer());

        assert reader != null;
        assert !F.isEmpty(keys);

        this.reader = reader;
        this.cctx = cctx;
        this.msgId = msgId;
        this.keys = keys;
        this.readThrough = readThrough;
        this.reload = reload;
        this.filters = filters;
        this.tx = tx;
        this.topVer = topVer;
        this.subjId = subjId;
        this.deserializePortable = deserializePortable;
        this.taskNameHash = taskNameHash;
        this.expiryPlc = expiryPlc;

        futId = IgniteUuid.randomUuid();

        ver = tx == null ? cctx.versions().next() : tx.xidVersion();

        log = U.logger(ctx, logRef, GridDhtGetFuture.class);

        syncNotify(true);
    }

    /**
     * Initializes future.
     */
    void init() {
        map(keys);

        markInitialized();
    }

    /**
     * @return Keys.
     */
    Collection<? extends K> keys() {
        return keys.keySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> invalidPartitions() {
        return retries;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @return Future version.
     */
    public GridCacheVersion version() {
        return ver;
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(Collection<GridCacheEntryInfo<K, V>> res, Throwable err) {
        if (super.onDone(res, err)) {
            // Release all partitions reserved by this future.
            for (GridDhtLocalPartition part : parts)
                part.release();

            return true;
        }

        return false;
    }

    /**
     * @param keys Keys.
     */
    private void map(final LinkedHashMap<? extends K, Boolean> keys) {
        GridDhtFuture<Object> fut = cctx.dht().dhtPreloader().request(keys.keySet(), topVer);

        if (!F.isEmpty(fut.invalidPartitions()))
            retries.addAll(fut.invalidPartitions());

        add(new GridEmbeddedFuture<>(cctx.kernalContext(), fut,
            new IgniteBiClosure<Object, Exception, Collection<GridCacheEntryInfo<K, V>>>() {
                @Override public Collection<GridCacheEntryInfo<K, V>> apply(Object o, Exception e) {
                    if (e != null) { // Check error first.
                        if (log.isDebugEnabled())
                            log.debug("Failed to request keys from preloader [keys=" + keys + ", err=" + e + ']');

                        onDone(e);
                    }

                    LinkedHashMap<K, Boolean> mappedKeys = U.newLinkedHashMap(keys.size());

                    // Assign keys to primary nodes.
                    for (Map.Entry<? extends K, Boolean> key : keys.entrySet()) {
                        int part = cctx.affinity().partition(key.getKey());

                        if (!retries.contains(part)) {
                            if (!map(key.getKey(), parts))
                                retries.add(part);
                            else
                                mappedKeys.put(key.getKey(), key.getValue());
                        }
                    }

                    // Add new future.
                    add(getAsync(mappedKeys));

                    // Finish this one.
                    return Collections.emptyList();
                }
            })
        );
    }

    /**
     * @param key Key.
     * @param parts Parts to map.
     * @return {@code True} if mapped.
     */
    private boolean map(K key, Collection<GridDhtLocalPartition> parts) {
        GridDhtLocalPartition part = topVer > 0 ?
            cache().topology().localPartition(cctx.affinity().partition(key), topVer, true) :
            cache().topology().localPartition(key, false);

        if (part == null)
            return false;

        if (!parts.contains(part)) {
            // By reserving, we make sure that partition won't be unloaded while processed.
            if (part.reserve()) {
                parts.add(part);

                return true;
            }
            else
                return false;
        }
        else
            return true;
    }

    /**
     * @param keys Keys to get.
     * @return Future for local get.
     */
    @SuppressWarnings( {"unchecked", "IfMayBeConditional"})
    private IgniteFuture<Collection<GridCacheEntryInfo<K, V>>> getAsync(final LinkedHashMap<? extends K, Boolean> keys) {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<Collection<GridCacheEntryInfo<K, V>>>(cctx.kernalContext(),
                Collections.<GridCacheEntryInfo<K, V>>emptyList());

        final Collection<GridCacheEntryInfo<K, V>> infos = new LinkedList<>();

        String taskName0 = ctx.job().currentTaskName();

        if (taskName0 == null)
            taskName0 = ctx.task().resolveTaskName(taskNameHash);

        final String taskName = taskName0;

        GridCompoundFuture<Boolean, Boolean> txFut = null;

        for (Map.Entry<? extends K, Boolean> k : keys.entrySet()) {
            while (true) {
                GridDhtCacheEntry<K, V> e = cache().entryExx(k.getKey(), topVer);

                try {
                    GridCacheEntryInfo<K, V> info = e.info();

                    // If entry is obsolete.
                    if (info == null)
                        continue;

                    // Register reader. If there are active transactions for this entry,
                    // then will wait for their completion before proceeding.
                    // TODO: GG-4003:
                    // TODO: What if any transaction we wait for actually removes this entry?
                    // TODO: In this case seems like we will be stuck with untracked near entry.
                    // TODO: To fix, check that reader is contained in the list of readers once
                    // TODO: again after the returned future completes - if not, try again.
                    // TODO: Also, why is info read before transactions are complete, and not after?
                    IgniteFuture<Boolean> f = (!e.deleted() && k.getValue()) ? e.addReader(reader, msgId, topVer) : null;

                    if (f != null) {
                        if (txFut == null)
                            txFut = new GridCompoundFuture<>(cctx.kernalContext(), CU.boolReducer());

                        txFut.add(f);
                    }

                    infos.add(info);

                    break;
                }
                catch (GridCacheEntryRemovedException ignore) {
                    if (log.isDebugEnabled())
                        log.debug("Got removed entry when getting a DHT value: " + e);
                }
                finally {
                    cctx.evicts().touch(e, topVer);
                }
            }
        }

        if (txFut != null)
            txFut.markInitialized();

        IgniteFuture<Map<K, V>> fut;

        if (txFut == null || txFut.isDone()) {
            if (reload && cctx.readThrough() && cctx.store().configured()) {
                fut = cache().reloadAllAsync(keys.keySet(),
                    true,
                    subjId,
                    taskName,
                    filters);
            }
            else {
                if (tx == null) {
                    fut = cache().getDhtAllAsync(keys.keySet(),
                        readThrough,
                        subjId,
                        taskName,
                        deserializePortable,
                        filters,
                        expiryPlc);
                }
                else {
                    fut = tx.getAllAsync(cctx,
                        keys.keySet(),
                        null,
                        deserializePortable,
                        filters);
                }
            }
        }
        else {
            // If we are here, then there were active transactions for some entries
            // when we were adding the reader. In that case we must wait for those
            // transactions to complete.
            fut = new GridEmbeddedFuture<>(
                txFut,
                new C2<Boolean, Exception, IgniteFuture<Map<K, V>>>() {
                    @Override public IgniteFuture<Map<K, V>> apply(Boolean b, Exception e) {
                        if (e != null)
                            throw new GridClosureException(e);

                        if (reload && cctx.readThrough() && cctx.store().configured()) {
                            return cache().reloadAllAsync(keys.keySet(),
                                true,
                                subjId,
                                taskName,
                                filters);
                        }
                        else {
                            if (tx == null) {
                                return cache().getDhtAllAsync(keys.keySet(),
                                    readThrough,
                                    subjId,
                                    taskName,
                                    deserializePortable,
                                    filters,
                                    expiryPlc);
                            }
                            else {
                                return tx.getAllAsync(cctx,
                                    keys.keySet(),
                                    null,
                                    deserializePortable,
                                    filters);
                            }
                        }
                    }
                },
                cctx.kernalContext());
        }

        return new GridEmbeddedFuture<>(cctx.kernalContext(), fut,
            new C2<Map<K, V>, Exception, Collection<GridCacheEntryInfo<K, V>>>() {
                @Override public Collection<GridCacheEntryInfo<K, V>> apply(Map<K, V> map, Exception e) {
                    if (e != null) {
                        onDone(e);

                        return Collections.emptyList();
                    }
                    else {
                        for (Iterator<GridCacheEntryInfo<K, V>> it = infos.iterator(); it.hasNext();) {
                            GridCacheEntryInfo<K, V> info = it.next();

                            V v = map.get(info.key());

                            if (v == null)
                                it.remove();
                            else
                                info.value(v);
                        }

                        return infos;
                    }
                }
            });
    }

    /**
     * @return DHT cache.
     */
    private GridDhtCacheAdapter<K, V> cache() {
        return (GridDhtCacheAdapter<K, V>)cctx.cache();
    }
}
