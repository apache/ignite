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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.affinity.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.cache.version.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public final class GridDhtGetFuture<K, V> extends GridCompoundIdentityFuture<Collection<GridCacheEntryInfo>>
    implements GridDhtFuture<Collection<GridCacheEntryInfo>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

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
    private LinkedHashMap<KeyCacheObject, Boolean> keys;

    /** Reserved partitions. */
    private Collection<GridDhtLocalPartition> parts = new GridLeanSet<>(5);

    /** Future ID. */
    private IgniteUuid futId;

    /** Version. */
    private GridCacheVersion ver;

    /** Topology version .*/
    private AffinityTopologyVersion topVer;

    /** Transaction. */
    private IgniteTxLocalEx tx;

    /** Retries because ownership changed. */
    private Collection<Integer> retries = new GridLeanSet<>();

    /** Subject ID. */
    private UUID subjId;

    /** Task name. */
    private int taskNameHash;

    /** Expiry policy. */
    private IgniteCacheExpiryPolicy expiryPlc;

    /** Skip values flag. */
    private boolean skipVals;

    /**
     * @param cctx Context.
     * @param msgId Message ID.
     * @param reader Reader.
     * @param keys Keys.
     * @param readThrough Read through flag.
     * @param reload Reload flag.
     * @param tx Transaction.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     */
    public GridDhtGetFuture(
        GridCacheContext<K, V> cctx,
        long msgId,
        UUID reader,
        LinkedHashMap<KeyCacheObject, Boolean> keys,
        boolean readThrough,
        boolean reload,
        @Nullable IgniteTxLocalEx tx,
        @NotNull AffinityTopologyVersion topVer,
        @Nullable UUID subjId,
        int taskNameHash,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals
    ) {
        super(cctx.kernalContext(), CU.<GridCacheEntryInfo>collectionsReducer());

        assert reader != null;
        assert !F.isEmpty(keys);

        this.reader = reader;
        this.cctx = cctx;
        this.msgId = msgId;
        this.keys = keys;
        this.readThrough = readThrough;
        this.reload = reload;
        this.tx = tx;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.expiryPlc = expiryPlc;
        this.skipVals = skipVals;

        futId = IgniteUuid.randomUuid();

        ver = tx == null ? cctx.versions().next() : tx.xidVersion();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtGetFuture.class);
    }

    /**
     * Initializes future.
     */
    void init() {
        map(keys);

        markInitialized();
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
    @Override public boolean onDone(Collection<GridCacheEntryInfo> res, Throwable err) {
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
    private void map(final LinkedHashMap<KeyCacheObject, Boolean> keys) {
        GridDhtFuture<Object> fut = cctx.dht().dhtPreloader().request(keys.keySet(), topVer);

        if (!F.isEmpty(fut.invalidPartitions()))
            retries.addAll(fut.invalidPartitions());

        add(new GridEmbeddedFuture<>(
            new IgniteBiClosure<Object, Exception, Collection<GridCacheEntryInfo>>() {
                @Override public Collection<GridCacheEntryInfo> apply(Object o, Exception e) {
                    if (e != null) { // Check error first.
                        if (log.isDebugEnabled())
                            log.debug("Failed to request keys from preloader [keys=" + keys + ", err=" + e + ']');

                        onDone(e);
                    }

                    LinkedHashMap<KeyCacheObject, Boolean> mappedKeys = U.newLinkedHashMap(keys.size());

                    // Assign keys to primary nodes.
                    for (Map.Entry<KeyCacheObject, Boolean> key : keys.entrySet()) {
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
            },
            fut));
    }

    /**
     * @param key Key.
     * @param parts Parts to map.
     * @return {@code True} if mapped.
     */
    private boolean map(KeyCacheObject key, Collection<GridDhtLocalPartition> parts) {
        GridDhtLocalPartition part = topVer.topologyVersion() > 0 ?
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
    private IgniteInternalFuture<Collection<GridCacheEntryInfo>> getAsync(
        final LinkedHashMap<KeyCacheObject, Boolean> keys)
    {
        if (F.isEmpty(keys))
            return new GridFinishedFuture<Collection<GridCacheEntryInfo>>(
                Collections.<GridCacheEntryInfo>emptyList());

        final Collection<GridCacheEntryInfo> infos = new LinkedList<>();

        String taskName0 = cctx.kernalContext().job().currentTaskName();

        if (taskName0 == null)
            taskName0 = cctx.kernalContext().task().resolveTaskName(taskNameHash);

        final String taskName = taskName0;

        GridCompoundFuture<Boolean, Boolean> txFut = null;

        for (Map.Entry<KeyCacheObject, Boolean> k : keys.entrySet()) {
            while (true) {
                GridDhtCacheEntry e = cache().entryExx(k.getKey(), topVer);

                try {
                    GridCacheEntryInfo info = e.info();

                    // If entry is obsolete.
                    if (info == null)
                        continue;

                    boolean addReader = (!e.deleted() && k.getValue() && !skipVals);

                    if (addReader)
                        e.unswap(false);

                    // Register reader. If there are active transactions for this entry,
                    // then will wait for their completion before proceeding.
                    // TODO: GG-4003:
                    // TODO: What if any transaction we wait for actually removes this entry?
                    // TODO: In this case seems like we will be stuck with untracked near entry.
                    // TODO: To fix, check that reader is contained in the list of readers once
                    // TODO: again after the returned future completes - if not, try again.
                    // TODO: Also, why is info read before transactions are complete, and not after?
                    IgniteInternalFuture<Boolean> f = addReader ? e.addReader(reader, msgId, topVer) : null;

                    if (f != null) {
                        if (txFut == null)
                            txFut = new GridCompoundFuture<>(CU.boolReducer());

                        txFut.add(f);
                    }

                    infos.add(info);

                    break;
                }
                catch (IgniteCheckedException err) {
                    return new GridFinishedFuture<>(err);
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

        IgniteInternalFuture<Map<KeyCacheObject, CacheObject>> fut;

        if (txFut == null || txFut.isDone()) {
            if (reload && cctx.readThrough() && cctx.store().configured()) {
                fut = cache().reloadAllAsync0(keys.keySet(),
                    true,
                    skipVals,
                    subjId,
                    taskName);
            }
            else {
                if (tx == null) {
                    fut = cache().getDhtAllAsync(
                        keys.keySet(),
                        readThrough,
                        subjId,
                        taskName,
                        expiryPlc,
                        skipVals,
                        /*can remap*/true);
                }
                else {
                    fut = tx.getAllAsync(cctx,
                        keys.keySet(),
                        null,
                        /*deserialize portable*/false,
                        skipVals,
                        /*keep cache objects*/true,
                        /*skip store*/!readThrough);
                }
            }
        }
        else {
            // If we are here, then there were active transactions for some entries
            // when we were adding the reader. In that case we must wait for those
            // transactions to complete.
            fut = new GridEmbeddedFuture<>(
                txFut,
                new C2<Boolean, Exception, IgniteInternalFuture<Map<KeyCacheObject, CacheObject>>>() {
                    @Override public IgniteInternalFuture<Map<KeyCacheObject, CacheObject>> apply(Boolean b, Exception e) {
                        if (e != null)
                            throw new GridClosureException(e);

                        if (reload && cctx.readThrough() && cctx.store().configured()) {
                            return cache().reloadAllAsync0(keys.keySet(),
                                true,
                                skipVals,
                                subjId,
                                taskName);
                        }
                        else {
                            if (tx == null) {
                                return cache().getDhtAllAsync(
                                    keys.keySet(),
                                    readThrough,
                                    subjId,
                                    taskName,
                                    expiryPlc,
                                    skipVals,
                                    /*can remap*/true);
                            }
                            else {
                                return tx.getAllAsync(cctx,
                                    keys.keySet(),
                                    null,
                                    /*deserialize portable*/false,
                                    skipVals,
                                    /*keep cache objects*/true,
                                    /*skip store*/!readThrough);
                            }
                        }
                    }
                }
            );
        }

        return new GridEmbeddedFuture<>(
            new C2<Map<KeyCacheObject, CacheObject>, Exception, Collection<GridCacheEntryInfo>>() {
                @Override public Collection<GridCacheEntryInfo> apply(Map<KeyCacheObject, CacheObject> map, Exception e) {
                    if (e != null) {
                        onDone(e);

                        return Collections.emptyList();
                    }
                    else {
                        for (Iterator<GridCacheEntryInfo> it = infos.iterator(); it.hasNext();) {
                            GridCacheEntryInfo info = it.next();

                            Object v = map.get(info.key());

                            if (v == null)
                                it.remove();
                            else if (!skipVals)
                                info.value((CacheObject)v);
                        }

                        return infos;
                    }
                }
            },
            fut);
    }

    /**
     * @return DHT cache.
     */
    private GridDhtCacheAdapter<K, V> cache() {
        return (GridDhtCacheAdapter<K, V>)cctx.cache();
    }
}
