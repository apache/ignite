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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryInfo;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.IgniteCacheExpiryPolicy;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.ReaderArguments;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public final class GridDhtGetSingleFuture<K, V> extends GridFutureAdapter<GridCacheEntryInfo>
    implements GridDhtFuture<GridCacheEntryInfo> {
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

    /** Read through flag. */
    private boolean readThrough;

    /** Context. */
    private GridCacheContext<K, V> cctx;

    /** Key. */
    private KeyCacheObject key;

    /** */
    private boolean addRdr;

    /** Reserved partitions. */
    private int part = -1;

    /** Future ID. */
    private IgniteUuid futId;

    /** Version. */
    private GridCacheVersion ver;

    /** Topology version .*/
    private AffinityTopologyVersion topVer;

    /** Retries because ownership changed. */
    private Collection<Integer> retries;

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
     * @param key Key.
     * @param addRdr Add reader flag.
     * @param readThrough Read through flag.
     * @param topVer Topology version.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param expiryPlc Expiry policy.
     * @param skipVals Skip values flag.
     */
    public GridDhtGetSingleFuture(
        GridCacheContext<K, V> cctx,
        long msgId,
        UUID reader,
        KeyCacheObject key,
        Boolean addRdr,
        boolean readThrough,
        @NotNull AffinityTopologyVersion topVer,
        @Nullable UUID subjId,
        int taskNameHash,
        @Nullable IgniteCacheExpiryPolicy expiryPlc,
        boolean skipVals
    ) {
        assert reader != null;
        assert key != null;

        this.reader = reader;
        this.cctx = cctx;
        this.msgId = msgId;
        this.key = key;
        this.addRdr = addRdr;
        this.readThrough = readThrough;
        this.topVer = topVer;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.expiryPlc = expiryPlc;
        this.skipVals = skipVals;

        futId = IgniteUuid.randomUuid();

        ver = cctx.versions().next();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridDhtGetSingleFuture.class);
    }

    /**
     * Initializes future.
     */
    void init() {
        map();
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
    @Override public boolean onDone(GridCacheEntryInfo res, Throwable err) {
        if (super.onDone(res, err)) {
            // Release all partitions reserved by this future.
            if (part != -1)
                cctx.topology().releasePartitions(part);

            return true;
        }

        return false;
    }

    /**
     *
     */
    private void map() {
        if (cctx.dht().dhtPreloader().needForceKeys()) {
            GridDhtFuture<Object> fut = cctx.dht().dhtPreloader().request(
                Collections.singleton(key),
                topVer);

            if (fut != null) {
                if (F.isEmpty(fut.invalidPartitions())) {
                    if (retries == null)
                        retries = new HashSet<>();

                    retries.addAll(fut.invalidPartitions());
                }

                fut.listen(
                    new IgniteInClosure<IgniteInternalFuture<Object>>() {
                        @Override public void apply(IgniteInternalFuture<Object> fut) {
                            Throwable e = fut.error();

                            if (e != null) { // Check error first.
                                if (log.isDebugEnabled())
                                    log.debug("Failed to request keys from preloader " +
                                        "[keys=" + key + ", err=" + e + ']');

                                if (e instanceof NodeStoppingException)
                                    return;

                                onDone(e);
                            }
                            else
                                map0();
                        }
                    }
                );

                return;
            }
        }

        map0();
    }

    /**
     *
     */
    private void map0() {
        // Assign keys to primary nodes.
        int part = cctx.affinity().partition(key);

        if (retries == null || !retries.contains(part)) {
            if (!map(key)) {
                retries = Collections.singleton(part);

                onDone((GridCacheEntryInfo)null);

                return;
            }
        }

        getAsync();
    }

    /** {@inheritDoc} */
    @Override public Collection<Integer> invalidPartitions() {
        return retries == null ? Collections.<Integer>emptyList() : retries;
    }

    /**
     * @param key Key.
     * @return {@code True} if mapped.
     */
    private boolean map(KeyCacheObject key) {
        GridDhtLocalPartition part = topVer.topologyVersion() > 0 ?
            cache().topology().localPartition(cctx.affinity().partition(key), topVer, true) :
            cache().topology().localPartition(key, false);

        if (part == null)
            return false;

        assert this.part == -1;

        // By reserving, we make sure that partition won't be unloaded while processed.
        if (part.reserve()) {
            this.part = part.id();

            return true;
        }
        else
            return false;
    }

    /**
     *
     */
    @SuppressWarnings( {"unchecked", "IfMayBeConditional"})
    private void getAsync() {
        assert part != -1;

        String taskName0 = cctx.kernalContext().job().currentTaskName();

        if (taskName0 == null)
            taskName0 = cctx.kernalContext().task().resolveTaskName(taskNameHash);

        final String taskName = taskName0;

        IgniteInternalFuture<Boolean> rdrFut = null;

        ClusterNode readerNode = cctx.discovery().node(reader);

        ReaderArguments readerArgs = null;

        if (readerNode != null && !readerNode.isLocal() && cctx.discovery().cacheNearNode(readerNode, cctx.name())) {
            while (true) {
                GridDhtCacheEntry e = cache().entryExx(key, topVer);

                try {
                    if (e.obsolete())
                        continue;

                    boolean addReader = (!e.deleted() && this.addRdr && !skipVals);

                    if (addReader) {
                        e.unswap(false);

                        // Entry will be removed on touch() if no data in cache,
                        // but they could be loaded from store,
                        // we have to add reader again later.
                        if (readerArgs == null)
                            readerArgs = new ReaderArguments(reader, msgId, topVer);
                    }

                    // Register reader. If there are active transactions for this entry,
                    // then will wait for their completion before proceeding.
                    // TODO: IGNITE-3498:
                    // TODO: What if any transaction we wait for actually removes this entry?
                    // TODO: In this case seems like we will be stuck with untracked near entry.
                    // TODO: To fix, check that reader is contained in the list of readers once
                    // TODO: again after the returned future completes - if not, try again.
                    rdrFut = addReader ? e.addReader(reader, msgId, topVer) : null;

                    break;
                }
                catch (IgniteCheckedException err) {
                    onDone(err);

                    return;
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

        IgniteInternalFuture<Map<KeyCacheObject, T2<CacheObject, GridCacheVersion>>> fut;

        if (rdrFut == null || rdrFut.isDone()) {
            fut = cache().getDhtAllAsync(
                Collections.singleton(key),
                readerArgs,
                readThrough,
                subjId,
                taskName,
                expiryPlc,
                skipVals,
                /*can remap*/true);
        }
        else {
            final ReaderArguments args = readerArgs;

            rdrFut.listen(
                new IgniteInClosure<IgniteInternalFuture<Boolean>>() {
                    @Override public void apply(IgniteInternalFuture<Boolean> fut) {
                        Throwable e = fut.error();

                        if (e != null) {
                            onDone(e);

                            return;
                        }

                        IgniteInternalFuture<Map<KeyCacheObject, T2<CacheObject, GridCacheVersion>>> fut0 =
                            cache().getDhtAllAsync(
                                Collections.singleton(key),
                                args,
                                readThrough,
                                subjId,
                                taskName,
                                expiryPlc,
                                skipVals,
                                /*can remap*/true);

                        fut0.listen(createGetFutureListener());
                    }
                }
            );

            return;
        }

        if (fut.isDone())
            onResult(fut);
        else
            fut.listen(createGetFutureListener());
    }

    /**
     * @return Listener for get future.
     */
    @NotNull private IgniteInClosure<IgniteInternalFuture<Map<KeyCacheObject, T2<CacheObject, GridCacheVersion>>>>
    createGetFutureListener() {
        return new IgniteInClosure<IgniteInternalFuture<Map<KeyCacheObject, T2<CacheObject, GridCacheVersion>>>>() {
            @Override public void apply(
                IgniteInternalFuture<Map<KeyCacheObject, T2<CacheObject, GridCacheVersion>>> fut
            ) {
                onResult(fut);
            }
        };
    }

    /**
     * @param fut Completed future to finish this process with.
     */
    private void onResult(IgniteInternalFuture<Map<KeyCacheObject, T2<CacheObject, GridCacheVersion>>> fut) {
        assert fut.isDone();

        if (fut.error() != null)
            onDone(fut.error());
        else {
            try {
                onDone(toEntryInfo(fut.get()));
            }
            catch (IgniteCheckedException ignored) {
                assert false; // Should never happen.
            }
        }
    }

    /**
     * @param map Map to convert.
     * @return List of infos.
     */
    private GridCacheEntryInfo toEntryInfo(Map<KeyCacheObject, T2<CacheObject, GridCacheVersion>> map) {
        if (map.isEmpty())
            return null;

        T2<CacheObject, GridCacheVersion> val = map.get(key);

        assert val != null;

        GridCacheEntryInfo info = new GridCacheEntryInfo();

        info.cacheId(cctx.cacheId());
        info.key(key);
        info.value(skipVals ? null : val.get1());
        info.version(val.get2());

        return info;
    }

    /**
     * @return DHT cache.
     */
    private GridDhtCacheAdapter<K, V> cache() {
        return (GridDhtCacheAdapter<K, V>)cctx.cache();
    }
}
