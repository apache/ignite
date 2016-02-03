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

package org.apache.ignite.internal.processors.cache.distributed.dht.atomic;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheAffinityManager;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearAtomicCache;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import javax.cache.expiry.ExpiryPolicy;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.processors.cache.GridCacheOperation.TRANSFORM;

/**
 * Base class for near atomic update futures.
 */
public abstract class GridNearAbstractAtomicUpdateFuture extends GridFutureAdapter<Object>
    implements GridCacheAtomicFuture<Object> {
    /** */
    public static final IgniteProductVersion SINGLE_PUT_MSG_SINCE = IgniteProductVersion.fromString("1.6.0");

    /** Logger reference. */
    protected static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Optional arguments for entry processor. */
    protected Object[] invokeArgs;

    /** Cache context. */
    protected final GridCacheContext cctx;

    /** Cache. */
    protected final GridDhtAtomicCache cache;

    /** Update operation. */
    protected final GridCacheOperation op;

    /** Return value require flag. */
    protected final boolean retval;

    /** Expiry policy. */
    protected final ExpiryPolicy expiryPlc;

    /** Optional filter. */
    protected final CacheEntryPredicate[] filter;

    /** Write synchronization mode. */
    protected final CacheWriteSynchronizationMode syncMode;

    /** Raw return value flag. */
    protected final boolean rawRetval;

    /** Fast map flag. */
    protected final boolean fastMap;

    /** Near cache flag. */
    protected final boolean nearEnabled;

    /** Subject ID. */
    protected final UUID subjId;

    /** Task name hash. */
    protected final int taskNameHash;

    /** Skip store flag. */
    protected final boolean skipStore;

    /** */
    protected final boolean keepBinary;

    /** Wait for topology future flag. */
    protected final boolean waitTopFut;

    /** Topology locked flag. Set if atomic update is performed inside a TX or explicit lock. */
    protected boolean topLocked;

    /** Remap count. */
    protected int remapCnt;

    /**
     * @param cctx Cache context.
     * @param cache Cache instance.
     * @param syncMode Write synchronization mode.
     * @param op Update operation.
     * @param invokeArgs Optional arguments for entry processor.
     * @param retval Return value require flag.
     * @param rawRetval {@code True} if should return {@code GridCacheReturn} as future result.
     * @param expiryPlc Expiry policy explicitly specified for cache operation.
     * @param filter Entry filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash code.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param remapCnt Maximum number of retries.
     * @param waitTopFut If {@code false} does not wait for affinity change future.
     */
    public GridNearAbstractAtomicUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        @Nullable Object[] invokeArgs,
        final boolean retval,
        final boolean rawRetval,
        @Nullable ExpiryPolicy expiryPlc,
        final CacheEntryPredicate[] filter,
        UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        int remapCnt,
        boolean waitTopFut
    ) {
        this.rawRetval = rawRetval;

        assert subjId != null;

        this.cctx = cctx;
        this.cache = cache;
        this.syncMode = syncMode;
        this.op = op;
        this.invokeArgs = invokeArgs;
        this.retval = retval;
        this.expiryPlc = expiryPlc;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.waitTopFut = waitTopFut;

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridFutureAdapter.class);

        fastMap = F.isEmpty(filter) && op != TRANSFORM && cctx.config().getWriteSynchronizationMode() == FULL_SYNC &&
            cctx.config().getAtomicWriteOrderMode() == CLOCK &&
            !(cctx.writeThrough() && cctx.config().getInterceptor() != null);

        nearEnabled = CU.isNearEnabled(cctx);

        if (!waitTopFut)
            remapCnt = 1;

        this.remapCnt = remapCnt;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // No-op.
    }

    /**
     * @return {@code True} if this future should block partition map exchange.
     */
    protected boolean waitForPartitionExchange() {
        // Wait fast-map near atomic update futures in CLOCK mode.
        return fastMap;
    }

    /**
     * Updates near cache.
     *
     * @param req Update request.
     * @param res Update response.
     */
    protected void updateNear(GridNearAtomicUpdateRequestInterface req, GridNearAtomicUpdateResponse res) {
        assert nearEnabled;

        if (res.remapKeys() != null || !req.hasPrimary())
            return;

        GridNearAtomicCache near = (GridNearAtomicCache)cctx.dht().near();

        near.processNearAtomicUpdateResponse(req, res);
    }

    /**
     * @return {@code True} future is stored by {@link GridCacheMvccManager#addAtomicFuture}.
     */
    protected boolean storeFuture() {
        return cctx.config().getAtomicWriteOrderMode() == CLOCK || syncMode != FULL_ASYNC;
    }

    /**
     * Maps key to nodes. If filters are absent and operation is not TRANSFORM, then we can assign version on near
     * node and send updates in parallel to all participating nodes.
     *
     * @param key Key to map.
     * @param topVer Topology version to map.
     * @param fastMap Flag indicating whether mapping is performed for fast-circuit update.
     * @return Collection of nodes to which key is mapped.
     */
    protected Collection<ClusterNode> mapKey(KeyCacheObject key, AffinityTopologyVersion topVer, boolean fastMap
    ) {
        GridCacheAffinityManager affMgr = cctx.affinity();

        // If we can send updates in parallel - do it.
        return fastMap ?
            cctx.topology().nodes(affMgr.partition(key), topVer) :
            Collections.singletonList(affMgr.primary(key, topVer));
    }
}
