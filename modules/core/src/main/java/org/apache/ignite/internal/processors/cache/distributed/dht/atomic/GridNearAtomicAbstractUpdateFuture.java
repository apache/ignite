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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.CachePartialUpdateCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheAtomicFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMvccManager;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_ASYNC;

/**
 * Base for near atomic update futures.
 */
public abstract class GridNearAtomicAbstractUpdateFuture extends GridFutureAdapter<Object>
    implements GridCacheAtomicFuture<Object> {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    protected static IgniteLogger log;

    /** Logger. */
    protected static IgniteLogger msgLog;

    /** Cache context. */
    protected final GridCacheContext cctx;

    /** Cache. */
    protected final GridDhtAtomicCache cache;

    /** Write synchronization mode. */
    protected final CacheWriteSynchronizationMode syncMode;

    /** Update operation. */
    protected final GridCacheOperation op;

    /** Optional arguments for entry processor. */
    protected final Object[] invokeArgs;

    /** Return value require flag. */
    protected final boolean retval;

    /** Raw return value flag. */
    protected final boolean rawRetval;

    /** Expiry policy. */
    protected final ExpiryPolicy expiryPlc;

    /** Optional filter. */
    protected final CacheEntryPredicate[] filter;

    /** Subject ID. */
    protected final UUID subjId;

    /** Task name hash. */
    protected final int taskNameHash;

    /** Skip store flag. */
    protected final boolean skipStore;

    /** Keep binary flag. */
    protected final boolean keepBinary;

    /** Wait for topology future flag. */
    protected final boolean waitTopFut;

    /** Near cache flag. */
    protected final boolean nearEnabled;

    /** Mutex to synchronize state updates. */
    protected final Object mux = new Object();

    /** Topology locked flag. Set if atomic update is performed inside a TX or explicit lock. */
    protected boolean topLocked;

    /** Remap count. */
    protected int remapCnt;

    /** Current topology version. */
    protected AffinityTopologyVersion topVer = AffinityTopologyVersion.ZERO;

    /** */
    protected GridCacheVersion updVer;

    /** Topology version when got mapping error. */
    protected AffinityTopologyVersion mapErrTopVer;

    /** */
    protected int resCnt;

    /** Error. */
    protected CachePartialUpdateCheckedException err;

    /** Future ID. */
    protected GridCacheVersion futVer;

    /** Completion future for a particular topology version. */
    protected GridFutureAdapter<Void> topCompleteFut;

    /** Operation result. */
    protected GridCacheReturn opRes;

    /**
     * Constructor.
     *
     * @param cctx Cache context.
     * @param cache Cache.
     * @param syncMode Synchronization mode.
     * @param op Operation.
     * @param invokeArgs Invoke arguments.
     * @param retval Return value flag.
     * @param rawRetval Raw return value flag.
     * @param expiryPlc Expiry policy.
     * @param filter Filter.
     * @param subjId Subject ID.
     * @param taskNameHash Task name hash.
     * @param skipStore Skip store flag.
     * @param keepBinary Keep binary flag.
     * @param remapCnt Remap count.
     * @param waitTopFut Wait topology future flag.
     */
    protected GridNearAtomicAbstractUpdateFuture(
        GridCacheContext cctx,
        GridDhtAtomicCache cache,
        CacheWriteSynchronizationMode syncMode,
        GridCacheOperation op,
        @Nullable Object[] invokeArgs,
        boolean retval,
        boolean rawRetval,
        @Nullable ExpiryPolicy expiryPlc,
        CacheEntryPredicate[] filter,
        UUID subjId,
        int taskNameHash,
        boolean skipStore,
        boolean keepBinary,
        int remapCnt,
        boolean waitTopFut
    ) {
        if (log == null) {
            msgLog = cctx.shared().atomicMessageLogger();
            log = U.logger(cctx.kernalContext(), logRef, GridFutureAdapter.class);
        }

        this.cctx = cctx;
        this.cache = cache;
        this.syncMode = syncMode;
        this.op = op;
        this.invokeArgs = invokeArgs;
        this.retval = retval;
        this.rawRetval = rawRetval;
        this.expiryPlc = expiryPlc;
        this.filter = filter;
        this.subjId = subjId;
        this.taskNameHash = taskNameHash;
        this.skipStore = skipStore;
        this.keepBinary = keepBinary;
        this.waitTopFut = waitTopFut;

        nearEnabled = CU.isNearEnabled(cctx);

        if (!waitTopFut)
            remapCnt = 1;

        this.remapCnt = remapCnt;
    }

    /**
     * Performs future mapping.
     */
    public void map() {
        AffinityTopologyVersion topVer = cctx.shared().lockedTopologyVersion(null);

        if (topVer == null)
            mapOnTopology();
        else {
            topLocked = true;

            // Cannot remap.
            remapCnt = 1;

            GridCacheVersion futVer = addAtomicFuture(topVer);

            if (futVer != null)
                map(topVer, futVer);
        }
    }

    /**
     * @param topVer Topology version.
     * @param futVer Future version
     */
    protected abstract void map(AffinityTopologyVersion topVer, GridCacheVersion futVer);

    /**
     * Maps future on ready topology.
     */
    protected abstract void mapOnTopology();

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
     * @return {@code True} future is stored by {@link GridCacheMvccManager#addAtomicFuture}.
     */
    protected boolean storeFuture() {
        return cctx.config().getAtomicWriteOrderMode() == CLOCK || syncMode != FULL_ASYNC;
    }

    /**
     * Maps future to single node.
     *
     * @param nodeId Node ID.
     * @param req Request.
     */
    protected void mapSingle(UUID nodeId, GridNearAtomicAbstractUpdateRequest req) {
        if (cctx.localNodeId().equals(nodeId)) {
            cache.updateAllAsyncInternal(nodeId, req,
                new CI2<GridNearAtomicAbstractUpdateRequest, GridNearAtomicUpdateResponse>() {
                    @Override public void apply(GridNearAtomicAbstractUpdateRequest req, GridNearAtomicUpdateResponse res) {
                        onResult(res.nodeId(), res, false);
                    }
                });
        }
        else {
            try {
                cctx.io().send(req.nodeId(), req, cctx.ioPolicy());

                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near update fut, sent request [futId=" + req.futureVersion() +
                        ", writeVer=" + req.updateVersion() +
                        ", node=" + req.nodeId() + ']');
                }

                if (syncMode == FULL_ASYNC)
                    onDone(new GridCacheReturn(cctx, true, true, null, true));
            }
            catch (IgniteCheckedException e) {
                if (msgLog.isDebugEnabled()) {
                    msgLog.debug("Near update fut, failed to send request [futId=" + req.futureVersion() +
                        ", writeVer=" + req.updateVersion() +
                        ", node=" + req.nodeId() +
                        ", err=" + e + ']');
                }

                onSendError(req, e);
            }
        }
    }

    /**
     * Response callback.
     *
     * @param nodeId Node ID.
     * @param res Update response.
     * @param nodeErr {@code True} if response was created on node failure.
     */
    public abstract void onResult(UUID nodeId, GridNearAtomicUpdateResponse res, boolean nodeErr);

    /**
     * @param req Request.
     * @param e Error.
     */
    protected final void onSendError(GridNearAtomicAbstractUpdateRequest req, IgniteCheckedException e) {
        synchronized (mux) {
            GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(cctx.cacheId(),
                req.nodeId(),
                req.futureVersion(),
                cctx.deploymentEnabled());

            res.addFailedKeys(req.keys(), e);

            onResult(req.nodeId(), res, true);
        }
    }

    /**
     * Adds future prevents topology change before operation complete.
     * Should be invoked before topology lock released.
     *
     * @param topVer Topology version.
     * @return Future version in case future added.
     */
    protected final GridCacheVersion addAtomicFuture(AffinityTopologyVersion topVer) {
        GridCacheVersion futVer = cctx.versions().next(topVer);

        synchronized (mux) {
            assert this.futVer == null : this;
            assert this.topVer == AffinityTopologyVersion.ZERO : this;

            this.topVer = topVer;
            this.futVer = futVer;
        }

        if (storeFuture() && !cctx.mvcc().addAtomicFuture(futVer, this))
            return null;

        return futVer;
    }
}
