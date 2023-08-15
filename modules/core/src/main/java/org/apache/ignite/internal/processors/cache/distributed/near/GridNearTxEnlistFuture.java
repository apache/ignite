/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheEntryPredicate;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxEnlistFuture;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.distributed.dht.NearTxResultHandler.createResponse;

/**
 * A future tracking requests for remote nodes transaction enlisting and locking produces by cache API operations.
 */
public class GridNearTxEnlistFuture extends GridNearTxAbstractEnlistBatchFuture<GridCacheReturn> {
    /** Res field updater. */
    private static final AtomicReferenceFieldUpdater<GridNearTxEnlistFuture, GridCacheReturn> RES_UPD =
        AtomicReferenceFieldUpdater.newUpdater(GridNearTxEnlistFuture.class, GridCacheReturn.class, "res");

    /** Filter. */
    private final CacheEntryPredicate filter;

    /** Need previous value flag. */
    private final boolean needRes;

    /** Keep binary flag. */
    private final boolean keepBinary;

    /** */
    @SuppressWarnings("unused")
    @GridToStringExclude
    protected volatile GridCacheReturn res;

    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param timeout Timeout.
     * @param it Rows iterator.
     * @param batchSize Batch size.
     * @param sequential Sequential locking flag.
     * @param filter Filter.
     * @param needRes Need previous value flag.
     * @param keepBinary Keep binary flag.
     */
    public GridNearTxEnlistFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        long timeout,
        UpdateSourceIterator<?> it,
        int batchSize,
        boolean sequential,
        @Nullable CacheEntryPredicate filter,
        boolean needRes,
        boolean keepBinary) {
        super(cctx, tx, timeout, null, it, batchSize, sequential);

        this.filter = filter;
        this.needRes = needRes;
        this.keepBinary = keepBinary;
    }

    /** {@inheritDoc} */
    @Override protected void map(boolean topLocked) {
        this.topLocked = topLocked;

        // Update write version to match current topology, otherwise version can lag behind local node's init version.
        // Reproduced by IgniteCacheEntryProcessorNodeJoinTest.testAllEntryProcessorNodeJoin.
        if (tx.local() && !topLocked)
            tx.writeVersion(cctx.versions().next(tx.topologyVersion().topologyVersion()));

        sendNextBatches(null);
    }

    /** */
    @Override protected boolean isLocalBackup(EnlistOperation op, KeyCacheObject key) {
        if (!cctx.affinityNode() || op == EnlistOperation.LOCK)
            return false;
        else if (cctx.isReplicated())
            return true;

        return cctx.topology().nodes(key.partition(), tx.topologyVersion()).indexOf(cctx.localNode()) > 0;
    }

    /** {@inheritDoc} */
    @Override protected void complete() {
        onDone(res);
    }

    /** {@inheritDoc} */
    @Override protected void sendBatch(int batchId, UUID nodeId, Batch batchFut, boolean clientFirst) throws IgniteCheckedException {
        assert batchFut != null;

        GridNearTxEnlistRequest req = new GridNearTxEnlistRequest(cctx.cacheId(),
            threadId,
            futId,
            batchId,
            topVer,
            lockVer,
            mvccSnapshot,
            clientFirst,
            remainingTime(),
            tx.remainingTime(),
            tx.taskNameHash(),
            batchFut.rows(),
            it.operation(),
            needRes,
            keepBinary,
            filter
        );

        sendRequest(req, nodeId);
    }

    /**
     * @param req Request.
     * @param nodeId Remote node ID
     * @throws IgniteCheckedException if failed to send.
     */
    private void sendRequest(GridCacheMessage req, UUID nodeId) throws IgniteCheckedException {
        cctx.io().send(nodeId, req, cctx.ioPolicy());
    }

    /** {@inheritDoc} */
    @Override protected void enlistLocal(int batchId, UUID nodeId, Batch batch) throws IgniteCheckedException {
        Collection<Object> rows = batch.rows();

        GridDhtTxEnlistFuture fut = new GridDhtTxEnlistFuture(nodeId,
            lockVer,
            mvccSnapshot,
            threadId,
            futId,
            batchId,
            tx,
            remainingTime(),
            cctx,
            rows,
            it.operation(),
            filter,
            needRes,
            keepBinary);

        updateLocalFuture(fut);

        fut.listen(() -> {
            try {
                clearLocalFuture(fut);

                GridNearTxEnlistResponse res = fut.error() == null ? createResponse(fut) : null;

                if (checkResponse(nodeId, res, fut.error()))
                    sendNextBatches(nodeId);
            }
            catch (IgniteCheckedException e) {
                checkResponse(nodeId, null, e);
            }
            finally {
                CU.unwindEvicts(cctx);
            }
        });

        fut.init();
    }

    /**
     * @param nodeId Sender node id.
     * @param res Response.
     */
    public void onResult(UUID nodeId, GridNearTxEnlistResponse res) {
        if (checkResponse(nodeId, res, res.error())) {

            Batch batch = batches.get(nodeId);

            if (batch != null && !F.isEmpty(batch.localBackupRows()) && res.dhtFutureId() != null)
                processBatchLocalBackupKeys(nodeId, batch.localBackupRows(), res.dhtVersion(), res.dhtFutureId());
            else
                sendNextBatches(nodeId);
        }
    }

    /**
     * @param nodeId Originating node ID.
     * @param res Response.
     * @param err Exception.
     * @return {@code True} if future was completed by this call.
     */
    public boolean checkResponse(UUID nodeId, GridNearTxEnlistResponse res, Throwable err) {
        assert res != null || err != null : this;

        if (err == null && res.error() != null)
            err = res.error();

        if (res != null)
            tx.mappings().get(nodeId).addBackups(res.newDhtNodes());

        if (err != null) {
            onDone(err);

            return false;
        }

        assert res != null;

        if (this.res != null || !RES_UPD.compareAndSet(this, null, res.result())) {
            GridCacheReturn res0 = this.res;

            if (res.result().invokeResult())
                res0.mergeEntryProcessResults(res.result());
            else if (res0.success() && !res.result().success())
                res0.success(false);
        }

        assert this.res != null && (this.res.emptyResult() || needRes || this.res.invokeResult() || !this.res.success());

        tx.hasRemoteLocks(true);

        return !isDone();
    }

    /** {@inheritDoc} */
    @Override public Set<UUID> pendingResponseNodes() {
        return batches.entrySet().stream()
            .filter(e -> e.getValue().ready())
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxEnlistFuture.class, this, super.toString());
    }
}
