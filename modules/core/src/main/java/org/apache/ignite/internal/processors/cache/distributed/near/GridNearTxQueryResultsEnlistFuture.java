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
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMessage;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryResultsEnlistFuture;
import org.apache.ignite.internal.processors.query.EnlistOperation;
import org.apache.ignite.internal.processors.query.UpdateSourceIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.distributed.dht.NearTxQueryEnlistResultHandler.createResponse;

/**
 * A future tracking requests for remote nodes transaction enlisting and locking
 * of entries produced with complex DML queries requiring reduce step.
 */
public class GridNearTxQueryResultsEnlistFuture extends GridNearTxAbstractEnlistBatchFuture<Long> {
    /** Res field updater. */
    private static final AtomicLongFieldUpdater<GridNearTxQueryResultsEnlistFuture> RES_UPD =
        AtomicLongFieldUpdater.newUpdater(GridNearTxQueryResultsEnlistFuture.class, "res");
    /**
     * @param cctx Cache context.
     * @param tx Transaction.
     * @param timeout Timeout.
     * @param it Rows iterator.
     * @param batchSize Batch size.
     * @param sequential Sequential locking flag.
     */

    public GridNearTxQueryResultsEnlistFuture(GridCacheContext<?, ?> cctx,
        GridNearTxLocal tx,
        long timeout,
        UpdateSourceIterator<?> it,
        int batchSize,
        boolean sequential) {
        super(cctx, tx, timeout, CU.longReducer(), it, batchSize, sequential);
    }

    /** {@inheritDoc} */
    @Override protected void map(boolean topLocked) {
        this.topLocked = topLocked;

        sendNextBatches(null);
    }

    /** */
    @Override protected boolean isLocalBackup(EnlistOperation op, KeyCacheObject key) {
        if (!cctx.affinityNode() || op == EnlistOperation.LOCK)
            return false;
        else if (cctx.isReplicated())
            return true;

        return cctx.topology().nodes(key.partition(), tx.topologyVersion()).contains(cctx.localNode());
    }

    /** {@inheritDoc} */
    @Override protected void sendBatch(int batchId, UUID nodeId, Batch batchFut, boolean clientFirst) throws IgniteCheckedException {
        assert batchFut != null;

        GridNearTxQueryResultsEnlistRequest req = new GridNearTxQueryResultsEnlistRequest(cctx.cacheId(),
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
            it.operation());

        sendRequest(req, nodeId);
    }

    /**
     *
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

        GridDhtTxQueryResultsEnlistFuture fut = new GridDhtTxQueryResultsEnlistFuture(nodeId,
            lockVer,
            mvccSnapshot,
            threadId,
            futId,
            batchId,
            tx,
            remainingTime(),
            cctx,
            rows,
            it.operation());

        updateLocalFuture(fut);

        fut.listen(() -> {
            assert fut.error() != null || fut.result() != null : fut;

            try {
                clearLocalFuture(fut);

                GridNearTxQueryResultsEnlistResponse res = fut.error() == null ? createResponse(fut) : null;

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
    public void onResult(UUID nodeId, GridNearTxQueryResultsEnlistResponse res) {
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
    public boolean checkResponse(UUID nodeId, GridNearTxQueryResultsEnlistResponse res, Throwable err) {
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

        RES_UPD.getAndAdd(this, res.result());

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
        return S.toString(GridNearTxQueryResultsEnlistFuture.class, this, super.toString());
    }
}
