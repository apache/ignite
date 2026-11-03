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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.GridNearUnlockRequest;
import org.apache.ignite.internal.transactions.IgniteTxTimeoutCheckedException;
import org.apache.ignite.lang.IgniteUuid;

/** Tracks an acknowledged unlock request without blocking a communication thread. */
public class GridCacheUnlockFuture extends GridCacheFutureAdapter<Void> {
    /** Future ID. */
    private final IgniteUuid futId = IgniteUuid.randomUuid();

    /** Recipient. */
    private final UUID nodeId;

    /**
     * @param nodeId Recipient.
     */
    private GridCacheUnlockFuture(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @param ctx Cache context.
     * @param nodeId Recipient.
     * @param req Unlock request.
     * @param timeout Remaining transaction timeout: zero is unlimited, minus one is expired.
     * @return Future completed after the recipient and its participants have finished cleanup.
     */
    public static IgniteInternalFuture<Void> send(
        GridCacheContext<?, ?> ctx,
        UUID nodeId,
        GridNearUnlockRequest req,
        long timeout
    ) {
        GridCacheUnlockFuture fut = new GridCacheUnlockFuture(nodeId);

        ctx.mvcc().addFuture(fut, fut.futureId());

        fut.listen(f -> ctx.mvcc().removeFuture(fut.futureId()));

        ctx.time().waitAsync(fut, timeout, (err, timedOut) -> {
            if (timedOut)
                fut.onDone(new IgniteTxTimeoutCheckedException("Timed out waiting for distributed unlock: " + nodeId));
        });

        if (!fut.isDone()) {
            req.unlockOptions = new GridCacheUnlockRequestOptions(fut.futureId(), timeout);

            try {
                ctx.io().send(nodeId, req, ctx.ioPolicy());
            }
            catch (IgniteCheckedException | RuntimeException e) {
                fut.onDone(e);
            }
        }

        return fut;
    }

    /**
     * @param nodeId Sender.
     * @param res Cleanup result.
     */
    public void onResult(UUID nodeId, GridCacheUnlockResponse res) {
        if (this.nodeId.equals(nodeId))
            onDone(null, res.error());
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() {
        // Cancelling the waiter does not retract the unlock; full rollback owns the remaining cleanup.
        return onCancelled();
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public boolean onNodeLeft(UUID nodeId) {
        if (!this.nodeId.equals(nodeId))
            return false;

        onDone(new ClusterTopologyCheckedException("Node left before acknowledging distributed unlock: " + nodeId));

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        // Always tracked until completion.
    }
}
