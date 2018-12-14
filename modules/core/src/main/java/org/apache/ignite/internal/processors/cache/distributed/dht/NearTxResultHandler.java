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

import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.lang.GridClosureException;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Response factory.
 */
public final class NearTxResultHandler implements CI1<IgniteInternalFuture<GridCacheReturn>> {
    /** */
    private static final long serialVersionUID = 0;

    /** Singleton instance.*/
    private static final NearTxResultHandler INSTANCE = new NearTxResultHandler();

    /** Constructor. */
    private NearTxResultHandler() {
    }

    /**
     * @return Handler instance.
     */
    public static NearTxResultHandler instance() {
        return INSTANCE;
    }

    /**
     * Response factory method.
     *
     * @param future Enlist future.
     * @return Enlist response.
     */
    @SuppressWarnings("unchecked")
    public static <T> T createResponse(IgniteInternalFuture<?> future) {
        assert future != null;

        Class<?> clazz = future.getClass();

        if (clazz == GridDhtTxEnlistFuture.class)
            return (T)createResponse((GridDhtTxEnlistFuture)future);
        else
            throw new IllegalStateException();
    }

    /**
     * Response factory method.
     *
     * @param fut Enlist future.
     * @return Enlist response.
     */
    public static GridNearTxEnlistResponse createResponse(GridDhtTxEnlistFuture fut) {
        try {
            GridCacheReturn res = fut.get();

            GridCacheVersion ver = null;
            IgniteUuid id = null;

            if (fut.hasNearNodeUpdates) {
                ver = fut.cctx.tm().mappedVersion(fut.nearLockVer);

                id = fut.futId;
            }

            return new GridNearTxEnlistResponse(fut.cctx.cacheId(), fut.nearFutId, fut.nearMiniId,
                fut.nearLockVer, res, ver, id, fut.newDhtNodes);
        }
        catch (IgniteCheckedException e) {
            return new GridNearTxEnlistResponse(fut.cctx.cacheId(), fut.nearFutId, fut.nearMiniId, fut.nearLockVer, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void apply(IgniteInternalFuture<GridCacheReturn> fut0) {
        GridDhtTxAbstractEnlistFuture fut = (GridDhtTxAbstractEnlistFuture)fut0;

        GridCacheContext<?, ?> cctx = fut.cctx;
        GridDhtTxLocal tx = (GridDhtTxLocal)fut.tx;
        UUID nearNodeId = fut.nearNodeId;

        GridNearTxEnlistResponse res = createResponse(fut);

        try {
            cctx.io().send(nearNodeId, res, cctx.ioPolicy());
        }
        catch (IgniteCheckedException e) {
            U.error(fut.log, "Failed to send near enlist response (will rollback transaction) [" +
                "tx=" + CU.txString(tx) +
                ", node=" + nearNodeId +
                ", res=" + res + ']', e);

            try {
                tx.rollbackDhtLocalAsync();
            }
            catch (Throwable e1) {
                e.addSuppressed(e1);
            }

            throw new GridClosureException(e);
        }
    }
}
