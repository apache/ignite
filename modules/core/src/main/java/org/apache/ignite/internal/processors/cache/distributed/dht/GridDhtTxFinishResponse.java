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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheReturn;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * DHT transaction finish response.
 */
public final class GridDhtTxFinishResponse extends GridDistributedTxFinishResponse {
    /** Mini future ID. */
    @Order(6)
    private int miniId;

    /** Error. */
    @Order(value = 7, method = "checkCommittedErrorMessage")
    private @Nullable ErrorMessage checkCommittedErrMsg;

    /** Cache return value. */
    @Order(value = 8, method = "returnValue")
    private GridCacheReturn retVal;

    /** Flag indicating if this is a check-committed response. */
    @GridToStringExclude
    @Order(9)
    private boolean checkCommitted;

    /**
     * Empty constructor.
     */
    public GridDhtTxFinishResponse() {
        // No-op.
    }

    /**
     * @param part Partition.
     * @param xid Xid version.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     */
    public GridDhtTxFinishResponse(int part, GridCacheVersion xid, IgniteUuid futId, int miniId) {
        super(part, xid, futId);

        assert miniId != 0;

        this.miniId = miniId;
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /** Sets mini future ID. */
    public void miniId(int miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Error for check committed backup requests.
     */
    @Nullable public Throwable checkCommittedError() {
        ErrorMessage checkCommittedErrMsg = this.checkCommittedErrMsg;

        return checkCommittedErrMsg == null ? null : checkCommittedErrMsg.toThrowable();
    }

    /**
     * @param checkCommittedErr Error for check committed backup requests.
     */
    public void checkCommittedError(@Nullable Throwable checkCommittedErr) {
        this.checkCommittedErrMsg = checkCommittedErr == null ? null : new ErrorMessage(checkCommittedErr);
    }

    /** @return The check committed error serialization message. */
    public ErrorMessage checkCommittedErrorMessage() {
        return checkCommittedErrMsg;
    }

    /** Sets the check committed error serialization message. */
    public void checkCommittedErrorMessage(ErrorMessage checkCommittedErrMsg) {
        this.checkCommittedErrMsg = checkCommittedErrMsg;
    }

    /** Sets the lag indicating if this is a check-committed response. */
    public void checkCommitted(boolean checkCommited) {
        this.checkCommitted = checkCommited;
    }

    /**
     * @return The lag indicating if this is a check-committed response.
     */
    public boolean checkCommitted() {
        return checkCommitted;
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheSharedContext<?, ?> ctx) throws IgniteCheckedException {
        super.prepareMarshal(ctx);

        if (retVal != null && retVal.cacheId() != 0) {
            GridCacheContext<?, ?> cctx = ctx.cacheContext(retVal.cacheId());

            assert cctx != null : retVal.cacheId();

            retVal.prepareMarshal(cctx);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheSharedContext<?, ?> ctx, ClassLoader ldr)
        throws IgniteCheckedException {
        super.finishUnmarshal(ctx, ldr);

        if (retVal != null && retVal.cacheId() != 0) {
            GridCacheContext<?, ?> cctx = ctx.cacheContext(retVal.cacheId());

            assert cctx != null : retVal.cacheId();

            retVal.finishUnmarshal(cctx, ldr);
        }
    }

    /**
     * @param retVal Return value.
     */
    public void returnValue(GridCacheReturn retVal) {
        this.retVal = retVal;
    }

    /**
     * @return Return value.
     */
    public GridCacheReturn returnValue() {
        return retVal;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 33;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtTxFinishResponse.class, this,
            "chechCommited", checkCommitted,
            "super", super.toString());
    }
}
