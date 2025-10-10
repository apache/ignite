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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.managers.communication.ErrorMessage;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxFinishResponse;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Reply for synchronous phase 2.
 */
public final class GridNearTxFinishResponse extends GridDistributedTxFinishResponse {
    /** Heuristic error. */
    @Order(value = 6, method = "errorMessage")
    @Nullable private ErrorMessage errMsg;

    /** Mini future ID. */
    @Order(7)
    private int miniId;

    /** Near tx thread ID. */
    @Order(value = 8, method = "threadId")
    private long nearThreadId;

    /**
     * Empty constructor.
     */
    public GridNearTxFinishResponse() {
        // No-op.
    }

    /**
     * @param part Partition.
     * @param xid Xid version.
     * @param nearThreadId Near tx thread ID.
     * @param futId Future ID.
     * @param miniId Mini future Id.
     * @param err Error.
     */
    public GridNearTxFinishResponse(int part,
        GridCacheVersion xid,
        long nearThreadId,
        IgniteUuid futId,
        int miniId,
        @Nullable Throwable err
    ) {
        super(part, xid, futId);

        assert miniId != 0;

        this.nearThreadId = nearThreadId;
        this.miniId = miniId;

        if (err != null)
            this.errMsg = new ErrorMessage(err);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Throwable error() {
        return errMsg == null ? null : errMsg.toThrowable();
    }

    /**
     * @return Mini future ID.
     */
    public int miniId() {
        return miniId;
    }

    /**
     * Sets mini future ID.
     */
    public void miniId(int miniId) {
        this.miniId = miniId;
    }

    /**
     * @return Near thread ID.
     */
    public long threadId() {
        return nearThreadId;
    }

    /**
     * Sets near thread ID.
     */
    public void threadId(long nearThreadId) {
        this.nearThreadId = nearThreadId;
    }

    /**
     * @return Error serialization message.
     */
    public @Nullable ErrorMessage errorMessage() {
        return errMsg;
    }

    /**
     * Sets error serialization message.
     */
    public void errorMessage(@Nullable ErrorMessage errMsg) {
        this.errMsg = errMsg;
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 54;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearTxFinishResponse.class, this, "super", super.toString());
    }
}
