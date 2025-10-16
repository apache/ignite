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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Transactions recovery check response.
 */
public class GridCacheTxRecoveryResponse extends GridDistributedBaseMessage implements IgniteTxStateAware {
    /** Future ID. */
    @Order(value = 7, method = "futureId")
    private IgniteUuid futId;

    /** Mini future ID. */
    @Order(8)
    private IgniteUuid miniId;

    /** Flag indicating if all remote transactions were prepared. */
    @Order(9)
    private boolean success;

    /** Transient TX state. */
    private IgniteTxState txState;

    /**
     * Empty constructor.
     */
    public GridCacheTxRecoveryResponse() {
        // No-op.
    }

    /**
     * @param txId Transaction ID.
     * @param futId Future ID.
     * @param miniId Mini future ID.
     * @param success {@code True} if all remote transactions were prepared, {@code false} otherwise.
     * @param addDepInfo Deployment info flag.
     */
    public GridCacheTxRecoveryResponse(GridCacheVersion txId,
        IgniteUuid futId,
        IgniteUuid miniId,
        boolean success,
        boolean addDepInfo) {
        super(txId, 0, addDepInfo);

        this.futId = futId;
        this.miniId = miniId;
        this.success = success;

        this.addDepInfo = addDepInfo;
    }

    /**
     * @return Future ID.
     */
    public IgniteUuid futureId() {
        return futId;
    }

    /**
     * @param futId Future ID.
     */
    public void futureId(IgniteUuid futId) {
        this.futId = futId;
    }

    /**
     * @return Mini future ID.
     */
    public IgniteUuid miniId() {
        return miniId;
    }

    /**
     * @param miniId Mini future ID.
     */
    public void miniId(IgniteUuid miniId) {
        this.miniId = miniId;
    }

    /**
     * @return {@code True} if all remote transactions were prepared.
     */
    public boolean success() {
        return success;
    }

    /**
     * @param success {@code True} if all remote transactions were prepared.
     */
    public void success(boolean success) {
        this.success = success;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxState txState() {
        return txState;
    }

    /** {@inheritDoc} */
    @Override public void txState(IgniteTxState txState) {
        this.txState = txState;
    }

    /** {@inheritDoc} */
    @Override public IgniteLogger messageLogger(GridCacheSharedContext<?, ?> ctx) {
        return ctx.txRecoveryMessageLogger();
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 17;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheTxRecoveryResponse.class, this, "super", super.toString());
    }
}
